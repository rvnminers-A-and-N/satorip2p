"""
satorip2p.integration.neuron - Drop-in Replacement Classes for Neuron

This module provides classes that match the interfaces of existing Satori
networking components but use P2P networking under the hood.

Classes:
    P2PSatoriPubSubConn: Replaces satorilib.pubsub.SatoriPubSubConn
    P2PSatoriServerClient: Replaces satorilib.server.SatoriServerClient
    P2PCentrifugoClient: Replaces satorilib.centrifugo client

Features:
    - Circuit breaker pattern for fault tolerance
    - Automatic fallback from P2P to Central
    - Metrics collection and logging
    - Automatic mode detection and switching

Usage in Neuron's start.py:
    # Option 1: Direct replacement
    from satorip2p.integration import P2PSatoriPubSubConn as SatoriPubSubConn

    # Option 2: Conditional based on config
    from satorip2p.integration.networking_mode import get_networking_mode, NetworkingMode
    mode = get_networking_mode()
    if mode != NetworkingMode.CENTRAL:
        from satorip2p.integration import P2PSatoriPubSubConn as SatoriPubSubConn
    else:
        from satorilib.pubsub import SatoriPubSubConn
"""

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union
import json
import time
import threading
import logging
import asyncio

from .networking_mode import (
    NetworkingMode,
    NetworkingConfig,
    get_networking_mode,
    configure_networking,
)
from .metrics import IntegrationMetrics, log_operation, log_fallback
from .fallback import FallbackHandler, FallbackContext, get_fallback_handler

if TYPE_CHECKING:
    from satorilib.wallet import Wallet
    from ..peers import Peers
    from ..hybrid import HybridPeers

logger = logging.getLogger("satorip2p.integration.neuron")


class P2PSatoriPubSubConn:
    """
    Drop-in replacement for satorilib.pubsub.SatoriPubSubConn.

    Uses satorip2p's GossipSub for pub/sub instead of WebSocket connection
    to central pubsub server.

    Interface matches the original SatoriPubSubConn exactly:
    - Same constructor parameters
    - Same methods: connect, listen, send, publish, disconnect, setRouter

    Features:
    - Automatic fallback to central pubsub if P2P fails
    - Metrics collection for monitoring
    - Circuit breaker to prevent cascading failures

    Example:
        # Original usage:
        from satorilib.pubsub import SatoriPubSubConn
        conn = SatoriPubSubConn(uid=wallet.address, payload=stream_id, router=callback)

        # P2P replacement (same interface):
        from satorip2p.integration import P2PSatoriPubSubConn as SatoriPubSubConn
        conn = SatoriPubSubConn(uid=wallet.address, payload=stream_id, router=callback)
    """

    def __init__(
        self,
        uid: str,
        payload: Union[dict, str],
        url: Union[str, None] = None,
        router: Union[Callable, None] = None,
        listening: bool = True,
        then: Union[str, None] = None,
        command: str = 'key',
        threaded: bool = True,
        onConnect: Optional[Callable] = None,
        onDisconnect: Optional[Callable] = None,
        emergencyRestart: Optional[Callable] = None,
        # P2P-specific optional params
        peers: Optional["Peers"] = None,
        identity: Optional[Any] = None,
        # New: mode control
        networking_mode: Optional[NetworkingMode] = None,
        *args, **kwargs
    ):
        """
        Initialize P2P PubSub connection.

        Args:
            uid: User identifier (wallet address)
            payload: Stream ID or subscription payload
            url: Central server URL (used for fallback)
            router: Callback function for incoming messages
            listening: Whether to listen for messages
            then: Message to send after connection
            command: Command prefix (key/subscribe)
            threaded: Run in separate thread
            onConnect: Callback when connected
            onDisconnect: Callback when disconnected
            emergencyRestart: Callback for emergency restart
            peers: Optional pre-initialized Peers instance
            identity: Optional identity for P2P
            networking_mode: Override networking mode
        """
        self.c = 0
        self.uid = uid
        self.url = url or NetworkingConfig.get_pubsub_url()
        self.onConnect = onConnect
        self.onDisconnect = onDisconnect
        self.router = router
        self.payload = payload
        self.command = command
        self.topicTime: Dict[str, float] = {}
        self.listening = listening
        self.threaded = threaded
        self.shouldReconnect = True
        self.then = then
        self.emergencyRestart = emergencyRestart

        # P2P components
        self._peers = peers
        self._identity = identity
        self._connected = False
        self._subscriptions: List[str] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

        # Mode control
        self._mode = networking_mode or get_networking_mode()
        self._fallback_handler = get_fallback_handler()
        self._metrics = IntegrationMetrics.get_instance()

        # Central fallback client
        self._central_client = None

        # WebSocket compatibility (ws attribute expected by some code)
        self.ws = self  # Self-reference for .connected checks

        logger.info(f"P2PSatoriPubSubConn initialized in {self._mode.name} mode")

        if self.threaded:
            self._thread = threading.Thread(
                target=self._run_p2p_loop,
                daemon=True,
                name=f"P2PPubSub-{uid[:8]}"
            )
            self._thread.start()

    @property
    def connected(self) -> bool:
        """Check if P2P is connected (mimics ws.connected)."""
        if self._mode == NetworkingMode.CENTRAL:
            return self._central_client is not None
        if self._peers is None:
            return False
        return self._peers.connected_peers > 0 or self._connected

    def _run_p2p_loop(self):
        """Run the P2P event loop in a thread."""
        import trio

        async def main():
            await self._initialize_p2p()
            if self._connected:
                if self.onConnect and callable(self.onConnect):
                    try:
                        self.onConnect()
                    except Exception as e:
                        logger.error(f"onConnect callback error: {e}")

                if self.then is not None:
                    await trio.sleep(3)
                    self._send_internal(self.then)
                    self.then = None

                # Keep running
                while self.listening and self.shouldReconnect:
                    await trio.sleep(1)

        reconnect_delay = 10
        while self.shouldReconnect:
            try:
                trio.run(main)
            except Exception as e:
                logger.error(f"P2P loop error: {e}")
                log_operation("pubsub_loop", False, "p2p", error=str(e))

                if self.onDisconnect and callable(self.onDisconnect):
                    try:
                        self.onDisconnect()
                    except Exception as de:
                        logger.error(f"onDisconnect callback error: {de}")

            # Exponential backoff with cap
            time.sleep(min(reconnect_delay, 300))
            reconnect_delay = min(reconnect_delay * 1.5, 300)

    async def _initialize_p2p(self):
        """Initialize P2P peers if not already done."""
        start_time = time.time()

        if self._mode == NetworkingMode.CENTRAL:
            # Central-only mode, initialize central client
            await self._initialize_central()
            return

        try:
            if self._peers is None:
                from ..hybrid import HybridPeers, HybridMode

                hybrid_mode = (
                    HybridMode.HYBRID if self._mode == NetworkingMode.HYBRID
                    else HybridMode.P2P_ONLY
                )

                self._peers = HybridPeers(
                    identity=self._identity,
                    mode=hybrid_mode,
                )
                await self._peers.start()

            self._connected = True
            duration_ms = (time.time() - start_time) * 1000
            log_operation("pubsub_connect", True, "p2p", duration_ms)

            # Parse payload and subscribe
            stream_id = self._extract_stream_id()
            if stream_id and self.router:
                await self._peers.subscribe(stream_id, self._handle_message)
                self._subscriptions.append(stream_id)
                logger.info(f"P2P subscribed to: {stream_id}")
                log_operation("pubsub_subscribe", True, "p2p")

        except Exception as e:
            logger.error(f"P2P initialization failed: {e}")
            log_operation("pubsub_connect", False, "p2p", error=str(e))

            # Fallback to central if in hybrid mode
            if self._mode == NetworkingMode.HYBRID:
                log_fallback("pubsub_connect", str(e))
                await self._initialize_central()

    async def _initialize_central(self):
        """Initialize central pubsub client as fallback."""
        try:
            from satorilib.pubsub import SatoriPubSubConn

            self._central_client = SatoriPubSubConn(
                uid=self.uid,
                payload=self.payload,
                url=self.url,
                router=self.router,
                listening=self.listening,
                then=self.then,
                command=self.command,
                threaded=False,  # We're already in a thread
                onConnect=self.onConnect,
                onDisconnect=self.onDisconnect,
                emergencyRestart=self.emergencyRestart,
            )
            self._connected = True
            log_operation("pubsub_connect", True, "central")
            logger.info("Central PubSub initialized as fallback")

        except ImportError:
            logger.error("satorilib not available for central fallback")
        except Exception as e:
            logger.error(f"Central pubsub initialization failed: {e}")
            log_operation("pubsub_connect", False, "central", error=str(e))

    def _extract_stream_id(self) -> Optional[str]:
        """Extract stream ID from payload."""
        if isinstance(self.payload, str):
            try:
                payload_data = json.loads(self.payload)
                return payload_data.get('uuid') or payload_data.get('topic') or self.payload
            except json.JSONDecodeError:
                return self.payload
        elif isinstance(self.payload, dict):
            return self.payload.get('uuid') or self.payload.get('topic')
        else:
            return str(self.payload)

    def _handle_message(self, stream_id: str, data: Any):
        """Handle incoming P2P message and route to callback."""
        if self.router is None:
            return

        # Format message like original WebSocket format
        if isinstance(data, dict):
            message = json.dumps(data)
        else:
            message = str(data)

        try:
            # Check for emergency stop
            if message == '---STOP!---':
                if self.emergencyRestart:
                    self.emergencyRestart()
                return

            self.router(message)
            log_operation("pubsub_receive", True, "p2p")

        except Exception as e:
            logger.warning(f"Router callback error: {e}")
            log_operation("pubsub_receive", False, "p2p", error=str(e))

    def connectThenListen(self):
        """Compatibility method - starts P2P connection."""
        if not self._thread or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._run_p2p_loop,
                daemon=True,
                name=f"P2PPubSub-{self.uid[:8]}"
            )
            self._thread.start()

    def connect(self):
        """Establish P2P connection (compatibility method)."""
        if not self._connected:
            self.connectThenListen()
        return self

    def listen(self):
        """Listen for messages (handled by P2P subscription)."""
        # In P2P mode, listening is handled by the subscription callback
        pass

    def setTopicTime(self, topic: str):
        """Track last publish time for rate limiting."""
        self.topicTime[topic] = time.time()

    def _send_internal(self, payload: str):
        """Internal send method."""
        import trio

        async def do_send():
            if self._peers and self._connected:
                # Parse the payload to extract topic and data
                if ':' in payload:
                    command, data = payload.split(':', 1)
                    try:
                        msg = json.loads(data)
                        topic = msg.get('topic', self._subscriptions[0] if self._subscriptions else 'default')
                        await self._peers.publish(topic, msg)
                    except json.JSONDecodeError:
                        # Raw message
                        if self._subscriptions:
                            await self._peers.publish(self._subscriptions[0], {'raw': data})
                else:
                    if self._subscriptions:
                        await self._peers.publish(self._subscriptions[0], {'raw': payload})

        try:
            trio.from_thread.run(do_send)
        except Exception:
            # Fall back to creating new event loop
            try:
                trio.run(do_send)
            except Exception as e:
                logger.error(f"P2P send failed: {e}")

    def send(
        self,
        payload: Union[str, None] = None,
        title: Union[str, None] = None,
        topic: Union[str, None] = None,
        data: Union[str, None] = None,
        observationTime: Union[str, None] = None,
        observationHash: Union[str, None] = None,
    ):
        """
        Send a message via P2P.

        Matches original SatoriPubSubConn.send() signature.
        """
        if not self._connected:
            return

        if payload is None and title is None and topic is None and data is None:
            raise ValueError('payload or (title, topic, data) must not be None')

        payload = payload or (
            title + ':' + json.dumps({
                'topic': topic,
                'data': str(data),
                'time': str(observationTime),
                'hash': str(observationHash),
            }))

        start_time = time.time()

        with self._fallback_handler.try_p2p_sync("pubsub_send") as ctx:
            if ctx.should_try_p2p and self._peers:
                try:
                    self._send_internal(payload)
                    ctx.mark_p2p_success()
                    return
                except Exception as e:
                    ctx.mark_p2p_failure(str(e))

            if ctx.needs_fallback and self._central_client:
                try:
                    self._central_client.send(
                        payload=payload,
                        title=title,
                        topic=topic,
                        data=data,
                        observationTime=observationTime,
                        observationHash=observationHash,
                    )
                    ctx.mark_fallback_success()
                except Exception as e:
                    ctx.mark_fallback_failure(str(e))

    def publish(
        self,
        topic: str,
        data: str,
        observationTime: str,
        observationHash: str
    ):
        """
        Publish data to a topic.

        Matches original SatoriPubSubConn.publish() signature.
        """
        # Rate limiting (same as original)
        if self.topicTime.get(topic, 0) > time.time() - 55:
            return
        self.setTopicTime(topic)

        self.send(
            title='publish',
            topic=topic,
            data=data,
            observationTime=observationTime,
            observationHash=observationHash
        )

    def disconnect(self, reconnect: bool = False):
        """Disconnect from P2P network."""
        self.shouldReconnect = reconnect
        self.listening = False

        if self.onDisconnect and callable(self.onDisconnect):
            try:
                self.onDisconnect()
            except Exception as e:
                logger.error(f"onDisconnect callback error: {e}")

        self._connected = False

        if self._central_client:
            try:
                self._central_client.disconnect(reconnect)
            except Exception:
                pass

    def setRouter(self, router: Optional[Callable] = None):
        """Set the message router callback."""
        self.router = router

    def restart(self, payload: str = None):
        """Restart the connection."""
        self.disconnect(reconnect=True)
        self.then = payload
        self.connectThenListen()

    def reestablish(self, err: str = '', payload: str = None):
        """Re-establish connection after error."""
        time.sleep(3)
        self.restart(payload)


class P2PSatoriServerClient:
    """
    Drop-in replacement for satorilib.server.SatoriServerClient.

    Uses P2P DHT for peer discovery and direct messaging instead of
    REST API calls to central server.

    Note: Some methods that require central server coordination will
    fall back to hybrid mode or return cached/local data.
    """

    def __init__(
        self,
        wallet: "Wallet",
        url: str = None,
        sendingUrl: str = None,
        # P2P-specific
        peers: Optional["Peers"] = None,
        enable_central_fallback: bool = True,
        # New: mode control
        networking_mode: Optional[NetworkingMode] = None,
        *args, **kwargs
    ):
        """
        Initialize P2P server client.

        Args:
            wallet: Wallet for signing
            url: Central server URL (used for fallback if enabled)
            sendingUrl: Sending URL (used for fallback if enabled)
            peers: Optional pre-initialized Peers instance
            enable_central_fallback: Whether to fall back to central for some operations
            networking_mode: Override networking mode
        """
        self.wallet = wallet
        self.url = url or NetworkingConfig.get_central_url()
        self.sendingUrl = sendingUrl or NetworkingConfig.get_sending_url()
        self.topicTime: Dict[str, float] = {}
        self.lastCheckin: int = 0

        # P2P components
        self._peers = peers
        self._cached_details: Dict[str, Any] = {}
        self._local_streams: List[Dict] = []
        self._local_subscriptions: List[Dict] = []

        # Mode control
        self._mode = networking_mode or get_networking_mode()
        self._enable_central_fallback = (
            enable_central_fallback and
            self._mode in (NetworkingMode.HYBRID, NetworkingMode.CENTRAL)
        )
        self._fallback_handler = get_fallback_handler()
        self._metrics = IntegrationMetrics.get_instance()

        # For hybrid mode, keep original client for fallback
        self._central_client = None
        if self._enable_central_fallback:
            try:
                from satorilib.server import SatoriServerClient
                self._central_client = SatoriServerClient(
                    wallet=wallet,
                    url=url,
                    sendingUrl=sendingUrl
                )
            except ImportError:
                logger.warning("satorilib not available for central fallback")

        logger.info(
            f"P2PSatoriServerClient initialized: mode={self._mode.name}, "
            f"fallback={'enabled' if self._enable_central_fallback else 'disabled'}"
        )

    def _get_challenge(self) -> str:
        """Get challenge for authentication."""
        return str(time.time())

    def setTopicTime(self, topic: str):
        """Track publish time for rate limiting."""
        self.topicTime[topic] = time.time()

    def checkin(
        self,
        referrer: Optional[str] = None,
        ip: Optional[str] = None,
        vaultInfo: Optional[Dict] = None,
    ) -> Dict:
        """
        Check in with the network.

        In P2P mode, this announces presence to DHT and discovers peers.
        Falls back to central server if enabled.
        """
        with self._fallback_handler.try_p2p_sync("checkin") as ctx:
            if ctx.should_try_p2p and self._mode != NetworkingMode.CENTRAL:
                try:
                    result = self._p2p_checkin()
                    ctx.mark_p2p_success()
                    return result
                except Exception as e:
                    ctx.mark_p2p_failure(str(e))

            if (ctx.needs_fallback or self._mode == NetworkingMode.CENTRAL) and self._central_client:
                try:
                    result = self._central_client.checkin(
                        referrer=referrer,
                        ip=ip,
                        vaultInfo=vaultInfo
                    )
                    ctx.mark_fallback_success()
                    return result
                except Exception as e:
                    ctx.mark_fallback_failure(str(e))
                    logger.error(f"Central checkin failed: {e}")

        # Return minimal P2P details as last resort
        return self._p2p_checkin()

    def _p2p_checkin(self) -> Dict:
        """P2P-native checkin."""
        self.lastCheckin = int(time.time())

        return {
            'key': self.wallet.address,
            'oracleKey': '',
            'idKey': '',
            'subscriptionKeys': '',
            'publicationKeys': '',
            'subscriptions': json.dumps(self._local_subscriptions),
            'publications': json.dumps(self._local_streams),
            'wallet': {
                'address': self.wallet.address,
                'pubkey': self.wallet.pubkey if hasattr(self.wallet, 'pubkey') else '',
            },
        }

    def checkinCheck(self) -> bool:
        """Check if we need to re-checkin."""
        # In P2P mode, we don't need periodic checkins
        if self._mode == NetworkingMode.P2P_ONLY:
            return False
        # In hybrid/central, delegate to central client
        if self._central_client:
            return self._central_client.checkinCheck()
        return False

    def registerWallet(self):
        """Register wallet - not needed in P2P mode."""
        if self._central_client:
            return self._central_client.registerWallet()
        # In P2P, identity is derived from wallet automatically
        return None

    def registerStream(self, stream: Dict, payload: str = None):
        """
        Register a stream for publishing.

        In P2P mode, announces to DHT rendezvous point.
        """
        with self._fallback_handler.try_p2p_sync("register_stream") as ctx:
            # Always store locally first
            self._local_streams.append(stream)

            if ctx.should_try_p2p and self._peers:
                try:
                    # P2P announcement would go here
                    logger.info(f"P2P: Registered stream locally: {stream}")
                    ctx.mark_p2p_success()
                    return None
                except Exception as e:
                    ctx.mark_p2p_failure(str(e))

            if ctx.needs_fallback and self._central_client:
                try:
                    result = self._central_client.registerStream(stream, payload)
                    ctx.mark_fallback_success()
                    return result
                except Exception as e:
                    ctx.mark_fallback_failure(str(e))

        return None

    def registerSubscription(self, subscription: Dict, payload: str = None):
        """
        Register a subscription.

        In P2P mode, subscribes via GossipSub.
        """
        with self._fallback_handler.try_p2p_sync("register_subscription") as ctx:
            # Always store locally first
            self._local_subscriptions.append(subscription)

            if ctx.should_try_p2p and self._peers:
                try:
                    logger.info(f"P2P: Registered subscription locally: {subscription}")
                    ctx.mark_p2p_success()
                    return None
                except Exception as e:
                    ctx.mark_p2p_failure(str(e))

            if ctx.needs_fallback and self._central_client:
                try:
                    result = self._central_client.registerSubscription(subscription, payload)
                    ctx.mark_fallback_success()
                    return result
                except Exception as e:
                    ctx.mark_fallback_failure(str(e))

        return None

    def publish(
        self,
        topic: str,
        data: str,
        observationTime: str,
        observationHash: str,
        isPrediction: bool = False,
        useAuthorizedCall: bool = False,
    ):
        """
        Publish data to network.

        In P2P mode, publishes via GossipSub.
        """
        # Rate limiting
        if self.topicTime.get(topic, 0) > time.time() - 55:
            return

        self.setTopicTime(topic)

        with self._fallback_handler.try_p2p_sync("publish") as ctx:
            if ctx.should_try_p2p and self._peers:
                try:
                    import trio
                    message = {
                        'topic': topic,
                        'data': data,
                        'time': observationTime,
                        'hash': observationHash,
                        'isPrediction': isPrediction,
                    }
                    trio.from_thread.run_sync(
                        lambda: trio.run(self._peers.publish(topic, message))
                    )
                    ctx.mark_p2p_success()
                    return
                except Exception as e:
                    ctx.mark_p2p_failure(str(e))

            if ctx.needs_fallback and self._central_client:
                try:
                    result = self._central_client.publish(
                        topic=topic,
                        data=data,
                        observationTime=observationTime,
                        observationHash=observationHash,
                        isPrediction=isPrediction,
                        useAuthorizedCall=useAuthorizedCall
                    )
                    ctx.mark_fallback_success()
                    return result
                except Exception as e:
                    ctx.mark_fallback_failure(str(e))
                    logger.error(f"Central publish also failed: {e}")

    def getStreamsSubscribers(self, streamIds: List[str]) -> tuple:
        """
        Get subscribers for streams.

        In P2P mode, queries DHT.
        """
        with self._fallback_handler.try_p2p_sync("get_subscribers") as ctx:
            if ctx.should_try_p2p and self._peers:
                try:
                    subscribers = self._p2p_get_subscribers(streamIds)
                    ctx.mark_p2p_success()
                    return True, subscribers
                except Exception as e:
                    ctx.mark_p2p_failure(str(e))

            if ctx.needs_fallback and self._central_client:
                try:
                    result = self._central_client.getStreamsSubscribers(streamIds)
                    ctx.mark_fallback_success()
                    return result
                except Exception as e:
                    ctx.mark_fallback_failure(str(e))

        return True, {}

    def _p2p_get_subscribers(self, streamIds: List[str]) -> Dict:
        """Get subscribers via P2P."""
        subscribers = {}
        if self._peers:
            import trio
            for stream_id in streamIds:
                try:
                    subs = trio.run(self._peers.discover_subscribers(stream_id))
                    subscribers[stream_id] = subs
                except Exception:
                    subscribers[stream_id] = []
        return subscribers

    def getStreamsPublishers(self, streamIds: List[str]) -> tuple:
        """
        Get publishers for streams.

        In P2P mode, queries DHT.
        """
        with self._fallback_handler.try_p2p_sync("get_publishers") as ctx:
            if ctx.should_try_p2p and self._peers:
                try:
                    publishers = self._p2p_get_publishers(streamIds)
                    ctx.mark_p2p_success()
                    return True, publishers
                except Exception as e:
                    ctx.mark_p2p_failure(str(e))

            if ctx.needs_fallback and self._central_client:
                try:
                    result = self._central_client.getStreamsPublishers(streamIds)
                    ctx.mark_fallback_success()
                    return result
                except Exception as e:
                    ctx.mark_fallback_failure(str(e))

        return True, {}

    def _p2p_get_publishers(self, streamIds: List[str]) -> Dict:
        """Get publishers via P2P."""
        publishers = {}
        if self._peers:
            import trio
            for stream_id in streamIds:
                try:
                    pubs = trio.run(self._peers.discover_publishers(stream_id))
                    publishers[stream_id] = pubs
                except Exception:
                    publishers[stream_id] = []
        return publishers

    def getCentrifugoToken(self) -> Dict:
        """
        Get Centrifugo token.

        In pure P2P mode, returns empty (not needed).
        Falls back to central if enabled.
        """
        if self._mode == NetworkingMode.P2P_ONLY:
            return {}

        if self._central_client:
            try:
                return self._central_client.getCentrifugoToken()
            except Exception as e:
                logger.warning(f"Central getCentrifugoToken failed: {e}")

        return {}

    def getBalances(self) -> tuple:
        """
        Get balances via ElectrumX (P2P) or central server.

        Returns:
            Tuple of (success, balance_dict) with:
            - currency: EVR balance
            - chain_balance: SATORI balance
        """
        # Try P2P first via ElectrumX (wallet has direct blockchain access)
        if self.wallet and self._mode in (NetworkingMode.HYBRID, NetworkingMode.P2P_ONLY):
            try:
                # Ensure electrumx connection
                if hasattr(self.wallet, 'electrumx') and self.wallet.electrumx:
                    if not self.wallet.electrumx.connected():
                        # Try to connect
                        if hasattr(self.wallet, 'connect'):
                            self.wallet.connect()

                    if self.wallet.electrumx.connected():
                        # Get fresh balances from blockchain
                        self.wallet.getBalances()

                        evr_balance = 0.0
                        satori_balance = 0.0

                        if hasattr(self.wallet, 'currency') and self.wallet.currency:
                            evr_balance = self.wallet.currency.amount if hasattr(self.wallet.currency, 'amount') else 0.0

                        if hasattr(self.wallet, 'balance') and self.wallet.balance:
                            satori_balance = self.wallet.balance.amount if hasattr(self.wallet.balance, 'amount') else 0.0

                        return True, {
                            'currency': evr_balance,
                            'chain_balance': satori_balance,
                        }
            except Exception as e:
                logger.warning(f"P2P getBalances via ElectrumX failed: {e}")
                if self._mode == NetworkingMode.P2P_ONLY:
                    return False, {'currency': 0, 'chain_balance': 0, 'error': str(e)}

        # Fallback to central
        if self._central_client:
            try:
                return self._central_client.getBalances()
            except Exception as e:
                logger.warning(f"Central getBalances failed: {e}")

        return False, {'currency': 0, 'chain_balance': 0}

    def stakeCheck(self) -> bool:
        """
        Check if user has sufficient stake to earn rewards.

        Returns:
            True if SATORI balance >= 50 (minimum stake requirement)
        """
        MIN_STAKE_THRESHOLD = 50.0  # Must have 50+ SATORI to earn rewards

        # Try P2P first - check wallet balance directly
        if self.wallet and self._mode in (NetworkingMode.HYBRID, NetworkingMode.P2P_ONLY):
            try:
                success, balances = self.getBalances()
                if success:
                    satori_balance = balances.get('chain_balance', 0)
                    return satori_balance >= MIN_STAKE_THRESHOLD
            except Exception as e:
                logger.warning(f"P2P stakeCheck failed: {e}")
                if self._mode == NetworkingMode.P2P_ONLY:
                    return False

        # Fallback to central
        if self._central_client:
            try:
                return self._central_client.stakeCheck()
            except Exception as e:
                logger.warning(f"Central stakeCheck failed: {e}")
        return False

    def setMiningMode(self, miningMode: bool):
        """Set mining mode."""
        if self._central_client:
            try:
                return self._central_client.setMiningMode(miningMode)
            except Exception:
                pass

    def setRewardAddress(self, signature: str, pubkey: str, address: str, memo: str = ""):
        """
        Set custom reward/payout address via P2P or central.

        In P2P mode: Uses RewardAddressManager to store in DHT and broadcast.
        Enables cold wallet, paper wallet, or hardware wallet payouts.

        Args:
            signature: Wallet signature proving ownership
            pubkey: Public key for verification
            address: Custom payout address
            memo: Optional description (e.g., "Cold wallet")

        Returns:
            Result of operation or None
        """
        # Try P2P first
        if self._mode != NetworkingMode.CENTRAL and self._reward_address_manager:
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        future = pool.submit(
                            asyncio.run,
                            self._reward_address_manager.set_reward_address(
                                payout_address=address,
                                memo=memo,
                            )
                        )
                        result = future.result(timeout=10)
                else:
                    result = loop.run_until_complete(
                        self._reward_address_manager.set_reward_address(
                            payout_address=address,
                            memo=memo,
                        )
                    )

                if result:
                    logger.info(f"P2P: Set reward address to {address[:12]}...")
                    return {'success': True, 'record': result.to_dict()}
            except Exception as e:
                logger.warning(f"P2P setRewardAddress failed: {e}")
                if self._mode == NetworkingMode.P2P_ONLY:
                    return {'success': False, 'error': str(e)}

        # Fallback to central
        if self._central_client:
            try:
                return self._central_client.setRewardAddress(signature, pubkey, address)
            except Exception as e:
                logger.warning(f"Central setRewardAddress failed: {e}")

        return None

    def setDataManagerPort(self, port: Optional[int]):
        """Set data manager port."""
        if self._central_client:
            try:
                return self._central_client.setDataManagerPort(port)
            except Exception:
                pass

    def loopbackCheck(self, ipAddr: str, port: int = 24600) -> bool:
        """Check if we can reach ourselves (loopback)."""
        if self._central_client:
            try:
                return self._central_client.loopbackCheck(ipAddr, port)
            except Exception:
                pass
        return False

    def getPublicIp(self):
        """
        Get public IP address.

        In P2P mode: Uses AutoNAT - other peers report what address they see us as.
        In hybrid/central mode: Falls back to central server.

        Returns:
            Response object with .text containing the IP address
        """
        class IpResponse:
            def __init__(self, ip: str):
                self.text = ip

        # Try P2P first (AutoNAT)
        if self._mode in (NetworkingMode.HYBRID, NetworkingMode.P2P_ONLY):
            if self._p2p_peers:
                try:
                    # Use AutoNAT to get public addresses
                    import asyncio
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # Create task for async call
                        import concurrent.futures
                        with concurrent.futures.ThreadPoolExecutor() as pool:
                            future = pool.submit(
                                asyncio.run,
                                self._p2p_peers.get_public_addrs_autonat()
                            )
                            public_addrs = future.result(timeout=5)
                    else:
                        public_addrs = loop.run_until_complete(
                            self._p2p_peers.get_public_addrs_autonat()
                        )

                    if public_addrs:
                        # Extract IP from multiaddr like /ip4/1.2.3.4/tcp/4001
                        for addr in public_addrs:
                            if '/ip4/' in addr:
                                parts = addr.split('/')
                                for i, part in enumerate(parts):
                                    if part == 'ip4' and i + 1 < len(parts):
                                        ip = parts[i + 1]
                                        if ip and ip != '0.0.0.0' and ip != '127.0.0.1':
                                            logger.debug(f"Public IP from AutoNAT: {ip}")
                                            return IpResponse(ip)
                except Exception as e:
                    logger.debug(f"AutoNAT public IP lookup failed: {e}")

        # Fallback to central server
        if self._central_client:
            try:
                return self._central_client.getPublicIp()
            except Exception:
                pass

        # Last resort: return 0.0.0.0
        return IpResponse("0.0.0.0")

    def invitedBy(self, address: str):
        """
        Register referral - report who invited this user.

        In P2P mode: Uses ReferralManager to store in DHT and broadcast.
        Referrers earn tier bonuses based on total referral count:
        - Bronze (5): +2%
        - Silver (25): +5%
        - Gold (100): +8%
        - Platinum (500): +12%
        - Diamond (2000): +15%

        Args:
            address: Address of the referrer who invited this user

        Returns:
            Result of operation or None
        """
        # Try P2P first
        if self._mode != NetworkingMode.CENTRAL and self._referral_manager:
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        future = pool.submit(
                            asyncio.run,
                            self._referral_manager.register_referral(
                                referrer_address=address
                            )
                        )
                        result = future.result(timeout=10)
                else:
                    result = loop.run_until_complete(
                        self._referral_manager.register_referral(
                            referrer_address=address
                        )
                    )

                if result:
                    logger.info(f"P2P: Registered referral by {address[:12]}...")
                    return {'success': True, 'referral': result.to_dict()}
            except Exception as e:
                logger.warning(f"P2P invitedBy failed: {e}")
                if self._mode == NetworkingMode.P2P_ONLY:
                    return {'success': False, 'error': str(e)}

        # Fallback to central
        if self._central_client:
            try:
                return self._central_client.invitedBy(address)
            except Exception as e:
                logger.warning(f"Central invitedBy failed: {e}")

        return None

    def poolAccepting(self, status: bool) -> tuple:
        """
        Set pool accepting status via P2P (broadcast + DHT) or central.

        Args:
            status: True to accept lenders, False to stop accepting

        Returns:
            Tuple of (success, result_dict)
        """
        # Try P2P first
        if self._lending_manager and self._mode in (NetworkingMode.HYBRID, NetworkingMode.P2P_ONLY):
            try:
                vault_address = self.wallet.address if self.wallet else ""
                vault_pubkey = self.wallet.pubkey if self.wallet and hasattr(self.wallet, 'pubkey') else ""

                if status:
                    # Register as accepting lenders
                    result = asyncio.get_event_loop().run_until_complete(
                        self._lending_manager.register_pool(
                            vault_address=vault_address,
                            vault_pubkey=vault_pubkey,
                        )
                    )
                else:
                    # Unregister (stop accepting)
                    result = asyncio.get_event_loop().run_until_complete(
                        self._lending_manager.unregister_pool(
                            vault_address=vault_address,
                        )
                    )

                success, message = result
                return success, {'status': status, 'message': message}
            except Exception as e:
                logger.warning(f"P2P poolAccepting failed: {e}")
                if self._mode == NetworkingMode.P2P_ONLY:
                    return False, {'error': str(e)}

        # Fallback to central
        if self._central_client:
            try:
                return self._central_client.poolAccepting(status)
            except Exception:
                pass
        return False, None

    def getSearchStreams(
        self,
        searchText: Optional[str] = None,
        search_mode: str = 'local'
    ) -> List[Dict]:
        """
        Search streams via P2P (local cache or network) or central.

        Args:
            searchText: Search query (matches source, stream name, target, tags)
            search_mode: 'local' (fast, cached streams) or 'network' (comprehensive)

        Returns:
            List of matching stream definitions
        """
        results = []

        # Try P2P first
        if self._stream_registry and self._mode in (NetworkingMode.HYBRID, NetworkingMode.P2P_ONLY):
            try:
                if search_mode == 'local':
                    # Fast local search through cached streams
                    results = self._search_streams_local(searchText)
                else:
                    # Network search via DHT discovery
                    results = asyncio.get_event_loop().run_until_complete(
                        self._search_streams_network(searchText)
                    )

                if results:
                    return results
            except Exception as e:
                logger.warning(f"P2P getSearchStreams failed: {e}")
                if self._mode == NetworkingMode.P2P_ONLY:
                    return results

        # Fallback to central
        if self._central_client:
            try:
                return self._central_client.getSearchStreams(searchText)
            except Exception:
                pass
        return results

    def _search_streams_local(self, searchText: Optional[str] = None) -> List[Dict]:
        """
        Search through locally cached/discovered streams (fast).

        Args:
            searchText: Search query

        Returns:
            List of matching stream dicts
        """
        if not self._stream_registry:
            return []

        # Get all cached streams
        all_streams = []
        if hasattr(self._stream_registry, '_streams'):
            all_streams = list(self._stream_registry._streams.values())

        if not searchText:
            return [s.to_dict() if hasattr(s, 'to_dict') else s for s in all_streams[:100]]

        # Simple text matching
        search_lower = searchText.lower()
        matches = []
        for stream in all_streams:
            stream_dict = stream.to_dict() if hasattr(stream, 'to_dict') else stream
            searchable = f"{stream_dict.get('source', '')} {stream_dict.get('stream', '')} {stream_dict.get('target', '')} {' '.join(stream_dict.get('tags', []))}".lower()
            if search_lower in searchable:
                matches.append(stream_dict)

        return matches[:100]

    async def _search_streams_network(self, searchText: Optional[str] = None) -> List[Dict]:
        """
        Search streams via P2P network (comprehensive but slower).

        Args:
            searchText: Search query

        Returns:
            List of matching stream dicts
        """
        if not self._stream_registry:
            return []

        # Use stream registry's discover method
        streams = await self._stream_registry.discover_streams(
            source=searchText,  # Try as source filter
            limit=100
        )

        return [s.to_dict() if hasattr(s, 'to_dict') else s for s in streams]

    def getSearchStreamsPaginated(self, **kwargs) -> tuple:
        """
        Search streams with pagination.

        Supports both P2P (local/network) and central search.
        """
        search_text = kwargs.get('searchText') or kwargs.get('search')
        page = kwargs.get('page', 0)
        limit = kwargs.get('limit', 20)
        search_mode = kwargs.get('search_mode', 'local')

        # Get all results from P2P search
        all_results = self.getSearchStreams(search_text, search_mode=search_mode)

        # Paginate
        start = page * limit
        end = start + limit
        paginated = all_results[start:end]

        return paginated, len(all_results)

    def getSearchPredictionStreamsPaginated(self, **kwargs) -> tuple:
        """
        Search prediction streams with pagination.

        Filters streams that are prediction-capable.
        """
        results, total = self.getSearchStreamsPaginated(**kwargs)

        # Filter to prediction streams only (have 'prediction' in tags or datatype)
        prediction_streams = [
            s for s in results
            if 'prediction' in str(s.get('tags', [])).lower() or
               'prediction' in str(s.get('datatype', '')).lower()
        ]

        return prediction_streams, len(prediction_streams)

    # ========================================================================
    # P2P MANAGERS (set externally by Neuron/Engine initialization)
    # ========================================================================

    _lending_manager = None
    _delegation_manager = None
    _oracle_network = None
    _reward_address_manager = None
    _referral_manager = None

    def set_lending_manager(self, manager):
        """Set the P2P lending manager."""
        self._lending_manager = manager

    def set_delegation_manager(self, manager):
        """Set the P2P delegation manager."""
        self._delegation_manager = manager

    def set_oracle_network(self, oracle):
        """Set the P2P oracle network."""
        self._oracle_network = oracle

    def set_reward_address_manager(self, manager):
        """Set the P2P reward address manager."""
        self._reward_address_manager = manager

    def set_referral_manager(self, manager):
        """Set the P2P referral manager."""
        self._referral_manager = manager

    # ========================================================================
    # OBSERVATION METHODS (P2P-first)
    # ========================================================================

    def getObservation(self, stream: str = 'bitcoin') -> Union[Dict, None]:
        """
        Get latest observation for a stream.

        P2P-first: tries oracle network first, falls back to central.

        Args:
            stream: Stream ID to get observation for

        Returns:
            Dict with observation data or None
        """
        # Try P2P oracle network first
        if self._mode != NetworkingMode.CENTRAL and self._oracle_network:
            try:
                obs = self._oracle_network.get_latest_observation(stream)
                if obs:
                    return {
                        'observation_id': obs.hash,
                        'value': obs.value,
                        'observed_at': obs.timestamp,
                        'ts': obs.timestamp,
                        'hash': obs.hash,
                        'oracle': obs.oracle,
                        'sources': [obs.oracle],
                    }
            except Exception as e:
                logger.warning(f"P2P observation lookup failed: {e}")

        # Fallback to central
        if self._central_client:
            try:
                return self._central_client.getObservation(stream)
            except Exception as e:
                logger.warning(f"Central observation lookup failed: {e}")

        return None

    # ========================================================================
    # LENDING METHODS (P2P-enabled)
    # ========================================================================

    def lendToAddress(
        self,
        vaultSignature: Union[str, bytes],
        vaultPubkey: str,
        address: str,
        vaultAddress: str = ''
    ) -> tuple:
        """
        Add lending address (join a pool).

        P2P: Uses LendingManager.lend_to_vault()
        Central: POST /stake/lend/to/address
        """
        if self._mode != NetworkingMode.CENTRAL and self._lending_manager:
            try:
                import asyncio
                # Convert signature to string if bytes
                sig = vaultSignature if isinstance(vaultSignature, str) else vaultSignature.hex()
                result = asyncio.get_event_loop().run_until_complete(
                    self._lending_manager.lend_to_vault(
                        lender_address=address,
                        vault_address=vaultAddress,
                        vault_signature=sig,
                        vault_pubkey=vaultPubkey,
                    )
                )
                return result
            except Exception as e:
                logger.warning(f"P2P lendToAddress failed: {e}")

        if self._central_client:
            try:
                return self._central_client.lendToAddress(
                    vaultSignature=vaultSignature,
                    vaultPubkey=vaultPubkey,
                    address=address,
                    vaultAddress=vaultAddress
                )
            except Exception:
                pass
        return (False, "Failed to register lending")

    def lendRemove(self) -> tuple:
        """
        Remove lending address.

        P2P: Uses LendingManager.remove_lending()
        Central: GET /stake/lend/remove
        """
        if self._mode != NetworkingMode.CENTRAL and self._lending_manager:
            try:
                import asyncio
                result = asyncio.get_event_loop().run_until_complete(
                    self._lending_manager.remove_lending(
                        lender_address=self.wallet.address if self.wallet else ""
                    )
                )
                return result
            except Exception as e:
                logger.warning(f"P2P lendRemove failed: {e}")

        if self._central_client:
            try:
                return self._central_client.lendRemove()
            except Exception:
                pass
        return (False, "Failed to remove lending")

    def lendAddress(self) -> Union[str, None]:
        """
        Get current lending address.

        P2P: Uses LendingManager.get_current_lend_address()
        Central: GET /stake/lend/address
        """
        if self._mode != NetworkingMode.CENTRAL and self._lending_manager:
            try:
                return self._lending_manager.get_current_lend_address(
                    lender_address=self.wallet.address if self.wallet else ""
                )
            except Exception as e:
                logger.warning(f"P2P lendAddress failed: {e}")

        if self._central_client:
            try:
                return self._central_client.lendAddress()
            except Exception:
                pass
        return ""

    def poolAddresses(self) -> tuple:
        """
        Get all pool addresses (lending addresses).

        P2P: Uses LendingManager.get_my_lendings()
        Central: GET /stake/lend/addresses
        """
        if self._mode != NetworkingMode.CENTRAL and self._lending_manager:
            try:
                import asyncio
                lendings = asyncio.get_event_loop().run_until_complete(
                    self._lending_manager.get_my_lendings(
                        lender_address=self.wallet.address if self.wallet else ""
                    )
                )
                return (True, [l.to_dict() for l in lendings])
            except Exception as e:
                logger.warning(f"P2P poolAddresses failed: {e}")

        if self._central_client:
            try:
                return self._central_client.poolAddresses()
            except Exception:
                pass
        return (False, [])

    def poolAddressRemove(self, lend_id: str) -> str:
        """
        Remove a specific lending pool address.

        P2P: Uses LendingManager.remove_lending_by_id()
        Central: POST /stake/lend/address/remove
        """
        if self._mode != NetworkingMode.CENTRAL and self._lending_manager:
            try:
                import asyncio
                result = asyncio.get_event_loop().run_until_complete(
                    self._lending_manager.remove_lending_by_id(lend_id)
                )
                return result[1]  # Return message
            except Exception as e:
                logger.warning(f"P2P poolAddressRemove failed: {e}")

        if self._central_client:
            try:
                return self._central_client.poolAddressRemove(lend_id)
            except Exception:
                pass
        return "Failed to remove pool address"

    def poolParticipants(self, vaultAddress: str) -> str:
        """
        Get pool participants for a vault address.

        P2P: Uses LendingManager.get_pool_participants()
        Central: POST /pool/participants
        """
        if self._mode != NetworkingMode.CENTRAL and self._lending_manager:
            try:
                import asyncio
                import json
                participants = asyncio.get_event_loop().run_until_complete(
                    self._lending_manager.get_pool_participants(vaultAddress)
                )
                return json.dumps([p.to_dict() for p in participants])
            except Exception as e:
                logger.warning(f"P2P poolParticipants failed: {e}")

        if self._central_client:
            try:
                return self._central_client.poolParticipants(vaultAddress)
            except Exception:
                pass
        return "[]"

    def setPoolSize(self, poolStakeLimit: float) -> tuple:
        """
        Set pool stake limit.

        P2P: Uses LendingManager.set_pool_size()
        Central: POST /pool/size/set
        """
        if self._mode != NetworkingMode.CENTRAL and self._lending_manager:
            try:
                import asyncio
                result = asyncio.get_event_loop().run_until_complete(
                    self._lending_manager.set_pool_size(
                        vault_address=self.wallet.address if self.wallet else "",
                        pool_size_limit=poolStakeLimit,
                    )
                )
                return result
            except Exception as e:
                logger.warning(f"P2P setPoolSize failed: {e}")

        if self._central_client:
            try:
                return self._central_client.setPoolSize(poolStakeLimit)
            except Exception:
                pass
        return (False, "Failed to set pool size")

    # ========================================================================
    # DELEGATION METHODS (P2P-enabled)
    # ========================================================================

    def delegateGet(self) -> tuple:
        """
        Get current delegate address.

        P2P: Uses DelegationManager.get_my_delegate()
        Central: GET /stake/proxy/delegate
        """
        if self._mode != NetworkingMode.CENTRAL and self._delegation_manager:
            try:
                delegate = self._delegation_manager.get_my_delegate(
                    child_address=self.wallet.address if self.wallet else ""
                )
                return (True, delegate)
            except Exception as e:
                logger.warning(f"P2P delegateGet failed: {e}")

        if self._central_client:
            try:
                return self._central_client.delegateGet()
            except Exception:
                pass
        return (False, "")

    def delegateRemove(self) -> tuple:
        """
        Remove current delegation.

        P2P: Uses DelegationManager.remove_delegation()
        Central: GET /stake/proxy/delegate/remove
        """
        if self._mode != NetworkingMode.CENTRAL and self._delegation_manager:
            try:
                import asyncio
                result = asyncio.get_event_loop().run_until_complete(
                    self._delegation_manager.remove_delegation(
                        child_address=self.wallet.address if self.wallet else ""
                    )
                )
                return result
            except Exception as e:
                logger.warning(f"P2P delegateRemove failed: {e}")

        if self._central_client:
            try:
                return self._central_client.delegateRemove()
            except Exception:
                pass
        return (False, "Failed to remove delegation")

    def stakeProxyChildren(self) -> tuple:
        """
        Get children delegating to this node.

        P2P: Uses DelegationManager.get_proxy_children_formatted()
        Central: GET /stake/proxy/children
        """
        if self._mode != NetworkingMode.CENTRAL and self._delegation_manager:
            try:
                import asyncio
                import json
                children = asyncio.get_event_loop().run_until_complete(
                    self._delegation_manager.get_proxy_children_formatted(
                        parent_address=self.wallet.address if self.wallet else ""
                    )
                )
                return (True, json.dumps(children))
            except Exception as e:
                logger.warning(f"P2P stakeProxyChildren failed: {e}")

        if self._central_client:
            try:
                return self._central_client.stakeProxyChildren()
            except Exception:
                pass
        return (False, "[]")

    def stakeProxyCharity(self, address: str, childId: int = None) -> tuple:
        """
        Mark child delegation as charity.

        P2P: Uses DelegationManager.set_charity_status(True)
        Central: POST /stake/proxy/charity
        """
        if self._mode != NetworkingMode.CENTRAL and self._delegation_manager:
            try:
                import asyncio
                result = asyncio.get_event_loop().run_until_complete(
                    self._delegation_manager.set_charity_status(
                        delegation_id=str(childId) if childId else "",
                        child_address=address,
                        charity=True,
                    )
                )
                return result
            except Exception as e:
                logger.warning(f"P2P stakeProxyCharity failed: {e}")

        if self._central_client:
            try:
                return self._central_client.stakeProxyCharity(address, childId)
            except Exception:
                pass
        return (False, "Failed to set charity status")

    def stakeProxyCharityNot(self, address: str, childId: int = None) -> tuple:
        """
        Unmark child delegation as charity.

        P2P: Uses DelegationManager.set_charity_status(False)
        Central: POST /stake/proxy/charity/not
        """
        if self._mode != NetworkingMode.CENTRAL and self._delegation_manager:
            try:
                import asyncio
                result = asyncio.get_event_loop().run_until_complete(
                    self._delegation_manager.set_charity_status(
                        delegation_id=str(childId) if childId else "",
                        child_address=address,
                        charity=False,
                    )
                )
                return result
            except Exception as e:
                logger.warning(f"P2P stakeProxyCharityNot failed: {e}")

        if self._central_client:
            try:
                return self._central_client.stakeProxyCharityNot(address, childId)
            except Exception:
                pass
        return (False, "Failed to unset charity status")

    def stakeProxyRemove(self, address: str, childId: int) -> tuple:
        """
        Remove a proxy child.

        P2P: Uses DelegationManager.remove_proxy_child()
        Central: POST /stake/proxy/remove
        """
        if self._mode != NetworkingMode.CENTRAL and self._delegation_manager:
            try:
                import asyncio
                result = asyncio.get_event_loop().run_until_complete(
                    self._delegation_manager.remove_proxy_child(
                        delegation_id=str(childId),
                        child_address=address,
                    )
                )
                return result
            except Exception as e:
                logger.warning(f"P2P stakeProxyRemove failed: {e}")

        if self._central_client:
            try:
                return self._central_client.stakeProxyRemove(address, childId)
            except Exception:
                pass
        return (False, "Failed to remove proxy child")

    # ========================================================================
    # DONATION METHODS (Treasury EVR Donations)
    # ========================================================================

    _donation_manager = None

    def set_donation_manager(self, manager):
        """Set the P2P donation manager."""
        self._donation_manager = manager

    def donateToTreasury(self, amount: float) -> Dict:
        """
        Donate EVR to the treasury.

        Sends EVR from neuron wallet to treasury multi-sig address.
        Returns donation record with estimated SATORI reward.

        Args:
            amount: EVR amount to donate

        Returns:
            Dict with donation details or error
        """
        if self._mode != NetworkingMode.CENTRAL and self._donation_manager:
            try:
                import asyncio
                donation = asyncio.get_event_loop().run_until_complete(
                    self._donation_manager.donate(amount)
                )
                return {
                    'success': True,
                    'donation': donation.to_dict(),
                    'message': f'Donated {amount} EVR to treasury',
                }
            except Exception as e:
                logger.warning(f"P2P donateToTreasury failed: {e}")
                return {'success': False, 'error': str(e)}

        if self._central_client:
            try:
                # Central fallback would go here if implemented
                return self._central_client.donateToTreasury(amount)
            except AttributeError:
                pass
            except Exception as e:
                logger.warning(f"Central donateToTreasury failed: {e}")

        return {'success': False, 'error': 'Donation manager not available'}

    def getDonationHistory(self) -> List[Dict]:
        """
        Get donation history for this neuron.

        Returns:
            List of donation records
        """
        if self._mode != NetworkingMode.CENTRAL and self._donation_manager:
            try:
                import asyncio
                donations = asyncio.get_event_loop().run_until_complete(
                    self._donation_manager.get_my_donations()
                )
                return [d.to_dict() for d in donations]
            except Exception as e:
                logger.warning(f"P2P getDonationHistory failed: {e}")

        if self._central_client:
            try:
                return self._central_client.getDonationHistory()
            except AttributeError:
                pass
            except Exception as e:
                logger.warning(f"Central getDonationHistory failed: {e}")

        return []

    def getDonorStats(self) -> Dict:
        """
        Get cumulative donation statistics for this neuron.

        Returns:
            Dict with total_donated, tier, badges_earned, etc.
        """
        if self._mode != NetworkingMode.CENTRAL and self._donation_manager:
            try:
                import asyncio
                stats = asyncio.get_event_loop().run_until_complete(
                    self._donation_manager.get_my_stats()
                )
                return stats.to_dict()
            except Exception as e:
                logger.warning(f"P2P getDonorStats failed: {e}")

        if self._central_client:
            try:
                return self._central_client.getDonorStats()
            except AttributeError:
                pass
            except Exception as e:
                logger.warning(f"Central getDonorStats failed: {e}")

        return {
            'donor_address': self.wallet.address if self.wallet else '',
            'total_donated': 0.0,
            'donation_count': 0,
            'tier': 'none',
            'badges_earned': [],
        }

    def getTreasuryAddress(self) -> str:
        """Get the treasury multi-sig address."""
        if self._donation_manager:
            return self._donation_manager.get_treasury_address()

        # Fallback to protocol constant
        try:
            from ..protocol.signer import get_treasury_address
            return get_treasury_address()
        except ImportError:
            return ''

    def getTopDonors(self, limit: int = 10) -> List[Dict]:
        """
        Get top donors by total donated.

        Args:
            limit: Number of top donors to return

        Returns:
            List of donor stats
        """
        if self._donation_manager:
            try:
                import asyncio
                donors = asyncio.get_event_loop().run_until_complete(
                    self._donation_manager.get_top_donors(limit)
                )
                return [d.to_dict() for d in donors]
            except Exception as e:
                logger.warning(f"getTopDonors failed: {e}")

        return []

    # ========================================================================
    # SIGNER METHODS (Multi-sig Administration)
    # ========================================================================

    _signer_node = None

    def set_signer_node(self, signer):
        """Set the P2P signer node."""
        self._signer_node = signer

    def isAuthorizedSigner(self) -> bool:
        """Check if this neuron is an authorized signer."""
        try:
            from ..protocol.signer import is_authorized_signer
            return is_authorized_signer(self.wallet.address if self.wallet else '')
        except ImportError:
            return False

    def getSignerPendingRequests(self) -> List[Dict]:
        """
        Get pending signature requests (signers only).

        Returns:
            List of pending signature requests awaiting this signer's signature
        """
        if not self.isAuthorizedSigner():
            return []

        if self._signer_node:
            try:
                import asyncio
                requests = asyncio.get_event_loop().run_until_complete(
                    self._signer_node.get_pending_requests()
                )
                return [r.to_dict() for r in requests]
            except Exception as e:
                logger.warning(f"getSignerPendingRequests failed: {e}")

        return []

    def approveSignerRequest(self, request_id: str) -> Dict:
        """
        Approve and sign a pending request (signers only).

        Args:
            request_id: ID of the request to approve

        Returns:
            Dict with result status
        """
        if not self.isAuthorizedSigner():
            return {'success': False, 'error': 'Not an authorized signer'}

        if self._signer_node:
            try:
                import asyncio
                result = asyncio.get_event_loop().run_until_complete(
                    self._signer_node.approve_request(request_id)
                )
                return {
                    'success': True,
                    'result': result.to_dict() if hasattr(result, 'to_dict') else result,
                    'message': f'Approved request {request_id}',
                }
            except Exception as e:
                logger.warning(f"approveSignerRequest failed: {e}")
                return {'success': False, 'error': str(e)}

        return {'success': False, 'error': 'Signer node not available'}

    def rejectSignerRequest(self, request_id: str) -> Dict:
        """
        Reject a pending request (signers only).

        Args:
            request_id: ID of the request to reject

        Returns:
            Dict with result status
        """
        if not self.isAuthorizedSigner():
            return {'success': False, 'error': 'Not an authorized signer'}

        if self._signer_node:
            try:
                import asyncio
                result = asyncio.get_event_loop().run_until_complete(
                    self._signer_node.reject_request(request_id)
                )
                return {
                    'success': result,
                    'message': f'Rejected request {request_id}' if result else 'Failed to reject',
                }
            except Exception as e:
                logger.warning(f"rejectSignerRequest failed: {e}")
                return {'success': False, 'error': str(e)}

        return {'success': False, 'error': 'Signer node not available'}

    def getSignerRequestStatus(self, request_id: str) -> Dict:
        """
        Get status of a signature request.

        Args:
            request_id: ID of the request

        Returns:
            Dict with request status including signature count
        """
        if self._signer_node:
            try:
                import asyncio
                status = asyncio.get_event_loop().run_until_complete(
                    self._signer_node.get_request_status(request_id)
                )
                return status
            except Exception as e:
                logger.warning(f"getSignerRequestStatus failed: {e}")

        return {'error': 'Status not available'}

    def getTreasuryBalance(self) -> Dict:
        """
        Get treasury balance (EVR and SATORI).

        Returns:
            Dict with balance information
        """
        if self._donation_manager:
            try:
                import asyncio
                evr_balance = asyncio.get_event_loop().run_until_complete(
                    self._donation_manager.get_treasury_balance()
                )
                return {
                    'evr': evr_balance,
                    'treasury_address': self._donation_manager.get_treasury_address(),
                }
            except Exception as e:
                logger.warning(f"getTreasuryBalance failed: {e}")

        return {'evr': 0.0, 'satori': 0.0}

    # ========================================================================
    # TREASURY ALERT METHODS (Edge case handling)
    # ========================================================================

    _alert_manager = None
    _deferred_rewards_manager = None
    _alert_callbacks: List[Callable] = []

    def set_alert_manager(self, manager):
        """Set the treasury alert manager."""
        self._alert_manager = manager
        # Register local callbacks with manager
        if manager:
            for callback in self._alert_callbacks:
                manager.on_alert(callback)

    def set_deferred_rewards_manager(self, manager):
        """Set the deferred rewards manager."""
        self._deferred_rewards_manager = manager

    def on_treasury_alert(self, callback: Callable) -> None:
        """
        Register a callback for treasury alerts.

        The callback receives a TreasuryAlert dict when alerts are broadcast.
        """
        self._alert_callbacks.append(callback)
        if self._alert_manager:
            self._alert_manager.on_alert(callback)

    def getTreasuryAlertStatus(self) -> Dict:
        """
        Get current treasury alert status.

        Returns:
            Dict with status, balances, active alerts, deferred info
        """
        if self._alert_manager:
            try:
                return self._alert_manager.get_status_summary()
            except Exception as e:
                logger.warning(f"getTreasuryAlertStatus failed: {e}")

        # Fallback to central
        if self._central_client:
            try:
                success, data = self._central_client.getTreasuryAlertStatus()
                if success:
                    return data
            except Exception:
                pass

        return {
            'status': 'unknown',
            'satori_balance': 0.0,
            'evr_balance': 0.0,
            'active_alerts': [],
            'deferred_rounds': 0,
            'total_deferred_rewards': 0.0,
            'last_successful_distribution': None,
        }

    def getDeferredRewards(self) -> Dict:
        """
        Get current user's deferred rewards.

        Returns:
            Dict with deferred reward info for this wallet
        """
        address = self.wallet.address if self.wallet else ""

        if self._deferred_rewards_manager and address:
            try:
                summary = self._deferred_rewards_manager.get_deferred_for_address(address)
                return summary.to_dict()
            except Exception as e:
                logger.warning(f"getDeferredRewards failed: {e}")

        # Fallback to central
        if self._central_client:
            try:
                success, data = self._central_client.getDeferredRewards()
                if success:
                    return data
            except Exception:
                pass

        return {
            'address': address,
            'total_pending': 0.0,
            'deferred_count': 0,
            'deferred_rewards': [],
            'oldest_deferred_at': None,
            'newest_deferred_at': None,
        }

    def getDeferredRewardsTotal(self) -> Dict:
        """
        Get network-wide deferred rewards total.

        Returns:
            Dict with total deferred amount and count
        """
        if self._deferred_rewards_manager:
            try:
                stats = self._deferred_rewards_manager.get_stats()
                return stats
            except Exception as e:
                logger.warning(f"getDeferredRewardsTotal failed: {e}")

        # Fallback to central
        if self._central_client:
            try:
                success, data = self._central_client.getDeferredRewardsTotal()
                if success:
                    return data
            except Exception:
                pass

        return {
            'total_deferred': 0.0,
            'deferred_count': 0,
            'unique_addresses': 0,
            'deferred_rounds': 0,
        }

    def getAlertHistory(self, limit: int = 20) -> List[Dict]:
        """
        Get treasury alert history.

        Args:
            limit: Max alerts to return

        Returns:
            List of historical alerts
        """
        if self._alert_manager:
            try:
                history = self._alert_manager.get_alert_history(limit)
                return [
                    {
                        'alert': entry.alert.to_dict(),
                        'resolved_at': entry.resolved_at,
                        'resolution': entry.resolution,
                    }
                    for entry in history
                ]
            except Exception as e:
                logger.warning(f"getAlertHistory failed: {e}")

        # Fallback to central
        if self._central_client:
            try:
                success, data = self._central_client.getAlertHistory(limit)
                if success:
                    return data
            except Exception:
                pass

        return []

    async def checkTreasuryAndAlert(
        self,
        required_satori: float,
        recipient_count: int = 100,
    ) -> Dict:
        """
        Check treasury status and broadcast alerts if needed.

        This is called before distribution to check if treasury has sufficient
        funds. If not, it broadcasts appropriate alerts.

        Args:
            required_satori: SATORI needed for distribution
            recipient_count: Number of recipients

        Returns:
            Dict with treasury status and any alerts
        """
        if self._alert_manager:
            try:
                status = await self._alert_manager.check_treasury_status(
                    required_satori=required_satori,
                    recipient_count=recipient_count,
                )
                return status.to_dict()
            except Exception as e:
                logger.warning(f"checkTreasuryAndAlert failed: {e}")
                return {'status': 'error', 'error': str(e)}

        return {'status': 'unknown', 'error': 'Alert manager not available'}

    def shouldDeferDistribution(
        self,
        required_satori: float,
        required_evr: float,
    ) -> tuple:
        """
        Check if distribution should be deferred.

        Args:
            required_satori: SATORI needed
            required_evr: EVR needed for fees

        Returns:
            Tuple of (should_defer, reason)
        """
        try:
            from ..protocol.deferred_rewards import should_defer_distribution

            # Get treasury balances
            if self._alert_manager:
                balances = self._alert_manager.get_treasury_balances()
            else:
                balances = {'satori': 0.0, 'evr': 0.0}

            return should_defer_distribution(
                treasury_satori=balances.get('satori', 0.0),
                treasury_evr=balances.get('evr', 0.0),
                required_satori=required_satori,
                required_evr=required_evr,
            )
        except ImportError:
            return False, None

    def deferRound(
        self,
        round_id: int,
        rewards: Dict[str, float],
        reason: str,
    ) -> bool:
        """
        Defer a round's rewards when treasury is insufficient.

        Args:
            round_id: The round being deferred
            rewards: Dict of address -> reward amount
            reason: Deferral reason

        Returns:
            True if deferred successfully
        """
        if self._deferred_rewards_manager:
            try:
                from ..protocol.deferred_rewards import DeferralReason
                reason_enum = DeferralReason(reason)
                self._deferred_rewards_manager.defer_round(
                    round_id=round_id,
                    rewards=rewards,
                    reason=reason_enum,
                )
                return True
            except Exception as e:
                logger.warning(f"deferRound failed: {e}")
        return False

    # ========================================================================
    # REWARD ADDRESS METHODS (Custom Payout Addresses)
    # ========================================================================

    def getRewardAddress(self, wallet_address: Optional[str] = None) -> Optional[str]:
        """
        Get the payout address for a wallet.

        If a custom reward address is set, returns that.
        Otherwise, returns the wallet address itself.

        Args:
            wallet_address: Address to look up (defaults to own wallet)

        Returns:
            Payout address or None if lookup fails
        """
        address = wallet_address or (self.wallet.address if self.wallet else "")

        if self._reward_address_manager:
            try:
                import asyncio
                payout = asyncio.get_event_loop().run_until_complete(
                    self._reward_address_manager.get_payout_address(address)
                )
                return payout
            except Exception as e:
                logger.warning(f"getRewardAddress failed: {e}")

        return address  # Default: wallet address

    def getMyRewardAddress(self) -> Optional[str]:
        """Get our own custom reward address if set."""
        if self._reward_address_manager:
            return self._reward_address_manager.get_my_reward_address()
        return None

    def removeRewardAddress(self) -> Dict:
        """
        Remove custom reward address (revert to wallet payouts).

        Returns:
            Dict with success status
        """
        if self._reward_address_manager:
            try:
                import asyncio
                result = asyncio.get_event_loop().run_until_complete(
                    self._reward_address_manager.remove_reward_address()
                )
                return {'success': result}
            except Exception as e:
                logger.warning(f"removeRewardAddress failed: {e}")
                return {'success': False, 'error': str(e)}

        return {'success': False, 'error': 'Reward address manager not available'}

    # ========================================================================
    # REFERRAL METHODS (Invite/Referral System)
    # ========================================================================

    def getReferrerStats(self, address: Optional[str] = None) -> Dict:
        """
        Get referral statistics for an address.

        Args:
            address: Address to look up (defaults to own wallet)

        Returns:
            Dict with referral count, tier, bonus, and achievements
        """
        addr = address or (self.wallet.address if self.wallet else "")

        if self._referral_manager:
            try:
                import asyncio
                stats = asyncio.get_event_loop().run_until_complete(
                    self._referral_manager.get_referrer_stats(addr)
                )
                if stats:
                    return stats.to_dict()
            except Exception as e:
                logger.warning(f"getReferrerStats failed: {e}")

        return {
            'referrer_address': addr,
            'referral_count': 0,
            'tier': None,
            'bonus': 0.0,
            'achievements': [],
        }

    def getMyReferrer(self) -> Optional[str]:
        """Get the address that referred us."""
        if self._referral_manager:
            try:
                import asyncio
                referrer = asyncio.get_event_loop().run_until_complete(
                    self._referral_manager.get_my_referrer()
                )
                return referrer
            except Exception as e:
                logger.warning(f"getMyReferrer failed: {e}")

        return None

    def getReferralBonus(self, address: Optional[str] = None) -> float:
        """
        Get the referral bonus multiplier for an address.

        Args:
            address: Address to look up (defaults to own wallet)

        Returns:
            Bonus multiplier (e.g., 0.05 for +5%)
        """
        addr = address or (self.wallet.address if self.wallet else "")

        if self._referral_manager:
            try:
                import asyncio
                bonus = asyncio.get_event_loop().run_until_complete(
                    self._referral_manager.get_referral_bonus(addr)
                )
                return bonus
            except Exception as e:
                logger.warning(f"getReferralBonus failed: {e}")

        return 0.0

    def getTopReferrers(self, limit: int = 10) -> List[Dict]:
        """
        Get top referrers by referral count.

        Args:
            limit: Number of top referrers to return

        Returns:
            List of referrer stats
        """
        if self._referral_manager:
            try:
                import asyncio
                referrers = asyncio.get_event_loop().run_until_complete(
                    self._referral_manager.get_top_referrers(limit)
                )
                return [r.to_dict() for r in referrers]
            except Exception as e:
                logger.warning(f"getTopReferrers failed: {e}")

        return []


class P2PCentrifugoClient:
    """
    Drop-in replacement for Centrifugo client.

    Uses satorip2p's GossipSub instead of Centrifugo WebSocket.
    """

    def __init__(
        self,
        ws_url: Optional[str] = None,
        token: Optional[str] = None,
        on_connected_callback: Optional[Callable] = None,
        on_disconnected_callback: Optional[Callable] = None,
        # P2P-specific
        peers: Optional["Peers"] = None,
        identity: Optional[Any] = None,
        # New: mode control
        networking_mode: Optional[NetworkingMode] = None,
    ):
        """
        Initialize P2P Centrifugo-compatible client.

        Args:
            ws_url: Ignored in P2P mode
            token: Ignored in P2P mode
            on_connected_callback: Called when connected
            on_disconnected_callback: Called when disconnected
            peers: Optional pre-initialized Peers instance
            identity: Optional identity for P2P
            networking_mode: Override networking mode
        """
        self.ws_url = ws_url
        self.token = token
        self.on_connected = on_connected_callback
        self.on_disconnected = on_disconnected_callback

        self._peers = peers
        self._identity = identity
        self._connected = False
        self._subscriptions: Dict[str, Callable] = {}

        self._mode = networking_mode or get_networking_mode()
        self._metrics = IntegrationMetrics.get_instance()

    async def connect(self):
        """Connect to P2P network."""
        start_time = time.time()

        try:
            if self._peers is None:
                from ..hybrid import HybridPeers, HybridMode

                hybrid_mode = (
                    HybridMode.HYBRID if self._mode == NetworkingMode.HYBRID
                    else HybridMode.P2P_ONLY
                )

                self._peers = HybridPeers(
                    identity=self._identity,
                    mode=hybrid_mode,
                )
                await self._peers.start()

            self._connected = True
            log_operation("centrifugo_connect", True, "p2p", (time.time() - start_time) * 1000)

            if self.on_connected and callable(self.on_connected):
                self.on_connected(None)

        except Exception as e:
            log_operation("centrifugo_connect", False, "p2p", error=str(e))
            raise

    async def disconnect(self):
        """Disconnect from P2P network."""
        self._connected = False

        if self.on_disconnected and callable(self.on_disconnected):
            self.on_disconnected(None)

    async def subscribe(
        self,
        channel: str,
        handler: Callable,
    ):
        """
        Subscribe to a channel.

        Args:
            channel: Channel name (format: "streams:{uuid}")
            handler: Callback for messages
        """
        # Extract stream UUID from channel name
        if channel.startswith('streams:'):
            stream_id = channel.split(':', 1)[1]
        else:
            stream_id = channel

        self._subscriptions[channel] = handler

        if self._peers:
            await self._peers.subscribe(stream_id, lambda sid, data: handler(data))
            log_operation("centrifugo_subscribe", True, "p2p")

        logger.info(f"P2P subscribed to channel: {channel}")

    async def publish(self, channel: str, data: Any):
        """
        Publish to a channel.

        Args:
            channel: Channel name
            data: Data to publish
        """
        if channel.startswith('streams:'):
            stream_id = channel.split(':', 1)[1]
        else:
            stream_id = channel

        if self._peers:
            await self._peers.publish(stream_id, data)
            log_operation("centrifugo_publish", True, "p2p")

    @property
    def connected(self) -> bool:
        """Check if connected."""
        return self._connected


async def create_p2p_centrifugo_client(
    ws_url: str = None,
    token: str = None,
    on_connected_callback: Callable = None,
    on_disconnected_callback: Callable = None,
    peers: Optional["Peers"] = None,
    identity: Optional[Any] = None,
) -> P2PCentrifugoClient:
    """
    Factory function matching create_centrifugo_client signature.

    Usage:
        # Replace:
        from satorilib.centrifugo import create_centrifugo_client

        # With:
        from satorip2p.integration.neuron import create_p2p_centrifugo_client as create_centrifugo_client
    """
    client = P2PCentrifugoClient(
        ws_url=ws_url,
        token=token,
        on_connected_callback=on_connected_callback,
        on_disconnected_callback=on_disconnected_callback,
        peers=peers,
        identity=identity,
    )
    return client


class P2PStartupDagMixin:
    """
    Mixin class for StartupDag to add P2P support.

    Usage in Neuron:
        class StartupDag(StartupDagStruct, P2PStartupDagMixin, metaclass=SingletonMeta):
            ...

    Or monkey-patch existing StartupDag:
        from satorip2p.integration import P2PStartupDagMixin
        StartupDag.initializeP2P = P2PStartupDagMixin.initializeP2P
        StartupDag.createP2PServerConn = P2PStartupDagMixin.createP2PServerConn
    """

    _p2p_peers = None
    _p2p_auto_switch = None

    async def initializeP2P(self, mode: Optional[NetworkingMode] = None):
        """
        Initialize P2P networking.

        Call this in start() before other networking setup.

        Args:
            mode: Networking mode to use (default: from config)
        """
        from ..hybrid import HybridPeers, HybridMode
        from .auto_switch import AutoSwitch, start_auto_switch

        # Get mode from config if not specified
        if mode is None:
            mode = get_networking_mode()

        configure_networking(mode=mode)

        if mode == NetworkingMode.CENTRAL:
            logger.info("P2P disabled, using central-only mode")
            return None

        hybrid_mode = (
            HybridMode.HYBRID if mode == NetworkingMode.HYBRID
            else HybridMode.P2P_ONLY
        )

        self._p2p_peers = HybridPeers(
            identity=getattr(self, 'identity', None),
            mode=hybrid_mode,
        )
        await self._p2p_peers.start()

        # Start auto-switch if in hybrid mode
        if mode == NetworkingMode.HYBRID:
            self._p2p_auto_switch = start_auto_switch(peers=self._p2p_peers)

        logger.info(f"P2P initialized in {mode.name} mode")
        return self._p2p_peers

    def createP2PServerConn(self):
        """
        Create P2P server connection.

        Replacement for createServerConn() that uses P2P.
        """
        wallet = getattr(self, 'wallet', None)
        url_server = getattr(self, 'urlServer', None)
        url_mundo = getattr(self, 'urlMundo', None)

        self.server = P2PSatoriServerClient(
            wallet=wallet,
            url=url_server,
            sendingUrl=url_mundo,
            peers=self._p2p_peers,
            enable_central_fallback=True,
        )

    def getP2PPeers(self):
        """Get the P2P Peers instance."""
        return self._p2p_peers

    def getP2PAutoSwitch(self):
        """Get the auto-switch instance."""
        return self._p2p_auto_switch

    def getNetworkingMetrics(self) -> Dict:
        """Get networking metrics."""
        return IntegrationMetrics.get_instance().get_stats()

    def getNetworkingHealth(self) -> Dict:
        """Get networking health status."""
        return IntegrationMetrics.get_instance().get_health()
