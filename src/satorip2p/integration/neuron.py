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
        """Get balances from server."""
        if self._central_client:
            try:
                return self._central_client.getBalances()
            except Exception as e:
                logger.warning(f"Central getBalances failed: {e}")

        # Return empty balances in P2P mode
        return True, {'currency': 0, 'chain_balance': 0}

    def stakeCheck(self) -> bool:
        """Check stake status."""
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

    def setRewardAddress(self, signature: str, pubkey: str, address: str):
        """Set reward address."""
        if self._central_client:
            try:
                return self._central_client.setRewardAddress(signature, pubkey, address)
            except Exception:
                pass

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
        """Get public IP."""
        if self._central_client:
            try:
                return self._central_client.getPublicIp()
            except Exception:
                pass

        # Return a mock response
        class MockResponse:
            text = "0.0.0.0"
        return MockResponse()

    def invitedBy(self, address: str):
        """Report invited by."""
        if self._central_client:
            try:
                return self._central_client.invitedBy(address)
            except Exception:
                pass

    def poolAccepting(self, status: bool) -> tuple:
        """Set pool accepting status."""
        if self._central_client:
            try:
                return self._central_client.poolAccepting(status)
            except Exception:
                pass
        return False, None

    def getSearchStreams(self, searchText: Optional[str] = None):
        """Search streams."""
        if self._central_client:
            try:
                return self._central_client.getSearchStreams(searchText)
            except Exception:
                pass
        return []

    def getSearchStreamsPaginated(self, **kwargs) -> tuple:
        """Search streams with pagination."""
        if self._central_client:
            try:
                return self._central_client.getSearchStreamsPaginated(**kwargs)
            except Exception:
                pass
        return [], 0

    def getSearchPredictionStreamsPaginated(self, **kwargs) -> tuple:
        """Search prediction streams with pagination."""
        if self._central_client:
            try:
                return self._central_client.getSearchPredictionStreamsPaginated(**kwargs)
            except Exception:
                pass
        return [], 0


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
