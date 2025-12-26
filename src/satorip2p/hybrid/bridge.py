"""
satorip2p.hybrid.bridge - Message Bridge between Central and P2P

Routes messages between the central Satori pubsub server and the
libp2p P2P network, enabling hybrid operation during migration.

Features:
    - Bidirectional message routing (Central ↔ P2P)
    - Subscription synchronization
    - Deduplication to prevent message loops
    - Health monitoring for both sides
"""

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set
import trio
import json
import time
import hashlib
import logging

from .config import HybridConfig, HybridMode, FailoverStrategy
from .central_client import CentralPubSubClient

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.hybrid.bridge")


class MessageDeduplicator:
    """
    Deduplicates messages to prevent routing loops.

    When a message is received from one side and forwarded to the other,
    we need to avoid re-forwarding it when we receive it back.
    """

    def __init__(self, ttl: float = 60.0, max_size: int = 10000):
        """
        Initialize deduplicator.

        Args:
            ttl: Time-to-live for message hashes in seconds
            max_size: Maximum number of hashes to store
        """
        self.ttl = ttl
        self.max_size = max_size
        self._seen: Dict[str, float] = {}  # hash -> timestamp

    def _hash_message(self, stream_id: str, data: Any) -> str:
        """Create hash of message for deduplication."""
        content = f"{stream_id}:{json.dumps(data, sort_keys=True) if isinstance(data, dict) else str(data)}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def is_duplicate(self, stream_id: str, data: Any) -> bool:
        """
        Check if message is a duplicate.

        Returns:
            True if message was seen recently
        """
        msg_hash = self._hash_message(stream_id, data)
        now = time.time()

        # Clean old entries
        self._cleanup(now)

        # Check if seen
        if msg_hash in self._seen:
            return True

        # Mark as seen
        self._seen[msg_hash] = now
        return False

    def _cleanup(self, now: float) -> None:
        """Remove expired entries."""
        if len(self._seen) > self.max_size:
            # Remove oldest half
            sorted_items = sorted(self._seen.items(), key=lambda x: x[1])
            self._seen = dict(sorted_items[len(sorted_items) // 2:])

        # Remove expired
        expired = [h for h, t in self._seen.items() if now - t > self.ttl]
        for h in expired:
            del self._seen[h]


class HybridBridge:
    """
    Bridge between Central pubsub and P2P network.

    Routes messages bidirectionally, handles subscription sync,
    and provides failover capabilities.

    Attributes:
        config: Hybrid configuration
        p2p: P2P Peers instance
        central: Central pubsub client
    """

    def __init__(
        self,
        config: HybridConfig,
        p2p: "Peers",
        uid: str,
    ):
        """
        Initialize HybridBridge.

        Args:
            config: Hybrid configuration
            p2p: P2P Peers instance
            uid: User ID (wallet address) for central server auth
        """
        self.config = config
        self.p2p = p2p
        self.uid = uid

        # Central client (created if central is enabled)
        self._central: Optional[CentralPubSubClient] = None

        # Message deduplication
        self._dedup = MessageDeduplicator()

        # Subscription tracking
        self._subscriptions: Set[str] = set()
        self._callbacks: Dict[str, List[Callable]] = {}  # stream_id -> callbacks

        # Health status
        self._p2p_healthy = False
        self._central_healthy = False
        self._p2p_failures = 0
        self._central_failures = 0

        # Stats
        self._messages_bridged_to_p2p = 0
        self._messages_bridged_to_central = 0

        # State
        self._started = False
        self._nursery: Optional[trio.Nursery] = None

    async def start(self) -> bool:
        """
        Start the bridge.

        Returns:
            True if started successfully
        """
        if self._started:
            return True

        logger.info(f"Starting HybridBridge in {self.config.mode.name} mode")

        # Start central client if enabled
        if self.config.is_central_enabled():
            self._central = CentralPubSubClient(
                uid=self.uid,
                url=self.config.central.pubsub_url,
                on_message=self._on_central_message,
                on_connect=self._on_central_connect,
                on_disconnect=self._on_central_disconnect,
                reconnect_delay=self.config.central.reconnect_delay,
            )
            await self._central.start()
            self._central_healthy = self._central.connected

        # P2P is assumed to be started externally
        self._p2p_healthy = True

        self._started = True
        logger.info("HybridBridge started")
        return True

    async def stop(self) -> None:
        """Stop the bridge."""
        if not self._started:
            return

        logger.info("Stopping HybridBridge...")

        if self._central:
            await self._central.stop()

        self._started = False
        logger.info("HybridBridge stopped")

    async def run_forever(self) -> None:
        """
        Run the bridge with all background tasks.

        Use in a trio nursery:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(bridge.run_forever)
        """
        if not self._started:
            await self.start()

        try:
            async with trio.open_nursery() as nursery:
                self._nursery = nursery

                # Run central client if enabled
                if self._central:
                    nursery.start_soon(self._central.run_forever)

                # Run health check loop
                if self.config.enable_health_checks:
                    nursery.start_soon(self._health_check_loop)

                # Keep running
                while True:
                    await trio.sleep(1)

        except trio.Cancelled:
            logger.info("HybridBridge cancelled")
            raise

    async def subscribe(
        self,
        stream_id: str,
        callback: Callable[[str, Any], None],
    ) -> bool:
        """
        Subscribe to a stream on both P2P and Central.

        Args:
            stream_id: Stream UUID to subscribe to
            callback: Function called with (stream_id, data) on new messages

        Returns:
            True if subscribed successfully on at least one side
        """
        success = False

        # Register callback
        if stream_id not in self._callbacks:
            self._callbacks[stream_id] = []
        self._callbacks[stream_id].append(callback)

        # Subscribe on P2P
        if self.config.is_p2p_enabled():
            try:
                def p2p_callback(sid: str, data: Any):
                    self._on_p2p_message(sid, data)

                await self.p2p.subscribe_async(stream_id, p2p_callback)
                success = True
                logger.debug(f"P2P subscription: {stream_id}")
            except Exception as e:
                logger.warning(f"P2P subscribe failed: {e}")

        # Subscribe on Central
        if self.config.is_central_enabled() and self._central:
            try:
                await self._central.subscribe(stream_id)
                success = True
                logger.debug(f"Central subscription: {stream_id}")
            except Exception as e:
                logger.warning(f"Central subscribe failed: {e}")

        if success:
            self._subscriptions.add(stream_id)

        return success

    async def unsubscribe(self, stream_id: str) -> None:
        """Unsubscribe from a stream."""
        self._subscriptions.discard(stream_id)
        self._callbacks.pop(stream_id, None)

        if self.config.is_p2p_enabled():
            try:
                await self.p2p.unsubscribe_async(stream_id)
            except Exception:
                pass

        if self._central:
            try:
                await self._central.unsubscribe(stream_id)
            except Exception:
                pass

    async def publish(
        self,
        stream_id: str,
        data: Any,
        observation_time: Optional[str] = None,
        observation_hash: Optional[str] = None,
    ) -> bool:
        """
        Publish data to a stream on both P2P and Central.

        Args:
            stream_id: Stream UUID to publish to
            data: Data to publish
            observation_time: Timestamp of observation
            observation_hash: Hash of observation

        Returns:
            True if published successfully on at least one side
        """
        success = False

        # Mark as seen to prevent loop back
        self._dedup.is_duplicate(stream_id, data)

        # Determine where to publish based on mode and failover strategy
        publish_p2p = False
        publish_central = False

        if self.config.mode == HybridMode.P2P_ONLY:
            publish_p2p = True
        elif self.config.mode == HybridMode.CENTRAL_ONLY:
            publish_central = True
        elif self.config.mode == HybridMode.HYBRID:
            if self.config.failover == FailoverStrategy.PARALLEL:
                publish_p2p = True
                publish_central = True
            elif self.config.failover == FailoverStrategy.PREFER_P2P:
                if self._p2p_healthy:
                    publish_p2p = True
                else:
                    publish_central = True
            else:  # PREFER_CENTRAL
                if self._central_healthy:
                    publish_central = True
                else:
                    publish_p2p = True

        # Publish to P2P
        if publish_p2p and self.config.is_p2p_enabled():
            try:
                message = {
                    "topic": stream_id,
                    "data": data,
                    "time": observation_time or str(time.time()),
                    "hash": observation_hash or "",
                }
                await self.p2p.publish(stream_id, message)
                success = True
                logger.debug(f"Published to P2P: {stream_id}")
            except Exception as e:
                logger.warning(f"P2P publish failed: {e}")
                self._p2p_failures += 1

        # Publish to Central
        if publish_central and self._central:
            try:
                central_success = await self._central.publish(
                    topic=stream_id,
                    data=data,
                    observation_time=observation_time,
                    observation_hash=observation_hash,
                )
                if central_success:
                    success = True
                    logger.debug(f"Published to Central: {stream_id}")
            except Exception as e:
                logger.warning(f"Central publish failed: {e}")
                self._central_failures += 1

        return success

    def _on_p2p_message(self, stream_id: str, data: Any) -> None:
        """Handle message received from P2P network."""
        # Check for duplicate
        if self._dedup.is_duplicate(stream_id, data):
            return

        # Call user callbacks
        self._deliver_to_callbacks(stream_id, data)

        # Bridge to central if configured
        if self.config.should_bridge() and self._central and self._central.connected:
            if self._nursery:
                self._nursery.start_soon(self._bridge_to_central, stream_id, data)

    def _on_central_message(self, stream_id: str, raw_message: str) -> None:
        """Handle message received from Central server."""
        # Parse message
        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError:
            data = raw_message

        # Check for duplicate
        if self._dedup.is_duplicate(stream_id, data):
            return

        # Call user callbacks
        self._deliver_to_callbacks(stream_id, data)

        # Bridge to P2P if configured
        if self.config.should_bridge():
            if self._nursery:
                self._nursery.start_soon(self._bridge_to_p2p, stream_id, data)

    def _deliver_to_callbacks(self, stream_id: str, data: Any) -> None:
        """Deliver message to registered callbacks."""
        callbacks = self._callbacks.get(stream_id, [])
        for callback in callbacks:
            try:
                callback(stream_id, data)
            except Exception as e:
                logger.warning(f"Callback error for {stream_id}: {e}")

    async def _bridge_to_central(self, stream_id: str, data: Any) -> None:
        """Bridge message from P2P to Central."""
        if not self._central or not self._central.connected:
            return

        try:
            # Extract fields if data is dict
            if isinstance(data, dict):
                obs_time = data.get("time")
                obs_hash = data.get("hash")
                actual_data = data.get("data", data)
            else:
                obs_time = None
                obs_hash = None
                actual_data = data

            await self._central.publish(
                topic=stream_id,
                data=actual_data,
                observation_time=obs_time,
                observation_hash=obs_hash,
            )
            self._messages_bridged_to_central += 1

            if self.config.log_bridge_events:
                logger.debug(f"Bridged P2P→Central: {stream_id}")

        except Exception as e:
            logger.warning(f"Bridge to central failed: {e}")

    async def _bridge_to_p2p(self, stream_id: str, data: Any) -> None:
        """Bridge message from Central to P2P."""
        try:
            await self.p2p.publish(stream_id, data)
            self._messages_bridged_to_p2p += 1

            if self.config.log_bridge_events:
                logger.debug(f"Bridged Central→P2P: {stream_id}")

        except Exception as e:
            logger.warning(f"Bridge to P2P failed: {e}")

    def _on_central_connect(self) -> None:
        """Handle central server connection."""
        self._central_healthy = True
        self._central_failures = 0
        logger.info("Central server connected")

    def _on_central_disconnect(self) -> None:
        """Handle central server disconnection."""
        self._central_healthy = False
        logger.warning("Central server disconnected")

    async def _health_check_loop(self) -> None:
        """Periodically check health of both sides."""
        while True:
            await trio.sleep(self.config.central.health_check_interval)

            # Check P2P health
            try:
                p2p_peers = self.p2p.connected_peers
                self._p2p_healthy = p2p_peers > 0
            except Exception:
                self._p2p_healthy = False

            # Check Central health
            if self._central:
                self._central_healthy = self._central.connected

            # Check for failover
            if self.config.failover_on_disconnect:
                if self._central_failures >= self.config.failover_threshold:
                    if self.config.failover == FailoverStrategy.PREFER_CENTRAL:
                        logger.warning("Central failures exceeded threshold, failing over to P2P")
                        # Mode remains HYBRID but we'll prefer P2P now
                        self._central_failures = 0

                if self._p2p_failures >= self.config.failover_threshold:
                    if self.config.failover == FailoverStrategy.PREFER_P2P:
                        logger.warning("P2P failures exceeded threshold, failing over to Central")
                        self._p2p_failures = 0

    def get_subscriptions(self) -> List[str]:
        """Get list of active subscriptions."""
        return list(self._subscriptions)

    def get_stats(self) -> Dict[str, Any]:
        """Get bridge statistics."""
        stats = {
            "mode": self.config.mode.name,
            "failover_strategy": self.config.failover.name,
            "p2p_healthy": self._p2p_healthy,
            "central_healthy": self._central_healthy,
            "subscriptions": len(self._subscriptions),
            "messages_bridged_to_p2p": self._messages_bridged_to_p2p,
            "messages_bridged_to_central": self._messages_bridged_to_central,
            "p2p_failures": self._p2p_failures,
            "central_failures": self._central_failures,
        }

        if self._central:
            stats["central"] = self._central.get_stats()

        return stats

    def __repr__(self) -> str:
        p2p = "P2P:OK" if self._p2p_healthy else "P2P:DOWN"
        central = "Central:OK" if self._central_healthy else "Central:DOWN"
        return f"HybridBridge({self.config.mode.name}, {p2p}, {central})"
