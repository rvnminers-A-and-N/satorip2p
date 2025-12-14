"""
satorip2p.hybrid.hybrid_peers - Dual-Mode Peers Class

HybridPeers provides a unified interface for P2P and Central server
connectivity with automatic failover and message bridging.

Usage:
    from satorip2p.hybrid import HybridPeers, HybridMode

    # Create with P2P and Central fallback
    peers = HybridPeers(
        identity=identity,
        mode=HybridMode.HYBRID,
    )

    await peers.start()

    # Subscribe works on both P2P and Central
    await peers.subscribe("stream-uuid", callback)

    # Publish goes to both (or preferred based on config)
    await peers.publish("stream-uuid", data)
"""

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional
import trio
import logging

from .config import HybridConfig, HybridMode, FailoverStrategy
from .bridge import HybridBridge
from .central_client import CentralPubSubClient

if TYPE_CHECKING:
    from ..identity.evrmore_bridge import EvrmoreIdentityBridge

logger = logging.getLogger("satorip2p.hybrid.hybrid_peers")


class HybridPeers:
    """
    Dual-mode Peers class supporting both P2P and Central servers.

    Provides the same interface as the base Peers class but with
    support for hybrid operation, automatic failover, and message
    bridging between P2P and Central networks.

    Attributes:
        mode: Current operating mode (P2P_ONLY, HYBRID, CENTRAL_ONLY)
        p2p: Underlying P2P Peers instance
        bridge: Message bridge for hybrid mode
    """

    def __init__(
        self,
        identity: Optional["EvrmoreIdentityBridge"] = None,
        mode: HybridMode = HybridMode.HYBRID,
        config: Optional[HybridConfig] = None,
        central_url: Optional[str] = None,
        listen_port: int = 4001,
        bootstrap_peers: Optional[List[str]] = None,
    ):
        """
        Initialize HybridPeers.

        Args:
            identity: Identity for signing/verification
            mode: Operating mode (default: HYBRID)
            config: Full HybridConfig (overrides other params)
            central_url: Central pubsub WebSocket URL
            listen_port: P2P listen port
            bootstrap_peers: P2P bootstrap peer addresses
        """
        # Build or use config
        if config:
            self.config = config
        else:
            self.config = HybridConfig(mode=mode)
            if central_url:
                self.config.central.pubsub_url = central_url
            self.config.p2p.listen_port = listen_port
            if bootstrap_peers:
                self.config.p2p.bootstrap_peers = bootstrap_peers

        self.identity = identity

        # P2P Peers instance (lazy initialized)
        self._p2p = None

        # Bridge for hybrid mode
        self._bridge: Optional[HybridBridge] = None

        # Central-only client (for CENTRAL_ONLY mode)
        self._central_only: Optional[CentralPubSubClient] = None

        # User ID for central server
        self._uid = identity.address if identity else "unknown"

        # Callbacks storage (for all modes)
        self._callbacks: Dict[str, List[Callable]] = {}

        # State
        self._started = False
        self._nursery: Optional[trio.Nursery] = None

    @property
    def mode(self) -> HybridMode:
        """Current operating mode."""
        return self.config.mode

    @property
    def p2p(self):
        """Get the underlying P2P Peers instance."""
        return self._p2p

    @property
    def connected_peers(self) -> int:
        """Number of connected P2P peers."""
        if self._p2p:
            return self._p2p.connected_peers
        return 0

    @property
    def is_p2p_connected(self) -> bool:
        """Whether P2P network is connected."""
        return self._p2p is not None and self._p2p.connected_peers > 0

    @property
    def is_central_connected(self) -> bool:
        """Whether Central server is connected."""
        if self._bridge:
            return self._bridge._central_healthy
        if self._central_only:
            return self._central_only.connected
        return False

    async def start(self) -> bool:
        """
        Start the hybrid peers.

        Initializes P2P and/or Central connections based on mode.

        Returns:
            True if started successfully
        """
        if self._started:
            return True

        logger.info(f"Starting HybridPeers in {self.config.mode.name} mode")

        try:
            # Initialize P2P if enabled
            if self.config.is_p2p_enabled():
                from ..peers import Peers

                self._p2p = Peers(
                    identity=self.identity,
                    port=self.config.p2p.listen_port,
                    enable_relay=self.config.p2p.enable_relay,
                )
                await self._p2p.start()
                logger.info("P2P networking started")

            # Initialize bridge or central-only client
            if self.config.mode == HybridMode.HYBRID:
                # Use bridge for hybrid mode
                self._bridge = HybridBridge(
                    config=self.config,
                    p2p=self._p2p,
                    uid=self._uid,
                )
                await self._bridge.start()
                logger.info("Hybrid bridge started")

            elif self.config.mode == HybridMode.CENTRAL_ONLY:
                # Direct central connection only
                self._central_only = CentralPubSubClient(
                    uid=self._uid,
                    url=self.config.central.pubsub_url,
                    on_message=self._on_central_message,
                )
                await self._central_only.start()
                logger.info("Central-only client started")

            self._started = True
            return True

        except Exception as e:
            logger.error(f"Failed to start HybridPeers: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def stop(self) -> None:
        """Stop the hybrid peers."""
        if not self._started:
            return

        logger.info("Stopping HybridPeers...")

        if self._bridge:
            await self._bridge.stop()

        if self._central_only:
            await self._central_only.stop()

        if self._p2p:
            await self._p2p.stop()

        self._started = False
        logger.info("HybridPeers stopped")

    async def run_forever(self) -> None:
        """
        Run the hybrid peers with all background tasks.

        Usage:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(peers.run_forever)
        """
        if not self._started:
            if not await self.start():
                return

        try:
            async with trio.open_nursery() as nursery:
                self._nursery = nursery

                # Run P2P if enabled
                if self._p2p:
                    nursery.start_soon(self._p2p.run_forever)

                # Run bridge or central-only client
                if self._bridge:
                    nursery.start_soon(self._bridge.run_forever)
                elif self._central_only:
                    nursery.start_soon(self._central_only.run_forever)

                # Keep running
                while True:
                    await trio.sleep(1)

        except trio.Cancelled:
            logger.info("HybridPeers cancelled")
            raise

    async def subscribe(
        self,
        stream_id: str,
        callback: Callable[[str, Any], None],
    ) -> bool:
        """
        Subscribe to a stream.

        In HYBRID mode, subscribes on both P2P and Central.
        In P2P_ONLY mode, subscribes only on P2P.
        In CENTRAL_ONLY mode, subscribes only on Central.

        Args:
            stream_id: Stream UUID to subscribe to
            callback: Function called with (stream_id, data) on new messages

        Returns:
            True if subscribed successfully
        """
        # Store callback
        if stream_id not in self._callbacks:
            self._callbacks[stream_id] = []
        self._callbacks[stream_id].append(callback)

        if self._bridge:
            # Hybrid mode - bridge handles both
            return await self._bridge.subscribe(stream_id, callback)

        elif self._central_only:
            # Central only mode
            return await self._central_only.subscribe(stream_id)

        elif self._p2p:
            # P2P only mode
            await self._p2p.subscribe_async(stream_id, callback)
            return True

        return False

    async def subscribe_async(
        self,
        stream_id: str,
        callback: Callable[[str, Any], None],
    ) -> bool:
        """Alias for subscribe()."""
        return await self.subscribe(stream_id, callback)

    async def unsubscribe(self, stream_id: str) -> None:
        """Unsubscribe from a stream."""
        self._callbacks.pop(stream_id, None)

        if self._bridge:
            await self._bridge.unsubscribe(stream_id)
        elif self._central_only:
            await self._central_only.unsubscribe(stream_id)
        elif self._p2p:
            await self._p2p.unsubscribe_async(stream_id)

    async def unsubscribe_async(self, stream_id: str) -> None:
        """Alias for unsubscribe()."""
        await self.unsubscribe(stream_id)

    async def publish(
        self,
        stream_id: str,
        data: Any,
        observation_time: Optional[str] = None,
        observation_hash: Optional[str] = None,
    ) -> bool:
        """
        Publish data to a stream.

        In HYBRID mode, publishes based on failover strategy.
        In P2P_ONLY mode, publishes only to P2P.
        In CENTRAL_ONLY mode, publishes only to Central.

        Args:
            stream_id: Stream UUID to publish to
            data: Data to publish
            observation_time: Timestamp of observation
            observation_hash: Hash of observation

        Returns:
            True if published successfully
        """
        if self._bridge:
            # Hybrid mode
            return await self._bridge.publish(
                stream_id, data, observation_time, observation_hash
            )

        elif self._central_only:
            # Central only mode
            return await self._central_only.publish(
                stream_id, data, observation_time, observation_hash
            )

        elif self._p2p:
            # P2P only mode
            message = {
                "topic": stream_id,
                "data": data,
                "time": observation_time,
                "hash": observation_hash,
            }
            await self._p2p.publish(stream_id, message)
            return True

        return False

    def _on_central_message(self, stream_id: str, raw_message: str) -> None:
        """Handle message from central-only client."""
        import json

        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError:
            data = raw_message

        # Deliver to callbacks
        callbacks = self._callbacks.get(stream_id, [])
        for callback in callbacks:
            try:
                callback(stream_id, data)
            except Exception as e:
                logger.warning(f"Callback error: {e}")

    def get_my_subscriptions(self) -> List[str]:
        """Get list of current subscriptions."""
        if self._bridge:
            return self._bridge.get_subscriptions()
        if self._central_only:
            return self._central_only.get_subscriptions()
        if self._p2p:
            return self._p2p.get_my_subscriptions()
        return []

    def get_my_publications(self) -> List[str]:
        """Get list of streams we're publishing."""
        if self._p2p:
            return self._p2p.get_my_publications()
        return []

    async def discover_publishers(self, stream_id: str) -> List[str]:
        """Discover publishers for a stream."""
        if self._p2p:
            return await self._p2p.discover_publishers(stream_id)
        return []

    async def discover_subscribers(self, stream_id: str) -> List[str]:
        """Discover subscribers for a stream."""
        if self._p2p:
            return await self._p2p.discover_subscribers(stream_id)
        return []

    def get_stats(self) -> Dict[str, Any]:
        """Get hybrid peers statistics."""
        stats = {
            "mode": self.config.mode.name,
            "started": self._started,
            "p2p_connected": self.is_p2p_connected,
            "central_connected": self.is_central_connected,
            "subscriptions": len(self._callbacks),
        }

        if self._bridge:
            stats["bridge"] = self._bridge.get_stats()

        if self._central_only:
            stats["central"] = self._central_only.get_stats()

        if self._p2p:
            stats["p2p"] = {
                "connected_peers": self._p2p.connected_peers,
                "peer_id": str(self._p2p.peer_id) if self._p2p.peer_id else None,
            }

        return stats

    def __repr__(self) -> str:
        mode = self.config.mode.name
        p2p = f"P2P:{self.connected_peers}" if self.is_p2p_connected else "P2P:OFF"
        central = "Central:OK" if self.is_central_connected else "Central:OFF"
        return f"HybridPeers({mode}, {p2p}, {central})"
