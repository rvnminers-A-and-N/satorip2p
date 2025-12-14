"""
satorip2p/protocol/rendezvous.py

Rendezvous protocol for namespace-based peer discovery.

Rendezvous allows peers to:
1. Register under namespaces (e.g., stream IDs)
2. Discover other peers registered under the same namespace
3. Get real-time updates when peers join/leave

This complements DHT by providing fast, topic-based discovery.
DHT is for general peer discovery; Rendezvous is for stream-specific discovery.

Architecture:
- Relay nodes run RendezvousService (server mode)
- Other peers use RendezvousDiscovery (client mode) to connect to relay

Usage:
    # Client mode (most peers)
    rendezvous = RendezvousManager(host, rendezvous_peer_id=relay_peer_id)
    await rendezvous.start()

    # Server mode (relay nodes)
    rendezvous = RendezvousManager(host, is_server=True)
    await rendezvous.start()

    # Register for a stream
    await rendezvous.register("stream-uuid-123")

    # Find peers interested in same stream
    peers = await rendezvous.discover("stream-uuid-123")

    # Unregister when done
    await rendezvous.unregister("stream-uuid-123")
"""

import logging
import time
import trio
from typing import Dict, List, Set, Optional, Any, Union
from dataclasses import dataclass, field

logger = logging.getLogger("satorip2p.protocol.rendezvous")


@dataclass
class RendezvousRegistration:
    """Record of a namespace registration."""
    peer_id: str
    namespace: str
    ttl: int = 7200  # 2 hours default
    registered_at: float = field(default_factory=time.time)

    def is_expired(self) -> bool:
        """Check if registration has expired."""
        return time.time() - self.registered_at > self.ttl

    def time_remaining(self) -> float:
        """Get seconds until expiration."""
        return max(0, self.ttl - (time.time() - self.registered_at))


class RendezvousManager:
    """
    Manages Rendezvous protocol for namespace-based peer discovery.

    In libp2p, Rendezvous works by:
    1. Peers register with rendezvous points under namespaces
    2. Other peers query rendezvous points for namespace members
    3. Registrations have TTL and must be refreshed

    For Satori:
    - Namespace = Stream UUID
    - Register when subscribing to a stream
    - Discover to find other subscribers/publishers

    This can run alongside DHT:
    - DHT for general peer discovery
    - Rendezvous for stream-specific discovery

    Modes:
    - Server mode: Relay nodes run RendezvousService
    - Client mode: Other peers use RendezvousDiscovery
    """

    # Namespace prefixes for Satori
    SUBSCRIBER_NS_PREFIX = "satori/sub/"
    PUBLISHER_NS_PREFIX = "satori/pub/"
    PEER_NS_PREFIX = "satori/peer/"

    DEFAULT_TTL = 7200  # 2 hours

    def __init__(
        self,
        host=None,
        rendezvous_peer_id: Optional[str] = None,
        is_server: bool = False,
    ):
        """
        Initialize Rendezvous manager.

        Args:
            host: py-libp2p host instance
            rendezvous_peer_id: Peer ID of rendezvous server (for client mode)
            is_server: If True, run as rendezvous server (for relay nodes)
        """
        self.host = host
        self._rendezvous_peer_id = rendezvous_peer_id
        self._is_server = is_server

        # libp2p instances
        self._service = None  # RendezvousService (server mode)
        self._discovery = None  # RendezvousDiscovery (client mode)

        # Local registration cache
        self._my_registrations: Dict[str, RendezvousRegistration] = {}

        # Known peers per namespace (cache from discoveries)
        self._namespace_peers: Dict[str, Set[str]] = {}

        # Background task management
        self._refresh_cancel_scope: Optional[trio.CancelScope] = None
        self._nursery: Optional[trio.Nursery] = None

    def set_host(self, host) -> None:
        """Set the libp2p host."""
        self.host = host

    def set_rendezvous_peer(self, peer_id: str) -> None:
        """Set the rendezvous server peer ID (for client mode)."""
        self._rendezvous_peer_id = peer_id

    async def start(self, nursery: Optional[trio.Nursery] = None) -> bool:
        """
        Initialize Rendezvous protocol.

        Args:
            nursery: Optional trio nursery for background tasks

        Returns:
            True if started successfully
        """
        if not self.host:
            logger.warning("No host set, Rendezvous disabled")
            return False

        self._nursery = nursery

        try:
            from libp2p.discovery.rendezvous import (
                RendezvousService,
                RendezvousDiscovery,
            )
            from libp2p.peer.id import ID

            if self._is_server:
                # Server mode: run RendezvousService
                self._service = RendezvousService(self.host)
                logger.info("Rendezvous server started")
                return True

            else:
                # Client mode: use RendezvousDiscovery
                if not self._rendezvous_peer_id:
                    logger.warning(
                        "No rendezvous peer ID set, using local-only mode"
                    )
                    return True

                # Parse peer ID
                if isinstance(self._rendezvous_peer_id, str):
                    rendezvous_peer = ID.from_base58(self._rendezvous_peer_id)
                else:
                    rendezvous_peer = self._rendezvous_peer_id

                # Create discovery client with auto-refresh
                self._discovery = RendezvousDiscovery(
                    self.host,
                    rendezvous_peer,
                    enable_refresh=True,
                )

                # Note: RendezvousDiscovery manages its own tasks via run()
                # The run() method should be called in a nursery if long-running
                # For now, discovery is on-demand

                logger.info(
                    f"Rendezvous client started, server: {self._rendezvous_peer_id}"
                )
                return True

        except ImportError as e:
            logger.warning(f"Rendezvous import failed: {e}")
            return False
        except Exception as e:
            logger.warning(f"Failed to start Rendezvous: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def stop(self) -> None:
        """Stop Rendezvous protocol."""
        # Cancel refresh task
        if self._refresh_cancel_scope:
            self._refresh_cancel_scope.cancel()
            self._refresh_cancel_scope = None

        # Unregister all namespaces
        for namespace in list(self._my_registrations.keys()):
            try:
                await self.unregister(namespace)
            except Exception:
                pass

        # Close discovery client
        if self._discovery:
            try:
                self._discovery.close()
            except Exception:
                pass
            self._discovery = None

        # Service doesn't need explicit stop
        self._service = None

        logger.info("Rendezvous protocol stopped")

    # ========== Registration Methods ==========

    async def register_subscriber(
        self,
        stream_id: str,
        ttl: int = None
    ) -> bool:
        """
        Register as a subscriber for a stream.

        Args:
            stream_id: Stream UUID
            ttl: Registration TTL in seconds

        Returns:
            True if registered successfully
        """
        namespace = f"{self.SUBSCRIBER_NS_PREFIX}{stream_id}"
        return await self.register(namespace, ttl)

    async def register_publisher(
        self,
        stream_id: str,
        ttl: int = None
    ) -> bool:
        """
        Register as a publisher for a stream.

        Args:
            stream_id: Stream UUID
            ttl: Registration TTL in seconds

        Returns:
            True if registered successfully
        """
        namespace = f"{self.PUBLISHER_NS_PREFIX}{stream_id}"
        return await self.register(namespace, ttl)

    async def register(
        self,
        namespace: str,
        ttl: int = None
    ) -> bool:
        """
        Register under a namespace.

        Args:
            namespace: Namespace to register under
            ttl: Registration TTL in seconds

        Returns:
            True if registered successfully
        """
        ttl = ttl or self.DEFAULT_TTL

        # Always track locally
        peer_id = str(self.host.get_id()) if self.host else "unknown"
        self._my_registrations[namespace] = RendezvousRegistration(
            peer_id=peer_id,
            namespace=namespace,
            ttl=ttl
        )

        if not self._discovery:
            logger.debug(f"Registered locally for {namespace}")
            return True

        try:
            # Use advertise() for RendezvousDiscovery
            actual_ttl = await self._discovery.advertise(namespace, ttl)
            logger.debug(
                f"Registered for namespace: {namespace} (TTL: {actual_ttl})"
            )
            return True

        except Exception as e:
            logger.warning(f"Failed to register for {namespace}: {e}")
            return False

    async def unregister(self, namespace: str) -> bool:
        """
        Unregister from a namespace.

        Args:
            namespace: Namespace to unregister from

        Returns:
            True if unregistered successfully
        """
        # Remove from local tracking
        self._my_registrations.pop(namespace, None)

        if not self._discovery:
            return True

        try:
            self._discovery.unregister(namespace)
            logger.debug(f"Unregistered from namespace: {namespace}")
            return True
        except Exception as e:
            logger.debug(f"Failed to unregister from {namespace}: {e}")
            return False

    async def unregister_subscriber(self, stream_id: str) -> bool:
        """Unregister as subscriber for a stream."""
        namespace = f"{self.SUBSCRIBER_NS_PREFIX}{stream_id}"
        return await self.unregister(namespace)

    async def unregister_publisher(self, stream_id: str) -> bool:
        """Unregister as publisher for a stream."""
        namespace = f"{self.PUBLISHER_NS_PREFIX}{stream_id}"
        return await self.unregister(namespace)

    # ========== Discovery Methods ==========

    async def discover_subscribers(
        self,
        stream_id: str,
        limit: int = 100
    ) -> List[str]:
        """
        Discover peers subscribed to a stream.

        Args:
            stream_id: Stream UUID
            limit: Maximum number of peers to return

        Returns:
            List of peer IDs
        """
        namespace = f"{self.SUBSCRIBER_NS_PREFIX}{stream_id}"
        return await self.discover(namespace, limit)

    async def discover_publishers(
        self,
        stream_id: str,
        limit: int = 100
    ) -> List[str]:
        """
        Discover peers publishing to a stream.

        Args:
            stream_id: Stream UUID
            limit: Maximum number of peers to return

        Returns:
            List of peer IDs
        """
        namespace = f"{self.PUBLISHER_NS_PREFIX}{stream_id}"
        return await self.discover(namespace, limit)

    async def discover(
        self,
        namespace: str,
        limit: int = 100,
        force_refresh: bool = False
    ) -> List[str]:
        """
        Discover peers registered under a namespace.

        Args:
            namespace: Namespace to query
            limit: Maximum number of peers to return
            force_refresh: If True, bypass cache

        Returns:
            List of peer IDs
        """
        # Check cache first (unless forced refresh)
        if not force_refresh and namespace in self._namespace_peers:
            cached = list(self._namespace_peers[namespace])
            if cached:
                return cached[:limit]

        if not self._discovery:
            return []

        try:
            # find_peers returns an async iterator
            peer_ids = []
            async for peer_info in self._discovery.find_peers(
                namespace,
                limit=limit,
                force_refresh=force_refresh
            ):
                peer_ids.append(str(peer_info.peer_id))
                if len(peer_ids) >= limit:
                    break

            # Update cache
            self._namespace_peers[namespace] = set(peer_ids)

            logger.debug(f"Discovered {len(peer_ids)} peers for {namespace}")
            return peer_ids

        except Exception as e:
            logger.warning(f"Discovery failed for {namespace}: {e}")
            return []

    async def find_all_peers(self, namespace: str) -> List[str]:
        """
        Find all peers for a namespace (exhaustive search).

        Args:
            namespace: Namespace to query

        Returns:
            List of all peer IDs
        """
        if not self._discovery:
            return []

        try:
            peer_ids = []
            async for peer_info in self._discovery.find_all_peers(namespace):
                peer_ids.append(str(peer_info.peer_id))

            self._namespace_peers[namespace] = set(peer_ids)
            return peer_ids

        except Exception as e:
            logger.warning(f"find_all_peers failed for {namespace}: {e}")
            return []

    # ========== Cache Methods ==========

    def get_cached_peers(self, namespace: str) -> List[str]:
        """Get cached peers for a namespace (no network query)."""
        return list(self._namespace_peers.get(namespace, set()))

    def get_cached_subscribers(self, stream_id: str) -> List[str]:
        """Get cached subscribers for a stream."""
        namespace = f"{self.SUBSCRIBER_NS_PREFIX}{stream_id}"
        return self.get_cached_peers(namespace)

    def get_cached_publishers(self, stream_id: str) -> List[str]:
        """Get cached publishers for a stream."""
        namespace = f"{self.PUBLISHER_NS_PREFIX}{stream_id}"
        return self.get_cached_peers(namespace)

    def get_my_registrations(self) -> List[str]:
        """Get namespaces we're registered under."""
        return list(self._my_registrations.keys())

    # ========== Background Tasks ==========

    async def run_refresh_task(self, task_status=trio.TASK_STATUS_IGNORED) -> None:
        """
        Run refresh task in a nursery.

        Usage:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(rendezvous.run_refresh_task)
        """
        self._refresh_cancel_scope = trio.CancelScope()
        task_status.started()

        with self._refresh_cancel_scope:
            await self._refresh_loop()

    async def _refresh_loop(self) -> None:
        """Background task to refresh registrations before they expire."""
        try:
            while True:
                await trio.sleep(60)  # Check every minute

                for namespace, reg in list(self._my_registrations.items()):
                    # Refresh if less than 10% of TTL remaining
                    if reg.time_remaining() < reg.ttl * 0.1:
                        logger.debug(f"Refreshing registration: {namespace}")
                        await self.register(namespace, reg.ttl)

        except trio.Cancelled:
            pass
        except Exception as e:
            logger.warning(f"Refresh loop error: {e}")

    # ========== Statistics ==========

    def get_stats(self) -> Dict[str, Any]:
        """Get Rendezvous manager statistics."""
        return {
            "active_registrations": len(self._my_registrations),
            "cached_namespaces": len(self._namespace_peers),
            "total_cached_peers": sum(
                len(peers) for peers in self._namespace_peers.values()
            ),
            "is_server": self._is_server,
            "has_service": self._service is not None,
            "has_discovery": self._discovery is not None,
            "rendezvous_peer": self._rendezvous_peer_id,
        }

    def clear_cache(self) -> None:
        """Clear discovery cache."""
        self._namespace_peers.clear()
        if self._discovery:
            self._discovery.clear_cache()
