"""
satorip2p/protocol/peer_registry.py

Decentralized peer registration via DHT/Rendezvous.

Peers announce themselves to the network with signed announcements,
allowing other peers to discover and verify them without a central server.

Works alongside central server in hybrid mode:
- Central mode: Not used (central server handles registration)
- Hybrid mode: Announces to P2P AND central server (both)
- P2P mode: Only P2P announcements

Usage:
    from satorip2p.protocol.peer_registry import PeerRegistry

    registry = PeerRegistry(peers)
    await registry.start()

    # Announce our presence
    await registry.announce(capabilities=["predictor"])

    # Look up a peer by Evrmore address
    peer = await registry.lookup("EX1abc...")

    # Get all known peers
    all_peers = await registry.get_all_peers()
"""

import logging
import time
import json
import trio
from typing import Dict, List, Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass, field, asdict

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.peer_registry")


@dataclass
class PeerAnnouncement:
    """
    Signed announcement of peer presence on the network.

    Contains all information needed for other peers to:
    1. Connect to this peer (peer_id, multiaddrs)
    2. Verify identity (evrmore_address, signature)
    3. Understand capabilities (capabilities list)
    """
    peer_id: str                    # libp2p peer ID
    evrmore_address: str            # Evrmore wallet address (identity)
    multiaddrs: List[str]           # How to reach this peer
    capabilities: List[str]         # ["predictor", "oracle", "relay"]
    timestamp: int                  # Unix timestamp of announcement
    signature: str = ""             # Signed by Evrmore wallet
    ttl: int = 3600                 # Time-to-live in seconds (1 hour default)

    def is_expired(self) -> bool:
        """Check if announcement has expired."""
        return time.time() - self.timestamp > self.ttl

    def time_remaining(self) -> float:
        """Get seconds until expiration."""
        return max(0, self.ttl - (time.time() - self.timestamp))

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PeerAnnouncement":
        """Create from dictionary."""
        return cls(**data)

    def get_signing_message(self) -> str:
        """Get the message that should be signed."""
        return f"{self.peer_id}:{self.evrmore_address}:{self.timestamp}"


class PeerRegistry:
    """
    Decentralized peer registry using Rendezvous protocol.

    Allows peers to announce themselves and discover other peers
    without relying on a central server.

    Architecture:
    - Uses Rendezvous namespaces for peer announcements
    - Announcements are signed with Evrmore wallet
    - Other peers can verify signatures to confirm identity
    - TTL ensures stale entries expire
    - Periodic refresh keeps announcements alive
    """

    # Namespace prefix for peer announcements
    PEER_NS_PREFIX = "satori/peers/"

    # GossipSub topic for real-time peer announcements
    ANNOUNCE_TOPIC = "satori/peer-announcements"

    # Default announcement TTL (1 hour)
    DEFAULT_TTL = 3600

    # Refresh interval (refresh when 20% TTL remaining)
    REFRESH_THRESHOLD = 0.2

    def __init__(self, peers: "Peers"):
        """
        Initialize PeerRegistry.

        Args:
            peers: Peers instance for P2P operations
        """
        self.peers = peers
        self._my_announcement: Optional[PeerAnnouncement] = None
        self._known_peers: Dict[str, PeerAnnouncement] = {}  # evrmore_address -> announcement
        self._announcement_callbacks: List[Callable] = []
        self._refresh_task_scope: Optional[trio.CancelScope] = None
        self._started = False

    @property
    def evrmore_address(self) -> str:
        """Get our Evrmore address."""
        if self.peers._identity_bridge:
            return self.peers._identity_bridge.evrmore_address
        return ""

    @property
    def peer_id(self) -> str:
        """Get our peer ID."""
        return self.peers.peer_id or ""

    async def start(self, nursery: Optional[trio.Nursery] = None) -> bool:
        """
        Start the peer registry.

        Subscribes to peer announcement topic and starts refresh task.

        Args:
            nursery: Optional trio nursery for background tasks

        Returns:
            True if started successfully
        """
        if self._started:
            return True

        try:
            # Subscribe to peer announcements via GossipSub with full network registration
            if self.peers._pubsub:
                await self.peers.subscribe_async(
                    self.ANNOUNCE_TOPIC,
                    self._on_announcement_received
                )
                logger.debug(f"Subscribed to {self.ANNOUNCE_TOPIC}")

            self._started = True
            logger.info("PeerRegistry started")
            return True

        except Exception as e:
            logger.error(f"Failed to start PeerRegistry: {e}")
            return False

    async def stop(self) -> None:
        """Stop the peer registry."""
        if self._refresh_task_scope:
            self._refresh_task_scope.cancel()
            self._refresh_task_scope = None

        # Unsubscribe from announcements
        if self.peers._pubsub:
            try:
                await self.peers.unsubscribe(self.ANNOUNCE_TOPIC)
            except Exception:
                pass

        self._started = False
        logger.info("PeerRegistry stopped")

    # ========== Announcement Methods ==========

    async def announce(
        self,
        capabilities: Optional[List[str]] = None,
        ttl: int = None
    ) -> Optional[PeerAnnouncement]:
        """
        Announce our presence to the network.

        Creates a signed announcement and publishes it via:
        1. Rendezvous registration (for discovery)
        2. GossipSub broadcast (for real-time notification)

        Args:
            capabilities: List of capabilities (e.g., ["predictor", "oracle"])
            ttl: Announcement TTL in seconds

        Returns:
            The announcement, or None if failed
        """
        if not self.peer_id or not self.evrmore_address:
            logger.warning("Cannot announce: missing peer_id or evrmore_address")
            return None

        ttl = ttl or self.DEFAULT_TTL
        capabilities = capabilities or ["predictor"]

        # Get our listening addresses
        multiaddrs = []
        if hasattr(self.peers, 'get_listening_addresses'):
            multiaddrs = self.peers.get_listening_addresses() or []

        # Create announcement
        announcement = PeerAnnouncement(
            peer_id=self.peer_id,
            evrmore_address=self.evrmore_address,
            multiaddrs=multiaddrs,
            capabilities=capabilities,
            timestamp=int(time.time()),
            ttl=ttl,
        )

        # Sign with Evrmore wallet
        try:
            message = announcement.get_signing_message()
            if self.peers._identity_bridge:
                signature = self.peers._identity_bridge.sign(message.encode())
                announcement.signature = signature if isinstance(signature, str) else signature.decode()
        except Exception as e:
            logger.warning(f"Failed to sign announcement: {e}")
            # Continue without signature in development
            announcement.signature = "unsigned"

        # Store our announcement
        self._my_announcement = announcement

        # Register via Rendezvous (for discovery)
        await self._register_rendezvous(announcement)

        # Broadcast via GossipSub (for real-time notification)
        await self._broadcast_announcement(announcement)

        logger.info(
            f"Announced: evrmore_address={self.evrmore_address} "
            f"capabilities={capabilities}"
        )

        return announcement

    async def _register_rendezvous(self, announcement: PeerAnnouncement) -> bool:
        """Register announcement via Rendezvous protocol."""
        if not self.peers._rendezvous:
            return False

        try:
            namespace = f"{self.PEER_NS_PREFIX}{announcement.evrmore_address}"
            return await self.peers._rendezvous.register(namespace, announcement.ttl)
        except Exception as e:
            logger.debug(f"Rendezvous registration failed: {e}")
            return False

    async def _broadcast_announcement(self, announcement: PeerAnnouncement) -> bool:
        """Broadcast announcement via GossipSub."""
        try:
            await self.peers.broadcast(
                self.ANNOUNCE_TOPIC,
                announcement.to_dict()
            )
            return True
        except Exception as e:
            logger.debug(f"Announcement broadcast failed: {e}")
            return False

    async def _on_announcement_received(self, stream_id: str, data: dict) -> None:
        """Handle received peer announcement."""
        try:
            announcement = PeerAnnouncement.from_dict(data)

            # Don't process our own announcements
            if announcement.evrmore_address == self.evrmore_address:
                return

            # Verify signature
            if not await self.verify_announcement(announcement):
                logger.debug(
                    f"Invalid signature from evrmore_address={announcement.evrmore_address}"
                )
                return

            # Check if expired
            if announcement.is_expired():
                logger.debug(
                    f"Expired announcement from evrmore_address={announcement.evrmore_address}"
                )
                return

            # Store/update
            self._known_peers[announcement.evrmore_address] = announcement

            logger.debug(
                f"Received announcement from evrmore_address={announcement.evrmore_address} "
                f"peer_id={announcement.peer_id}"
            )

            # Notify callbacks
            for callback in self._announcement_callbacks:
                try:
                    if trio.lowlevel.current_trio_token():
                        callback(announcement)
                except Exception:
                    callback(announcement)

        except Exception as e:
            logger.debug(f"Failed to process announcement: {e}")

    # ========== Lookup Methods ==========

    async def lookup(self, evrmore_address: str) -> Optional[PeerAnnouncement]:
        """
        Look up a peer by their Evrmore address.

        Checks local cache first, then queries Rendezvous.

        Args:
            evrmore_address: Evrmore wallet address

        Returns:
            PeerAnnouncement if found, None otherwise
        """
        # Check cache first
        if evrmore_address in self._known_peers:
            announcement = self._known_peers[evrmore_address]
            if not announcement.is_expired():
                return announcement

        # Query Rendezvous
        if self.peers._rendezvous:
            try:
                namespace = f"{self.PEER_NS_PREFIX}{evrmore_address}"
                peer_ids = await self.peers._rendezvous.discover(namespace, limit=1)
                if peer_ids:
                    # We found the peer, but need to get their announcement
                    # For now, return what we have from cache or None
                    pass
            except Exception as e:
                logger.debug(f"Rendezvous lookup failed: {e}")

        return self._known_peers.get(evrmore_address)

    async def get_all_peers(self) -> List[PeerAnnouncement]:
        """
        Get all known non-expired peer announcements.

        Returns:
            List of valid PeerAnnouncements
        """
        valid_peers = []
        expired_addrs = []

        for addr, announcement in self._known_peers.items():
            if announcement.is_expired():
                expired_addrs.append(addr)
            else:
                valid_peers.append(announcement)

        # Clean up expired entries
        for addr in expired_addrs:
            del self._known_peers[addr]

        return valid_peers

    async def get_peers_by_capability(
        self,
        capability: str
    ) -> List[PeerAnnouncement]:
        """
        Get peers with a specific capability.

        Args:
            capability: Capability to filter by (e.g., "oracle", "relay")

        Returns:
            List of peers with the capability
        """
        all_peers = await self.get_all_peers()
        return [p for p in all_peers if capability in p.capabilities]

    def get_cached_peer(self, evrmore_address: str) -> Optional[PeerAnnouncement]:
        """Get peer from local cache without network query."""
        announcement = self._known_peers.get(evrmore_address)
        if announcement and not announcement.is_expired():
            return announcement
        return None

    def forget_peer(self, peer_id: str = None, evrmore_address: str = None) -> bool:
        """
        Remove a peer from the known peers list.

        Args:
            peer_id: libp2p peer ID to forget
            evrmore_address: Evrmore address to forget

        Returns:
            True if peer was found and removed, False otherwise
        """
        if evrmore_address and evrmore_address in self._known_peers:
            del self._known_peers[evrmore_address]
            logger.info(f"Forgot peer with evrmore_address: {evrmore_address}")
            return True

        if peer_id:
            # Find peer by peer_id
            for addr, announcement in list(self._known_peers.items()):
                if announcement.peer_id == peer_id:
                    del self._known_peers[addr]
                    logger.info(f"Forgot peer with peer_id: {peer_id}")
                    return True

        return False

    # ========== Verification Methods ==========

    async def verify_announcement(self, announcement: PeerAnnouncement) -> bool:
        """
        Verify an announcement's signature.

        Args:
            announcement: Announcement to verify

        Returns:
            True if signature is valid
        """
        # Allow unsigned in development
        if announcement.signature == "unsigned":
            logger.debug("Accepting unsigned announcement (development mode)")
            return True

        if not announcement.signature:
            return False

        try:
            message = announcement.get_signing_message()
            if self.peers._identity_bridge:
                return self.peers._identity_bridge.verify(
                    message.encode(),
                    announcement.signature.encode() if isinstance(announcement.signature, str) else announcement.signature,
                    announcement.evrmore_address
                )
        except Exception as e:
            logger.debug(f"Signature verification failed: {e}")

        return False

    # ========== Callback Methods ==========

    def on_peer_announced(self, callback: Callable[[PeerAnnouncement], None]) -> None:
        """
        Register callback for new peer announcements.

        Args:
            callback: Function called with PeerAnnouncement when peer announces
        """
        self._announcement_callbacks.append(callback)

    def remove_callback(self, callback: Callable) -> None:
        """Remove a registered callback."""
        if callback in self._announcement_callbacks:
            self._announcement_callbacks.remove(callback)

    # ========== Refresh Methods ==========

    async def start_auto_refresh(
        self,
        nursery: trio.Nursery,
        interval: int = 300
    ) -> None:
        """
        Start automatic announcement refresh.

        Args:
            nursery: Trio nursery for background task
            interval: Check interval in seconds (default 5 minutes)
        """
        nursery.start_soon(self._refresh_loop, interval)

    async def _refresh_loop(self, interval: int) -> None:
        """Background task to refresh our announcement before expiration."""
        self._refresh_task_scope = trio.CancelScope()

        with self._refresh_task_scope:
            while True:
                await trio.sleep(interval)

                if self._my_announcement:
                    remaining = self._my_announcement.time_remaining()
                    threshold = self._my_announcement.ttl * self.REFRESH_THRESHOLD

                    if remaining < threshold:
                        logger.debug("Refreshing peer announcement")
                        await self.announce(
                            capabilities=self._my_announcement.capabilities,
                            ttl=self._my_announcement.ttl
                        )

    # ========== Statistics ==========

    def get_stats(self) -> dict:
        """Get registry statistics."""
        return {
            "known_peers": len(self._known_peers),
            "my_announcement": self._my_announcement is not None,
            "callbacks_registered": len(self._announcement_callbacks),
            "started": self._started,
        }
