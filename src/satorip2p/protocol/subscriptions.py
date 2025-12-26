"""
satorip2p/protocol/subscriptions.py

Manages stream subscriptions using DHT for distributed storage.
"""

import logging
import time
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass, field

logger = logging.getLogger("satorip2p.protocol.subscriptions")


@dataclass
class SubscriptionRecord:
    """Record of a subscription in the DHT."""
    peer_id: str
    stream_id: str
    evrmore_address: str = ""
    timestamp: float = field(default_factory=time.time)
    is_publisher: bool = False

    def is_stale(self, max_age_seconds: float = 3600) -> bool:
        """Check if record is stale."""
        return time.time() - self.timestamp > max_age_seconds


class SubscriptionManager:
    """
    Manages stream subscriptions using Kademlia DHT.

    Subscriptions are stored as DHT records with:
    - Key: "/satori/sub/{stream_id}" -> list of subscriber peer IDs
    - Key: "/satori/pub/{stream_id}" -> list of publisher peer IDs
    - Key: "/satori/peer/{peer_id}/subs" -> list of stream IDs

    The DHT is used for global discovery, while local caches
    provide fast lookups for connected peers.

    Usage:
        manager = SubscriptionManager(dht)

        # Announce our subscription
        await manager.announce_subscription(my_peer_id, stream_id)

        # Find subscribers for a stream
        subscribers = await manager.find_subscribers(stream_id)

        # Get local cache
        local_subs = manager.get_subscribers(stream_id)
    """

    # DHT key prefixes
    SUB_KEY_PREFIX = "/satori/sub/"
    PUB_KEY_PREFIX = "/satori/pub/"
    PEER_KEY_PREFIX = "/satori/peer/"

    def __init__(self, dht=None):
        """
        Initialize subscription manager.

        Args:
            dht: py-libp2p DHT instance (optional, can set later)
        """
        self.dht = dht

        # Local caches
        self._subscribers: Dict[str, Set[str]] = {}      # stream_id -> peer_ids
        self._publishers: Dict[str, Set[str]] = {}       # stream_id -> peer_ids
        self._peer_subs: Dict[str, Set[str]] = {}        # peer_id -> stream_ids
        self._peer_pubs: Dict[str, Set[str]] = {}        # peer_id -> stream_ids
        self._peer_addresses: Dict[str, str] = {}        # peer_id -> evrmore_address
        self._records: Dict[str, SubscriptionRecord] = {}  # For detailed info

    def set_dht(self, dht) -> None:
        """Set or update the DHT instance."""
        self.dht = dht

    # ========== Announce Methods ==========

    async def announce_subscription(
        self,
        peer_id: str,
        stream_id: str,
        evrmore_address: str = ""
    ) -> bool:
        """
        Announce subscription to the DHT and update local cache.

        Args:
            peer_id: Our peer ID
            stream_id: Stream we're subscribing to
            evrmore_address: Our Evrmore address

        Returns:
            True if successfully announced
        """
        # Update local cache
        if stream_id not in self._subscribers:
            self._subscribers[stream_id] = set()
        self._subscribers[stream_id].add(peer_id)

        if peer_id not in self._peer_subs:
            self._peer_subs[peer_id] = set()
        self._peer_subs[peer_id].add(stream_id)

        if evrmore_address:
            self._peer_addresses[peer_id] = evrmore_address

        # Store record
        record = SubscriptionRecord(
            peer_id=peer_id,
            stream_id=stream_id,
            evrmore_address=evrmore_address,
            is_publisher=False
        )
        self._records[f"{stream_id}:{peer_id}"] = record

        # Announce to DHT
        if self.dht:
            try:
                # Ensure stream_id is a string - libp2p DHT expects string keys
                stream_id_str = stream_id if isinstance(stream_id, str) else str(stream_id)
                key = f"{self.SUB_KEY_PREFIX}{stream_id_str}"
                await self.dht.provide(key)
                logger.debug(f"Announced subscription: peer_id={peer_id} -> stream_id={stream_id_str}")
                return True
            except Exception as e:
                logger.warning(f"Failed to announce subscription to DHT: {e}")
                return False

        return True

    async def announce_publication(
        self,
        peer_id: str,
        stream_id: str,
        evrmore_address: str = ""
    ) -> bool:
        """
        Announce publication to the DHT.

        Args:
            peer_id: Our peer ID
            stream_id: Stream we're publishing
            evrmore_address: Our Evrmore address

        Returns:
            True if successfully announced
        """
        # Update local cache
        if stream_id not in self._publishers:
            self._publishers[stream_id] = set()
        self._publishers[stream_id].add(peer_id)

        if peer_id not in self._peer_pubs:
            self._peer_pubs[peer_id] = set()
        self._peer_pubs[peer_id].add(stream_id)

        if evrmore_address:
            self._peer_addresses[peer_id] = evrmore_address

        # Store record
        record = SubscriptionRecord(
            peer_id=peer_id,
            stream_id=stream_id,
            evrmore_address=evrmore_address,
            is_publisher=True
        )
        self._records[f"pub:{stream_id}:{peer_id}"] = record

        # Announce to DHT
        if self.dht:
            try:
                key = f"{self.PUB_KEY_PREFIX}{stream_id}"
                await self.dht.provide(key.encode())
                logger.debug(f"Announced publication: peer_id={peer_id} -> stream_id={stream_id}")
                return True
            except Exception as e:
                logger.warning(f"Failed to announce publication to DHT: {e}")
                return False

        return True

    async def remove_subscription(
        self,
        peer_id: str,
        stream_id: str
    ) -> None:
        """Remove a subscription from local cache."""
        if stream_id in self._subscribers:
            self._subscribers[stream_id].discard(peer_id)
        if peer_id in self._peer_subs:
            self._peer_subs[peer_id].discard(stream_id)
        self._records.pop(f"{stream_id}:{peer_id}", None)

    async def remove_publication(
        self,
        peer_id: str,
        stream_id: str
    ) -> None:
        """Remove a publication from local cache."""
        if stream_id in self._publishers:
            self._publishers[stream_id].discard(peer_id)
        if peer_id in self._peer_pubs:
            self._peer_pubs[peer_id].discard(stream_id)
        self._records.pop(f"pub:{stream_id}:{peer_id}", None)

    # ========== Query Methods ==========

    async def find_subscribers(self, stream_id: str) -> List[str]:
        """
        Find all subscribers for a stream via DHT.

        First checks local cache, then queries DHT.

        Args:
            stream_id: Stream UUID

        Returns:
            List of peer IDs
        """
        # Check local cache first
        if stream_id in self._subscribers:
            cached = list(self._subscribers[stream_id])
            if cached:
                return cached

        # Query DHT
        if self.dht:
            try:
                # libp2p DHT expects string keys
                stream_id_str = stream_id if isinstance(stream_id, str) else str(stream_id)
                key = f"{self.SUB_KEY_PREFIX}{stream_id_str}"
                providers = await self.dht.find_providers(key)
                peer_ids = [str(p) for p in providers]

                # Update cache
                self._subscribers[stream_id] = set(peer_ids)
                logger.debug(f"Found {len(peer_ids)} subscribers for stream_id={stream_id_str}")
                return peer_ids
            except Exception as e:
                logger.warning(f"DHT query failed: {e}")

        return []

    async def find_publishers(self, stream_id: str) -> List[str]:
        """
        Find all publishers for a stream via DHT.

        Args:
            stream_id: Stream UUID

        Returns:
            List of peer IDs
        """
        # Check local cache
        if stream_id in self._publishers:
            cached = list(self._publishers[stream_id])
            if cached:
                return cached

        # Query DHT
        if self.dht:
            try:
                key = f"{self.PUB_KEY_PREFIX}{stream_id}"
                providers = await self.dht.find_providers(key.encode())
                peer_ids = [str(p) for p in providers]
                self._publishers[stream_id] = set(peer_ids)
                return peer_ids
            except Exception as e:
                logger.warning(f"DHT query failed: {e}")

        return []

    # ========== Local Cache Methods ==========

    def get_subscribers(self, stream_id: str) -> List[str]:
        """Get cached subscribers for stream (no DHT query)."""
        return list(self._subscribers.get(stream_id, set()))

    def get_publishers(self, stream_id: str) -> List[str]:
        """Get cached publishers for stream (no DHT query)."""
        return list(self._publishers.get(stream_id, set()))

    def get_peer_subscriptions(self, peer_id: str) -> List[str]:
        """Get streams a peer is subscribed to."""
        return list(self._peer_subs.get(peer_id, set()))

    def get_peer_publications(self, peer_id: str) -> List[str]:
        """Get streams a peer publishes."""
        return list(self._peer_pubs.get(peer_id, set()))

    def get_peer_evrmore_address(self, peer_id: str) -> Optional[str]:
        """Get Evrmore address for a peer ID."""
        return self._peer_addresses.get(peer_id)

    def add_subscriber(self, stream_id: str, peer_id: str, evrmore_address: str = "") -> None:
        """Add subscriber to local cache (from received announcement)."""
        if stream_id not in self._subscribers:
            self._subscribers[stream_id] = set()
        self._subscribers[stream_id].add(peer_id)

        if peer_id not in self._peer_subs:
            self._peer_subs[peer_id] = set()
        self._peer_subs[peer_id].add(stream_id)

        if evrmore_address:
            self._peer_addresses[peer_id] = evrmore_address

    def add_publisher(self, stream_id: str, peer_id: str, evrmore_address: str = "") -> None:
        """Add publisher to local cache (from received announcement)."""
        if stream_id not in self._publishers:
            self._publishers[stream_id] = set()
        self._publishers[stream_id].add(peer_id)

        if peer_id not in self._peer_pubs:
            self._peer_pubs[peer_id] = set()
        self._peer_pubs[peer_id].add(stream_id)

        if evrmore_address:
            self._peer_addresses[peer_id] = evrmore_address

    def remove_peer(self, peer_id: str) -> None:
        """Remove all records for a peer (e.g., on disconnect)."""
        # Remove from subscriber sets
        for stream_id in list(self._peer_subs.get(peer_id, [])):
            if stream_id in self._subscribers:
                self._subscribers[stream_id].discard(peer_id)

        # Remove from publisher sets
        for stream_id in list(self._peer_pubs.get(peer_id, [])):
            if stream_id in self._publishers:
                self._publishers[stream_id].discard(peer_id)

        # Remove peer entries
        self._peer_subs.pop(peer_id, None)
        self._peer_pubs.pop(peer_id, None)
        self._peer_addresses.pop(peer_id, None)

    # ========== Utility Methods ==========

    def get_all_subscriptions(self) -> Dict[str, List[str]]:
        """Get all stream -> subscribers mappings."""
        return {
            stream_id: list(peers)
            for stream_id, peers in self._subscribers.items()
        }

    def get_all_publications(self) -> Dict[str, List[str]]:
        """Get all stream -> publishers mappings."""
        return {
            stream_id: list(peers)
            for stream_id, peers in self._publishers.items()
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get subscription manager statistics."""
        return {
            "total_streams": len(self._subscribers) + len(self._publishers),
            "total_subscriptions": sum(len(s) for s in self._subscribers.values()),
            "total_publications": sum(len(p) for p in self._publishers.values()),
            "known_peers": len(self._peer_addresses),
            "has_dht": self.dht is not None,
        }

    def clear(self) -> None:
        """Clear all local caches."""
        self._subscribers.clear()
        self._publishers.clear()
        self._peer_subs.clear()
        self._peer_pubs.clear()
        self._peer_addresses.clear()
        self._records.clear()
