"""
satorip2p/protocol/archiver.py

Archiver protocol for historical data preservation and retrieval.

Signers act as archivers who store and serve historical prediction data,
oracle observations, and network state. This ensures data integrity and
availability across the network.

Key Features:
- Historical data storage (predictions, observations, rewards)
- Data integrity verification (merkle proofs)
- Archive request/response protocol
- Data redundancy across archiver nodes
- Periodic archive announcements

Archiver Responsibilities:
1. Store historical prediction rounds
2. Store oracle observation history
3. Store reward distribution records
4. Serve archive requests from peers
5. Verify data integrity via merkle proofs
6. Announce available archives to network

Usage:
    from satorip2p.protocol.archiver import ArchiverProtocol

    archiver = ArchiverProtocol(peers, storage_path="/data/archives")
    await archiver.start()

    # Store a round's data (archivers only)
    await archiver.archive_round(round_id, predictions, observations, rewards)

    # Request archived data from network
    data = await archiver.request_archive(round_id)

    # Get archive availability
    available = archiver.get_available_rounds()
"""

import logging
import time
import json
import hashlib
import os
from typing import Dict, List, Optional, Set, Callable, TYPE_CHECKING, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.archiver")


# ============================================================================
# CONSTANTS
# ============================================================================

# Archive configuration
ARCHIVER_COUNT = 5                    # Same as signers
MIN_ARCHIVE_REDUNDANCY = 3            # Minimum copies for data safety
ARCHIVE_ANNOUNCEMENT_INTERVAL = 3600  # Announce archives hourly

# Storage limits (per archiver)
MAX_ROUNDS_STORED = 365 * 2           # Keep 2 years of daily rounds
MAX_ARCHIVE_SIZE_MB = 10000           # 10 GB max archive size
COMPRESSION_ENABLED = True            # Compress archived data

# Archive request timeouts
ARCHIVE_REQUEST_TIMEOUT = 30          # 30 seconds to respond
ARCHIVE_CHUNK_SIZE = 1024 * 1024      # 1 MB chunks for large transfers

# Topics
ARCHIVE_ANNOUNCE_TOPIC = "satori/archiver/announce"
ARCHIVE_REQUEST_TOPIC = "satori/archiver/request"
ARCHIVE_RESPONSE_TOPIC = "satori/archiver/response"
ARCHIVE_SYNC_TOPIC = "satori/archiver/sync"


# ============================================================================
# ENUMS
# ============================================================================

class ArchiveType(Enum):
    """Types of archived data."""
    PREDICTIONS = "predictions"       # Prediction round data
    OBSERVATIONS = "observations"     # Oracle observation data
    REWARDS = "rewards"              # Reward distribution records
    CONSENSUS = "consensus"          # Consensus results
    FULL_ROUND = "full_round"        # Complete round data (all above)


class ArchiveStatus(Enum):
    """Status of an archive."""
    PENDING = "pending"              # Being archived
    COMPLETE = "complete"            # Fully archived
    PARTIAL = "partial"              # Partially available
    CORRUPTED = "corrupted"          # Data integrity check failed
    MISSING = "missing"              # Not available


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class ArchiveMetadata:
    """Metadata about an archived round."""
    round_id: str
    archive_type: ArchiveType
    merkle_root: str                 # Root hash for integrity verification
    size_bytes: int
    record_count: int                # Number of records in archive
    timestamp: int                   # When archived
    archiver_address: str            # Who archived this
    signature: str = ""              # Signed by archiver
    chunks: int = 1                  # Number of chunks (for large archives)

    def to_dict(self) -> dict:
        return {
            **asdict(self),
            "archive_type": self.archive_type.value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ArchiveMetadata":
        data = dict(data)
        data["archive_type"] = ArchiveType(data["archive_type"])
        return cls(**data)

    def get_signing_message(self) -> str:
        """Get message for signing."""
        return f"archive:{self.round_id}:{self.archive_type.value}:{self.merkle_root}:{self.timestamp}"


@dataclass
class ArchiveAnnouncement:
    """Announcement of available archives from an archiver."""
    archiver_address: str
    peer_id: str
    available_rounds: List[str]      # List of round_ids
    oldest_round: str
    newest_round: str
    total_size_mb: float
    timestamp: int
    signature: str = ""
    integrity: float = 100.0         # Data integrity percentage (0-100)
    valid_archives: int = 0          # Number of verified valid archives
    total_archives: int = 0          # Total number of archives

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ArchiveAnnouncement":
        # Handle older announcements without integrity fields
        data = dict(data)
        data.setdefault('integrity', 100.0)
        data.setdefault('valid_archives', 0)
        data.setdefault('total_archives', len(data.get('available_rounds', [])))
        return cls(**data)


@dataclass
class ArchiveRequest:
    """Request for archived data."""
    requester_id: str
    round_id: str
    archive_type: ArchiveType
    chunk_index: int = 0             # For multi-chunk retrieval
    timestamp: int = 0

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = int(time.time())

    def to_dict(self) -> dict:
        return {
            **asdict(self),
            "archive_type": self.archive_type.value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ArchiveRequest":
        data = dict(data)
        data["archive_type"] = ArchiveType(data["archive_type"])
        return cls(**data)


@dataclass
class ArchiveResponse:
    """Response with archived data."""
    round_id: str
    archive_type: ArchiveType
    status: ArchiveStatus
    data: Optional[bytes] = None     # The actual archived data
    metadata: Optional[ArchiveMetadata] = None
    chunk_index: int = 0
    total_chunks: int = 1
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "round_id": self.round_id,
            "archive_type": self.archive_type.value,
            "status": self.status.value,
            "data": self.data.hex() if self.data else None,
            "metadata": self.metadata.to_dict() if self.metadata else None,
            "chunk_index": self.chunk_index,
            "total_chunks": self.total_chunks,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ArchiveResponse":
        return cls(
            round_id=data["round_id"],
            archive_type=ArchiveType(data["archive_type"]),
            status=ArchiveStatus(data["status"]),
            data=bytes.fromhex(data["data"]) if data.get("data") else None,
            metadata=ArchiveMetadata.from_dict(data["metadata"]) if data.get("metadata") else None,
            chunk_index=data.get("chunk_index", 0),
            total_chunks=data.get("total_chunks", 1),
            error=data.get("error", ""),
        )


@dataclass
class ArchivedRound:
    """Complete archived round data."""
    round_id: str
    predictions: List[Dict[str, Any]] = field(default_factory=list)
    observations: List[Dict[str, Any]] = field(default_factory=list)
    rewards: List[Dict[str, Any]] = field(default_factory=list)
    consensus: Optional[Dict[str, Any]] = None
    merkle_root: str = ""
    archived_at: int = 0
    archiver: str = ""

    def __post_init__(self):
        if self.archived_at == 0:
            self.archived_at = int(time.time())
        if not self.merkle_root:
            self.merkle_root = self.compute_merkle_root()

    def compute_merkle_root(self) -> str:
        """Compute merkle root of all data."""
        # Simple hash-based merkle root
        items = []
        for p in self.predictions:
            items.append(json.dumps(p, sort_keys=True))
        for o in self.observations:
            items.append(json.dumps(o, sort_keys=True))
        for r in self.rewards:
            items.append(json.dumps(r, sort_keys=True))
        if self.consensus:
            items.append(json.dumps(self.consensus, sort_keys=True))

        if not items:
            return hashlib.sha256(b"empty").hexdigest()

        # Build merkle tree
        hashes = [hashlib.sha256(item.encode()).digest() for item in items]
        while len(hashes) > 1:
            if len(hashes) % 2 == 1:
                hashes.append(hashes[-1])  # Duplicate last for odd count
            new_hashes = []
            for i in range(0, len(hashes), 2):
                combined = hashes[i] + hashes[i + 1]
                new_hashes.append(hashlib.sha256(combined).digest())
            hashes = new_hashes

        return hashes[0].hex()

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ArchivedRound":
        return cls(**data)

    def to_bytes(self) -> bytes:
        """Serialize to bytes for storage/transfer."""
        return json.dumps(self.to_dict()).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "ArchivedRound":
        """Deserialize from bytes."""
        return cls.from_dict(json.loads(data.decode()))


# ============================================================================
# ARCHIVER PROTOCOL
# ============================================================================

class ArchiverProtocol:
    """
    Archiver protocol for historical data storage and retrieval.

    Archivers (signers) store and serve historical network data.
    Data is stored with merkle proofs for integrity verification.
    """

    def __init__(self, peers: "Peers", storage_path: Optional[str] = None):
        """
        Initialize ArchiverProtocol.

        Args:
            peers: Peers instance for network access
            storage_path: Path to store archives (default: ~/.satori/archives)
        """
        self._peers = peers
        self._storage_path = Path(storage_path or os.path.expanduser("~/.satori/archives"))
        self._storage_path.mkdir(parents=True, exist_ok=True)

        # In-memory index of available archives
        self._local_archives: Dict[str, ArchiveMetadata] = {}
        self._network_archives: Dict[str, Dict[str, ArchiveAnnouncement]] = {}  # round_id -> {archiver -> announcement}

        # Track archiver integrity scores from announcements
        self._archiver_integrity: Dict[str, ArchiveAnnouncement] = {}  # archiver_address -> latest announcement

        # Pending requests
        self._pending_requests: Dict[str, Any] = {}

        self._started = False
        self._announcement_task = None

    @property
    def is_archiver(self) -> bool:
        """Check if we are an archiver (signer)."""
        from .signer import is_authorized_signer
        return is_authorized_signer(self._peers.evrmore_address or "")

    async def start(self) -> None:
        """Start the archiver protocol."""
        if self._started:
            return

        # Load local archive index
        self._load_local_index()

        # Subscribe to archiver topics
        await self._peers.subscribe_async(ARCHIVE_ANNOUNCE_TOPIC, self._on_announcement)
        await self._peers.subscribe_async(ARCHIVE_REQUEST_TOPIC, self._on_request)
        await self._peers.subscribe_async(ARCHIVE_RESPONSE_TOPIC, self._on_response)

        self._started = True
        logger.info(f"Archiver protocol started (is_archiver={self.is_archiver}, local_archives={len(self._local_archives)})")

        # Start periodic announcement if we're an archiver
        if self.is_archiver and hasattr(self._peers, '_nursery') and self._peers._nursery:
            self._peers._nursery.start_soon(self._announcement_loop)

    async def stop(self) -> None:
        """Stop the archiver protocol."""
        if not self._started:
            return

        self._peers.unsubscribe(ARCHIVE_ANNOUNCE_TOPIC)
        self._peers.unsubscribe(ARCHIVE_REQUEST_TOPIC)
        self._peers.unsubscribe(ARCHIVE_RESPONSE_TOPIC)

        self._started = False
        logger.info("Archiver protocol stopped")

    # ========================================================================
    # ARCHIVER ACTIONS (Signers Only)
    # ========================================================================

    async def archive_round(
        self,
        round_id: str,
        predictions: List[Dict[str, Any]],
        observations: List[Dict[str, Any]],
        rewards: List[Dict[str, Any]],
        consensus: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Archive a complete round's data (archivers only).

        Args:
            round_id: Round identifier
            predictions: List of predictions
            observations: List of observations
            rewards: List of reward records
            consensus: Consensus result (optional)

        Returns:
            True if archived successfully
        """
        if not self.is_archiver:
            logger.warning("Only archivers can store archive data")
            return False

        # Create archived round
        archived = ArchivedRound(
            round_id=round_id,
            predictions=predictions,
            observations=observations,
            rewards=rewards,
            consensus=consensus,
            archiver=self._peers.evrmore_address or "",
        )

        # Store locally
        archive_path = self._storage_path / f"{round_id}.json"
        try:
            with open(archive_path, 'w') as f:
                json.dump(archived.to_dict(), f)
        except Exception as e:
            logger.error(f"Failed to store archive: {e}")
            return False

        # Create metadata
        data_bytes = archived.to_bytes()
        metadata = ArchiveMetadata(
            round_id=round_id,
            archive_type=ArchiveType.FULL_ROUND,
            merkle_root=archived.merkle_root,
            size_bytes=len(data_bytes),
            record_count=len(predictions) + len(observations) + len(rewards),
            timestamp=int(time.time()),
            archiver_address=self._peers.evrmore_address or "",
        )

        # Sign metadata
        if hasattr(self._peers, '_identity_bridge') and self._peers._identity_bridge:
            metadata.signature = self._peers._identity_bridge.sign_message(
                metadata.get_signing_message()
            )

        self._local_archives[round_id] = metadata
        self._save_local_index()

        logger.info(f"Archived round {round_id} ({metadata.record_count} records, {metadata.size_bytes} bytes)")
        return True

    async def archive_predictions(self, round_id: str, predictions: List[Dict[str, Any]]) -> bool:
        """Archive just predictions for a round."""
        if not self.is_archiver:
            return False

        # Load existing or create new
        existing = self._load_round(round_id)
        if existing:
            existing.predictions = predictions
            existing.merkle_root = existing.compute_merkle_root()
        else:
            existing = ArchivedRound(round_id=round_id, predictions=predictions)

        return await self._save_round(existing)

    async def archive_observations(self, round_id: str, observations: List[Dict[str, Any]]) -> bool:
        """Archive observations for a round."""
        if not self.is_archiver:
            return False

        existing = self._load_round(round_id)
        if existing:
            existing.observations = observations
            existing.merkle_root = existing.compute_merkle_root()
        else:
            existing = ArchivedRound(round_id=round_id, observations=observations)

        return await self._save_round(existing)

    async def archive_rewards(self, round_id: str, rewards: List[Dict[str, Any]]) -> bool:
        """Archive rewards for a round."""
        if not self.is_archiver:
            return False

        existing = self._load_round(round_id)
        if existing:
            existing.rewards = rewards
            existing.merkle_root = existing.compute_merkle_root()
        else:
            existing = ArchivedRound(round_id=round_id, rewards=rewards)

        return await self._save_round(existing)

    # ========================================================================
    # DATA RETRIEVAL (Public)
    # ========================================================================

    async def request_archive(
        self,
        round_id: str,
        archive_type: ArchiveType = ArchiveType.FULL_ROUND
    ) -> Optional[ArchivedRound]:
        """
        Request archived data from network.

        Args:
            round_id: Round to retrieve
            archive_type: Type of archive to retrieve

        Returns:
            ArchivedRound if found, None otherwise
        """
        # Check local first
        local = self._load_round(round_id)
        if local:
            return local

        # Request from network
        request = ArchiveRequest(
            requester_id=self._peers.peer_id or "",
            round_id=round_id,
            archive_type=archive_type,
        )

        try:
            await self._peers.broadcast(
                ARCHIVE_REQUEST_TOPIC,
                json.dumps(request.to_dict()).encode()
            )
            logger.debug(f"Requested archive for round {round_id}")

            # Wait for response (simplified - in production would use async wait)
            import trio
            with trio.move_on_after(ARCHIVE_REQUEST_TIMEOUT):
                while round_id not in self._pending_requests:
                    await trio.sleep(0.5)

            if round_id in self._pending_requests:
                result = self._pending_requests.pop(round_id)
                return result

        except Exception as e:
            logger.warning(f"Failed to request archive: {e}")

        return None

    async def sync_archive(
        self,
        round_id: str,
        archive_type: ArchiveType = ArchiveType.FULL_ROUND
    ) -> bool:
        """
        Download and store an archive from the network.

        Unlike request_archive(), this method stores the downloaded archive
        locally for future retrieval without needing to request again.

        Args:
            round_id: Round to download
            archive_type: Type of archive to download

        Returns:
            True if archive was downloaded and stored successfully
        """
        # Check if we already have it locally
        if round_id in self._local_archives:
            logger.debug(f"Archive {round_id} already exists locally")
            return True

        # Request from network
        archived = await self.request_archive(round_id, archive_type)
        if not archived:
            logger.warning(f"Could not download archive {round_id}")
            return False

        # Store locally
        archive_path = self._storage_path / f"{round_id}.json"
        try:
            with open(archive_path, 'w') as f:
                json.dump(archived.to_dict(), f)
        except Exception as e:
            logger.error(f"Failed to store downloaded archive: {e}")
            return False

        # Create and store metadata
        data_bytes = archived.to_bytes()
        metadata = ArchiveMetadata(
            round_id=round_id,
            archive_type=archive_type,
            merkle_root=archived.merkle_root,
            size_bytes=len(data_bytes),
            record_count=len(archived.predictions) + len(archived.observations) + len(archived.rewards),
            timestamp=int(time.time()),
            archiver_address=archived.archiver,  # Original archiver
        )
        self._local_archives[round_id] = metadata
        self._save_local_index()

        logger.info(f"Synced archive {round_id} ({metadata.record_count} records, {metadata.size_bytes} bytes)")
        return True

    async def sync_all_available(
        self,
        max_rounds: int = 100,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> Dict[str, Any]:
        """
        Sync all available archives from the network.

        Downloads archives that are available from network archivers but
        not stored locally. Useful for bootstrapping a new node or
        recovering missing data.

        Args:
            max_rounds: Maximum number of rounds to sync (to avoid overwhelming)
            progress_callback: Optional callback(current, total, round_id) for progress

        Returns:
            Dict with sync results:
                - synced: Number of archives successfully synced
                - failed: Number of archives that failed to sync
                - skipped: Number already available locally
                - rounds_synced: List of round_ids synced
                - rounds_failed: List of round_ids that failed
        """
        # Get all rounds available on network that we don't have locally
        network_rounds = set(self._network_archives.keys())
        local_rounds = set(self._local_archives.keys())
        missing_rounds = network_rounds - local_rounds

        if not missing_rounds:
            logger.info("All network archives already synced locally")
            return {
                'synced': 0,
                'failed': 0,
                'skipped': len(local_rounds),
                'rounds_synced': [],
                'rounds_failed': []
            }

        # Sort and limit
        rounds_to_sync = sorted(missing_rounds)[:max_rounds]
        total = len(rounds_to_sync)

        logger.info(f"Starting sync of {total} archives (total missing: {len(missing_rounds)})")

        synced = 0
        failed = 0
        rounds_synced = []
        rounds_failed = []

        for i, round_id in enumerate(rounds_to_sync):
            if progress_callback:
                progress_callback(i + 1, total, round_id)

            try:
                success = await self.sync_archive(round_id)
                if success:
                    synced += 1
                    rounds_synced.append(round_id)
                else:
                    failed += 1
                    rounds_failed.append(round_id)
            except Exception as e:
                logger.warning(f"Error syncing {round_id}: {e}")
                failed += 1
                rounds_failed.append(round_id)

            # Small delay between requests to avoid flooding
            import trio
            await trio.sleep(0.1)

        logger.info(f"Sync complete: {synced} synced, {failed} failed, {len(local_rounds)} skipped")

        return {
            'synced': synced,
            'failed': failed,
            'skipped': len(local_rounds),
            'rounds_synced': rounds_synced,
            'rounds_failed': rounds_failed
        }

    async def sync_missing_from_archiver(
        self,
        archiver_address: str,
        max_rounds: int = 50
    ) -> Dict[str, Any]:
        """
        Sync missing archives from a specific archiver.

        Args:
            archiver_address: Address of the archiver to sync from
            max_rounds: Maximum rounds to sync

        Returns:
            Dict with sync results
        """
        # Find what this archiver has that we don't
        archiver_rounds = set()
        for round_id, archivers in self._network_archives.items():
            if archiver_address in archivers:
                archiver_rounds.add(round_id)

        local_rounds = set(self._local_archives.keys())
        missing = archiver_rounds - local_rounds

        if not missing:
            return {
                'synced': 0,
                'failed': 0,
                'archiver': archiver_address,
                'message': 'Already have all archives from this archiver'
            }

        rounds_to_sync = sorted(missing)[:max_rounds]
        logger.info(f"Syncing {len(rounds_to_sync)} archives from {archiver_address}")

        synced = 0
        failed = 0

        for round_id in rounds_to_sync:
            try:
                success = await self.sync_archive(round_id)
                if success:
                    synced += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

            import trio
            await trio.sleep(0.1)

        return {
            'synced': synced,
            'failed': failed,
            'archiver': archiver_address,
            'total_available': len(archiver_rounds)
        }

    def get_sync_status(self) -> Dict[str, Any]:
        """
        Get current sync status comparing local vs network archives.

        Returns:
            Dict with:
                - local_count: Archives stored locally
                - network_count: Archives available on network
                - missing_count: Archives we don't have
                - sync_percent: Percentage synced
                - missing_rounds: List of missing round_ids
        """
        network_rounds = set(self._network_archives.keys())
        local_rounds = set(self._local_archives.keys())
        missing_rounds = network_rounds - local_rounds

        network_count = len(network_rounds)
        sync_percent = (len(local_rounds) / network_count * 100) if network_count > 0 else 100.0

        return {
            'local_count': len(local_rounds),
            'network_count': network_count,
            'missing_count': len(missing_rounds),
            'sync_percent': round(sync_percent, 1),
            'missing_rounds': sorted(missing_rounds)[:100]  # Limit to first 100
        }

    def get_local_archive(self, round_id: str) -> Optional[ArchivedRound]:
        """Get archive from local storage."""
        return self._load_round(round_id)

    def get_available_rounds(self) -> List[str]:
        """Get list of locally available round IDs."""
        return list(self._local_archives.keys())

    def get_network_availability(self, round_id: str) -> List[str]:
        """Get list of archivers that have a round."""
        if round_id not in self._network_archives:
            return []
        return list(self._network_archives[round_id].keys())

    def get_archive_metadata(self, round_id: str) -> Optional[ArchiveMetadata]:
        """Get metadata for a local archive."""
        return self._local_archives.get(round_id)

    def verify_archive_integrity(self, round_id: str) -> bool:
        """Verify integrity of a local archive."""
        archived = self._load_round(round_id)
        if not archived:
            return False

        metadata = self._local_archives.get(round_id)
        if not metadata:
            return False

        # Recompute merkle root and compare
        computed = archived.compute_merkle_root()
        return computed == metadata.merkle_root

    def get_stats(self) -> dict:
        """Get archiver protocol statistics."""
        total_size = sum(m.size_bytes for m in self._local_archives.values())
        return {
            "started": self._started,
            "is_archiver": self.is_archiver,
            "local_archives": len(self._local_archives),
            "total_size_mb": total_size / (1024 * 1024),
            "network_archivers": len(set(
                archiver
                for round_archives in self._network_archives.values()
                for archiver in round_archives.keys()
            )),
            "storage_path": str(self._storage_path),
        }

    def get_network_integrity(self) -> dict:
        """Get network-wide integrity statistics from archiver announcements.

        Returns:
            dict with:
                - network_integrity: Average integrity across all archivers (0-100)
                - archiver_count: Number of archivers reporting
                - archivers: List of {address, integrity, valid, total, last_seen}
        """
        if not self._archiver_integrity:
            return {
                'network_integrity': None,
                'archiver_count': 0,
                'archivers': []
            }

        now = int(time.time())
        archivers = []
        total_integrity = 0.0

        for addr, announcement in self._archiver_integrity.items():
            age_seconds = now - announcement.timestamp
            archivers.append({
                'address': addr,
                'integrity': announcement.integrity,
                'valid_archives': announcement.valid_archives,
                'total_archives': announcement.total_archives,
                'last_seen': age_seconds,
                'stale': age_seconds > 3600,  # Stale if over 1 hour old
            })
            total_integrity += announcement.integrity

        # Calculate average integrity
        avg_integrity = round(total_integrity / len(archivers), 1) if archivers else None

        return {
            'network_integrity': avg_integrity,
            'archiver_count': len(archivers),
            'archivers': archivers
        }

    # ========================================================================
    # ANNOUNCEMENT METHODS
    # ========================================================================

    async def announce_archives(self) -> None:
        """Announce our available archives to network."""
        if not self.is_archiver or not self._local_archives:
            return

        rounds = sorted(self._local_archives.keys())
        total_size = sum(m.size_bytes for m in self._local_archives.values())

        # Calculate integrity by verifying all archives
        valid_count = 0
        for round_id in rounds:
            try:
                if self.verify_archive_integrity(round_id):
                    valid_count += 1
            except Exception:
                pass  # Count as invalid

        integrity = round((valid_count / len(rounds)) * 100, 1) if rounds else 100.0

        announcement = ArchiveAnnouncement(
            archiver_address=self._peers.evrmore_address or "",
            peer_id=self._peers.peer_id or "",
            available_rounds=rounds,
            oldest_round=rounds[0] if rounds else "",
            newest_round=rounds[-1] if rounds else "",
            total_size_mb=total_size / (1024 * 1024),
            timestamp=int(time.time()),
            integrity=integrity,
            valid_archives=valid_count,
            total_archives=len(rounds),
        )

        # Sign
        if hasattr(self._peers, '_identity_bridge') and self._peers._identity_bridge:
            message = f"archive_announce:{announcement.archiver_address}:{len(rounds)}:{announcement.timestamp}"
            announcement.signature = self._peers._identity_bridge.sign_message(message)

        try:
            await self._peers.broadcast(
                ARCHIVE_ANNOUNCE_TOPIC,
                json.dumps(announcement.to_dict()).encode()
            )
            logger.debug(f"Announced {len(rounds)} archives (integrity={integrity}%) to network")
        except Exception as e:
            logger.warning(f"Failed to announce archives: {e}")

    async def _announcement_loop(self) -> None:
        """Periodically announce archives."""
        import trio
        logger.info("Archive announcement loop started")
        try:
            while self._started:
                await trio.sleep(ARCHIVE_ANNOUNCEMENT_INTERVAL)
                if self._started:
                    await self.announce_archives()
        except trio.Cancelled:
            pass
        logger.info("Archive announcement loop stopped")

    # ========================================================================
    # INTERNAL METHODS
    # ========================================================================

    def _load_local_index(self) -> None:
        """Load index of local archives."""
        index_path = self._storage_path / "index.json"
        if index_path.exists():
            try:
                with open(index_path) as f:
                    data = json.load(f)
                    self._local_archives = {
                        k: ArchiveMetadata.from_dict(v)
                        for k, v in data.items()
                    }
            except Exception as e:
                logger.warning(f"Failed to load archive index: {e}")

    def _save_local_index(self) -> None:
        """Save index of local archives."""
        index_path = self._storage_path / "index.json"
        try:
            with open(index_path, 'w') as f:
                json.dump(
                    {k: v.to_dict() for k, v in self._local_archives.items()},
                    f
                )
        except Exception as e:
            logger.warning(f"Failed to save archive index: {e}")

    def _load_round(self, round_id: str) -> Optional[ArchivedRound]:
        """Load a round from local storage."""
        archive_path = self._storage_path / f"{round_id}.json"
        if not archive_path.exists():
            return None
        try:
            with open(archive_path) as f:
                return ArchivedRound.from_dict(json.load(f))
        except Exception as e:
            logger.warning(f"Failed to load round {round_id}: {e}")
            return None

    async def _save_round(self, archived: ArchivedRound) -> bool:
        """Save a round to local storage."""
        archive_path = self._storage_path / f"{archived.round_id}.json"
        try:
            with open(archive_path, 'w') as f:
                json.dump(archived.to_dict(), f)

            # Update metadata
            data_bytes = archived.to_bytes()
            metadata = ArchiveMetadata(
                round_id=archived.round_id,
                archive_type=ArchiveType.FULL_ROUND,
                merkle_root=archived.merkle_root,
                size_bytes=len(data_bytes),
                record_count=len(archived.predictions) + len(archived.observations) + len(archived.rewards),
                timestamp=int(time.time()),
                archiver_address=self._peers.evrmore_address or "",
            )
            self._local_archives[archived.round_id] = metadata
            self._save_local_index()
            return True
        except Exception as e:
            logger.error(f"Failed to save round {archived.round_id}: {e}")
            return False

    def _on_announcement(self, topic: str, data: Any) -> None:
        """Handle incoming archive announcement."""
        try:
            if isinstance(data, bytes):
                announcement_data = json.loads(data.decode())
            elif isinstance(data, dict):
                announcement_data = data
            else:
                return

            announcement = ArchiveAnnouncement.from_dict(announcement_data)

            # Verify from authorized archiver
            from .signer import is_authorized_signer
            if not is_authorized_signer(announcement.archiver_address):
                return

            # Update network index
            for round_id in announcement.available_rounds:
                if round_id not in self._network_archives:
                    self._network_archives[round_id] = {}
                self._network_archives[round_id][announcement.archiver_address] = announcement

            # Track archiver integrity scores (latest announcement wins)
            self._archiver_integrity[announcement.archiver_address] = announcement

            logger.debug(f"Received announcement from {announcement.archiver_address}: {len(announcement.available_rounds)} rounds, integrity={announcement.integrity}%")

        except Exception as e:
            logger.warning(f"Error handling announcement: {e}")

    def _on_request(self, topic: str, data: Any) -> None:
        """Handle incoming archive request."""
        if not self.is_archiver:
            return

        try:
            if isinstance(data, bytes):
                request_data = json.loads(data.decode())
            elif isinstance(data, dict):
                request_data = data
            else:
                return

            request = ArchiveRequest.from_dict(request_data)

            # Don't respond to our own requests
            if request.requester_id == self._peers.peer_id:
                return

            # Check if we have the data
            archived = self._load_round(request.round_id)
            metadata = self._local_archives.get(request.round_id)

            if archived and metadata:
                response = ArchiveResponse(
                    round_id=request.round_id,
                    archive_type=request.archive_type,
                    status=ArchiveStatus.COMPLETE,
                    data=archived.to_bytes(),
                    metadata=metadata,
                )
            else:
                response = ArchiveResponse(
                    round_id=request.round_id,
                    archive_type=request.archive_type,
                    status=ArchiveStatus.MISSING,
                    error="Archive not found",
                )

            # Send response (schedule in nursery)
            if hasattr(self._peers, '_nursery') and self._peers._nursery:
                self._peers._nursery.start_soon(
                    self._send_response,
                    request.requester_id,
                    response
                )

        except Exception as e:
            logger.warning(f"Error handling request: {e}")

    async def _send_response(self, requester_id: str, response: ArchiveResponse) -> None:
        """Send archive response."""
        try:
            await self._peers.broadcast(
                ARCHIVE_RESPONSE_TOPIC,
                json.dumps(response.to_dict()).encode()
            )
        except Exception as e:
            logger.warning(f"Failed to send response: {e}")

    def _on_response(self, topic: str, data: Any) -> None:
        """Handle incoming archive response."""
        try:
            if isinstance(data, bytes):
                response_data = json.loads(data.decode())
            elif isinstance(data, dict):
                response_data = data
            else:
                return

            response = ArchiveResponse.from_dict(response_data)

            if response.status == ArchiveStatus.COMPLETE and response.data:
                # Parse the archived round
                archived = ArchivedRound.from_bytes(response.data)

                # Verify integrity
                if response.metadata:
                    if archived.merkle_root != response.metadata.merkle_root:
                        logger.warning(f"Archive integrity check failed for {response.round_id}")
                        return

                # Store in pending requests for retrieval
                self._pending_requests[response.round_id] = archived
                logger.info(f"Received archive for round {response.round_id}")

        except Exception as e:
            logger.warning(f"Error handling response: {e}")
