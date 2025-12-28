"""
satorip2p/protocol/storage.py

Redundant storage layer for critical protocol data.

Provides three-tier storage:
1. Memory cache - Fast access
2. Local disk - Crash recovery
3. DHT - Network redundancy

Used by:
- DeferredRewardsManager - Ensure deferred rewards survive restarts
- TreasuryAlertManager - Persist alert history
- Any other protocol needing crash-safe storage
"""

import os
import json
import time
import logging
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, TypeVar, Generic, Callable
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
import asyncio

logger = logging.getLogger("satorip2p.protocol.storage")


# ============================================================================
# CONSTANTS
# ============================================================================

# DHT key prefixes
DHT_DEFERRED_REWARDS_PREFIX = "satori:deferred:"
DHT_ALERT_HISTORY_PREFIX = "satori:alerts:"
DHT_STORAGE_PREFIX = "satori:storage:"

# Default storage paths
DEFAULT_STORAGE_DIR = Path.home() / ".satori" / "storage"

# TTL for DHT entries (24 hours)
DHT_TTL_SECONDS = 86400

# Sync interval (5 minutes)
SYNC_INTERVAL_SECONDS = 300


# ============================================================================
# STORAGE BACKENDS
# ============================================================================

class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    @abstractmethod
    async def get(self, key: str) -> Optional[bytes]:
        """Get a value by key."""
        pass

    @abstractmethod
    async def put(self, key: str, value: bytes, ttl: int = 0) -> bool:
        """Store a value with optional TTL."""
        pass

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete a value."""
        pass

    @abstractmethod
    async def list_keys(self, prefix: str = "") -> List[str]:
        """List keys with optional prefix filter."""
        pass


class MemoryBackend(StorageBackend):
    """In-memory storage backend."""

    def __init__(self):
        self._data: Dict[str, tuple[bytes, float]] = {}  # key -> (value, expires_at)

    async def get(self, key: str) -> Optional[bytes]:
        if key in self._data:
            value, expires_at = self._data[key]
            if expires_at == 0 or time.time() < expires_at:
                return value
            else:
                del self._data[key]
        return None

    async def put(self, key: str, value: bytes, ttl: int = 0) -> bool:
        expires_at = time.time() + ttl if ttl > 0 else 0
        self._data[key] = (value, expires_at)
        return True

    async def delete(self, key: str) -> bool:
        if key in self._data:
            del self._data[key]
            return True
        return False

    async def list_keys(self, prefix: str = "") -> List[str]:
        now = time.time()
        keys = []
        expired = []
        for key, (_, expires_at) in self._data.items():
            if expires_at > 0 and now >= expires_at:
                expired.append(key)
            elif key.startswith(prefix):
                keys.append(key)
        # Cleanup expired
        for key in expired:
            del self._data[key]
        return keys


class FileBackend(StorageBackend):
    """Local file storage backend."""

    def __init__(self, storage_dir: Path = None):
        self.storage_dir = storage_dir or DEFAULT_STORAGE_DIR
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._metadata_file = self.storage_dir / "metadata.json"
        self._metadata: Dict[str, dict] = self._load_metadata()

    def _load_metadata(self) -> Dict[str, dict]:
        """Load metadata from disk."""
        if self._metadata_file.exists():
            try:
                with open(self._metadata_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load metadata: {e}")
        return {}

    def _save_metadata(self) -> None:
        """Save metadata to disk."""
        try:
            with open(self._metadata_file, "w") as f:
                json.dump(self._metadata, f)
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")

    def _key_to_path(self, key: str) -> Path:
        """Convert key to file path."""
        # Use hash to avoid filesystem issues with special chars
        hash_name = hashlib.sha256(key.encode()).hexdigest()[:32]
        return self.storage_dir / f"{hash_name}.dat"

    async def get(self, key: str) -> Optional[bytes]:
        meta = self._metadata.get(key)
        if not meta:
            return None

        # Check expiration
        expires_at = meta.get("expires_at", 0)
        if expires_at > 0 and time.time() >= expires_at:
            await self.delete(key)
            return None

        path = self._key_to_path(key)
        if path.exists():
            try:
                return path.read_bytes()
            except Exception as e:
                logger.error(f"Failed to read {key}: {e}")
        return None

    async def put(self, key: str, value: bytes, ttl: int = 0) -> bool:
        path = self._key_to_path(key)
        try:
            path.write_bytes(value)
            self._metadata[key] = {
                "path": str(path),
                "created_at": time.time(),
                "expires_at": time.time() + ttl if ttl > 0 else 0,
            }
            self._save_metadata()
            return True
        except Exception as e:
            logger.error(f"Failed to write {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        if key not in self._metadata:
            return False

        path = self._key_to_path(key)
        try:
            if path.exists():
                path.unlink()
            del self._metadata[key]
            self._save_metadata()
            return True
        except Exception as e:
            logger.error(f"Failed to delete {key}: {e}")
            return False

    async def list_keys(self, prefix: str = "") -> List[str]:
        now = time.time()
        keys = []
        expired = []

        for key, meta in self._metadata.items():
            expires_at = meta.get("expires_at", 0)
            if expires_at > 0 and now >= expires_at:
                expired.append(key)
            elif key.startswith(prefix):
                keys.append(key)

        # Cleanup expired
        for key in expired:
            await self.delete(key)

        return keys


class DHTBackend(StorageBackend):
    """DHT storage backend using P2P network."""

    def __init__(self, peers=None):
        self._peers = peers

    def set_peers(self, peers) -> None:
        """Set the P2P peers instance."""
        self._peers = peers

    async def get(self, key: str) -> Optional[bytes]:
        if not self._peers:
            return None

        try:
            data = await self._peers.get_dht(key)
            if data:
                return data if isinstance(data, bytes) else data.encode()
        except Exception as e:
            logger.debug(f"DHT get failed for {key}: {e}")
        return None

    async def put(self, key: str, value: bytes, ttl: int = 0) -> bool:
        if not self._peers:
            return False

        try:
            await self._peers.put_dht(key, value)
            return True
        except Exception as e:
            logger.debug(f"DHT put failed for {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        # DHT doesn't support explicit deletes; entries expire via TTL
        return True

    async def list_keys(self, prefix: str = "") -> List[str]:
        # DHT doesn't support key listing
        return []


# ============================================================================
# REDUNDANT STORAGE
# ============================================================================

T = TypeVar('T')


class RedundantStorage(Generic[T]):
    """
    Three-tier redundant storage with automatic sync.

    Provides crash recovery and network redundancy for critical data.

    Layers:
    1. Memory - Fastest, volatile
    2. Disk - Survives restarts, local only
    3. DHT - Network-wide redundancy

    Read order: Memory -> Disk -> DHT
    Write order: Memory + Disk (sync), then DHT (async)
    """

    def __init__(
        self,
        namespace: str,
        peers=None,
        storage_dir: Path = None,
        serializer: Callable[[T], bytes] = None,
        deserializer: Callable[[bytes], T] = None,
        enable_dht: bool = True,
    ):
        """
        Initialize redundant storage.

        Args:
            namespace: Key namespace for isolation
            peers: P2P peers instance for DHT
            storage_dir: Directory for local file storage
            serializer: Function to convert objects to bytes
            deserializer: Function to convert bytes to objects
            enable_dht: Whether to use DHT backend
        """
        self.namespace = namespace
        self.enable_dht = enable_dht

        # Backends
        self._memory = MemoryBackend()
        self._disk = FileBackend(storage_dir)
        self._dht = DHTBackend(peers) if enable_dht else None

        # Serialization
        self._serialize = serializer or self._default_serialize
        self._deserialize = deserializer or self._default_deserialize

        # Sync state
        self._pending_dht_sync: Dict[str, bytes] = {}
        self._last_sync = 0

    def _default_serialize(self, obj: Any) -> bytes:
        """Default JSON serialization."""
        if hasattr(obj, 'to_dict'):
            return json.dumps(obj.to_dict()).encode()
        elif isinstance(obj, dict):
            return json.dumps(obj).encode()
        else:
            return json.dumps(obj).encode()

    def _default_deserialize(self, data: bytes) -> Any:
        """Default JSON deserialization."""
        return json.loads(data.decode())

    def _make_key(self, key: str) -> str:
        """Create namespaced key."""
        return f"{DHT_STORAGE_PREFIX}{self.namespace}:{key}"

    def set_peers(self, peers) -> None:
        """Set P2P peers instance for DHT backend."""
        if self._dht:
            self._dht.set_peers(peers)

    async def get(self, key: str) -> Optional[T]:
        """
        Get a value by key.

        Reads from fastest available source.

        Args:
            key: Storage key

        Returns:
            Deserialized value or None
        """
        full_key = self._make_key(key)

        # Try memory first
        data = await self._memory.get(full_key)
        if data:
            return self._deserialize(data)

        # Try disk
        data = await self._disk.get(full_key)
        if data:
            # Repopulate memory cache
            await self._memory.put(full_key, data)
            return self._deserialize(data)

        # Try DHT
        if self._dht:
            data = await self._dht.get(full_key)
            if data:
                # Repopulate memory and disk
                await self._memory.put(full_key, data)
                await self._disk.put(full_key, data)
                return self._deserialize(data)

        return None

    async def put(self, key: str, value: T, ttl: int = 0) -> bool:
        """
        Store a value with optional TTL.

        Writes to memory and disk synchronously, DHT asynchronously.

        Args:
            key: Storage key
            value: Value to store
            ttl: Time-to-live in seconds (0 = no expiration)

        Returns:
            True if stored successfully
        """
        full_key = self._make_key(key)
        data = self._serialize(value)

        # Write to memory and disk (synchronous for durability)
        mem_ok = await self._memory.put(full_key, data, ttl)
        disk_ok = await self._disk.put(full_key, data, ttl)

        # Queue DHT write (async, don't block)
        if self._dht:
            self._pending_dht_sync[full_key] = data

        return mem_ok and disk_ok

    async def delete(self, key: str) -> bool:
        """
        Delete a value.

        Args:
            key: Storage key

        Returns:
            True if deleted
        """
        full_key = self._make_key(key)

        mem_ok = await self._memory.delete(full_key)
        disk_ok = await self._disk.delete(full_key)

        # Remove from pending sync
        self._pending_dht_sync.pop(full_key, None)

        return mem_ok or disk_ok

    async def list_keys(self) -> List[str]:
        """
        List all keys in this namespace.

        Returns:
            List of keys (without namespace prefix)
        """
        prefix = self._make_key("")
        keys = set()

        # Get from memory
        keys.update(await self._memory.list_keys(prefix))

        # Get from disk
        keys.update(await self._disk.list_keys(prefix))

        # Strip namespace prefix
        prefix_len = len(prefix)
        return [k[prefix_len:] for k in keys]

    async def sync_to_dht(self) -> int:
        """
        Sync pending writes to DHT.

        Called periodically or on demand.

        Returns:
            Number of keys synced
        """
        if not self._dht or not self._pending_dht_sync:
            return 0

        synced = 0
        failed = []

        for key, data in list(self._pending_dht_sync.items()):
            try:
                if await self._dht.put(key, data, DHT_TTL_SECONDS):
                    synced += 1
                else:
                    failed.append(key)
            except Exception as e:
                logger.debug(f"DHT sync failed for {key}: {e}")
                failed.append(key)

        # Remove successful syncs
        for key in list(self._pending_dht_sync.keys()):
            if key not in failed:
                del self._pending_dht_sync[key]

        if synced > 0:
            logger.debug(f"Synced {synced} keys to DHT")

        self._last_sync = time.time()
        return synced

    async def recover_from_dht(self, keys: List[str]) -> int:
        """
        Recover specific keys from DHT to local storage.

        Args:
            keys: Keys to recover

        Returns:
            Number of keys recovered
        """
        if not self._dht:
            return 0

        recovered = 0
        for key in keys:
            full_key = self._make_key(key)
            data = await self._dht.get(full_key)
            if data:
                await self._memory.put(full_key, data)
                await self._disk.put(full_key, data)
                recovered += 1

        if recovered > 0:
            logger.info(f"Recovered {recovered} keys from DHT")

        return recovered

    def get_pending_sync_count(self) -> int:
        """Get number of pending DHT syncs."""
        return len(self._pending_dht_sync)

    def get_stats(self) -> dict:
        """Get storage statistics."""
        return {
            "namespace": self.namespace,
            "pending_dht_sync": len(self._pending_dht_sync),
            "last_sync": self._last_sync,
            "dht_enabled": self._dht is not None,
        }


# ============================================================================
# SPECIALIZED STORAGE CLASSES
# ============================================================================

@dataclass
class StoredDeferredReward:
    """Deferred reward stored with metadata."""
    address: str
    round_id: int
    amount: float
    reason: str
    created_at: int
    paid_at: Optional[int] = None
    paid_tx_hash: Optional[str] = None


class DeferredRewardsStorage(RedundantStorage[List[Dict]]):
    """
    Specialized storage for deferred rewards.

    Keys: address -> list of deferred rewards
    """

    def __init__(self, peers=None, storage_dir: Path = None):
        super().__init__(
            namespace="deferred_rewards",
            peers=peers,
            storage_dir=storage_dir,
        )

    async def add_reward(self, reward: StoredDeferredReward) -> bool:
        """Add a deferred reward for an address."""
        rewards = await self.get(reward.address) or []
        rewards.append(asdict(reward))
        return await self.put(reward.address, rewards)

    async def get_rewards_for_address(self, address: str) -> List[StoredDeferredReward]:
        """Get all deferred rewards for an address."""
        data = await self.get(address)
        if not data:
            return []
        return [StoredDeferredReward(**r) for r in data]

    async def mark_paid(
        self,
        address: str,
        round_ids: List[int],
        tx_hash: str,
    ) -> int:
        """Mark rewards as paid."""
        rewards = await self.get(address) or []
        paid_at = int(time.time())
        count = 0

        for reward in rewards:
            if reward.get("paid_at") is None and reward.get("round_id") in round_ids:
                reward["paid_at"] = paid_at
                reward["paid_tx_hash"] = tx_hash
                count += 1

        if count > 0:
            await self.put(address, rewards)

        return count

    async def get_all_unpaid(self) -> List[StoredDeferredReward]:
        """Get all unpaid rewards across all addresses."""
        all_keys = await self.list_keys()
        unpaid = []

        for key in all_keys:
            rewards = await self.get_rewards_for_address(key)
            unpaid.extend([r for r in rewards if r.paid_at is None])

        return unpaid

    async def get_total_deferred(self) -> float:
        """Get total deferred amount."""
        unpaid = await self.get_all_unpaid()
        return sum(r.amount for r in unpaid)


@dataclass
class StoredAlert:
    """Alert stored with metadata."""
    alert_id: str
    alert_type: str
    severity: str
    message: str
    details: Dict[str, Any]
    timestamp: int
    resolved_at: Optional[int] = None
    resolution: Optional[str] = None


class AlertStorage(RedundantStorage[List[Dict]]):
    """
    Specialized storage for treasury alerts.

    Keys:
    - "active" -> list of active alerts
    - "history" -> list of historical alerts
    """

    def __init__(self, peers=None, storage_dir: Path = None):
        super().__init__(
            namespace="alerts",
            peers=peers,
            storage_dir=storage_dir,
        )

    async def add_active_alert(self, alert: StoredAlert) -> bool:
        """Add an active alert."""
        alerts = await self.get("active") or []

        # Remove duplicate by type
        alerts = [a for a in alerts if a.get("alert_type") != alert.alert_type]
        alerts.append(asdict(alert))

        return await self.put("active", alerts)

    async def get_active_alerts(self) -> List[StoredAlert]:
        """Get all active alerts."""
        data = await self.get("active") or []
        return [StoredAlert(**a) for a in data]

    async def resolve_alert(
        self,
        alert_type: str,
        resolution: str = "resolved",
    ) -> bool:
        """Resolve an alert and move to history."""
        active = await self.get("active") or []
        history = await self.get("history") or []

        resolved_at = int(time.time())
        new_active = []

        for alert in active:
            if alert.get("alert_type") == alert_type:
                alert["resolved_at"] = resolved_at
                alert["resolution"] = resolution
                history.append(alert)
            else:
                new_active.append(alert)

        await self.put("active", new_active)
        await self.put("history", history[-100:])  # Keep last 100

        return len(active) != len(new_active)

    async def clear_alerts_by_type(self, alert_types: List[str]) -> int:
        """Clear alerts of specific types."""
        active = await self.get("active") or []
        new_active = [a for a in active if a.get("alert_type") not in alert_types]

        if len(new_active) != len(active):
            await self.put("active", new_active)

        return len(active) - len(new_active)

    async def get_history(self, limit: int = 20) -> List[StoredAlert]:
        """Get alert history."""
        data = await self.get("history") or []
        return [StoredAlert(**a) for a in data[-limit:]]


# ============================================================================
# STORAGE MANAGER
# ============================================================================

class StorageManager:
    """
    Manages all redundant storage instances.

    Provides unified sync and recovery operations.
    """

    def __init__(self, peers=None, storage_dir: Path = None):
        self._peers = peers
        self._storage_dir = storage_dir or DEFAULT_STORAGE_DIR

        # Backend enabled states
        # Note: Memory and File are always enabled (required for operation)
        # Only DHT is toggleable
        self._dht_enabled = True

        # Storage instances
        self.deferred_rewards = DeferredRewardsStorage(peers, self._storage_dir)
        self.alerts = AlertStorage(peers, self._storage_dir)

        # Custom storages
        self._custom: Dict[str, RedundantStorage] = {}

    def set_peers(self, peers) -> None:
        """Set P2P peers for all storage instances."""
        self._peers = peers
        self.deferred_rewards.set_peers(peers)
        self.alerts.set_peers(peers)
        for storage in self._custom.values():
            storage.set_peers(peers)

    def register_storage(
        self,
        name: str,
        storage: RedundantStorage,
    ) -> None:
        """Register a custom storage instance."""
        self._custom[name] = storage
        if self._peers:
            storage.set_peers(self._peers)

    async def sync_all(self) -> Dict[str, int]:
        """Sync all storage instances to DHT."""
        results = {
            "deferred_rewards": await self.deferred_rewards.sync_to_dht(),
            "alerts": await self.alerts.sync_to_dht(),
        }

        for name, storage in self._custom.items():
            results[name] = await storage.sync_to_dht()

        return results

    def get_all_stats(self) -> Dict[str, dict]:
        """Get stats for all storage instances."""
        stats = {
            "deferred_rewards": self.deferred_rewards.get_stats(),
            "alerts": self.alerts.get_stats(),
        }

        for name, storage in self._custom.items():
            stats[name] = storage.get_stats()

        return stats

    def get_pending_sync_total(self) -> int:
        """Get total pending DHT syncs across all storage."""
        total = (
            self.deferred_rewards.get_pending_sync_count() +
            self.alerts.get_pending_sync_count()
        )
        for storage in self._custom.values():
            total += storage.get_pending_sync_count()
        return total

    def get_status(self) -> Dict[str, Any]:
        """
        Get status of all storage backends.

        Returns:
            Dict with backend status and item counts
        """
        return {
            'backends': {
                'memory': {
                    'enabled': True,  # Memory is always enabled (required)
                    'items': self._get_memory_item_count(),
                },
                'file': {
                    'enabled': True,  # File is always enabled (core persistence)
                    'items': self._get_file_item_count(),
                },
                'dht': {
                    'enabled': self._dht_enabled,
                    'items': self._get_dht_item_count(),
                },
            },
            'pending_sync': self.get_pending_sync_total(),
        }

    def _get_memory_item_count(self) -> int:
        """Get total items in memory across all storage instances."""
        count = 0
        if hasattr(self.deferred_rewards, '_memory') and self.deferred_rewards._memory:
            count += len(getattr(self.deferred_rewards._memory, '_data', {}))
        if hasattr(self.alerts, '_memory') and self.alerts._memory:
            count += len(getattr(self.alerts._memory, '_data', {}))
        return count

    def _get_file_item_count(self) -> int:
        """Get total items in file storage across all storage instances."""
        count = 0
        if hasattr(self.deferred_rewards, '_disk') and self.deferred_rewards._disk:
            count += getattr(self.deferred_rewards._disk, '_item_count', 0)
        if hasattr(self.alerts, '_disk') and self.alerts._disk:
            count += getattr(self.alerts._disk, '_item_count', 0)
        return count

    def _get_dht_item_count(self) -> int:
        """Get total items synced to DHT."""
        count = 0
        if hasattr(self.deferred_rewards, '_dht_synced_count'):
            count += self.deferred_rewards._dht_synced_count
        if hasattr(self.alerts, '_dht_synced_count'):
            count += self.alerts._dht_synced_count
        return count

    def toggle_backend(self, backend: str, enabled: bool) -> bool:
        """
        Enable or disable a storage backend.

        Only DHT can be toggled. Memory and File are always enabled as they
        are required for the storage system to function.

        Args:
            backend: 'dht' (only toggleable backend)
            enabled: True to enable, False to disable

        Returns:
            True if successful, False if invalid backend
        """
        if backend == 'memory':
            # Memory is required for read/write operations
            logger.warning("Memory storage cannot be toggled - required for operation")
            return False
        elif backend == 'file':
            # File storage cannot be disabled - it's the core persistence layer
            logger.warning("File storage cannot be toggled - always enabled")
            return False
        elif backend == 'dht':
            self._dht_enabled = enabled
            self._toggle_backend_on_storage(self.deferred_rewards, 'dht', enabled)
            self._toggle_backend_on_storage(self.alerts, 'dht', enabled)
            return True
        return False

    def _toggle_backend_on_storage(
        self,
        storage: 'RedundantStorage',
        backend: str,
        enabled: bool
    ) -> None:
        """Toggle a specific backend on a storage instance.

        Only DHT is toggleable. Memory and File are always enabled.
        """
        if backend == 'dht':
            if enabled:
                if not storage._dht and self._peers:
                    storage._dht = DHTBackend(self._peers)
                storage.enable_dht = True
            else:
                storage._dht = None
                storage.enable_dht = False
        # Note: 'memory' and 'file' backends are not toggleable - always enabled
