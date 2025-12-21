"""
satorip2p/tests/test_storage.py

Tests for redundant storage layer.
"""

import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, AsyncMock
import asyncio

from satorip2p.protocol.storage import (
    MemoryBackend,
    FileBackend,
    DHTBackend,
    RedundantStorage,
    DeferredRewardsStorage,
    AlertStorage,
    StorageManager,
    StoredDeferredReward,
    StoredAlert,
    DHT_STORAGE_PREFIX,
)


class TestMemoryBackend:
    """Test MemoryBackend class."""

    @pytest.fixture
    def backend(self):
        return MemoryBackend()

    @pytest.mark.asyncio
    async def test_put_and_get(self, backend):
        """Test basic put and get."""
        await backend.put("key1", b"value1")
        result = await backend.get("key1")
        assert result == b"value1"

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, backend):
        """Test getting nonexistent key."""
        result = await backend.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete(self, backend):
        """Test deleting a key."""
        await backend.put("key1", b"value1")
        result = await backend.delete("key1")
        assert result is True
        assert await backend.get("key1") is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, backend):
        """Test deleting nonexistent key."""
        result = await backend.delete("nonexistent")
        assert result is False

    @pytest.mark.asyncio
    async def test_ttl_expiration(self, backend):
        """Test TTL expiration."""
        await backend.put("key1", b"value1", ttl=1)
        assert await backend.get("key1") == b"value1"

        # Wait for expiration
        await asyncio.sleep(1.1)
        assert await backend.get("key1") is None

    @pytest.mark.asyncio
    async def test_list_keys(self, backend):
        """Test listing keys."""
        await backend.put("prefix:key1", b"v1")
        await backend.put("prefix:key2", b"v2")
        await backend.put("other:key3", b"v3")

        keys = await backend.list_keys("prefix:")
        assert len(keys) == 2
        assert "prefix:key1" in keys
        assert "prefix:key2" in keys

    @pytest.mark.asyncio
    async def test_list_keys_all(self, backend):
        """Test listing all keys."""
        await backend.put("key1", b"v1")
        await backend.put("key2", b"v2")

        keys = await backend.list_keys("")
        assert len(keys) == 2


class TestFileBackend:
    """Test FileBackend class."""

    @pytest.fixture
    def backend(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield FileBackend(Path(tmpdir))

    @pytest.mark.asyncio
    async def test_put_and_get(self, backend):
        """Test basic put and get."""
        await backend.put("key1", b"value1")
        result = await backend.get("key1")
        assert result == b"value1"

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, backend):
        """Test getting nonexistent key."""
        result = await backend.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete(self, backend):
        """Test deleting a key."""
        await backend.put("key1", b"value1")
        result = await backend.delete("key1")
        assert result is True
        assert await backend.get("key1") is None

    @pytest.mark.asyncio
    async def test_persistence(self, backend):
        """Test that data persists."""
        await backend.put("key1", b"value1")

        # Create new backend instance with same dir
        backend2 = FileBackend(backend.storage_dir)
        result = await backend2.get("key1")
        assert result == b"value1"

    @pytest.mark.asyncio
    async def test_list_keys(self, backend):
        """Test listing keys."""
        await backend.put("prefix:key1", b"v1")
        await backend.put("prefix:key2", b"v2")
        await backend.put("other:key3", b"v3")

        keys = await backend.list_keys("prefix:")
        assert len(keys) == 2

    @pytest.mark.asyncio
    async def test_special_characters_in_key(self, backend):
        """Test keys with special characters."""
        key = "namespace:user@domain.com:data"
        await backend.put(key, b"value")
        result = await backend.get(key)
        assert result == b"value"


class TestDHTBackend:
    """Test DHTBackend class."""

    @pytest.fixture
    def mock_peers(self):
        peers = Mock()
        peers.get_dht = AsyncMock(return_value=None)
        peers.put_dht = AsyncMock()
        return peers

    @pytest.fixture
    def backend(self, mock_peers):
        return DHTBackend(mock_peers)

    @pytest.mark.asyncio
    async def test_put(self, backend, mock_peers):
        """Test putting to DHT."""
        result = await backend.put("key1", b"value1")
        assert result is True
        mock_peers.put_dht.assert_called_once_with("key1", b"value1")

    @pytest.mark.asyncio
    async def test_get(self, backend, mock_peers):
        """Test getting from DHT."""
        mock_peers.get_dht.return_value = b"value1"
        result = await backend.get("key1")
        assert result == b"value1"

    @pytest.mark.asyncio
    async def test_get_none_when_no_peers(self):
        """Test get returns None when no peers."""
        backend = DHTBackend(None)
        result = await backend.get("key1")
        assert result is None

    @pytest.mark.asyncio
    async def test_put_false_when_no_peers(self):
        """Test put returns False when no peers."""
        backend = DHTBackend(None)
        result = await backend.put("key1", b"value1")
        assert result is False

    @pytest.mark.asyncio
    async def test_set_peers(self, backend):
        """Test setting peers after creation."""
        new_peers = Mock()
        new_peers.get_dht = AsyncMock(return_value=b"new_value")

        backend.set_peers(new_peers)
        result = await backend.get("key1")
        assert result == b"new_value"


class TestRedundantStorage:
    """Test RedundantStorage class."""

    @pytest.fixture
    def storage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield RedundantStorage(
                namespace="test",
                storage_dir=Path(tmpdir),
                enable_dht=False,
            )

    @pytest.mark.asyncio
    async def test_put_and_get(self, storage):
        """Test basic put and get."""
        await storage.put("key1", {"value": 123})
        result = await storage.get("key1")
        assert result == {"value": 123}

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, storage):
        """Test getting nonexistent key."""
        result = await storage.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete(self, storage):
        """Test deleting a key."""
        await storage.put("key1", {"value": 123})
        result = await storage.delete("key1")
        assert result is True
        assert await storage.get("key1") is None

    @pytest.mark.asyncio
    async def test_list_keys(self, storage):
        """Test listing keys."""
        await storage.put("key1", {"v": 1})
        await storage.put("key2", {"v": 2})

        keys = await storage.list_keys()
        assert len(keys) == 2
        assert "key1" in keys
        assert "key2" in keys

    @pytest.mark.asyncio
    async def test_namespace_isolation(self):
        """Test namespaces are isolated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage1 = RedundantStorage("ns1", storage_dir=Path(tmpdir), enable_dht=False)
            storage2 = RedundantStorage("ns2", storage_dir=Path(tmpdir), enable_dht=False)

            await storage1.put("key", {"ns": 1})
            await storage2.put("key", {"ns": 2})

            assert await storage1.get("key") == {"ns": 1}
            assert await storage2.get("key") == {"ns": 2}

    @pytest.mark.asyncio
    async def test_disk_persistence(self):
        """Test data persists on disk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage1 = RedundantStorage("test", storage_dir=Path(tmpdir), enable_dht=False)
            await storage1.put("key1", {"value": "test"})

            # Create new instance
            storage2 = RedundantStorage("test", storage_dir=Path(tmpdir), enable_dht=False)
            result = await storage2.get("key1")
            assert result == {"value": "test"}

    @pytest.mark.asyncio
    async def test_memory_cache(self, storage):
        """Test memory cache is populated."""
        await storage.put("key1", {"value": 123})

        # Should be in memory
        mem_result = await storage._memory.get(storage._make_key("key1"))
        assert mem_result is not None

    @pytest.mark.asyncio
    async def test_custom_serializer(self):
        """Test custom serializer/deserializer."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = RedundantStorage(
                "test",
                storage_dir=Path(tmpdir),
                enable_dht=False,
                serializer=lambda x: x.encode() if isinstance(x, str) else str(x).encode(),
                deserializer=lambda x: x.decode(),
            )

            await storage.put("key1", "hello")
            result = await storage.get("key1")
            assert result == "hello"

    @pytest.mark.asyncio
    async def test_get_stats(self, storage):
        """Test getting stats."""
        stats = storage.get_stats()
        assert stats["namespace"] == "test"
        assert stats["pending_dht_sync"] == 0
        assert stats["dht_enabled"] is False


class TestRedundantStorageWithDHT:
    """Test RedundantStorage with DHT enabled."""

    @pytest.fixture
    def mock_peers(self):
        peers = Mock()
        peers.get_dht = AsyncMock(return_value=None)
        peers.put_dht = AsyncMock()
        return peers

    @pytest.fixture
    def storage(self, mock_peers):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = RedundantStorage(
                namespace="test",
                peers=mock_peers,
                storage_dir=Path(tmpdir),
                enable_dht=True,
            )
            yield storage

    @pytest.mark.asyncio
    async def test_put_queues_dht_sync(self, storage):
        """Test that put queues DHT sync."""
        await storage.put("key1", {"value": 123})
        assert storage.get_pending_sync_count() == 1

    @pytest.mark.asyncio
    async def test_sync_to_dht(self, storage, mock_peers):
        """Test syncing to DHT."""
        await storage.put("key1", {"value": 123})
        await storage.put("key2", {"value": 456})

        synced = await storage.sync_to_dht()
        assert synced == 2
        assert storage.get_pending_sync_count() == 0
        assert mock_peers.put_dht.call_count == 2

    @pytest.mark.asyncio
    async def test_get_from_dht_fallback(self, storage, mock_peers):
        """Test falling back to DHT for get."""
        # Not in memory or disk, but in DHT
        mock_peers.get_dht.return_value = b'{"value": 789}'

        result = await storage.get("remote_key")
        assert result == {"value": 789}

    @pytest.mark.asyncio
    async def test_dht_recovery_populates_local(self, storage, mock_peers):
        """Test that DHT recovery populates local storage."""
        mock_peers.get_dht.return_value = b'{"recovered": true}'

        # First get from DHT
        await storage.get("recovered_key")

        # Should now be in memory (reset DHT mock to verify)
        mock_peers.get_dht.return_value = None
        result = await storage.get("recovered_key")
        assert result == {"recovered": True}


class TestDeferredRewardsStorage:
    """Test DeferredRewardsStorage class."""

    @pytest.fixture
    def storage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield DeferredRewardsStorage(storage_dir=Path(tmpdir))

    @pytest.mark.asyncio
    async def test_add_reward(self, storage):
        """Test adding a deferred reward."""
        reward = StoredDeferredReward(
            address="EAddr123",
            round_id=100,
            amount=50.5,
            reason="insufficient_satori",
            created_at=int(time.time()),
        )

        result = await storage.add_reward(reward)
        assert result is True

    @pytest.mark.asyncio
    async def test_get_rewards_for_address(self, storage):
        """Test getting rewards for an address."""
        reward1 = StoredDeferredReward(
            address="EAddr123",
            round_id=100,
            amount=50.5,
            reason="insufficient_satori",
            created_at=int(time.time()),
        )
        reward2 = StoredDeferredReward(
            address="EAddr123",
            round_id=101,
            amount=25.0,
            reason="insufficient_evr",
            created_at=int(time.time()),
        )

        await storage.add_reward(reward1)
        await storage.add_reward(reward2)

        rewards = await storage.get_rewards_for_address("EAddr123")
        assert len(rewards) == 2
        assert rewards[0].amount == 50.5
        assert rewards[1].amount == 25.0

    @pytest.mark.asyncio
    async def test_get_rewards_nonexistent_address(self, storage):
        """Test getting rewards for nonexistent address."""
        rewards = await storage.get_rewards_for_address("ENoSuchAddr")
        assert rewards == []

    @pytest.mark.asyncio
    async def test_mark_paid(self, storage):
        """Test marking rewards as paid."""
        reward = StoredDeferredReward(
            address="EAddr123",
            round_id=100,
            amount=50.5,
            reason="insufficient_satori",
            created_at=int(time.time()),
        )
        await storage.add_reward(reward)

        count = await storage.mark_paid("EAddr123", [100], "tx123")
        assert count == 1

        rewards = await storage.get_rewards_for_address("EAddr123")
        assert rewards[0].paid_at is not None
        assert rewards[0].paid_tx_hash == "tx123"

    @pytest.mark.asyncio
    async def test_get_all_unpaid(self, storage):
        """Test getting all unpaid rewards."""
        reward1 = StoredDeferredReward(
            address="EAddr1",
            round_id=100,
            amount=50.0,
            reason="test",
            created_at=int(time.time()),
        )
        reward2 = StoredDeferredReward(
            address="EAddr2",
            round_id=101,
            amount=30.0,
            reason="test",
            created_at=int(time.time()),
        )

        await storage.add_reward(reward1)
        await storage.add_reward(reward2)

        # Mark one as paid
        await storage.mark_paid("EAddr1", [100], "tx123")

        unpaid = await storage.get_all_unpaid()
        assert len(unpaid) == 1
        assert unpaid[0].address == "EAddr2"

    @pytest.mark.asyncio
    async def test_get_total_deferred(self, storage):
        """Test getting total deferred amount."""
        reward1 = StoredDeferredReward(
            address="EAddr1",
            round_id=100,
            amount=50.0,
            reason="test",
            created_at=int(time.time()),
        )
        reward2 = StoredDeferredReward(
            address="EAddr2",
            round_id=101,
            amount=30.0,
            reason="test",
            created_at=int(time.time()),
        )

        await storage.add_reward(reward1)
        await storage.add_reward(reward2)

        total = await storage.get_total_deferred()
        assert total == 80.0


class TestAlertStorage:
    """Test AlertStorage class."""

    @pytest.fixture
    def storage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield AlertStorage(storage_dir=Path(tmpdir))

    @pytest.mark.asyncio
    async def test_add_active_alert(self, storage):
        """Test adding an active alert."""
        alert = StoredAlert(
            alert_id="alert123",
            alert_type="insufficient_satori",
            severity="warning",
            message="Test alert",
            details={"shortfall": 100.0},
            timestamp=int(time.time()),
        )

        result = await storage.add_active_alert(alert)
        assert result is True

        alerts = await storage.get_active_alerts()
        assert len(alerts) == 1
        assert alerts[0].alert_type == "insufficient_satori"

    @pytest.mark.asyncio
    async def test_duplicate_alert_replaces(self, storage):
        """Test that duplicate alert types are replaced."""
        alert1 = StoredAlert(
            alert_id="alert1",
            alert_type="insufficient_satori",
            severity="warning",
            message="First alert",
            details={},
            timestamp=int(time.time()),
        )
        alert2 = StoredAlert(
            alert_id="alert2",
            alert_type="insufficient_satori",
            severity="critical",
            message="Second alert",
            details={},
            timestamp=int(time.time()),
        )

        await storage.add_active_alert(alert1)
        await storage.add_active_alert(alert2)

        alerts = await storage.get_active_alerts()
        assert len(alerts) == 1
        assert alerts[0].message == "Second alert"

    @pytest.mark.asyncio
    async def test_resolve_alert(self, storage):
        """Test resolving an alert."""
        alert = StoredAlert(
            alert_id="alert123",
            alert_type="insufficient_satori",
            severity="warning",
            message="Test alert",
            details={},
            timestamp=int(time.time()),
        )
        await storage.add_active_alert(alert)

        result = await storage.resolve_alert("insufficient_satori", "treasury_refilled")
        assert result is True

        # Should be moved to history
        active = await storage.get_active_alerts()
        assert len(active) == 0

        history = await storage.get_history()
        assert len(history) == 1
        assert history[0].resolution == "treasury_refilled"

    @pytest.mark.asyncio
    async def test_clear_alerts_by_type(self, storage):
        """Test clearing alerts by type."""
        alert1 = StoredAlert(
            alert_id="alert1",
            alert_type="insufficient_satori",
            severity="warning",
            message="Alert 1",
            details={},
            timestamp=int(time.time()),
        )
        alert2 = StoredAlert(
            alert_id="alert2",
            alert_type="insufficient_evr",
            severity="critical",
            message="Alert 2",
            details={},
            timestamp=int(time.time()),
        )

        await storage.add_active_alert(alert1)
        await storage.add_active_alert(alert2)

        cleared = await storage.clear_alerts_by_type(["insufficient_satori"])
        assert cleared == 1

        alerts = await storage.get_active_alerts()
        assert len(alerts) == 1
        assert alerts[0].alert_type == "insufficient_evr"

    @pytest.mark.asyncio
    async def test_get_history(self, storage):
        """Test getting alert history."""
        for i in range(5):
            alert = StoredAlert(
                alert_id=f"alert{i}",
                alert_type=f"type{i}",
                severity="info",
                message=f"Alert {i}",
                details={},
                timestamp=int(time.time()),
            )
            await storage.add_active_alert(alert)
            await storage.resolve_alert(f"type{i}", "resolved")

        history = await storage.get_history(limit=3)
        assert len(history) == 3


class TestStorageManager:
    """Test StorageManager class."""

    @pytest.fixture
    def manager(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield StorageManager(storage_dir=Path(tmpdir))

    def test_creation(self, manager):
        """Test manager creation."""
        assert manager.deferred_rewards is not None
        assert manager.alerts is not None

    @pytest.mark.asyncio
    async def test_sync_all(self, manager):
        """Test syncing all storage."""
        # Add some data
        reward = StoredDeferredReward(
            address="EAddr1",
            round_id=100,
            amount=50.0,
            reason="test",
            created_at=int(time.time()),
        )
        await manager.deferred_rewards.add_reward(reward)

        results = await manager.sync_all()
        assert "deferred_rewards" in results
        assert "alerts" in results

    def test_get_all_stats(self, manager):
        """Test getting all stats."""
        stats = manager.get_all_stats()
        assert "deferred_rewards" in stats
        assert "alerts" in stats

    def test_register_custom_storage(self, manager):
        """Test registering custom storage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom = RedundantStorage("custom", storage_dir=Path(tmpdir), enable_dht=False)
            manager.register_storage("custom", custom)

            stats = manager.get_all_stats()
            assert "custom" in stats

    def test_set_peers_propagates(self):
        """Test that set_peers propagates to all storage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StorageManager(storage_dir=Path(tmpdir))

            mock_peers = Mock()
            mock_peers.get_dht = AsyncMock()
            mock_peers.put_dht = AsyncMock()

            manager.set_peers(mock_peers)

            # Custom storage registered after should also get peers
            custom = RedundantStorage("custom", storage_dir=Path(tmpdir))
            manager.register_storage("custom", custom)

            # Verify peers were set
            assert custom._dht._peers == mock_peers


class TestStorageEdgeCases:
    """Test edge cases for storage."""

    @pytest.mark.asyncio
    async def test_large_data(self):
        """Test storing large data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = RedundantStorage("test", storage_dir=Path(tmpdir), enable_dht=False)

            # Store 1MB of data
            large_data = {"data": "x" * (1024 * 1024)}
            await storage.put("large", large_data)

            result = await storage.get("large")
            assert len(result["data"]) == 1024 * 1024

    @pytest.mark.asyncio
    async def test_unicode_keys_and_values(self):
        """Test unicode in keys and values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = RedundantStorage("test", storage_dir=Path(tmpdir), enable_dht=False)

            await storage.put("key_日本語", {"value": "中文"})
            result = await storage.get("key_日本語")
            assert result["value"] == "中文"

    @pytest.mark.asyncio
    async def test_concurrent_writes(self):
        """Test concurrent writes don't corrupt data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = RedundantStorage("test", storage_dir=Path(tmpdir), enable_dht=False)

            async def write(i):
                await storage.put(f"key{i}", {"value": i})

            # Concurrent writes
            await asyncio.gather(*[write(i) for i in range(100)])

            # All should be readable
            for i in range(100):
                result = await storage.get(f"key{i}")
                assert result["value"] == i

    @pytest.mark.asyncio
    async def test_empty_value(self):
        """Test storing empty values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = RedundantStorage("test", storage_dir=Path(tmpdir), enable_dht=False)

            await storage.put("empty_dict", {})
            await storage.put("empty_list", [])

            assert await storage.get("empty_dict") == {}
            assert await storage.get("empty_list") == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
