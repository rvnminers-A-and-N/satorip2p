"""
satorip2p/tests/test_load.py

Load tests for high message volume and stress testing.

Run with: pytest tests/test_load.py -v --timeout=300
Skip with: pytest tests/ -v --ignore=tests/test_load.py
"""

import pytest
import time
import asyncio
import statistics
from unittest.mock import Mock, AsyncMock, MagicMock
from typing import List, Dict, Any
from dataclasses import dataclass, field


class MockEvrmoreIdentity:
    """Mock Evrmore identity for tests."""

    def __init__(self, seed: int = 1):
        self.address = f"ETestAddress{seed:032d}"
        self.pubkey = "02" + f"{seed:02x}" * 32
        self._entropy = bytes([seed % 256] * 32)

    def sign(self, msg: str) -> bytes:
        return b"signature"

    def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3


@dataclass
class LoadTestResult:
    """Results from a load test."""
    total_operations: int
    successful_operations: int
    failed_operations: int
    duration_seconds: float
    operations_per_second: float
    latencies: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.total_operations == 0:
            return 0.0
        return self.successful_operations / self.total_operations * 100

    @property
    def avg_latency(self) -> float:
        if not self.latencies:
            return 0.0
        return statistics.mean(self.latencies)

    @property
    def p95_latency(self) -> float:
        if not self.latencies:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[idx]

    @property
    def p99_latency(self) -> float:
        if not self.latencies:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.99)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]


class TestSubscriptionLoad:
    """Load tests for subscription operations."""

    def test_high_volume_subscriptions(self):
        """Test handling many subscriptions."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=1)
        peers = Peers(identity=identity, bootstrap_peers=[])

        num_subscriptions = 1000
        callbacks = []

        start = time.time()

        # Subscribe to many streams
        for i in range(num_subscriptions):
            stream_id = f"stream-{i:05d}"

            def callback(sid, data, idx=i):
                pass

            callbacks.append(callback)
            peers.subscribe(stream_id, callback)

        subscribe_time = time.time() - start

        # Verify all subscriptions
        my_subs = peers.get_my_subscriptions()
        assert len(my_subs) == num_subscriptions

        # Unsubscribe from all
        start = time.time()
        for i in range(num_subscriptions):
            stream_id = f"stream-{i:05d}"
            peers.unsubscribe(stream_id)

        unsubscribe_time = time.time() - start

        # Verify all unsubscribed
        assert len(peers.get_my_subscriptions()) == 0

        # Performance assertions
        assert subscribe_time < 5.0, f"Subscribing took too long: {subscribe_time}s"
        assert unsubscribe_time < 5.0, f"Unsubscribing took too long: {unsubscribe_time}s"

        print(f"\nSubscription Load Test Results:")
        print(f"  Subscriptions: {num_subscriptions}")
        print(f"  Subscribe time: {subscribe_time:.3f}s ({num_subscriptions/subscribe_time:.0f} ops/s)")
        print(f"  Unsubscribe time: {unsubscribe_time:.3f}s ({num_subscriptions/unsubscribe_time:.0f} ops/s)")

    def test_subscription_callback_stress(self):
        """Test rapid callback invocations."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=2)
        peers = Peers(identity=identity, bootstrap_peers=[])

        received_count = 0

        def callback(stream_id, data):
            nonlocal received_count
            received_count += 1

        stream_id = "stress-stream"
        peers.subscribe(stream_id, callback)

        # Simulate rapid message delivery
        num_messages = 10000
        start = time.time()

        for i in range(num_messages):
            peers._handle_stream_data(stream_id, {"data": f"message-{i}"})

        duration = time.time() - start

        assert received_count == num_messages
        assert duration < 2.0, f"Callback handling too slow: {duration}s"

        print(f"\nCallback Stress Test Results:")
        print(f"  Messages: {num_messages}")
        print(f"  Duration: {duration:.3f}s")
        print(f"  Rate: {num_messages/duration:.0f} msg/s")


class TestMessageStoreLoad:
    """Load tests for MessageStore."""

    def test_high_volume_message_storage(self):
        """Test storing many messages."""
        from satorip2p.protocol.message_store import MessageStore
        from satorip2p.config import MessageEnvelope

        store = MessageStore(host=None, dht=None, max_queue_size=10000)

        num_peers = 100
        messages_per_peer = 100
        total_messages = num_peers * messages_per_peer

        start = time.time()

        # Store messages for many peers
        for peer_idx in range(num_peers):
            peer_id = f"peer-{peer_idx:05d}"
            for msg_idx in range(messages_per_peer):
                envelope = MessageEnvelope(
                    message_id=f"msg-{peer_idx}-{msg_idx}",
                    target_peer=peer_id,
                    source_peer="source-peer",
                    stream_id="test-stream",
                    payload=f"Message {msg_idx} for peer {peer_idx}".encode(),
                )
                store._store_locally(envelope)

        store_time = time.time() - start

        # Verify storage
        stats = store.get_stats()
        assert stats["peers_with_pending"] == num_peers
        assert stats["total_pending_messages"] == total_messages

        # Test cleanup performance
        start = time.time()
        store.clear()
        clear_time = time.time() - start

        assert stats["total_pending_messages"] == total_messages  # Before clear
        assert store.get_stats()["total_pending_messages"] == 0  # After clear

        print(f"\nMessageStore Load Test Results:")
        print(f"  Messages stored: {total_messages}")
        print(f"  Store time: {store_time:.3f}s ({total_messages/store_time:.0f} msg/s)")
        print(f"  Clear time: {clear_time:.3f}s")

    def test_message_expiration_cleanup(self):
        """Test cleanup of expired messages."""
        from satorip2p.protocol.message_store import MessageStore
        from satorip2p.config import MessageEnvelope
        import time as time_module

        store = MessageStore(host=None, dht=None, max_age_hours=0)  # Immediate expiration

        # Store messages
        num_messages = 1000
        for i in range(num_messages):
            envelope = MessageEnvelope(
                message_id=f"msg-{i}",
                target_peer="target",
                source_peer="source",
                stream_id="test-stream",
                payload=b"test",
                ttl_hours=0,  # Expired immediately
            )
            # Manually set timestamp to past
            envelope.timestamp = time_module.time() - 3600  # 1 hour ago
            store._local_store["target"].append(envelope)

        assert store.get_stats()["total_pending_messages"] == num_messages

        # Cleanup
        start = time_module.time()
        removed = store.cleanup_expired()
        cleanup_time = time_module.time() - start

        assert removed == num_messages
        assert store.get_stats()["total_pending_messages"] == 0

        print(f"\nExpiration Cleanup Test Results:")
        print(f"  Messages: {num_messages}")
        print(f"  Cleanup time: {cleanup_time:.3f}s ({num_messages/cleanup_time:.0f} msg/s)")


class TestMetricsLoad:
    """Load tests for metrics collection."""

    def test_high_frequency_metric_recording(self):
        """Test recording metrics at high frequency."""
        from satorip2p.metrics import MetricsCollector

        # Create mock peers
        mock_peers = Mock()
        mock_peers.connected_peers = 10
        mock_peers._peer_info = {}
        mock_peers.nat_type = "PUBLIC"
        mock_peers.enable_relay = True
        mock_peers._dht = Mock()
        mock_peers._dht.get_routing_table_size.return_value = 50
        mock_peers.get_my_subscriptions.return_value = ["s1", "s2"]
        mock_peers.get_my_publications.return_value = ["p1"]
        mock_peers.peer_id = "test-peer-id"

        metrics = MetricsCollector(mock_peers)

        num_operations = 100000
        start = time.time()

        # Record many operations
        for i in range(num_operations):
            if i % 3 == 0:
                metrics.record_message_sent(100)
            elif i % 3 == 1:
                metrics.record_message_received(150)
            else:
                metrics.record_ping_latency(0.05)

        record_time = time.time() - start

        # Verify counts: for 100000 operations split by i % 3:
        # i % 3 == 0: indices 0, 3, 6, ..., 99999 -> 33334 items
        # i % 3 == 1: indices 1, 4, 7, ..., 99997 -> 33333 items
        # i % 3 == 2: indices 2, 5, 8, ..., 99998 -> 33333 items
        expected_sent = (num_operations + 2) // 3  # 33334
        expected_received = num_operations // 3     # 33333
        assert metrics._messages_sent == expected_sent
        assert metrics._messages_received == expected_received

        # Test collection performance
        start = time.time()
        for _ in range(100):
            output = metrics.collect()
        collect_time = time.time() - start

        assert len(output) > 100  # Should have substantial output

        print(f"\nMetrics Load Test Results:")
        print(f"  Operations: {num_operations}")
        print(f"  Record time: {record_time:.3f}s ({num_operations/record_time:.0f} ops/s)")
        print(f"  Collect time (100x): {collect_time:.3f}s ({100/collect_time:.0f} collections/s)")


class TestAPILoad:
    """Load tests for REST API handlers."""

    async def test_concurrent_api_requests(self):
        """Test handling many concurrent API requests."""
        from satorip2p.api import PeersAPI, Request

        # Create mock peers
        mock_peers = Mock()
        mock_peers._started = True
        mock_peers.is_connected = True
        mock_peers.peer_id = "test-peer"
        mock_peers.evrmore_address = "ETestAddress"
        mock_peers.public_key = "02" + "ab" * 32
        mock_peers.public_addresses = ["/ip4/127.0.0.1/tcp/4001"]
        mock_peers.nat_type = "PUBLIC"
        mock_peers.is_relay = True
        mock_peers.connected_peers = 5
        mock_peers.enable_dht = True
        mock_peers.enable_pubsub = True
        mock_peers.enable_relay = True
        mock_peers.enable_rendezvous = True
        mock_peers.enable_mdns = True
        mock_peers.enable_ping = True
        mock_peers.enable_autonat = True
        mock_peers.enable_identify = True
        mock_peers.enable_quic = False
        mock_peers.enable_websocket = False
        mock_peers._peer_info = {}
        mock_peers.get_my_subscriptions.return_value = ["s1"]
        mock_peers.get_my_publications.return_value = []
        mock_peers.get_connected_peers.return_value = ["p1", "p2"]
        mock_peers.get_peer_subscriptions.return_value = []
        mock_peers.get_network_map.return_value = {"self": {}}

        api = PeersAPI(mock_peers, enable_metrics=True)

        num_requests = 1000
        latencies = []

        request = Request(
            method="GET",
            path="/status",
            query={},
            headers={},
            body=b"",
        )

        start = time.time()

        for _ in range(num_requests):
            req_start = time.time()
            response = await api._handle_status(request)
            latencies.append(time.time() - req_start)
            assert response.status == 200

        total_time = time.time() - start

        result = LoadTestResult(
            total_operations=num_requests,
            successful_operations=num_requests,
            failed_operations=0,
            duration_seconds=total_time,
            operations_per_second=num_requests / total_time,
            latencies=latencies,
        )

        assert result.success_rate == 100.0
        assert result.operations_per_second > 1000  # Should handle >1k req/s

        print(f"\nAPI Load Test Results:")
        print(f"  Requests: {result.total_operations}")
        print(f"  Duration: {result.duration_seconds:.3f}s")
        print(f"  Rate: {result.operations_per_second:.0f} req/s")
        print(f"  Avg latency: {result.avg_latency*1000:.2f}ms")
        print(f"  P95 latency: {result.p95_latency*1000:.2f}ms")
        print(f"  P99 latency: {result.p99_latency*1000:.2f}ms")


class TestNetworkMapLoad:
    """Load tests for network map operations."""

    def test_large_network_map(self):
        """Test network map with many peers and streams."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=100)
        peers = Peers(identity=identity, bootstrap_peers=[])

        # Add many subscriptions and publications
        num_streams = 500
        for i in range(num_streams):
            peers.subscribe(f"sub-stream-{i}", lambda s, d: None)
            peers._my_publications.add(f"pub-stream-{i}")

        # Add many known peers
        from satorip2p.config import PeerInfo
        for i in range(1000):
            peers._peer_info[f"peer-{i}"] = PeerInfo(
                peer_id=f"peer-{i}",
                evrmore_address=f"EAddr{i}",
                public_key="02" + "ab" * 32,
                addresses=[f"/ip4/10.0.0.{i % 256}/tcp/4001"],
                nat_type="PRIVATE",
                is_relay=False,
            )

        # Generate network map
        start = time.time()
        for _ in range(100):
            network_map = peers.get_network_map()
        map_time = time.time() - start

        assert len(network_map["my_subscriptions"]) == num_streams
        assert len(network_map["my_publications"]) == num_streams
        assert network_map["known_peers"] == 1000

        print(f"\nNetwork Map Load Test Results:")
        print(f"  Streams: {num_streams * 2}")
        print(f"  Known peers: 1000")
        print(f"  Map generation (100x): {map_time:.3f}s ({100/map_time:.0f} maps/s)")


class TestMemoryUsage:
    """Tests for memory usage patterns."""

    def test_subscription_memory_cleanup(self):
        """Test that unsubscribing properly releases memory."""
        from satorip2p.peers import Peers
        import sys

        identity = MockEvrmoreIdentity(seed=200)
        peers = Peers(identity=identity, bootstrap_peers=[])

        # Subscribe to many streams
        num_streams = 1000
        for i in range(num_streams):
            peers.subscribe(f"stream-{i}", lambda s, d: None)

        # Check memory after subscribing
        subscriptions_size = sys.getsizeof(peers._callbacks)

        # Unsubscribe from all
        for i in range(num_streams):
            peers.unsubscribe(f"stream-{i}")

        # Verify cleanup
        assert len(peers._callbacks) == 0
        assert len(peers._my_subscriptions) == 0

    def test_message_store_memory_limit(self):
        """Test that message store respects queue size limit."""
        from satorip2p.protocol.message_store import MessageStore
        from satorip2p.config import MessageEnvelope

        max_queue = 100
        store = MessageStore(host=None, dht=None, max_queue_size=max_queue)

        # Try to store more than max
        target_peer = "target"
        for i in range(max_queue * 2):
            envelope = MessageEnvelope(
                message_id=f"msg-{i}",
                target_peer=target_peer,
                source_peer="source",
                stream_id="test-stream",
                payload=b"test" * 100,
            )
            store._store_locally(envelope)

        # Should be capped at max_queue
        assert len(store._local_store[target_peer]) == max_queue

        print(f"\nMemory Limit Test Results:")
        print(f"  Max queue: {max_queue}")
        print(f"  Stored: {len(store._local_store[target_peer])}")
        print(f"  Overflow handled correctly: Yes")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--timeout=300"])
