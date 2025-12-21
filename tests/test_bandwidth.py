"""
satorip2p/tests/test_bandwidth.py

Tests for bandwidth accounting and QoS.
"""

import pytest
import time
import asyncio

from satorip2p.protocol.bandwidth import (
    BandwidthTracker,
    QoSManager,
    QoSPolicy,
    MessagePriority,
    BandwidthMetrics,
    WindowedCounter,
    RateLimiter,
    create_qos_manager,
    get_priority_for_message_type,
    DEFAULT_GLOBAL_BYTES_PER_SECOND,
    DEFAULT_TOPIC_BYTES_PER_SECOND,
)


class TestBandwidthMetrics:
    """Test BandwidthMetrics dataclass."""

    def test_creation(self):
        """Test creating metrics."""
        metrics = BandwidthMetrics()
        assert metrics.bytes_sent == 0
        assert metrics.bytes_received == 0
        assert metrics.messages_sent == 0
        assert metrics.messages_received == 0

    def test_record_sent(self):
        """Test recording sent messages."""
        metrics = BandwidthMetrics()
        metrics.record_sent(100)
        assert metrics.bytes_sent == 100
        assert metrics.messages_sent == 1

        metrics.record_sent(200)
        assert metrics.bytes_sent == 300
        assert metrics.messages_sent == 2

    def test_record_received(self):
        """Test recording received messages."""
        metrics = BandwidthMetrics()
        metrics.record_received(150)
        assert metrics.bytes_received == 150
        assert metrics.messages_received == 1

    def test_last_activity_updated(self):
        """Test last activity timestamp updates."""
        metrics = BandwidthMetrics()
        initial = metrics.last_activity

        time.sleep(0.01)
        metrics.record_sent(100)

        assert metrics.last_activity > initial

    def test_to_dict(self):
        """Test conversion to dictionary."""
        metrics = BandwidthMetrics()
        metrics.record_sent(100)
        metrics.record_received(50)

        data = metrics.to_dict()
        assert data["bytes_sent"] == 100
        assert data["bytes_received"] == 50
        assert data["messages_sent"] == 1
        assert data["messages_received"] == 1
        assert "last_activity" in data


class TestWindowedCounter:
    """Test WindowedCounter class."""

    def test_creation(self):
        """Test creating counter."""
        counter = WindowedCounter(1.0)
        assert counter.total() == 0

    def test_add(self):
        """Test adding to counter."""
        counter = WindowedCounter(1.0)
        counter.add(10)
        assert counter.total() == 10

    def test_multiple_adds(self):
        """Test multiple adds."""
        counter = WindowedCounter(1.0)
        counter.add(10)
        counter.add(20)
        counter.add(30)
        assert counter.total() == 60

    def test_rate(self):
        """Test rate calculation."""
        counter = WindowedCounter(1.0)
        counter.add(100)
        rate = counter.rate()
        assert rate == 100.0  # 100 per second

    def test_larger_window(self):
        """Test larger window size."""
        counter = WindowedCounter(10.0)
        counter.add(100)
        rate = counter.rate()
        assert rate == 10.0  # 100 in 10 seconds = 10/s


class TestRateLimiter:
    """Test RateLimiter class."""

    def test_creation(self):
        """Test creating rate limiter."""
        limiter = RateLimiter(rate=100, burst=200)
        assert limiter.rate == 100
        assert limiter.burst == 200
        assert limiter.available() == 200

    def test_try_consume_success(self):
        """Test successful token consumption."""
        limiter = RateLimiter(rate=100, burst=200)
        assert limiter.try_consume(50) is True
        assert limiter.available() < 200

    def test_try_consume_fail(self):
        """Test failed token consumption."""
        limiter = RateLimiter(rate=100, burst=100)
        assert limiter.try_consume(50) is True
        assert limiter.try_consume(50) is True
        assert limiter.try_consume(50) is False  # Out of tokens

    def test_refill(self):
        """Test token refill."""
        limiter = RateLimiter(rate=1000, burst=100)
        limiter.try_consume(100)  # Drain tokens
        initial = limiter.available()

        time.sleep(0.1)  # Wait for refill
        limiter.refill()

        assert limiter.available() > initial

    def test_wait_time(self):
        """Test wait time calculation."""
        limiter = RateLimiter(rate=100, burst=100)
        limiter.try_consume(100)  # Drain all

        wait = limiter.wait_time(10)
        assert wait > 0  # Should need to wait
        assert wait <= 0.1  # 10 tokens at 100/s = 0.1s


class TestBandwidthTracker:
    """Test BandwidthTracker class."""

    @pytest.fixture
    def tracker(self):
        return BandwidthTracker()

    @pytest.mark.asyncio
    async def test_account_publish(self, tracker):
        """Test accounting for published messages."""
        await tracker.account_publish("topic1", 1000)

        metrics = tracker.get_global_metrics()
        assert metrics["cumulative"]["bytes_sent"] == 1000
        assert metrics["cumulative"]["messages_sent"] == 1

    @pytest.mark.asyncio
    async def test_account_receive(self, tracker):
        """Test accounting for received messages."""
        await tracker.account_receive("topic1", 500, "peer1")

        metrics = tracker.get_global_metrics()
        assert metrics["cumulative"]["bytes_received"] == 500
        assert metrics["cumulative"]["messages_received"] == 1

    @pytest.mark.asyncio
    async def test_topic_metrics(self, tracker):
        """Test per-topic metrics."""
        await tracker.account_publish("topic1", 100)
        await tracker.account_publish("topic1", 200)
        await tracker.account_publish("topic2", 50)

        topic1 = tracker.get_topic_metrics("topic1")
        assert topic1["cumulative"]["bytes_sent"] == 300

        topic2 = tracker.get_topic_metrics("topic2")
        assert topic2["cumulative"]["bytes_sent"] == 50

    @pytest.mark.asyncio
    async def test_peer_metrics(self, tracker):
        """Test per-peer metrics."""
        await tracker.account_receive("topic1", 100, "peer1")
        await tracker.account_receive("topic1", 200, "peer1")
        await tracker.account_receive("topic1", 50, "peer2")

        peer1 = tracker.get_peer_metrics("peer1")
        assert peer1["cumulative"]["bytes_received"] == 300

        peer2 = tracker.get_peer_metrics("peer2")
        assert peer2["cumulative"]["bytes_received"] == 50

    @pytest.mark.asyncio
    async def test_rate_calculation(self, tracker):
        """Test rate calculations."""
        # Send some messages
        for _ in range(10):
            await tracker.account_publish("topic1", 100)

        metrics = tracker.get_global_metrics()
        # Should have non-zero rate
        assert metrics["rates"]["bytes_out_1s"] > 0
        assert metrics["rates"]["msgs_out_1s"] > 0

    @pytest.mark.asyncio
    async def test_top_topics(self, tracker):
        """Test getting top topics."""
        await tracker.account_publish("high_traffic", 10000)
        await tracker.account_publish("low_traffic", 100)
        await tracker.account_publish("medium_traffic", 1000)

        top = tracker.get_top_topics(3)
        assert len(top) == 3
        assert top[0][0] == "high_traffic"

    @pytest.mark.asyncio
    async def test_top_peers(self, tracker):
        """Test getting top peers."""
        await tracker.account_receive("topic", 10000, "peer1")
        await tracker.account_receive("topic", 100, "peer2")
        await tracker.account_receive("topic", 1000, "peer3")

        top = tracker.get_top_peers(3)
        assert len(top) == 3
        assert top[0][0] == "peer1"

    @pytest.mark.asyncio
    async def test_peak_tracking(self, tracker):
        """Test peak bandwidth tracking."""
        await tracker.account_publish("topic1", 100000)

        metrics = tracker.get_global_metrics()
        assert metrics["peaks"]["bytes_per_second"] > 0

    @pytest.mark.asyncio
    async def test_stats(self, tracker):
        """Test comprehensive stats."""
        await tracker.account_publish("topic1", 100)
        await tracker.account_receive("topic1", 50, "peer1")

        stats = tracker.get_stats()
        assert "global" in stats
        assert "topic_count" in stats
        assert "peer_count" in stats
        assert stats["topic_count"] == 1
        assert stats["peer_count"] == 1


class TestQoSPolicy:
    """Test QoSPolicy dataclass."""

    def test_default_policy(self):
        """Test default policy values."""
        policy = QoSPolicy()
        assert policy.max_bytes_per_second == DEFAULT_TOPIC_BYTES_PER_SECOND
        assert policy.enabled is True

    def test_custom_policy(self):
        """Test custom policy."""
        policy = QoSPolicy(
            max_bytes_per_second=500000,
            max_messages_per_second=50,
            min_priority=MessagePriority.HIGH,
        )
        assert policy.max_bytes_per_second == 500000
        assert policy.min_priority == MessagePriority.HIGH


class TestQoSManager:
    """Test QoSManager class."""

    @pytest.fixture
    def manager(self):
        tracker = BandwidthTracker()
        return QoSManager(
            tracker,
            global_bytes_limit=10000,  # 10KB/s for testing
            global_msgs_limit=100,
            enforce=True,
        )

    @pytest.mark.asyncio
    async def test_can_send_allowed(self, manager):
        """Test sending is allowed."""
        result = await manager.can_send("topic1", 100)
        assert result is True

    @pytest.mark.asyncio
    async def test_critical_always_allowed(self, manager):
        """Test critical messages always allowed."""
        # Exhaust tokens
        for _ in range(200):
            await manager.can_send("topic1", 100)

        # Critical should still work
        result = await manager.can_send("topic1", 100, MessagePriority.CRITICAL)
        assert result is True

    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """Test rate limiting kicks in."""
        # Create manager with very low limits to trigger drops quickly
        tracker = BandwidthTracker()
        manager = QoSManager(
            tracker,
            global_bytes_limit=100,  # Only 100 bytes/s
            global_msgs_limit=5,     # Only 5 msgs/s
            enforce=True,
        )

        drops = 0
        for _ in range(50):
            if not await manager.can_send("topic1", 50, MessagePriority.NORMAL):
                drops += 1

        # Should have some drops after exceeding limit
        assert drops > 0

    @pytest.mark.asyncio
    async def test_topic_policy(self, manager):
        """Test setting topic-specific policy."""
        policy = QoSPolicy(
            max_bytes_per_second=1000,
            min_priority=MessagePriority.HIGH,
        )
        manager.set_topic_policy("restricted", policy)

        # Normal priority should be rejected for this topic
        result = await manager.can_send("restricted", 100, MessagePriority.NORMAL)
        assert result is False

        # High priority should work
        result = await manager.can_send("restricted", 100, MessagePriority.HIGH)
        assert result is True

    @pytest.mark.asyncio
    async def test_dropped_stats(self, manager):
        """Test dropped message statistics."""
        # Cause some drops
        manager.set_topic_policy("test", QoSPolicy(min_priority=MessagePriority.HIGH))

        for _ in range(10):
            await manager.can_send("test", 100, MessagePriority.LOW)

        stats = manager.get_dropped_stats()
        assert stats["total_dropped"] == 10
        assert "LOW" in stats["by_priority"]

    @pytest.mark.asyncio
    async def test_reset_dropped_stats(self, manager):
        """Test resetting dropped stats."""
        manager.set_topic_policy("test", QoSPolicy(min_priority=MessagePriority.HIGH))
        await manager.can_send("test", 100, MessagePriority.LOW)

        manager.reset_dropped_stats()
        stats = manager.get_dropped_stats()
        assert stats["total_dropped"] == 0

    @pytest.mark.asyncio
    async def test_can_receive(self, manager):
        """Test receive checking."""
        result = await manager.can_receive("topic1", 100)
        assert result is True

    @pytest.mark.asyncio
    async def test_capacity_info(self, manager):
        """Test getting capacity info."""
        info = manager.get_capacity_info("topic1")
        assert "global_tokens_available" in info
        assert "topic_tokens_available" in info
        assert "current_global_rate" in info

    @pytest.mark.asyncio
    async def test_wait_for_capacity(self, manager):
        """Test waiting for capacity."""
        # Should have capacity immediately
        result = await manager.wait_for_capacity("topic1", 100, timeout=1.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_enforce_disabled(self):
        """Test with enforcement disabled."""
        tracker = BandwidthTracker()
        manager = QoSManager(tracker, enforce=False)

        # Should always allow when not enforcing
        for _ in range(1000):
            result = await manager.can_send("topic1", 10000, MessagePriority.LOW)
            assert result is True

    @pytest.mark.asyncio
    async def test_get_stats(self, manager):
        """Test getting comprehensive stats."""
        stats = manager.get_stats()
        assert "enforce" in stats
        assert "global_limits" in stats
        assert "dropped" in stats
        assert "bandwidth" in stats


class TestMessagePriority:
    """Test MessagePriority enum."""

    def test_priority_ordering(self):
        """Test priority ordering."""
        assert MessagePriority.CRITICAL < MessagePriority.HIGH
        assert MessagePriority.HIGH < MessagePriority.NORMAL
        assert MessagePriority.NORMAL < MessagePriority.LOW

    def test_priority_values(self):
        """Test priority values."""
        assert MessagePriority.CRITICAL.value == 0
        assert MessagePriority.LOW.value == 3


class TestConvenienceFunctions:
    """Test convenience functions."""

    def test_create_qos_manager(self):
        """Test creating QoS manager pair."""
        tracker, qos = create_qos_manager()
        assert isinstance(tracker, BandwidthTracker)
        assert isinstance(qos, QoSManager)

    def test_create_qos_manager_custom(self):
        """Test creating with custom limits."""
        tracker, qos = create_qos_manager(
            global_bytes_limit=5000,
            global_msgs_limit=50,
            enforce=False,
        )
        assert qos._global_bytes.rate == 5000
        assert qos.enforce is False

    def test_get_priority_critical(self):
        """Test getting priority for critical messages."""
        assert get_priority_for_message_type("heartbeat") == MessagePriority.CRITICAL
        assert get_priority_for_message_type("consensus_vote") == MessagePriority.CRITICAL

    def test_get_priority_high(self):
        """Test getting priority for high-priority messages."""
        assert get_priority_for_message_type("prediction") == MessagePriority.HIGH
        assert get_priority_for_message_type("observation") == MessagePriority.HIGH

    def test_get_priority_low(self):
        """Test getting priority for low-priority messages."""
        assert get_priority_for_message_type("peer_announce") == MessagePriority.LOW
        assert get_priority_for_message_type("discovery") == MessagePriority.LOW

    def test_get_priority_normal(self):
        """Test getting priority for unknown messages."""
        assert get_priority_for_message_type("unknown_type") == MessagePriority.NORMAL


class TestIntegration:
    """Integration tests for bandwidth and QoS."""

    @pytest.mark.asyncio
    async def test_full_workflow(self):
        """Test complete workflow."""
        tracker, qos = create_qos_manager(
            global_bytes_limit=10000,
            enforce=True,
        )

        # Simulate message flow
        for i in range(50):
            topic = f"topic{i % 5}"
            byte_size = 100 + (i * 10)
            priority = MessagePriority.NORMAL if i % 3 else MessagePriority.HIGH

            if await qos.can_send(topic, byte_size, priority):
                await tracker.account_publish(topic, byte_size)

        # Check stats
        stats = tracker.get_stats()
        assert stats["topic_count"] == 5
        assert stats["global"]["cumulative"]["messages_sent"] > 0

        qos_stats = qos.get_stats()
        assert qos_stats["bandwidth"] is not None

    @pytest.mark.asyncio
    async def test_backpressure_scenario(self):
        """Test backpressure under load."""
        tracker, qos = create_qos_manager(
            global_bytes_limit=1000,  # Very low limit
            enforce=True,
        )

        allowed = 0
        dropped = 0

        # Simulate burst of messages
        for _ in range(100):
            if await qos.can_send("topic1", 100, MessagePriority.NORMAL):
                await tracker.account_publish("topic1", 100)
                allowed += 1
            else:
                dropped += 1

        # Should have dropped some
        assert dropped > 0
        assert allowed > 0

    @pytest.mark.asyncio
    async def test_priority_fairness(self):
        """Test that priorities are respected."""
        tracker, qos = create_qos_manager(
            global_bytes_limit=500,  # Very low limit
            enforce=True,
        )

        critical_allowed = 0
        normal_dropped = 0

        for _ in range(50):
            # Try normal
            if not await qos.can_send("topic1", 100, MessagePriority.NORMAL):
                normal_dropped += 1

            # Critical always allowed
            if await qos.can_send("topic1", 100, MessagePriority.CRITICAL):
                critical_allowed += 1

        # Critical should have higher success rate
        assert critical_allowed == 50
        assert normal_dropped > 0


class TestEdgeCases:
    """Test edge cases."""

    @pytest.mark.asyncio
    async def test_zero_byte_message(self):
        """Test handling zero-byte message."""
        tracker = BandwidthTracker()
        await tracker.account_publish("topic1", 0)

        metrics = tracker.get_global_metrics()
        assert metrics["cumulative"]["messages_sent"] == 1

    @pytest.mark.asyncio
    async def test_large_message(self):
        """Test handling very large message."""
        tracker = BandwidthTracker()
        await tracker.account_publish("topic1", 100 * 1024 * 1024)  # 100MB

        metrics = tracker.get_global_metrics()
        assert metrics["cumulative"]["bytes_sent"] == 100 * 1024 * 1024

    @pytest.mark.asyncio
    async def test_many_topics(self):
        """Test with many topics."""
        tracker = BandwidthTracker()

        for i in range(1000):
            await tracker.account_publish(f"topic{i}", 100)

        stats = tracker.get_stats()
        assert stats["topic_count"] == 1000

    @pytest.mark.asyncio
    async def test_many_peers(self):
        """Test with many peers."""
        tracker = BandwidthTracker()

        for i in range(500):
            await tracker.account_receive("topic1", 100, f"peer{i}")

        stats = tracker.get_stats()
        assert stats["peer_count"] == 500

    @pytest.mark.asyncio
    async def test_no_peer_id(self):
        """Test receiving without peer ID."""
        tracker = BandwidthTracker()
        await tracker.account_receive("topic1", 100, None)

        stats = tracker.get_stats()
        assert stats["peer_count"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
