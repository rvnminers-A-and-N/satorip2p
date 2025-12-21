"""
satorip2p/protocol/bandwidth.py

Bandwidth accounting and QoS for GossipSub messaging.

Provides:
- Per-topic, per-peer, and global bandwidth tracking
- Rate limiting with token bucket algorithm
- Priority-based QoS enforcement
- Metrics export for monitoring

Usage:
    from satorip2p.protocol.bandwidth import BandwidthTracker, QoSManager

    tracker = BandwidthTracker()
    qos = QoSManager(tracker)

    # Track outgoing message
    await tracker.account_publish("stream123", 1024)

    # Track incoming message
    await tracker.account_receive("stream123", 512, "peer456")

    # Check if sending is allowed
    if await qos.can_send("stream123", MessagePriority.NORMAL):
        await publish(data)
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import IntEnum
from collections import defaultdict
import asyncio

logger = logging.getLogger("satorip2p.protocol.bandwidth")


# ============================================================================
# CONSTANTS
# ============================================================================

# Default limits
DEFAULT_GLOBAL_BYTES_PER_SECOND = 10 * 1024 * 1024  # 10 MB/s
DEFAULT_TOPIC_BYTES_PER_SECOND = 1 * 1024 * 1024    # 1 MB/s per topic
DEFAULT_PEER_BYTES_PER_SECOND = 512 * 1024          # 512 KB/s per peer

DEFAULT_GLOBAL_MESSAGES_PER_SECOND = 10000
DEFAULT_TOPIC_MESSAGES_PER_SECOND = 100
DEFAULT_PEER_MESSAGES_PER_SECOND = 50

# Accounting windows
WINDOW_SIZES = [1, 10, 60]  # 1s, 10s, 60s buckets

# Token bucket refill rate
BUCKET_REFILL_INTERVAL = 0.1  # 100ms


# ============================================================================
# ENUMS AND DATA CLASSES
# ============================================================================

class MessagePriority(IntEnum):
    """Message priority levels for QoS."""
    CRITICAL = 0  # Never dropped (heartbeats, consensus)
    HIGH = 1      # Rarely dropped (predictions, observations)
    NORMAL = 2    # Standard messages (stream data)
    LOW = 3       # Droppable (announcements, discovery)


@dataclass
class BandwidthMetrics:
    """Bandwidth metrics for a single entity (topic, peer, or global)."""
    bytes_sent: int = 0
    bytes_received: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    last_activity: float = field(default_factory=time.time)

    def record_sent(self, byte_size: int) -> None:
        """Record a sent message."""
        self.bytes_sent += byte_size
        self.messages_sent += 1
        self.last_activity = time.time()

    def record_received(self, byte_size: int) -> None:
        """Record a received message."""
        self.bytes_received += byte_size
        self.messages_received += 1
        self.last_activity = time.time()

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "last_activity": self.last_activity,
        }


@dataclass
class WindowedCounter:
    """Time-windowed counter for rate calculations."""
    window_size: float  # Window size in seconds
    buckets: Dict[int, int] = field(default_factory=dict)  # bucket_id -> count

    def _bucket_id(self) -> int:
        """Get current bucket ID."""
        return int(time.time() / self.window_size)

    def add(self, count: int = 1) -> None:
        """Add to current bucket."""
        bucket = self._bucket_id()
        self.buckets[bucket] = self.buckets.get(bucket, 0) + count
        self._cleanup()

    def _cleanup(self) -> None:
        """Remove old buckets."""
        current = self._bucket_id()
        # Keep only current and previous bucket
        self.buckets = {
            k: v for k, v in self.buckets.items()
            if k >= current - 1
        }

    def rate(self) -> float:
        """Get rate per second over window."""
        self._cleanup()
        total = sum(self.buckets.values())
        return total / self.window_size

    def total(self) -> int:
        """Get total count in window."""
        self._cleanup()
        return sum(self.buckets.values())


@dataclass
class RateLimiter:
    """Token bucket rate limiter."""
    rate: float  # Tokens per second
    burst: float  # Max bucket size
    tokens: float = field(init=False)
    last_refill: float = field(default_factory=time.time)

    def __post_init__(self):
        self.tokens = self.burst

    def refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.last_refill = now

    def try_consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if allowed."""
        self.refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def available(self) -> float:
        """Get available tokens."""
        self.refill()
        return self.tokens

    def wait_time(self, tokens: int = 1) -> float:
        """Calculate wait time for tokens to become available."""
        self.refill()
        if self.tokens >= tokens:
            return 0
        needed = tokens - self.tokens
        return needed / self.rate


# ============================================================================
# BANDWIDTH TRACKER
# ============================================================================

class BandwidthTracker:
    """
    Tracks bandwidth usage across topics, peers, and globally.

    Maintains windowed counters for rate calculations and
    cumulative metrics for total usage.
    """

    def __init__(self):
        # Cumulative metrics
        self._global = BandwidthMetrics()
        self._by_topic: Dict[str, BandwidthMetrics] = defaultdict(BandwidthMetrics)
        self._by_peer: Dict[str, BandwidthMetrics] = defaultdict(BandwidthMetrics)

        # Rate counters (1s, 10s, 60s windows)
        self._global_rate_bytes_out: Dict[int, WindowedCounter] = {
            w: WindowedCounter(w) for w in WINDOW_SIZES
        }
        self._global_rate_bytes_in: Dict[int, WindowedCounter] = {
            w: WindowedCounter(w) for w in WINDOW_SIZES
        }
        self._global_rate_msgs_out: Dict[int, WindowedCounter] = {
            w: WindowedCounter(w) for w in WINDOW_SIZES
        }
        self._global_rate_msgs_in: Dict[int, WindowedCounter] = {
            w: WindowedCounter(w) for w in WINDOW_SIZES
        }

        # Per-topic rate counters (1s window only for efficiency)
        self._topic_rate_bytes: Dict[str, WindowedCounter] = defaultdict(
            lambda: WindowedCounter(1.0)
        )
        self._topic_rate_msgs: Dict[str, WindowedCounter] = defaultdict(
            lambda: WindowedCounter(1.0)
        )

        # Per-peer rate counters
        self._peer_rate_bytes: Dict[str, WindowedCounter] = defaultdict(
            lambda: WindowedCounter(1.0)
        )

        # Peak tracking
        self._peak_bytes_per_second = 0.0
        self._peak_messages_per_second = 0.0
        self._peak_timestamp = 0

    async def account_publish(self, topic: str, byte_size: int) -> None:
        """
        Account for an outgoing published message.

        Args:
            topic: Topic/stream ID
            byte_size: Size in bytes
        """
        # Global metrics
        self._global.record_sent(byte_size)

        # Per-topic metrics
        self._by_topic[topic].record_sent(byte_size)

        # Rate counters
        for counter in self._global_rate_bytes_out.values():
            counter.add(byte_size)
        for counter in self._global_rate_msgs_out.values():
            counter.add(1)

        self._topic_rate_bytes[topic].add(byte_size)
        self._topic_rate_msgs[topic].add(1)

        # Update peaks
        self._update_peaks()

    async def account_receive(
        self,
        topic: str,
        byte_size: int,
        peer_id: Optional[str] = None,
    ) -> None:
        """
        Account for an incoming received message.

        Args:
            topic: Topic/stream ID
            byte_size: Size in bytes
            peer_id: Sending peer ID (optional)
        """
        # Global metrics
        self._global.record_received(byte_size)

        # Per-topic metrics
        self._by_topic[topic].record_received(byte_size)

        # Per-peer metrics
        if peer_id:
            self._by_peer[peer_id].record_received(byte_size)
            self._peer_rate_bytes[peer_id].add(byte_size)

        # Rate counters
        for counter in self._global_rate_bytes_in.values():
            counter.add(byte_size)
        for counter in self._global_rate_msgs_in.values():
            counter.add(1)

        # Update peaks
        self._update_peaks()

    def _update_peaks(self) -> None:
        """Update peak metrics."""
        bytes_rate = self._global_rate_bytes_out[1].rate() + self._global_rate_bytes_in[1].rate()
        msgs_rate = self._global_rate_msgs_out[1].rate() + self._global_rate_msgs_in[1].rate()

        if bytes_rate > self._peak_bytes_per_second:
            self._peak_bytes_per_second = bytes_rate
            self._peak_timestamp = time.time()

        if msgs_rate > self._peak_messages_per_second:
            self._peak_messages_per_second = msgs_rate

    def get_global_metrics(self) -> dict:
        """Get global bandwidth metrics."""
        return {
            "cumulative": self._global.to_dict(),
            "rates": {
                "bytes_out_1s": self._global_rate_bytes_out[1].rate(),
                "bytes_out_10s": self._global_rate_bytes_out[10].rate(),
                "bytes_out_60s": self._global_rate_bytes_out[60].rate(),
                "bytes_in_1s": self._global_rate_bytes_in[1].rate(),
                "bytes_in_10s": self._global_rate_bytes_in[10].rate(),
                "bytes_in_60s": self._global_rate_bytes_in[60].rate(),
                "msgs_out_1s": self._global_rate_msgs_out[1].rate(),
                "msgs_in_1s": self._global_rate_msgs_in[1].rate(),
            },
            "peaks": {
                "bytes_per_second": self._peak_bytes_per_second,
                "messages_per_second": self._peak_messages_per_second,
                "peak_timestamp": self._peak_timestamp,
            },
        }

    def get_topic_metrics(self, topic: str) -> dict:
        """Get metrics for a specific topic."""
        metrics = self._by_topic.get(topic, BandwidthMetrics())
        return {
            "cumulative": metrics.to_dict(),
            "rates": {
                "bytes_1s": self._topic_rate_bytes[topic].rate(),
                "msgs_1s": self._topic_rate_msgs[topic].rate(),
            },
        }

    def get_peer_metrics(self, peer_id: str) -> dict:
        """Get metrics for a specific peer."""
        metrics = self._by_peer.get(peer_id, BandwidthMetrics())
        return {
            "cumulative": metrics.to_dict(),
            "rates": {
                "bytes_1s": self._peer_rate_bytes[peer_id].rate(),
            },
        }

    def get_all_topic_rates(self) -> Dict[str, float]:
        """Get current bytes/second for all topics."""
        return {
            topic: counter.rate()
            for topic, counter in self._topic_rate_bytes.items()
        }

    def get_top_topics(self, limit: int = 10) -> List[Tuple[str, float]]:
        """Get top topics by bandwidth usage."""
        rates = self.get_all_topic_rates()
        sorted_topics = sorted(rates.items(), key=lambda x: x[1], reverse=True)
        return sorted_topics[:limit]

    def get_top_peers(self, limit: int = 10) -> List[Tuple[str, float]]:
        """Get top peers by bandwidth usage."""
        rates = {
            peer: counter.rate()
            for peer, counter in self._peer_rate_bytes.items()
        }
        sorted_peers = sorted(rates.items(), key=lambda x: x[1], reverse=True)
        return sorted_peers[:limit]

    def get_stats(self) -> dict:
        """Get comprehensive bandwidth statistics."""
        return {
            "global": self.get_global_metrics(),
            "topic_count": len(self._by_topic),
            "peer_count": len(self._by_peer),
            "top_topics": self.get_top_topics(5),
            "top_peers": self.get_top_peers(5),
        }


# ============================================================================
# QOS MANAGER
# ============================================================================

@dataclass
class QoSPolicy:
    """QoS policy for a topic or globally."""
    max_bytes_per_second: float = DEFAULT_TOPIC_BYTES_PER_SECOND
    max_messages_per_second: float = DEFAULT_TOPIC_MESSAGES_PER_SECOND
    min_priority: MessagePriority = MessagePriority.LOW
    enabled: bool = True


class QoSManager:
    """
    Manages Quality of Service for GossipSub messaging.

    Provides:
    - Rate limiting (token bucket)
    - Priority-based message dropping
    - Per-topic and global policies
    - Backpressure signaling
    """

    def __init__(
        self,
        tracker: BandwidthTracker,
        global_bytes_limit: float = DEFAULT_GLOBAL_BYTES_PER_SECOND,
        global_msgs_limit: float = DEFAULT_GLOBAL_MESSAGES_PER_SECOND,
        enforce: bool = True,
    ):
        """
        Initialize QoS manager.

        Args:
            tracker: BandwidthTracker instance
            global_bytes_limit: Global bytes/second limit
            global_msgs_limit: Global messages/second limit
            enforce: Whether to enforce limits (False = monitor only)
        """
        self.tracker = tracker
        self.enforce = enforce

        # Global rate limiters
        self._global_bytes = RateLimiter(
            rate=global_bytes_limit,
            burst=global_bytes_limit * 2,  # Allow 2x burst
        )
        self._global_msgs = RateLimiter(
            rate=global_msgs_limit,
            burst=global_msgs_limit * 2,
        )

        # Per-topic rate limiters
        self._topic_limiters: Dict[str, RateLimiter] = {}

        # Per-topic policies
        self._topic_policies: Dict[str, QoSPolicy] = {}

        # Default policy
        self._default_policy = QoSPolicy()

        # Dropped message counters
        self._dropped_by_topic: Dict[str, int] = defaultdict(int)
        self._dropped_by_priority: Dict[MessagePriority, int] = defaultdict(int)
        self._total_dropped = 0

    def set_topic_policy(self, topic: str, policy: QoSPolicy) -> None:
        """Set QoS policy for a topic."""
        self._topic_policies[topic] = policy

        # Create/update rate limiter
        self._topic_limiters[topic] = RateLimiter(
            rate=policy.max_bytes_per_second,
            burst=policy.max_bytes_per_second * 2,
        )

    def get_topic_policy(self, topic: str) -> QoSPolicy:
        """Get QoS policy for a topic."""
        return self._topic_policies.get(topic, self._default_policy)

    def _get_topic_limiter(self, topic: str) -> RateLimiter:
        """Get or create rate limiter for topic."""
        if topic not in self._topic_limiters:
            policy = self.get_topic_policy(topic)
            self._topic_limiters[topic] = RateLimiter(
                rate=policy.max_bytes_per_second,
                burst=policy.max_bytes_per_second * 2,
            )
        return self._topic_limiters[topic]

    async def can_send(
        self,
        topic: str,
        byte_size: int,
        priority: MessagePriority = MessagePriority.NORMAL,
    ) -> bool:
        """
        Check if sending is allowed under QoS policy.

        Args:
            topic: Topic/stream ID
            byte_size: Message size in bytes
            priority: Message priority

        Returns:
            True if sending is allowed
        """
        if not self.enforce:
            return True

        policy = self.get_topic_policy(topic)

        # Always allow critical messages
        if priority == MessagePriority.CRITICAL:
            return True

        # Check priority threshold
        if priority > policy.min_priority:
            self._record_drop(topic, priority, "priority_threshold")
            return False

        # Check global limits
        if not self._global_bytes.try_consume(byte_size):
            if priority >= MessagePriority.NORMAL:
                self._record_drop(topic, priority, "global_bytes_limit")
                return False

        if not self._global_msgs.try_consume(1):
            if priority >= MessagePriority.NORMAL:
                self._record_drop(topic, priority, "global_msgs_limit")
                return False

        # Check topic-specific limits
        topic_limiter = self._get_topic_limiter(topic)
        if not topic_limiter.try_consume(byte_size):
            if priority >= MessagePriority.NORMAL:
                self._record_drop(topic, priority, "topic_bytes_limit")
                return False

        return True

    async def can_receive(
        self,
        topic: str,
        byte_size: int,
        peer_id: Optional[str] = None,
        priority: MessagePriority = MessagePriority.NORMAL,
    ) -> bool:
        """
        Check if receiving is allowed under QoS policy.

        Used to implement backpressure on incoming messages.

        Args:
            topic: Topic/stream ID
            byte_size: Message size in bytes
            peer_id: Sending peer ID
            priority: Message priority

        Returns:
            True if processing is allowed
        """
        if not self.enforce:
            return True

        # Always process critical messages
        if priority == MessagePriority.CRITICAL:
            return True

        # Check if we're under pressure globally
        global_rate = (
            self.tracker._global_rate_bytes_in[1].rate() +
            self.tracker._global_rate_bytes_out[1].rate()
        )

        # If over 80% capacity, start dropping low priority
        capacity = self._global_bytes.rate
        if global_rate > capacity * 0.8:
            if priority >= MessagePriority.LOW:
                self._record_drop(topic, priority, "backpressure")
                return False

        # If over 95% capacity, also drop normal
        if global_rate > capacity * 0.95:
            if priority >= MessagePriority.NORMAL:
                self._record_drop(topic, priority, "backpressure_critical")
                return False

        return True

    def _record_drop(
        self,
        topic: str,
        priority: MessagePriority,
        reason: str,
    ) -> None:
        """Record a dropped message."""
        self._dropped_by_topic[topic] += 1
        self._dropped_by_priority[priority] += 1
        self._total_dropped += 1

        logger.debug(
            f"QoS drop: topic={topic}, priority={priority.name}, reason={reason}"
        )

    async def wait_for_capacity(
        self,
        topic: str,
        byte_size: int,
        timeout: float = 5.0,
    ) -> bool:
        """
        Wait until capacity is available.

        Args:
            topic: Topic/stream ID
            byte_size: Required bytes
            timeout: Max wait time in seconds

        Returns:
            True if capacity became available
        """
        start = time.time()

        while time.time() - start < timeout:
            if await self.can_send(topic, byte_size, MessagePriority.CRITICAL):
                return True

            # Calculate wait time
            global_wait = self._global_bytes.wait_time(byte_size)
            topic_wait = self._get_topic_limiter(topic).wait_time(byte_size)
            wait = max(global_wait, topic_wait, 0.01)

            await asyncio.sleep(min(wait, timeout - (time.time() - start)))

        return False

    def get_capacity_info(self, topic: str) -> dict:
        """Get current capacity information."""
        topic_limiter = self._get_topic_limiter(topic)

        return {
            "global_tokens_available": self._global_bytes.available(),
            "global_rate_limit": self._global_bytes.rate,
            "topic_tokens_available": topic_limiter.available(),
            "topic_rate_limit": topic_limiter.rate,
            "current_global_rate": (
                self.tracker._global_rate_bytes_out[1].rate() +
                self.tracker._global_rate_bytes_in[1].rate()
            ),
        }

    def get_dropped_stats(self) -> dict:
        """Get dropped message statistics."""
        return {
            "total_dropped": self._total_dropped,
            "by_topic": dict(self._dropped_by_topic),
            "by_priority": {p.name: c for p, c in self._dropped_by_priority.items()},
        }

    def reset_dropped_stats(self) -> None:
        """Reset dropped message counters."""
        self._dropped_by_topic.clear()
        self._dropped_by_priority.clear()
        self._total_dropped = 0

    def get_stats(self) -> dict:
        """Get comprehensive QoS statistics."""
        return {
            "enforce": self.enforce,
            "global_limits": {
                "bytes_per_second": self._global_bytes.rate,
                "messages_per_second": self._global_msgs.rate,
            },
            "topic_policies": len(self._topic_policies),
            "dropped": self.get_dropped_stats(),
            "bandwidth": self.tracker.get_stats(),
        }


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_qos_manager(
    global_bytes_limit: float = DEFAULT_GLOBAL_BYTES_PER_SECOND,
    global_msgs_limit: float = DEFAULT_GLOBAL_MESSAGES_PER_SECOND,
    enforce: bool = True,
) -> Tuple[BandwidthTracker, QoSManager]:
    """
    Create a BandwidthTracker and QoSManager pair.

    Args:
        global_bytes_limit: Global bytes/second limit
        global_msgs_limit: Global messages/second limit
        enforce: Whether to enforce limits

    Returns:
        Tuple of (BandwidthTracker, QoSManager)
    """
    tracker = BandwidthTracker()
    qos = QoSManager(
        tracker,
        global_bytes_limit=global_bytes_limit,
        global_msgs_limit=global_msgs_limit,
        enforce=enforce,
    )
    return tracker, qos


def get_priority_for_message_type(message_type: str) -> MessagePriority:
    """
    Get recommended priority for a message type.

    Args:
        message_type: Type of message

    Returns:
        Recommended MessagePriority
    """
    critical_types = {
        "heartbeat",
        "consensus_vote",
        "signature_request",
        "signature_response",
    }
    high_types = {
        "prediction",
        "observation",
        "oracle_data",
        "reward_claim",
    }
    low_types = {
        "peer_announce",
        "subscription_announce",
        "discovery",
    }

    if message_type in critical_types:
        return MessagePriority.CRITICAL
    elif message_type in high_types:
        return MessagePriority.HIGH
    elif message_type in low_types:
        return MessagePriority.LOW
    else:
        return MessagePriority.NORMAL
