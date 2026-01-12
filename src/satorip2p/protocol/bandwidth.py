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


# ============================================================================
# BANDWIDTH ALERTS
# ============================================================================

class BandwidthAlertLevel(IntEnum):
    """Severity levels for bandwidth alerts."""
    INFO = 0           # Informational
    WARNING = 1        # Approaching limits
    CRITICAL = 2       # At or over limits
    THROTTLED = 3      # Actively throttling


@dataclass
class BandwidthAlert:
    """A bandwidth usage alert."""
    alert_id: str
    level: BandwidthAlertLevel
    alert_type: str             # "global_capacity", "peer_abuse", "topic_spike", etc.
    message: str
    timestamp: float
    details: Dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "alert_id": self.alert_id,
            "level": self.level.name,
            "alert_type": self.alert_type,
            "message": self.message,
            "timestamp": self.timestamp,
            "details": self.details,
        }


class BandwidthAlertManager:
    """
    Monitors bandwidth usage and generates alerts.

    Detects:
    - Approaching global capacity limits
    - Per-peer abuse (excessive bandwidth from single peer)
    - Topic spikes (sudden increases in topic bandwidth)
    - Sustained high usage
    """

    def __init__(
        self,
        tracker: BandwidthTracker,
        qos: QoSManager,
        warning_threshold: float = 0.70,    # 70% capacity = warning
        critical_threshold: float = 0.90,   # 90% capacity = critical
        peer_abuse_threshold: float = 0.20,  # Single peer using 20%+ of bandwidth
    ):
        """
        Initialize the alert manager.

        Args:
            tracker: BandwidthTracker instance
            qos: QoSManager instance
            warning_threshold: Capacity ratio to trigger warning
            critical_threshold: Capacity ratio to trigger critical
            peer_abuse_threshold: Per-peer ratio to trigger abuse alert
        """
        self.tracker = tracker
        self.qos = qos
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.peer_abuse_threshold = peer_abuse_threshold

        # Alert history
        self._alerts: List[BandwidthAlert] = []
        self._max_alerts = 100

        # Alert deduplication
        self._last_alert_by_type: Dict[str, float] = {}
        self._alert_cooldown = 60.0  # Seconds between same alert type

        # Callbacks
        self._on_alert_callbacks: List[callable] = []

        # Monitoring state
        self._monitoring = False
        self._last_check = 0.0

        # Baseline for spike detection
        self._topic_baselines: Dict[str, float] = {}

        # Alert counter for IDs
        self._alert_counter = 0

    def _generate_alert_id(self) -> str:
        """Generate unique alert ID."""
        self._alert_counter += 1
        return f"bw-{int(time.time())}-{self._alert_counter}"

    def _should_alert(self, alert_type: str) -> bool:
        """Check if we should generate this alert type (respecting cooldown)."""
        now = time.time()
        last = self._last_alert_by_type.get(alert_type, 0)
        if now - last < self._alert_cooldown:
            return False
        self._last_alert_by_type[alert_type] = now
        return True

    def _emit_alert(self, alert: BandwidthAlert) -> None:
        """Emit an alert."""
        self._alerts.append(alert)

        # Trim history
        if len(self._alerts) > self._max_alerts:
            self._alerts = self._alerts[-self._max_alerts:]

        # Log
        if alert.level >= BandwidthAlertLevel.CRITICAL:
            logger.warning(f"Bandwidth alert: {alert.message}")
        else:
            logger.info(f"Bandwidth alert: {alert.message}")

        # Callbacks
        for callback in self._on_alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.debug(f"Alert callback error: {e}")

    def check_global_capacity(self) -> Optional[BandwidthAlert]:
        """Check global bandwidth capacity and generate alerts if needed."""
        metrics = self.tracker.get_global_metrics()
        rates = metrics["rates"]

        # Calculate total rate (in + out)
        current_rate = rates["bytes_out_1s"] + rates["bytes_in_1s"]
        capacity = self.qos._global_bytes.rate

        ratio = current_rate / capacity if capacity > 0 else 0

        if ratio >= self.critical_threshold:
            if self._should_alert("global_critical"):
                alert = BandwidthAlert(
                    alert_id=self._generate_alert_id(),
                    level=BandwidthAlertLevel.CRITICAL,
                    alert_type="global_capacity",
                    message=f"Bandwidth CRITICAL: {ratio*100:.1f}% capacity ({current_rate/1024:.1f} KB/s)",
                    timestamp=time.time(),
                    details={
                        "current_rate_bytes": current_rate,
                        "capacity_bytes": capacity,
                        "utilization_percent": ratio * 100,
                    },
                )
                self._emit_alert(alert)
                return alert

        elif ratio >= self.warning_threshold:
            if self._should_alert("global_warning"):
                alert = BandwidthAlert(
                    alert_id=self._generate_alert_id(),
                    level=BandwidthAlertLevel.WARNING,
                    alert_type="global_capacity",
                    message=f"Bandwidth WARNING: {ratio*100:.1f}% capacity ({current_rate/1024:.1f} KB/s)",
                    timestamp=time.time(),
                    details={
                        "current_rate_bytes": current_rate,
                        "capacity_bytes": capacity,
                        "utilization_percent": ratio * 100,
                    },
                )
                self._emit_alert(alert)
                return alert

        return None

    def check_peer_abuse(self) -> List[BandwidthAlert]:
        """Check for peers using excessive bandwidth."""
        alerts = []
        metrics = self.tracker.get_global_metrics()
        total_rate = metrics["rates"]["bytes_in_1s"]

        if total_rate == 0:
            return alerts

        for peer_id, rate in self.tracker.get_top_peers(10):
            if rate == 0:
                continue

            ratio = rate / total_rate
            if ratio >= self.peer_abuse_threshold:
                alert_type = f"peer_abuse_{peer_id[:16]}"
                if self._should_alert(alert_type):
                    alert = BandwidthAlert(
                        alert_id=self._generate_alert_id(),
                        level=BandwidthAlertLevel.WARNING,
                        alert_type="peer_abuse",
                        message=f"Peer {peer_id[:16]}... using {ratio*100:.1f}% of bandwidth ({rate/1024:.1f} KB/s)",
                        timestamp=time.time(),
                        details={
                            "peer_id": peer_id,
                            "peer_rate_bytes": rate,
                            "total_rate_bytes": total_rate,
                            "percent_of_total": ratio * 100,
                        },
                    )
                    self._emit_alert(alert)
                    alerts.append(alert)

        return alerts

    def check_topic_spikes(self, spike_multiplier: float = 3.0) -> List[BandwidthAlert]:
        """
        Check for sudden spikes in topic bandwidth.

        Args:
            spike_multiplier: Rate must be this many times baseline to alert
        """
        alerts = []

        for topic, rate in self.tracker.get_all_topic_rates().items():
            baseline = self._topic_baselines.get(topic, rate)

            # Update baseline with exponential moving average
            alpha = 0.1
            self._topic_baselines[topic] = alpha * rate + (1 - alpha) * baseline

            # Check for spike
            if baseline > 0 and rate > baseline * spike_multiplier:
                alert_type = f"topic_spike_{topic}"
                if self._should_alert(alert_type):
                    alert = BandwidthAlert(
                        alert_id=self._generate_alert_id(),
                        level=BandwidthAlertLevel.INFO,
                        alert_type="topic_spike",
                        message=f"Topic '{topic[:32]}' spike: {rate/1024:.1f} KB/s ({rate/baseline:.1f}x baseline)",
                        timestamp=time.time(),
                        details={
                            "topic": topic,
                            "current_rate": rate,
                            "baseline_rate": baseline,
                            "spike_ratio": rate / baseline if baseline > 0 else 0,
                        },
                    )
                    self._emit_alert(alert)
                    alerts.append(alert)

        return alerts

    def check_dropped_messages(self) -> Optional[BandwidthAlert]:
        """Check if QoS is actively dropping messages."""
        dropped = self.qos.get_dropped_stats()
        total_dropped = dropped["total_dropped"]

        # Alert if we've dropped messages recently
        # (Would need to track delta from last check for rate-based alerting)
        if total_dropped > 0:
            alert_type = "messages_dropped"
            if self._should_alert(alert_type):
                alert = BandwidthAlert(
                    alert_id=self._generate_alert_id(),
                    level=BandwidthAlertLevel.THROTTLED,
                    alert_type="messages_dropped",
                    message=f"QoS throttling: {total_dropped} messages dropped",
                    timestamp=time.time(),
                    details=dropped,
                )
                self._emit_alert(alert)
                return alert

        return None

    def run_all_checks(self) -> List[BandwidthAlert]:
        """Run all alert checks and return any generated alerts."""
        alerts = []

        # Global capacity
        alert = self.check_global_capacity()
        if alert:
            alerts.append(alert)

        # Peer abuse
        alerts.extend(self.check_peer_abuse())

        # Topic spikes
        alerts.extend(self.check_topic_spikes())

        # Dropped messages
        alert = self.check_dropped_messages()
        if alert:
            alerts.append(alert)

        self._last_check = time.time()
        return alerts

    def get_recent_alerts(self, limit: int = 20) -> List[BandwidthAlert]:
        """Get recent alerts."""
        return self._alerts[-limit:]

    def get_alerts_by_level(self, level: BandwidthAlertLevel) -> List[BandwidthAlert]:
        """Get alerts of a specific level."""
        return [a for a in self._alerts if a.level == level]

    def get_status(self) -> Dict:
        """Get alert manager status."""
        metrics = self.tracker.get_global_metrics()
        rates = metrics["rates"]
        current_rate = rates["bytes_out_1s"] + rates["bytes_in_1s"]
        capacity = self.qos._global_bytes.rate

        return {
            "current_utilization_percent": (current_rate / capacity * 100) if capacity > 0 else 0,
            "warning_threshold_percent": self.warning_threshold * 100,
            "critical_threshold_percent": self.critical_threshold * 100,
            "total_alerts": len(self._alerts),
            "recent_alerts": [a.to_dict() for a in self._alerts[-5:]],
            "last_check": self._last_check,
        }

    def on_alert(self, callback: callable) -> None:
        """Register callback for alerts: callback(BandwidthAlert)."""
        self._on_alert_callbacks.append(callback)

    def clear_alerts(self) -> None:
        """Clear alert history."""
        self._alerts.clear()
        self._last_alert_by_type.clear()


# ============================================================================
# PEER RATE LIMITER
# ============================================================================

class PeerRateLimiter:
    """
    Per-peer rate limiting to prevent abuse.

    Tracks bandwidth per peer and enforces limits to prevent
    any single peer from consuming excessive resources.
    """

    def __init__(
        self,
        bytes_per_second: float = DEFAULT_PEER_BYTES_PER_SECOND,
        messages_per_second: float = DEFAULT_PEER_MESSAGES_PER_SECOND,
        burst_multiplier: float = 2.0,
    ):
        """
        Initialize per-peer rate limiter.

        Args:
            bytes_per_second: Bytes/second limit per peer
            messages_per_second: Messages/second limit per peer
            burst_multiplier: Allow this multiple for bursts
        """
        self.bytes_per_second = bytes_per_second
        self.messages_per_second = messages_per_second
        self.burst_multiplier = burst_multiplier

        # Per-peer limiters
        self._byte_limiters: Dict[str, RateLimiter] = {}
        self._msg_limiters: Dict[str, RateLimiter] = {}

        # Blocked peers
        self._blocked_peers: Dict[str, float] = {}  # peer_id -> blocked_until
        self._block_duration = 300.0  # 5 minute block

        # Violation tracking
        self._violations: Dict[str, int] = defaultdict(int)
        self._violation_threshold = 10  # Block after N violations

    def _get_byte_limiter(self, peer_id: str) -> RateLimiter:
        """Get or create byte limiter for peer."""
        if peer_id not in self._byte_limiters:
            self._byte_limiters[peer_id] = RateLimiter(
                rate=self.bytes_per_second,
                burst=self.bytes_per_second * self.burst_multiplier,
            )
        return self._byte_limiters[peer_id]

    def _get_msg_limiter(self, peer_id: str) -> RateLimiter:
        """Get or create message limiter for peer."""
        if peer_id not in self._msg_limiters:
            self._msg_limiters[peer_id] = RateLimiter(
                rate=self.messages_per_second,
                burst=self.messages_per_second * self.burst_multiplier,
            )
        return self._msg_limiters[peer_id]

    def is_blocked(self, peer_id: str) -> bool:
        """Check if peer is blocked."""
        if peer_id not in self._blocked_peers:
            return False

        blocked_until = self._blocked_peers[peer_id]
        if time.time() >= blocked_until:
            # Unblock
            del self._blocked_peers[peer_id]
            self._violations[peer_id] = 0
            return False

        return True

    def allow_receive(
        self,
        peer_id: str,
        byte_size: int,
    ) -> Tuple[bool, str]:
        """
        Check if we should accept data from this peer.

        Args:
            peer_id: Peer ID
            byte_size: Message size

        Returns:
            (allowed, reason)
        """
        # Check if blocked
        if self.is_blocked(peer_id):
            return False, "peer_blocked"

        # Check byte limit
        byte_limiter = self._get_byte_limiter(peer_id)
        if not byte_limiter.try_consume(byte_size):
            self._record_violation(peer_id, "bytes")
            return False, "peer_byte_limit"

        # Check message limit
        msg_limiter = self._get_msg_limiter(peer_id)
        if not msg_limiter.try_consume(1):
            self._record_violation(peer_id, "messages")
            return False, "peer_msg_limit"

        return True, "allowed"

    def _record_violation(self, peer_id: str, violation_type: str) -> None:
        """Record a rate limit violation."""
        self._violations[peer_id] += 1

        if self._violations[peer_id] >= self._violation_threshold:
            self._blocked_peers[peer_id] = time.time() + self._block_duration
            logger.warning(
                f"Peer {peer_id[:16]}... blocked for {self._block_duration}s "
                f"after {self._violations[peer_id]} violations"
            )

    def block_peer(self, peer_id: str, duration: float = None) -> None:
        """Manually block a peer."""
        self._blocked_peers[peer_id] = time.time() + (duration or self._block_duration)

    def unblock_peer(self, peer_id: str) -> None:
        """Manually unblock a peer."""
        self._blocked_peers.pop(peer_id, None)
        self._violations[peer_id] = 0

    def get_blocked_peers(self) -> List[str]:
        """Get list of currently blocked peers."""
        now = time.time()
        return [
            peer_id for peer_id, blocked_until in self._blocked_peers.items()
            if blocked_until > now
        ]

    def get_peer_stats(self, peer_id: str) -> Dict:
        """Get stats for a specific peer."""
        byte_limiter = self._get_byte_limiter(peer_id)
        msg_limiter = self._get_msg_limiter(peer_id)

        return {
            "peer_id": peer_id,
            "bytes_available": byte_limiter.available(),
            "bytes_limit": self.bytes_per_second,
            "msgs_available": msg_limiter.available(),
            "msgs_limit": self.messages_per_second,
            "violations": self._violations.get(peer_id, 0),
            "is_blocked": self.is_blocked(peer_id),
        }

    def get_status(self) -> Dict:
        """Get overall status."""
        return {
            "bytes_per_second_limit": self.bytes_per_second,
            "messages_per_second_limit": self.messages_per_second,
            "tracked_peers": len(self._byte_limiters),
            "blocked_peers": len(self.get_blocked_peers()),
            "total_violations": sum(self._violations.values()),
        }

    def cleanup_stale(self, max_idle: float = 3600.0) -> int:
        """
        Clean up limiters for peers we haven't heard from.

        Args:
            max_idle: Remove limiters idle longer than this (seconds)

        Returns:
            Number of limiters cleaned up
        """
        now = time.time()
        cleaned = 0

        # Find stale peers
        stale_peers = []
        for peer_id, limiter in self._byte_limiters.items():
            if now - limiter.last_refill > max_idle:
                stale_peers.append(peer_id)

        # Clean up
        for peer_id in stale_peers:
            self._byte_limiters.pop(peer_id, None)
            self._msg_limiters.pop(peer_id, None)
            self._violations.pop(peer_id, None)
            cleaned += 1

        return cleaned
