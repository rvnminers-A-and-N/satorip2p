"""
satorip2p.integration.metrics - Metrics and Logging for Integration Layer

Provides metrics collection and logging for:
- Fallback transitions (P2P -> Central and vice versa)
- Operation success/failure rates
- Mode switching events
- Network health indicators

Usage:
    from satorip2p.integration.metrics import IntegrationMetrics

    metrics = IntegrationMetrics.get_instance()
    metrics.record_operation("publish", success=True, mode="p2p")
    metrics.record_fallback("publish", "p2p_timeout")

    # Get statistics
    stats = metrics.get_stats()
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
from threading import Lock
import time
import logging

logger = logging.getLogger("satorip2p.integration.metrics")


@dataclass
class OperationRecord:
    """Record of a single operation."""
    operation: str
    success: bool
    mode: str  # p2p, central, hybrid
    duration_ms: float
    timestamp: float
    error: Optional[str] = None


@dataclass
class FallbackRecord:
    """Record of a fallback event."""
    operation: str
    reason: str
    from_mode: str
    to_mode: str
    timestamp: float


class IntegrationMetrics:
    """
    Metrics collector for integration layer operations.

    Thread-safe singleton that tracks:
    - Operation counts and success rates by mode
    - Fallback events and reasons
    - Mode switches
    - Timing statistics
    """
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialize()
            return cls._instance

    def _initialize(self):
        """Initialize metrics storage."""
        self._operations: List[OperationRecord] = []
        self._fallbacks: List[FallbackRecord] = []
        self._mode_switches: List[Dict] = []
        self._start_time = time.time()
        self._current_mode = "hybrid"
        self._peer_count_history: List[tuple] = []  # (timestamp, count)

        # Counters
        self._operation_counts = defaultdict(lambda: defaultdict(int))
        self._fallback_counts = defaultdict(lambda: defaultdict(int))

        # Max records to keep (prevent memory bloat)
        self._max_operations = 10000
        self._max_fallbacks = 1000

    @classmethod
    def get_instance(cls) -> "IntegrationMetrics":
        """Get the singleton instance."""
        return cls()

    def record_operation(
        self,
        operation: str,
        success: bool,
        mode: str,
        duration_ms: float = 0,
        error: Optional[str] = None,
    ):
        """
        Record an operation.

        Args:
            operation: Operation name (publish, subscribe, checkin, etc.)
            success: Whether the operation succeeded
            mode: Mode used (p2p, central, hybrid)
            duration_ms: Operation duration in milliseconds
            error: Error message if failed
        """
        record = OperationRecord(
            operation=operation,
            success=success,
            mode=mode,
            duration_ms=duration_ms,
            timestamp=time.time(),
            error=error,
        )

        with self._lock:
            self._operations.append(record)
            if len(self._operations) > self._max_operations:
                self._operations = self._operations[-self._max_operations // 2:]

            # Update counters
            status = "success" if success else "failure"
            self._operation_counts[operation][f"{mode}_{status}"] += 1
            self._operation_counts[operation]["total"] += 1

        # Log significant events
        if not success:
            logger.warning(
                f"Operation failed: {operation} via {mode} - {error or 'unknown error'}"
            )
        elif duration_ms > 5000:  # Slow operation
            logger.info(
                f"Slow operation: {operation} via {mode} took {duration_ms:.0f}ms"
            )

    def record_fallback(
        self,
        operation: str,
        reason: str,
        from_mode: str = "p2p",
        to_mode: str = "central",
    ):
        """
        Record a fallback event.

        Args:
            operation: Operation that triggered fallback
            reason: Reason for fallback (timeout, error, no_peers, etc.)
            from_mode: Mode that failed
            to_mode: Mode falling back to
        """
        record = FallbackRecord(
            operation=operation,
            reason=reason,
            from_mode=from_mode,
            to_mode=to_mode,
            timestamp=time.time(),
        )

        with self._lock:
            self._fallbacks.append(record)
            if len(self._fallbacks) > self._max_fallbacks:
                self._fallbacks = self._fallbacks[-self._max_fallbacks // 2:]

            self._fallback_counts[operation][reason] += 1
            self._fallback_counts["_total"][reason] += 1

        logger.info(
            f"Fallback: {operation} from {from_mode} to {to_mode} - {reason}"
        )

    def record_mode_switch(
        self,
        from_mode: str,
        to_mode: str,
        reason: str,
        peer_count: int = 0,
    ):
        """
        Record a mode switch event.

        Args:
            from_mode: Previous mode
            to_mode: New mode
            reason: Reason for switch
            peer_count: Current peer count
        """
        switch = {
            "from_mode": from_mode,
            "to_mode": to_mode,
            "reason": reason,
            "peer_count": peer_count,
            "timestamp": time.time(),
        }

        with self._lock:
            self._mode_switches.append(switch)
            self._current_mode = to_mode

        logger.info(
            f"Mode switch: {from_mode} -> {to_mode} (reason: {reason}, peers: {peer_count})"
        )

    def record_peer_count(self, count: int):
        """Record current peer count for history."""
        with self._lock:
            self._peer_count_history.append((time.time(), count))
            # Keep last 1000 entries
            if len(self._peer_count_history) > 1000:
                self._peer_count_history = self._peer_count_history[-500:]

    def get_current_mode(self) -> str:
        """Get current operating mode."""
        return self._current_mode

    def get_stats(self, window_seconds: int = 3600) -> Dict[str, Any]:
        """
        Get statistics for the specified time window.

        Args:
            window_seconds: Time window in seconds (default: 1 hour)

        Returns:
            Dictionary with statistics
        """
        cutoff = time.time() - window_seconds

        with self._lock:
            recent_ops = [op for op in self._operations if op.timestamp > cutoff]
            recent_fallbacks = [fb for fb in self._fallbacks if fb.timestamp > cutoff]
            recent_switches = [sw for sw in self._mode_switches if sw["timestamp"] > cutoff]

        # Calculate operation stats
        op_stats = defaultdict(lambda: {
            "total": 0,
            "p2p_success": 0,
            "p2p_failure": 0,
            "central_success": 0,
            "central_failure": 0,
            "avg_duration_ms": 0,
        })

        for op in recent_ops:
            stats = op_stats[op.operation]
            stats["total"] += 1
            key = f"{op.mode}_{'success' if op.success else 'failure'}"
            if key in stats:
                stats[key] += 1

        # Calculate averages
        for op_name, stats in op_stats.items():
            durations = [
                op.duration_ms for op in recent_ops
                if op.operation == op_name and op.duration_ms > 0
            ]
            if durations:
                stats["avg_duration_ms"] = sum(durations) / len(durations)

        # Calculate success rates
        for op_name, stats in op_stats.items():
            total = stats["total"]
            if total > 0:
                p2p_total = stats["p2p_success"] + stats["p2p_failure"]
                central_total = stats["central_success"] + stats["central_failure"]

                stats["p2p_success_rate"] = (
                    stats["p2p_success"] / p2p_total * 100 if p2p_total > 0 else 0
                )
                stats["central_success_rate"] = (
                    stats["central_success"] / central_total * 100 if central_total > 0 else 0
                )
                stats["overall_success_rate"] = (
                    (stats["p2p_success"] + stats["central_success"]) / total * 100
                )

        # Fallback stats
        fallback_stats = defaultdict(int)
        for fb in recent_fallbacks:
            fallback_stats[fb.reason] += 1
            fallback_stats["_total"] += 1

        # Peer count stats
        recent_peer_counts = [
            count for ts, count in self._peer_count_history
            if ts > cutoff
        ]
        avg_peers = (
            sum(recent_peer_counts) / len(recent_peer_counts)
            if recent_peer_counts else 0
        )

        return {
            "window_seconds": window_seconds,
            "current_mode": self._current_mode,
            "uptime_seconds": time.time() - self._start_time,
            "operations": dict(op_stats),
            "fallbacks": dict(fallback_stats),
            "mode_switches": recent_switches,
            "mode_switch_count": len(recent_switches),
            "avg_peer_count": avg_peers,
            "current_peer_count": (
                recent_peer_counts[-1] if recent_peer_counts else 0
            ),
        }

    def get_health(self) -> Dict[str, Any]:
        """
        Get network health indicators.

        Returns:
            Dictionary with health status and indicators
        """
        stats = self.get_stats(window_seconds=300)  # Last 5 minutes

        # Calculate health score (0-100)
        health_score = 100

        # Deduct for fallbacks
        fallback_count = stats["fallbacks"].get("_total", 0)
        health_score -= min(fallback_count * 5, 30)

        # Deduct for low peer count
        peer_count = stats["current_peer_count"]
        if peer_count < 3:
            health_score -= 20
        elif peer_count < 5:
            health_score -= 10

        # Deduct for mode switches (instability)
        switch_count = stats["mode_switch_count"]
        health_score -= min(switch_count * 10, 20)

        # Determine status
        if health_score >= 80:
            status = "healthy"
        elif health_score >= 50:
            status = "degraded"
        else:
            status = "unhealthy"

        return {
            "status": status,
            "score": max(0, health_score),
            "current_mode": self._current_mode,
            "peer_count": peer_count,
            "fallback_count_5min": fallback_count,
            "mode_switches_5min": switch_count,
            "recommendations": self._get_recommendations(stats),
        }

    def _get_recommendations(self, stats: Dict) -> List[str]:
        """Generate recommendations based on stats."""
        recommendations = []

        peer_count = stats["current_peer_count"]
        fallback_count = stats["fallbacks"].get("_total", 0)

        if peer_count < 3:
            recommendations.append(
                "Low peer count. Consider adding more bootstrap peers or checking network connectivity."
            )

        if fallback_count > 10:
            recommendations.append(
                "High fallback rate. P2P network may be unstable. Consider staying in hybrid mode."
            )

        if stats["mode_switch_count"] > 5:
            recommendations.append(
                "Frequent mode switches. Network conditions are unstable. "
                "Consider disabling auto-switch temporarily."
            )

        timeout_fallbacks = stats["fallbacks"].get("timeout", 0)
        if timeout_fallbacks > 5:
            recommendations.append(
                "Many timeout fallbacks. Network latency may be high."
            )

        return recommendations

    def reset(self):
        """Reset all metrics."""
        with self._lock:
            self._initialize()

    def export_prometheus(self) -> str:
        """
        Export metrics in Prometheus format.

        Returns:
            Prometheus-formatted metrics string
        """
        lines = []
        stats = self.get_stats()

        # Uptime
        lines.append(f"satorip2p_uptime_seconds {stats['uptime_seconds']:.0f}")

        # Current mode (as labels)
        mode_value = {"central": 0, "hybrid": 1, "p2p": 2}.get(self._current_mode, 1)
        lines.append(f'satorip2p_networking_mode{{mode="{self._current_mode}"}} {mode_value}')

        # Peer count
        lines.append(f"satorip2p_peer_count {stats['current_peer_count']}")
        lines.append(f"satorip2p_peer_count_avg {stats['avg_peer_count']:.1f}")

        # Operations
        for op_name, op_stats in stats["operations"].items():
            lines.append(f'satorip2p_operations_total{{operation="{op_name}"}} {op_stats["total"]}')
            lines.append(f'satorip2p_operations_p2p_success{{operation="{op_name}"}} {op_stats["p2p_success"]}')
            lines.append(f'satorip2p_operations_central_success{{operation="{op_name}"}} {op_stats["central_success"]}')

        # Fallbacks
        for reason, count in stats["fallbacks"].items():
            if reason != "_total":
                lines.append(f'satorip2p_fallbacks_total{{reason="{reason}"}} {count}')

        lines.append(f"satorip2p_fallbacks_total_all {stats['fallbacks'].get('_total', 0)}")

        # Mode switches
        lines.append(f"satorip2p_mode_switches_total {stats['mode_switch_count']}")

        # Health
        health = self.get_health()
        lines.append(f"satorip2p_health_score {health['score']}")

        return "\n".join(lines)


# Convenience function for quick logging
def log_operation(
    operation: str,
    success: bool,
    mode: str,
    duration_ms: float = 0,
    error: Optional[str] = None,
):
    """
    Convenience function to log an operation.

    Example:
        from satorip2p.integration.metrics import log_operation

        start = time.time()
        try:
            result = do_something()
            log_operation("publish", True, "p2p", (time.time()-start)*1000)
        except Exception as e:
            log_operation("publish", False, "p2p", error=str(e))
    """
    IntegrationMetrics.get_instance().record_operation(
        operation=operation,
        success=success,
        mode=mode,
        duration_ms=duration_ms,
        error=error,
    )


def log_fallback(operation: str, reason: str):
    """Convenience function to log a fallback event."""
    IntegrationMetrics.get_instance().record_fallback(
        operation=operation,
        reason=reason,
    )
