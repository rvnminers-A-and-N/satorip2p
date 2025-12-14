"""
satorip2p.integration.auto_switch - Automatic Mode Switching

Monitors network conditions and automatically switches between modes:
- Hybrid -> P2P_ONLY when enough peers are available and stable
- P2P_ONLY -> Hybrid when peer count drops or failures increase
- Any -> Central when P2P is completely unavailable

Usage:
    from satorip2p.integration.auto_switch import AutoSwitch

    auto_switch = AutoSwitch(peers_instance)
    auto_switch.start()

    # Will automatically monitor and switch modes
"""

from typing import TYPE_CHECKING, Optional, Callable
from enum import Enum
import threading
import time
import logging

from .networking_mode import NetworkingMode, NetworkingConfig
from .metrics import IntegrationMetrics

if TYPE_CHECKING:
    from ..peers import Peers
    from ..hybrid import HybridPeers

logger = logging.getLogger("satorip2p.integration.auto_switch")


class SwitchReason(Enum):
    """Reasons for mode switches."""
    MANUAL = "manual"
    HIGH_PEER_COUNT = "high_peer_count"
    LOW_PEER_COUNT = "low_peer_count"
    STABLE_P2P = "stable_p2p"
    P2P_FAILURES = "p2p_failures"
    P2P_UNAVAILABLE = "p2p_unavailable"
    CENTRAL_FAILURES = "central_failures"
    USER_OVERRIDE = "user_override"
    STARTUP = "startup"


class AutoSwitchConfig:
    """Configuration for automatic mode switching."""

    def __init__(
        self,
        enabled: bool = True,
        # Thresholds for switching to pure P2P
        min_peers_for_p2p: int = 5,
        stable_duration_seconds: float = 300.0,  # 5 minutes
        max_fallback_rate: float = 0.1,  # 10% fallback rate
        # Thresholds for switching back to hybrid
        max_peers_for_hybrid: int = 2,
        max_failures_for_hybrid: int = 5,
        failure_window_seconds: float = 60.0,
        # Check interval
        check_interval_seconds: float = 30.0,
        # Minimum time between switches (prevent flapping)
        min_switch_interval_seconds: float = 60.0,
    ):
        self.enabled = enabled
        self.min_peers_for_p2p = min_peers_for_p2p
        self.stable_duration_seconds = stable_duration_seconds
        self.max_fallback_rate = max_fallback_rate
        self.max_peers_for_hybrid = max_peers_for_hybrid
        self.max_failures_for_hybrid = max_failures_for_hybrid
        self.failure_window_seconds = failure_window_seconds
        self.check_interval_seconds = check_interval_seconds
        self.min_switch_interval_seconds = min_switch_interval_seconds


class AutoSwitch:
    """
    Automatic mode switching based on network conditions.

    Monitors:
    - Peer count over time
    - Fallback rates
    - P2P success rates
    - Network stability

    Makes intelligent decisions about when to:
    - Promote from Hybrid to P2P_ONLY (when P2P is reliable)
    - Demote from P2P_ONLY to Hybrid (when P2P is struggling)
    - Fall back to Central (when P2P is unavailable)
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        config: Optional[AutoSwitchConfig] = None,
        on_mode_change: Optional[Callable[[NetworkingMode, SwitchReason], None]] = None,
    ):
        """
        Initialize auto-switch.

        Args:
            peers: Peers instance to monitor
            config: Configuration options
            on_mode_change: Callback when mode changes
        """
        self._peers = peers
        self._config = config or AutoSwitchConfig()
        self._on_mode_change = on_mode_change
        self._metrics = IntegrationMetrics.get_instance()
        self._networking_config = NetworkingConfig()

        # State tracking
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_switch_time: float = 0
        self._stable_since: Optional[float] = None
        self._peer_history: list[tuple[float, int]] = []

        # Current effective mode
        self._current_mode = self._networking_config.get_mode()

    @property
    def current_mode(self) -> NetworkingMode:
        """Get current networking mode."""
        return self._current_mode

    @property
    def is_running(self) -> bool:
        """Check if auto-switch is running."""
        return self._running

    def start(self):
        """Start the auto-switch monitor."""
        if self._running:
            return

        if not self._config.enabled:
            logger.info("Auto-switch is disabled by configuration")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="AutoSwitch-Monitor"
        )
        self._thread.start()
        logger.info("Auto-switch monitor started")

    def stop(self):
        """Stop the auto-switch monitor."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None
        logger.info("Auto-switch monitor stopped")

    def set_peers(self, peers: "Peers"):
        """Set or update the Peers instance."""
        self._peers = peers

    def force_mode(self, mode: NetworkingMode, reason: str = "manual"):
        """
        Force a specific mode (bypasses auto-switch logic).

        Args:
            mode: Mode to force
            reason: Reason for forcing
        """
        self._switch_mode(mode, SwitchReason.USER_OVERRIDE)
        logger.info(f"Mode forced to {mode.name}: {reason}")

    def _monitor_loop(self):
        """Main monitoring loop."""
        while self._running:
            try:
                self._check_and_switch()
            except Exception as e:
                logger.error(f"Auto-switch error: {e}")

            time.sleep(self._config.check_interval_seconds)

    def _check_and_switch(self):
        """Check conditions and switch mode if needed."""
        # Get current state
        peer_count = self._get_peer_count()
        self._record_peer_count(peer_count)

        stats = self._metrics.get_stats(
            window_seconds=int(self._config.failure_window_seconds)
        )

        # Check for mode switch
        decision = self._evaluate_switch(peer_count, stats)

        if decision is not None:
            new_mode, reason = decision
            if self._can_switch():
                self._switch_mode(new_mode, reason)

    def _get_peer_count(self) -> int:
        """Get current peer count."""
        if self._peers is None:
            return 0
        try:
            return self._peers.connected_peers
        except Exception:
            return 0

    def _record_peer_count(self, count: int):
        """Record peer count for history."""
        now = time.time()
        self._peer_history.append((now, count))

        # Keep last hour
        cutoff = now - 3600
        self._peer_history = [
            (t, c) for t, c in self._peer_history if t > cutoff
        ]

        # Also record to metrics
        self._metrics.record_peer_count(count)

    def _get_average_peer_count(self, window_seconds: float = 300.0) -> float:
        """Get average peer count over time window."""
        cutoff = time.time() - window_seconds
        recent = [c for t, c in self._peer_history if t > cutoff]
        return sum(recent) / len(recent) if recent else 0

    def _get_fallback_rate(self, stats: dict) -> float:
        """Calculate fallback rate from stats."""
        total_ops = sum(
            op_stats["total"]
            for op_stats in stats.get("operations", {}).values()
        )
        total_fallbacks = stats.get("fallbacks", {}).get("_total", 0)

        if total_ops == 0:
            return 0.0
        return total_fallbacks / total_ops

    def _evaluate_switch(
        self,
        peer_count: int,
        stats: dict
    ) -> Optional[tuple[NetworkingMode, SwitchReason]]:
        """
        Evaluate if we should switch modes.

        Returns:
            Tuple of (new_mode, reason) if switch needed, None otherwise
        """
        current = self._current_mode
        fallback_rate = self._get_fallback_rate(stats)
        avg_peers = self._get_average_peer_count()

        # ===== From HYBRID: Check if we can promote to P2P_ONLY =====
        if current == NetworkingMode.HYBRID:
            if self._should_promote_to_p2p(peer_count, avg_peers, fallback_rate):
                return (NetworkingMode.P2P_ONLY, SwitchReason.STABLE_P2P)

        # ===== From P2P_ONLY: Check if we need to demote to HYBRID =====
        elif current == NetworkingMode.P2P_ONLY:
            if peer_count <= self._config.max_peers_for_hybrid:
                return (NetworkingMode.HYBRID, SwitchReason.LOW_PEER_COUNT)

            if fallback_rate > self._config.max_fallback_rate * 2:
                # High failure rate in P2P mode
                return (NetworkingMode.HYBRID, SwitchReason.P2P_FAILURES)

        # ===== From any P2P mode: Check if P2P is completely unavailable =====
        if current in (NetworkingMode.HYBRID, NetworkingMode.P2P_ONLY):
            if peer_count == 0 and avg_peers < 1:
                # No peers at all, might need to go central
                logger.warning("No P2P peers available")
                # Don't auto-switch to central, just log warning
                # Central should be used via fallback mechanism

        return None

    def _should_promote_to_p2p(
        self,
        current_peers: int,
        avg_peers: float,
        fallback_rate: float
    ) -> bool:
        """Check if conditions are good enough for pure P2P."""
        # Need minimum peers
        if current_peers < self._config.min_peers_for_p2p:
            self._stable_since = None
            return False

        if avg_peers < self._config.min_peers_for_p2p * 0.8:
            self._stable_since = None
            return False

        # Need low fallback rate
        if fallback_rate > self._config.max_fallback_rate:
            self._stable_since = None
            return False

        # Need to be stable for duration
        now = time.time()
        if self._stable_since is None:
            self._stable_since = now
            return False

        stable_duration = now - self._stable_since
        if stable_duration < self._config.stable_duration_seconds:
            return False

        logger.info(
            f"P2P promotion conditions met: "
            f"peers={current_peers}, avg={avg_peers:.1f}, "
            f"fallback_rate={fallback_rate:.2%}, "
            f"stable_for={stable_duration:.0f}s"
        )
        return True

    def _can_switch(self) -> bool:
        """Check if enough time has passed since last switch."""
        if self._last_switch_time == 0:
            return True

        elapsed = time.time() - self._last_switch_time
        return elapsed >= self._config.min_switch_interval_seconds

    def _switch_mode(self, new_mode: NetworkingMode, reason: SwitchReason):
        """Perform mode switch."""
        old_mode = self._current_mode

        if old_mode == new_mode:
            return

        # Update mode
        self._current_mode = new_mode
        self._networking_config.set_mode(new_mode)
        self._last_switch_time = time.time()
        self._stable_since = None

        # Record to metrics
        self._metrics.record_mode_switch(
            from_mode=old_mode.name.lower(),
            to_mode=new_mode.name.lower(),
            reason=reason.value,
            peer_count=self._get_peer_count(),
        )

        logger.info(
            f"Mode switched: {old_mode.name} -> {new_mode.name} "
            f"(reason: {reason.value})"
        )

        # Callback
        if self._on_mode_change:
            try:
                self._on_mode_change(new_mode, reason)
            except Exception as e:
                logger.error(f"Mode change callback error: {e}")

    def get_status(self) -> dict:
        """Get auto-switch status."""
        peer_count = self._get_peer_count()
        avg_peers = self._get_average_peer_count()
        stats = self._metrics.get_stats(
            window_seconds=int(self._config.failure_window_seconds)
        )

        return {
            "enabled": self._config.enabled,
            "running": self._running,
            "current_mode": self._current_mode.name,
            "peer_count": peer_count,
            "avg_peer_count_5min": avg_peers,
            "fallback_rate": self._get_fallback_rate(stats),
            "last_switch_time": self._last_switch_time,
            "stable_since": self._stable_since,
            "can_switch": self._can_switch(),
            "config": {
                "min_peers_for_p2p": self._config.min_peers_for_p2p,
                "stable_duration_seconds": self._config.stable_duration_seconds,
                "max_fallback_rate": self._config.max_fallback_rate,
            }
        }


# Global auto-switch instance
_global_auto_switch: Optional[AutoSwitch] = None


def get_auto_switch() -> AutoSwitch:
    """Get global auto-switch instance."""
    global _global_auto_switch
    if _global_auto_switch is None:
        _global_auto_switch = AutoSwitch()
    return _global_auto_switch


def start_auto_switch(
    peers: Optional["Peers"] = None,
    config: Optional[AutoSwitchConfig] = None,
) -> AutoSwitch:
    """
    Start global auto-switch with given configuration.

    Args:
        peers: Peers instance to monitor
        config: Configuration options

    Returns:
        AutoSwitch instance
    """
    global _global_auto_switch
    _global_auto_switch = AutoSwitch(peers=peers, config=config)
    _global_auto_switch.start()
    return _global_auto_switch


def stop_auto_switch():
    """Stop global auto-switch."""
    global _global_auto_switch
    if _global_auto_switch:
        _global_auto_switch.stop()
