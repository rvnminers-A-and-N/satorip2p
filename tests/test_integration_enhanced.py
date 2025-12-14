"""
Tests for Phase 7.5 enhanced integration components:
- NetworkingMode
- Metrics
- Fallback/Circuit Breaker
- Auto-switch
"""

import pytest
import time
from unittest.mock import MagicMock, patch


class TestNetworkingMode:
    """Test NetworkingMode enum and configuration."""

    def test_import(self):
        """Test NetworkingMode can be imported."""
        from satorip2p.integration import NetworkingMode
        assert NetworkingMode is not None

    def test_enum_values(self):
        """Test enum has expected values."""
        from satorip2p.integration import NetworkingMode

        assert hasattr(NetworkingMode, 'CENTRAL')
        assert hasattr(NetworkingMode, 'HYBRID')
        assert hasattr(NetworkingMode, 'P2P_ONLY')

    def test_from_string(self):
        """Test string to enum conversion."""
        from satorip2p.integration import NetworkingMode

        assert NetworkingMode.from_string('central') == NetworkingMode.CENTRAL
        assert NetworkingMode.from_string('hybrid') == NetworkingMode.HYBRID
        assert NetworkingMode.from_string('p2p') == NetworkingMode.P2P_ONLY
        assert NetworkingMode.from_string('P2P_ONLY') == NetworkingMode.P2P_ONLY
        assert NetworkingMode.from_string('p2p-only') == NetworkingMode.P2P_ONLY

    def test_from_string_invalid(self):
        """Test invalid string raises ValueError."""
        from satorip2p.integration import NetworkingMode

        with pytest.raises(ValueError):
            NetworkingMode.from_string('invalid')

    def test_to_string(self):
        """Test enum to string conversion."""
        from satorip2p.integration import NetworkingMode

        assert str(NetworkingMode.CENTRAL) == 'central'
        assert str(NetworkingMode.HYBRID) == 'hybrid'
        assert str(NetworkingMode.P2P_ONLY) == 'p2p-only'


class TestNetworkingConfig:
    """Test NetworkingConfig singleton."""

    def test_import(self):
        """Test NetworkingConfig can be imported."""
        from satorip2p.integration import NetworkingConfig
        assert NetworkingConfig is not None

    def test_singleton(self):
        """Test NetworkingConfig is singleton."""
        from satorip2p.integration.networking_mode import NetworkingConfig

        config1 = NetworkingConfig()
        config2 = NetworkingConfig()
        assert config1 is config2

    def test_get_mode(self):
        """Test get_mode returns a mode."""
        from satorip2p.integration import NetworkingConfig, NetworkingMode

        mode = NetworkingConfig.get_mode()
        assert isinstance(mode, NetworkingMode)

    def test_set_mode(self):
        """Test set_mode changes mode."""
        from satorip2p.integration import NetworkingConfig, NetworkingMode

        original = NetworkingConfig.get_mode()
        NetworkingConfig.set_mode(NetworkingMode.P2P_ONLY)
        assert NetworkingConfig.get_mode() == NetworkingMode.P2P_ONLY
        NetworkingConfig.set_mode(original)

    def test_configure(self):
        """Test configure method."""
        from satorip2p.integration import NetworkingConfig, NetworkingMode

        original = NetworkingConfig.get_mode()
        NetworkingConfig.configure(
            mode=NetworkingMode.CENTRAL,
            auto_switch_enabled=False,
        )
        assert NetworkingConfig.get_mode() == NetworkingMode.CENTRAL
        assert NetworkingConfig.is_auto_switch_enabled() is False
        NetworkingConfig.set_mode(original)
        NetworkingConfig.set_auto_switch_enabled(True)


class TestGetNetworkingMode:
    """Test get_networking_mode function."""

    def test_import(self):
        """Test function can be imported."""
        from satorip2p.integration import get_networking_mode
        assert get_networking_mode is not None

    def test_returns_mode(self):
        """Test returns NetworkingMode."""
        from satorip2p.integration import get_networking_mode, NetworkingMode

        mode = get_networking_mode()
        assert isinstance(mode, NetworkingMode)

    def test_env_override(self):
        """Test environment variable override."""
        import os
        from satorip2p.integration.networking_mode import (
            NetworkingMode, NetworkingConfig, get_networking_mode
        )

        # Reset config to not initialized
        NetworkingConfig._initialized = False

        os.environ['SATORI_NETWORKING_MODE'] = 'p2p'
        try:
            mode = get_networking_mode()
            assert mode == NetworkingMode.P2P_ONLY
        finally:
            del os.environ['SATORI_NETWORKING_MODE']
            NetworkingConfig._initialized = True


class TestConfigureNetworking:
    """Test configure_networking function."""

    def test_import(self):
        """Test function can be imported."""
        from satorip2p.integration import configure_networking
        assert configure_networking is not None

    def test_configure(self):
        """Test configure sets values."""
        from satorip2p.integration import (
            configure_networking, NetworkingMode, NetworkingConfig
        )

        original_mode = NetworkingConfig.get_mode()
        configure_networking(
            mode=NetworkingMode.HYBRID,
            auto_switch_threshold=10,
        )
        assert NetworkingConfig.get_mode() == NetworkingMode.HYBRID
        assert NetworkingConfig.get_auto_switch_threshold() == 10

        # Reset
        NetworkingConfig.set_mode(original_mode)
        NetworkingConfig.set_auto_switch_threshold(5)


class TestIntegrationMetrics:
    """Test IntegrationMetrics class."""

    def test_import(self):
        """Test can be imported."""
        from satorip2p.integration import IntegrationMetrics
        assert IntegrationMetrics is not None

    def test_singleton(self):
        """Test is singleton."""
        from satorip2p.integration import IntegrationMetrics

        m1 = IntegrationMetrics.get_instance()
        m2 = IntegrationMetrics.get_instance()
        assert m1 is m2

    def test_record_operation(self):
        """Test record_operation."""
        from satorip2p.integration import IntegrationMetrics

        metrics = IntegrationMetrics.get_instance()
        metrics.reset()

        metrics.record_operation("test_op", success=True, mode="p2p", duration_ms=100)
        stats = metrics.get_stats()

        assert "test_op" in stats["operations"]
        assert stats["operations"]["test_op"]["total"] >= 1

    def test_record_fallback(self):
        """Test record_fallback."""
        from satorip2p.integration import IntegrationMetrics

        metrics = IntegrationMetrics.get_instance()
        metrics.reset()

        metrics.record_fallback("test_op", "timeout")
        stats = metrics.get_stats()

        assert stats["fallbacks"].get("timeout", 0) >= 1

    def test_record_mode_switch(self):
        """Test record_mode_switch."""
        from satorip2p.integration import IntegrationMetrics

        metrics = IntegrationMetrics.get_instance()
        metrics.reset()

        metrics.record_mode_switch("hybrid", "p2p", "stable_p2p", peer_count=10)
        stats = metrics.get_stats()

        assert stats["mode_switch_count"] >= 1

    def test_get_health(self):
        """Test get_health."""
        from satorip2p.integration import IntegrationMetrics

        metrics = IntegrationMetrics.get_instance()
        health = metrics.get_health()

        assert "status" in health
        assert "score" in health
        assert health["status"] in ["healthy", "degraded", "unhealthy"]
        assert 0 <= health["score"] <= 100


class TestCircuitBreaker:
    """Test CircuitBreaker class."""

    def test_import(self):
        """Test can be imported."""
        from satorip2p.integration import CircuitBreaker
        assert CircuitBreaker is not None

    def test_initial_state(self):
        """Test initial state is CLOSED."""
        from satorip2p.integration.fallback import CircuitBreaker, CircuitState

        cb = CircuitBreaker("test")
        assert cb.state == CircuitState.CLOSED

    def test_is_available_closed(self):
        """Test is_available when CLOSED."""
        from satorip2p.integration.fallback import CircuitBreaker

        cb = CircuitBreaker("test")
        assert cb.is_available() is True

    def test_opens_after_failures(self):
        """Test circuit opens after failure threshold."""
        from satorip2p.integration.fallback import (
            CircuitBreaker, CircuitBreakerConfig, CircuitState
        )

        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker("test", config)

        for _ in range(3):
            cb.record_failure()

        assert cb.state == CircuitState.OPEN
        assert cb.is_available() is False

    def test_closes_after_success(self):
        """Test circuit closes after successes in half-open."""
        from satorip2p.integration.fallback import (
            CircuitBreaker, CircuitBreakerConfig, CircuitState
        )

        config = CircuitBreakerConfig(
            failure_threshold=2,
            success_threshold=2,
            timeout_seconds=0.1
        )
        cb = CircuitBreaker("test", config)

        # Open the circuit
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        # Wait for timeout
        time.sleep(0.2)

        # Should be half-open now
        assert cb.state == CircuitState.HALF_OPEN

        # Successes should close it
        cb.record_success()
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_reset(self):
        """Test reset method."""
        from satorip2p.integration.fallback import CircuitBreaker, CircuitState

        cb = CircuitBreaker("test")
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()

        assert cb.state == CircuitState.OPEN

        cb.reset()
        assert cb.state == CircuitState.CLOSED


class TestFallbackHandler:
    """Test FallbackHandler class."""

    def test_import(self):
        """Test can be imported."""
        from satorip2p.integration import FallbackHandler
        assert FallbackHandler is not None

    def test_creation(self):
        """Test creation."""
        from satorip2p.integration import FallbackHandler

        handler = FallbackHandler()
        assert handler is not None

    def test_should_try_p2p(self):
        """Test should_try_p2p returns True initially."""
        from satorip2p.integration import FallbackHandler

        handler = FallbackHandler()
        assert handler.should_try_p2p("test_op") is True

    def test_get_circuit_states(self):
        """Test get_circuit_states."""
        from satorip2p.integration import FallbackHandler

        handler = FallbackHandler()
        handler.should_try_p2p("op1")
        handler.should_try_p2p("op2")

        states = handler.get_circuit_states()
        # Circuit names may or may not have p2p_ prefix depending on implementation
        assert len(states) >= 2
        assert any("op1" in k for k in states.keys())
        assert any("op2" in k for k in states.keys())

    def test_context_manager(self):
        """Test try_p2p_sync context manager."""
        from satorip2p.integration.fallback import FallbackHandler

        handler = FallbackHandler()

        with handler.try_p2p_sync("test") as ctx:
            assert ctx.should_try_p2p is True
            ctx.mark_p2p_success()

        assert ctx.overall_success is True


class TestAutoSwitch:
    """Test AutoSwitch class."""

    def test_import(self):
        """Test can be imported."""
        from satorip2p.integration import AutoSwitch
        assert AutoSwitch is not None

    def test_creation(self):
        """Test creation."""
        from satorip2p.integration import AutoSwitch

        auto_switch = AutoSwitch()
        assert auto_switch is not None

    def test_current_mode(self):
        """Test current_mode property."""
        from satorip2p.integration import AutoSwitch, NetworkingMode

        auto_switch = AutoSwitch()
        mode = auto_switch.current_mode
        assert isinstance(mode, NetworkingMode)

    def test_is_running(self):
        """Test is_running property."""
        from satorip2p.integration import AutoSwitch

        auto_switch = AutoSwitch()
        assert auto_switch.is_running is False

    def test_get_status(self):
        """Test get_status."""
        from satorip2p.integration import AutoSwitch

        auto_switch = AutoSwitch()
        status = auto_switch.get_status()

        assert "enabled" in status
        assert "running" in status
        assert "current_mode" in status
        assert "peer_count" in status


class TestAutoSwitchConfig:
    """Test AutoSwitchConfig class."""

    def test_import(self):
        """Test can be imported."""
        from satorip2p.integration import AutoSwitchConfig
        assert AutoSwitchConfig is not None

    def test_defaults(self):
        """Test default values."""
        from satorip2p.integration import AutoSwitchConfig

        config = AutoSwitchConfig()
        assert config.enabled is True
        assert config.min_peers_for_p2p == 5
        assert config.stable_duration_seconds == 300.0

    def test_custom_values(self):
        """Test custom values."""
        from satorip2p.integration import AutoSwitchConfig

        config = AutoSwitchConfig(
            enabled=False,
            min_peers_for_p2p=10,
            stable_duration_seconds=600.0,
        )
        assert config.enabled is False
        assert config.min_peers_for_p2p == 10
        assert config.stable_duration_seconds == 600.0


class TestLogHelpers:
    """Test log helper functions."""

    def test_log_operation_import(self):
        """Test log_operation can be imported."""
        from satorip2p.integration.metrics import log_operation
        assert log_operation is not None

    def test_log_fallback_import(self):
        """Test log_fallback can be imported."""
        from satorip2p.integration.metrics import log_fallback
        assert log_fallback is not None

    def test_log_operation(self):
        """Test log_operation records to metrics."""
        from satorip2p.integration import IntegrationMetrics
        from satorip2p.integration.metrics import log_operation

        metrics = IntegrationMetrics.get_instance()
        metrics.reset()

        log_operation("test_log", True, "p2p", 50.0)
        stats = metrics.get_stats()

        assert "test_log" in stats["operations"]

    def test_log_fallback(self):
        """Test log_fallback records to metrics."""
        from satorip2p.integration import IntegrationMetrics
        from satorip2p.integration.metrics import log_fallback

        metrics = IntegrationMetrics.get_instance()
        metrics.reset()

        log_fallback("test_fallback", "test_reason")
        stats = metrics.get_stats()

        assert stats["fallbacks"].get("test_reason", 0) >= 1


class TestWithFallbackDecorator:
    """Test with_fallback decorator."""

    def test_import(self):
        """Test can be imported."""
        from satorip2p.integration.fallback import with_fallback
        assert with_fallback is not None

    def test_decorator_sync(self):
        """Test decorator on sync function."""
        from satorip2p.integration.fallback import with_fallback, FallbackHandler

        handler = FallbackHandler()
        call_log = []

        @with_fallback(handler, operation="test_decorated")
        def my_func(data, use_central=False):
            call_log.append(f"central={use_central}")
            return f"result_{use_central}"

        result = my_func("test_data")
        assert "central=False" in call_log


class TestPrometheusExport:
    """Test Prometheus metrics export."""

    def test_export_prometheus(self):
        """Test export_prometheus returns string."""
        from satorip2p.integration import IntegrationMetrics

        metrics = IntegrationMetrics.get_instance()
        prometheus_output = metrics.export_prometheus()

        assert isinstance(prometheus_output, str)
        assert "satorip2p_" in prometheus_output


class TestMainPackageExports:
    """Test exports from main package."""

    def test_networking_mode_export(self):
        """Test NetworkingMode exported from main package."""
        from satorip2p import NetworkingMode
        assert NetworkingMode is not None

    def test_configure_networking_export(self):
        """Test configure_networking exported from main package."""
        from satorip2p import configure_networking
        assert configure_networking is not None

    def test_get_networking_mode_export(self):
        """Test get_networking_mode exported from main package."""
        from satorip2p import get_networking_mode
        assert get_networking_mode is not None

    def test_integration_metrics_export(self):
        """Test IntegrationMetrics exported from main package."""
        from satorip2p import IntegrationMetrics
        assert IntegrationMetrics is not None

    def test_fallback_handler_export(self):
        """Test FallbackHandler exported from main package."""
        from satorip2p import FallbackHandler
        assert FallbackHandler is not None

    def test_auto_switch_export(self):
        """Test AutoSwitch exported from main package."""
        from satorip2p import AutoSwitch
        assert AutoSwitch is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
