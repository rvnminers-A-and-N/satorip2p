"""
satorip2p.integration - Neuron Integration Wrappers

This module provides drop-in replacement classes for existing Satori
networking components, allowing seamless migration to P2P.

Features:
- NetworkingMode: Choose between CENTRAL, HYBRID, and P2P_ONLY modes
- Circuit breaker pattern for fault tolerance
- Automatic fallback from P2P to Central
- Metrics collection and logging
- Automatic mode detection and switching based on network conditions

Usage:
    # Replace SatoriPubSubConn with P2P version
    from satorip2p.integration import P2PSatoriPubSubConn as SatoriPubSubConn

    # Replace SatoriServerClient with P2P version
    from satorip2p.integration import P2PSatoriServerClient as SatoriServerClient

    # Replace CentrifugoClient with P2P version
    from satorip2p.integration import P2PCentrifugoClient

    # Configure networking mode
    from satorip2p.integration import NetworkingMode, configure_networking
    configure_networking(mode=NetworkingMode.HYBRID)

    # Get metrics
    from satorip2p.integration import IntegrationMetrics
    metrics = IntegrationMetrics.get_instance()
    stats = metrics.get_stats()
"""

from .neuron import (
    P2PSatoriPubSubConn,
    P2PSatoriServerClient,
    P2PCentrifugoClient,
    P2PStartupDagMixin,
    create_p2p_centrifugo_client,
)

from .networking_mode import (
    NetworkingMode,
    NetworkingConfig,
    get_networking_mode,
    configure_networking,
    get_networking_config,
    networking_mode_option,
)

from .metrics import (
    IntegrationMetrics,
    log_operation,
    log_fallback,
)

from .fallback import (
    FallbackHandler,
    FallbackContext,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    RetryConfig,
    get_fallback_handler,
    with_fallback,
)

from .auto_switch import (
    AutoSwitch,
    AutoSwitchConfig,
    SwitchReason,
    get_auto_switch,
    start_auto_switch,
    stop_auto_switch,
)

__all__ = [
    # Drop-in replacement classes
    "P2PSatoriPubSubConn",
    "P2PSatoriServerClient",
    "P2PCentrifugoClient",
    "P2PStartupDagMixin",
    "create_p2p_centrifugo_client",
    # Networking mode
    "NetworkingMode",
    "NetworkingConfig",
    "get_networking_mode",
    "configure_networking",
    "get_networking_config",
    "networking_mode_option",
    # Metrics
    "IntegrationMetrics",
    "log_operation",
    "log_fallback",
    # Fallback & Circuit Breaker
    "FallbackHandler",
    "FallbackContext",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "RetryConfig",
    "get_fallback_handler",
    "with_fallback",
    # Auto-switch
    "AutoSwitch",
    "AutoSwitchConfig",
    "SwitchReason",
    "get_auto_switch",
    "start_auto_switch",
    "stop_auto_switch",
]
