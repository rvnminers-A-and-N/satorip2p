"""
satorip2p.hybrid.config - Hybrid Mode Configuration

Defines operating modes and configuration for hybrid networking.
"""

from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Optional


class HybridMode(Enum):
    """
    Operating mode for hybrid networking.

    P2P_ONLY: Pure libp2p networking, no central server connections
    HYBRID: Both P2P and central, messages bridged between them
    CENTRAL_ONLY: Legacy mode, only central servers (for testing/fallback)
    """
    P2P_ONLY = auto()
    HYBRID = auto()
    CENTRAL_ONLY = auto()


class FailoverStrategy(Enum):
    """
    Strategy for handling failures in hybrid mode.

    PREFER_P2P: Try P2P first, fall back to central if P2P fails
    PREFER_CENTRAL: Try central first, fall back to P2P if central fails
    PARALLEL: Send to both simultaneously for redundancy
    """
    PREFER_P2P = auto()
    PREFER_CENTRAL = auto()
    PARALLEL = auto()


@dataclass
class CentralServerConfig:
    """Configuration for central server connections."""

    # PubSub WebSocket server
    pubsub_url: str = "ws://pubsub.satorinet.io:24603"

    # Central REST API
    api_url: str = "https://central.satorinet.io"

    # Fallback API for transactions
    sending_url: str = "https://mundo.satorinet.io"

    # Connection settings
    reconnect_delay: float = 60.0  # seconds
    socket_timeout: float = 3600.0  # 1 hour
    health_check_interval: float = 30.0  # seconds

    # Retry settings
    max_retries: int = 3
    retry_backoff: float = 2.0  # exponential backoff multiplier


@dataclass
class P2PConfig:
    """Configuration for P2P networking."""

    # Bootstrap peers
    bootstrap_peers: List[str] = field(default_factory=list)

    # Port settings
    listen_port: int = 4001

    # Relay settings
    enable_relay: bool = True
    enable_relay_hop: bool = False  # Only for dedicated relay nodes

    # DHT settings
    enable_dht: bool = True
    dht_mode: str = "client"  # "client" or "server"

    # Health check
    health_check_interval: float = 30.0


@dataclass
class HybridConfig:
    """
    Complete configuration for hybrid mode operation.

    Usage:
        config = HybridConfig(
            mode=HybridMode.HYBRID,
            failover=FailoverStrategy.PREFER_P2P,
        )
    """

    # Operating mode
    mode: HybridMode = HybridMode.HYBRID

    # Failover strategy (only used in HYBRID mode)
    failover: FailoverStrategy = FailoverStrategy.PREFER_P2P

    # Central server configuration
    central: CentralServerConfig = field(default_factory=CentralServerConfig)

    # P2P configuration
    p2p: P2PConfig = field(default_factory=P2PConfig)

    # Bridge settings
    bridge_all_messages: bool = True  # Bridge messages between modes
    sync_subscriptions: bool = True  # Sync subscriptions across modes

    # Health monitoring
    enable_health_checks: bool = True
    failover_on_disconnect: bool = True
    failover_threshold: int = 3  # Number of failures before failover

    # Logging
    log_bridge_events: bool = True

    @classmethod
    def p2p_only(cls) -> "HybridConfig":
        """Create P2P-only configuration."""
        return cls(mode=HybridMode.P2P_ONLY)

    @classmethod
    def central_only(cls, pubsub_url: Optional[str] = None) -> "HybridConfig":
        """Create central-only configuration."""
        config = cls(mode=HybridMode.CENTRAL_ONLY)
        if pubsub_url:
            config.central.pubsub_url = pubsub_url
        return config

    @classmethod
    def hybrid(
        cls,
        failover: FailoverStrategy = FailoverStrategy.PREFER_P2P,
        pubsub_url: Optional[str] = None,
    ) -> "HybridConfig":
        """Create hybrid configuration."""
        config = cls(
            mode=HybridMode.HYBRID,
            failover=failover,
        )
        if pubsub_url:
            config.central.pubsub_url = pubsub_url
        return config

    def is_p2p_enabled(self) -> bool:
        """Check if P2P networking is enabled."""
        return self.mode in (HybridMode.P2P_ONLY, HybridMode.HYBRID)

    def is_central_enabled(self) -> bool:
        """Check if central server is enabled."""
        return self.mode in (HybridMode.CENTRAL_ONLY, HybridMode.HYBRID)

    def should_bridge(self) -> bool:
        """Check if messages should be bridged between modes."""
        return self.mode == HybridMode.HYBRID and self.bridge_all_messages
