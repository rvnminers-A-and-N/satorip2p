"""
satorip2p.integration.networking_mode - Networking Mode Configuration

Provides NetworkingMode enum and configuration helpers for selecting
between Central, Hybrid, and P2P networking modes.

Usage:
    from satorip2p.integration.networking_mode import (
        NetworkingMode,
        get_networking_mode,
        configure_networking,
    )

    # Get mode from config/env/cli
    mode = get_networking_mode()

    # Or configure directly
    configure_networking(mode=NetworkingMode.HYBRID)
"""

from enum import Enum, auto
from typing import Optional, Dict, Any
import os
import logging

logger = logging.getLogger("satorip2p.integration.networking_mode")


class NetworkingMode(Enum):
    """
    Networking mode for Satori network communication.

    CENTRAL: Use only central servers (current production behavior)
    HYBRID: P2P primary with central fallback (recommended for migration)
    P2P_ONLY: Pure P2P, no central server dependency

    The mode can be set via:
    1. Environment variable: SATORI_NETWORKING_MODE
    2. Config file: networking.mode in config.yaml
    3. Command-line flag: --networking-mode
    4. Programmatic configuration
    """
    CENTRAL = auto()
    HYBRID = auto()
    P2P_ONLY = auto()

    @classmethod
    def from_string(cls, value: str) -> "NetworkingMode":
        """Convert string to NetworkingMode."""
        mapping = {
            'central': cls.CENTRAL,
            'hybrid': cls.HYBRID,
            'p2p': cls.P2P_ONLY,
            'p2p_only': cls.P2P_ONLY,
            'p2ponly': cls.P2P_ONLY,
        }
        normalized = value.lower().strip().replace('-', '_').replace(' ', '_')
        if normalized in mapping:
            return mapping[normalized]
        raise ValueError(
            f"Invalid networking mode: {value}. "
            f"Valid options: central, hybrid, p2p"
        )

    def __str__(self) -> str:
        return self.name.lower().replace('_', '-')


class NetworkingConfig:
    """
    Global networking configuration singleton.

    Stores the current networking mode and related settings.
    """
    _instance = None
    _mode: NetworkingMode = NetworkingMode.HYBRID  # Default to hybrid
    _bootstrap_peers: list = []
    _central_url: str = "https://central.satorinet.io"
    _sending_url: str = "https://mundo.satorinet.io"
    _pubsub_url: str = "ws://pubsub.satorinet.io:24603"
    _auto_switch_enabled: bool = True
    _auto_switch_threshold: int = 5  # Min peers to disable central fallback
    _initialized: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_mode(cls) -> NetworkingMode:
        """Get current networking mode."""
        return cls._mode

    @classmethod
    def set_mode(cls, mode: NetworkingMode):
        """Set networking mode."""
        old_mode = cls._mode
        cls._mode = mode
        if old_mode != mode:
            logger.info(f"Networking mode changed: {old_mode} -> {mode}")

    @classmethod
    def get_bootstrap_peers(cls) -> list:
        """Get bootstrap peer addresses."""
        return cls._bootstrap_peers

    @classmethod
    def set_bootstrap_peers(cls, peers: list):
        """Set bootstrap peer addresses."""
        cls._bootstrap_peers = peers

    @classmethod
    def get_central_url(cls) -> str:
        """Get central server URL."""
        return cls._central_url

    @classmethod
    def get_sending_url(cls) -> str:
        """Get sending (mundo) server URL."""
        return cls._sending_url

    @classmethod
    def get_pubsub_url(cls) -> str:
        """Get pubsub server URL."""
        return cls._pubsub_url

    @classmethod
    def is_auto_switch_enabled(cls) -> bool:
        """Check if automatic mode switching is enabled."""
        return cls._auto_switch_enabled

    @classmethod
    def set_auto_switch_enabled(cls, enabled: bool):
        """Enable/disable automatic mode switching."""
        cls._auto_switch_enabled = enabled

    @classmethod
    def get_auto_switch_threshold(cls) -> int:
        """Get minimum peers needed for pure P2P mode."""
        return cls._auto_switch_threshold

    @classmethod
    def set_auto_switch_threshold(cls, threshold: int):
        """Set minimum peers needed for pure P2P mode."""
        cls._auto_switch_threshold = threshold

    @classmethod
    def configure(
        cls,
        mode: Optional[NetworkingMode] = None,
        bootstrap_peers: Optional[list] = None,
        central_url: Optional[str] = None,
        sending_url: Optional[str] = None,
        pubsub_url: Optional[str] = None,
        auto_switch_enabled: Optional[bool] = None,
        auto_switch_threshold: Optional[int] = None,
    ):
        """Configure networking settings."""
        if mode is not None:
            cls.set_mode(mode)
        if bootstrap_peers is not None:
            cls._bootstrap_peers = bootstrap_peers
        if central_url is not None:
            cls._central_url = central_url
        if sending_url is not None:
            cls._sending_url = sending_url
        if pubsub_url is not None:
            cls._pubsub_url = pubsub_url
        if auto_switch_enabled is not None:
            cls._auto_switch_enabled = auto_switch_enabled
        if auto_switch_threshold is not None:
            cls._auto_switch_threshold = auto_switch_threshold
        cls._initialized = True

    @classmethod
    def is_initialized(cls) -> bool:
        """Check if configuration has been explicitly set."""
        return cls._initialized

    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Export configuration as dictionary."""
        return {
            'mode': str(cls._mode),
            'bootstrap_peers': cls._bootstrap_peers,
            'central_url': cls._central_url,
            'sending_url': cls._sending_url,
            'pubsub_url': cls._pubsub_url,
            'auto_switch_enabled': cls._auto_switch_enabled,
            'auto_switch_threshold': cls._auto_switch_threshold,
        }


def get_networking_mode() -> NetworkingMode:
    """
    Get the current networking mode from various sources.

    Priority (highest to lowest):
    1. Programmatic configuration (if set)
    2. Environment variable: SATORI_NETWORKING_MODE
    3. Config file: networking.mode (if satorineuron config available)
    4. Default: HYBRID

    Returns:
        NetworkingMode: The current networking mode
    """
    config = NetworkingConfig()

    # If explicitly configured, use that
    if config.is_initialized():
        return config.get_mode()

    # Check environment variable
    env_mode = os.environ.get('SATORI_NETWORKING_MODE')
    if env_mode:
        try:
            mode = NetworkingMode.from_string(env_mode)
            logger.info(f"Networking mode from env: {mode}")
            return mode
        except ValueError as e:
            logger.warning(f"Invalid SATORI_NETWORKING_MODE: {e}")

    # Try to read from Neuron config
    try:
        from satorineuron import config as neuron_config
        neuron_cfg = neuron_config.get()
        if neuron_cfg:
            networking_cfg = neuron_cfg.get('networking', {})
            if isinstance(networking_cfg, dict):
                mode_str = networking_cfg.get('mode')
            else:
                mode_str = neuron_cfg.get('networking mode')

            if mode_str:
                mode = NetworkingMode.from_string(mode_str)
                logger.info(f"Networking mode from config: {mode}")
                return mode
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"Could not read Neuron config: {e}")

    # Default to hybrid
    return NetworkingMode.HYBRID


def configure_networking(
    mode: Optional[NetworkingMode] = None,
    bootstrap_peers: Optional[list] = None,
    central_url: Optional[str] = None,
    sending_url: Optional[str] = None,
    pubsub_url: Optional[str] = None,
    auto_switch_enabled: Optional[bool] = None,
    auto_switch_threshold: Optional[int] = None,
):
    """
    Configure networking settings programmatically.

    Args:
        mode: Networking mode (CENTRAL, HYBRID, P2P_ONLY)
        bootstrap_peers: List of bootstrap peer multiaddresses
        central_url: Central server URL
        sending_url: Sending (mundo) server URL
        pubsub_url: PubSub server WebSocket URL
        auto_switch_enabled: Enable automatic mode switching
        auto_switch_threshold: Min peers to switch to pure P2P
    """
    NetworkingConfig.configure(
        mode=mode,
        bootstrap_peers=bootstrap_peers,
        central_url=central_url,
        sending_url=sending_url,
        pubsub_url=pubsub_url,
        auto_switch_enabled=auto_switch_enabled,
        auto_switch_threshold=auto_switch_threshold,
    )


def get_networking_config() -> Dict[str, Any]:
    """Get the current networking configuration as a dictionary."""
    return NetworkingConfig.to_dict()


# Click option decorator for CLI support
def networking_mode_option():
    """
    Click option decorator for --networking-mode flag.

    Usage:
        import click
        from satorip2p.integration.networking_mode import networking_mode_option

        @click.command()
        @networking_mode_option()
        def my_command(networking_mode):
            configure_networking(mode=networking_mode)
    """
    try:
        import click

        def decorator(f):
            return click.option(
                '--networking-mode',
                type=click.Choice(['central', 'hybrid', 'p2p'], case_sensitive=False),
                default=None,
                help='Networking mode: central (server only), hybrid (P2P with fallback), p2p (pure P2P)',
                callback=lambda ctx, param, value: (
                    NetworkingMode.from_string(value) if value else None
                ),
            )(f)
        return decorator
    except ImportError:
        # Click not available, return no-op decorator
        def decorator(f):
            return f
        return decorator
