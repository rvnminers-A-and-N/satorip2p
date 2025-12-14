"""
satorip2p - Peer-to-peer networking module for Satori Network

Built on py-libp2p with:
- Kademlia DHT for peer discovery
- GossipSub for pub/sub messaging
- AutoNAT + Circuit Relay for NAT traversal
- Custom MessageStore for offline message delivery
- REST API for external integrations
- Prometheus metrics for monitoring

Usage:
    from satorip2p import Peers
    from satorilib.wallet.evrmore.identity import EvrmoreIdentity

    identity = EvrmoreIdentity('/path/to/wallet.yaml')
    peers = Peers(identity=identity)

    await peers.start()

    # Subscribe to a stream
    peers.subscribe("stream-uuid", my_callback)

    # Publish data
    peers.publish("my-stream-uuid", data)

    # Send direct message
    await peers.send(peer_id, message)

REST API Usage:
    from satorip2p import Peers
    from satorip2p.api import PeersAPI

    peers = Peers(identity=identity)
    await peers.start()

    api = PeersAPI(peers, host="0.0.0.0", port=8080)
    await api.start()

Metrics Usage:
    from satorip2p.metrics import MetricsCollector

    metrics = MetricsCollector(peers)
    prometheus_output = metrics.collect()
"""

from .peers import Peers
from .protocol.subscriptions import SubscriptionManager
from .protocol.message_store import MessageStore
from .identity.evrmore_bridge import EvrmoreIdentityBridge
from .api import PeersAPI
from .metrics import MetricsCollector
from .compat import (
    PubSubServer,
    CentrifugoServer,
    ServerAPI,
    DataManagerBridge,
    Message as DataManagerMessage,
)
from .hybrid import (
    HybridPeers,
    HybridMode,
    HybridConfig,
    HybridBridge,
)
from .config import (
    PeerInfo,
    StreamInfo,
    DEFAULT_PORT,
    BOOTSTRAP_PEERS,
)
from .integration import (
    # Drop-in replacement classes
    P2PSatoriPubSubConn,
    P2PSatoriServerClient,
    P2PCentrifugoClient,
    P2PStartupDagMixin,
    create_p2p_centrifugo_client,
    # Networking mode
    NetworkingMode,
    NetworkingConfig,
    get_networking_mode,
    configure_networking,
    get_networking_config,
    # Metrics
    IntegrationMetrics,
    # Fallback & Circuit Breaker
    FallbackHandler,
    CircuitBreaker,
    get_fallback_handler,
    # Auto-switch
    AutoSwitch,
    AutoSwitchConfig,
    start_auto_switch,
    stop_auto_switch,
)

__version__ = "1.0.0"
__all__ = [
    # Core
    "Peers",
    "PeerInfo",
    "StreamInfo",
    # API & Metrics
    "PeersAPI",
    "MetricsCollector",
    # Protocol
    "SubscriptionManager",
    "MessageStore",
    # Identity
    "EvrmoreIdentityBridge",
    # Compatibility Layer
    "PubSubServer",
    "CentrifugoServer",
    "ServerAPI",
    "DataManagerBridge",
    "DataManagerMessage",
    # Hybrid Mode
    "HybridPeers",
    "HybridMode",
    "HybridConfig",
    "HybridBridge",
    # Config
    "DEFAULT_PORT",
    "BOOTSTRAP_PEERS",
    # Integration (Drop-in Replacements)
    "P2PSatoriPubSubConn",
    "P2PSatoriServerClient",
    "P2PCentrifugoClient",
    "P2PStartupDagMixin",
    "create_p2p_centrifugo_client",
    # Networking Mode
    "NetworkingMode",
    "NetworkingConfig",
    "get_networking_mode",
    "configure_networking",
    "get_networking_config",
    # Integration Metrics
    "IntegrationMetrics",
    # Fallback & Circuit Breaker
    "FallbackHandler",
    "CircuitBreaker",
    "get_fallback_handler",
    # Auto-switch
    "AutoSwitch",
    "AutoSwitchConfig",
    "start_auto_switch",
    "stop_auto_switch",
]
