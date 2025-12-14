"""
satorip2p.hybrid - Hybrid Mode for Central ↔ P2P Migration

This module provides hybrid networking that bridges legacy central servers
with the P2P network, allowing gradual migration.

Architecture:
```
┌─────────────────────────────────────────────────────────────────┐
│                      HYBRID MODE                                │
│                                                                 │
│  ┌─────────────┐     ┌─────────────────┐     ┌─────────────┐   │
│  │   Legacy    │     │   satorip2p     │     │    P2P      │   │
│  │   Neurons   │◄───►│   Bridge Node   │◄───►│   Neurons   │   │
│  │ (Central)   │     │                 │     │ (libp2p)    │   │
│  └─────────────┘     └─────────────────┘     └─────────────┘   │
│         │                    │                      │           │
│         ▼                    ▼                      ▼           │
│  ┌─────────────┐     ┌─────────────────┐     ┌─────────────┐   │
│  │  Central    │     │   DHT + Gossip  │     │   DHT +     │   │
│  │  Servers    │     │   + REST/WS     │     │   Gossip    │   │
│  └─────────────┘     └─────────────────┘     └─────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

Modes:
    - P2P_ONLY: Pure libp2p networking (no central servers)
    - HYBRID: Both P2P and central, messages bridged between them
    - CENTRAL_ONLY: Legacy mode, only central servers (for testing)

Usage:
    from satorip2p.hybrid import HybridPeers, HybridMode

    # Create hybrid peer with automatic failover
    peers = HybridPeers(
        identity=identity,
        mode=HybridMode.HYBRID,
        central_url="ws://pubsub.satorinet.io:24603",
    )

    # Messages automatically flow between P2P and Central
    await peers.start()
    await peers.subscribe("stream-uuid", on_message)
    await peers.publish("stream-uuid", data)
"""

from .config import HybridMode, HybridConfig
from .bridge import HybridBridge
from .hybrid_peers import HybridPeers
from .central_client import CentralPubSubClient

__all__ = [
    "HybridMode",
    "HybridConfig",
    "HybridBridge",
    "HybridPeers",
    "CentralPubSubClient",
]
