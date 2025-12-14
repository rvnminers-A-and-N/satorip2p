# satorip2p Configuration Guide

Complete guide to all configuration options.

## Table of Contents

1. [Quick Configuration](#quick-configuration)
2. [Peers Configuration](#peers-configuration)
3. [HybridConfig](#hybridconfig)
4. [Network Configuration](#network-configuration)
5. [Environment Variables](#environment-variables)
6. [Docker Configuration](#docker-configuration)
7. [Configuration Examples](#configuration-examples)

---

## Quick Configuration

### Minimal P2P Setup

```python
from satorip2p import Peers
from satorilib.wallet.evrmore.identity import EvrmoreIdentity

# Load wallet
identity = EvrmoreIdentity('/path/to/wallet.yaml')

# Create with defaults
peers = Peers(identity=identity)
await peers.start()
```

### Minimal Hybrid Setup

```python
from satorip2p import HybridPeers, HybridMode

peers = HybridPeers(
    identity=identity,
    mode=HybridMode.HYBRID,
)
await peers.start()
```

---

## Peers Configuration

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `identity` | `EvrmoreIdentityBridge` | `None` | Wallet identity for signing |
| `port` | `int` | `4001` | P2P listen port |
| `enable_relay` | `bool` | `True` | Enable Circuit Relay |
| `enable_dht` | `bool` | `True` | Enable Kademlia DHT |
| `bootstrap_peers` | `List[str]` | `[]` | Bootstrap multiaddrs |
| `message_handler` | `Callable` | `None` | Global message handler |

### Example

```python
from satorip2p import Peers

peers = Peers(
    identity=identity,
    port=4001,
    enable_relay=True,
    enable_dht=True,
    bootstrap_peers=[
        "/ip4/1.2.3.4/tcp/4001/p2p/QmBootstrap...",
        "/ip4/5.6.7.8/tcp/4001/p2p/QmBootstrap2...",
    ],
    message_handler=lambda stream, data: print(f"Got: {data}"),
)
```

---

## HybridConfig

### Full Configuration Class

```python
from satorip2p import HybridConfig, HybridMode
from satorip2p.hybrid.config import (
    FailoverStrategy,
    CentralServerConfig,
    P2PConfig,
)

config = HybridConfig(
    # Operating mode
    mode=HybridMode.HYBRID,

    # Failover strategy
    failover=FailoverStrategy.PREFER_P2P,

    # Bridge settings
    bridge_all_messages=True,  # Bridge messages between modes
    sync_subscriptions=True,   # Sync subscriptions across modes

    # Health check settings
    enable_health_checks=True,
    failover_on_disconnect=True,
    failover_threshold=3,  # Failures before failover
)
```

### HybridMode Options

```python
class HybridMode(Enum):
    P2P_ONLY = auto()      # Pure P2P, no central servers
    HYBRID = auto()        # Both P2P and Central (default)
    CENTRAL_ONLY = auto()  # Central servers only (legacy mode)
```

### FailoverStrategy Options

```python
class FailoverStrategy(Enum):
    PREFER_P2P = auto()      # P2P primary, Central backup
    PREFER_CENTRAL = auto()  # Central primary, P2P backup
    PARALLEL = auto()        # Send to both simultaneously
```

### CentralServerConfig

```python
config.central = CentralServerConfig(
    # WebSocket pubsub URL
    pubsub_url="ws://pubsub.satorinet.io:24603",

    # REST API URL
    api_url="https://central.satorinet.io",

    # Connection settings
    reconnect_delay=60.0,     # Seconds between reconnects
    socket_timeout=3600.0,    # Socket timeout (1 hour)
    max_reconnect_attempts=0, # 0 = infinite

    # Authentication
    use_uid_auth=True,        # Use ?uid= query param
)
```

### P2PConfig

```python
config.p2p = P2PConfig(
    # Network settings
    listen_port=4001,
    listen_addresses=[
        "/ip4/0.0.0.0/tcp/4001",
        "/ip4/0.0.0.0/udp/4001/quic-v1",
    ],

    # Features
    enable_relay=True,     # Circuit Relay for NAT traversal
    enable_dht=True,       # Kademlia DHT for discovery
    enable_upnp=True,      # UPnP port mapping
    enable_autonat=True,   # NAT detection

    # Bootstrap peers
    bootstrap_peers=[],

    # GossipSub settings
    gossipsub_d=6,         # Desired mesh degree
    gossipsub_dlo=4,       # Lower bound
    gossipsub_dhi=12,      # Upper bound
    gossipsub_heartbeat=1.0,  # Heartbeat interval (seconds)

    # DHT settings
    dht_bucket_size=20,    # k-value
    dht_concurrency=3,     # alpha value
)
```

---

## Network Configuration

### Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 4001 | TCP | libp2p connections |
| 4001 | UDP | QUIC connections |
| 8080 | TCP | REST API (optional) |
| 24600 | TCP | DataManager compat (optional) |
| 24603 | TCP | PubSub compat (optional) |

### Firewall Rules

For full P2P functionality, open:

```bash
# TCP for libp2p
sudo ufw allow 4001/tcp

# UDP for QUIC
sudo ufw allow 4001/udp

# REST API (if using)
sudo ufw allow 8080/tcp
```

### UPnP

If your router supports UPnP, satorip2p will automatically request port mappings:

```python
# UPnP is enabled by default
peers = Peers(identity=identity)

# Explicitly disable
config = HybridConfig()
config.p2p.enable_upnp = False
```

---

## Environment Variables

satorip2p supports configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SATORIP2P_PORT` | `4001` | P2P listen port |
| `SATORIP2P_MODE` | `HYBRID` | Operating mode |
| `SATORIP2P_CENTRAL_URL` | `ws://pubsub.satorinet.io:24603` | Central pubsub URL |
| `SATORIP2P_BOOTSTRAP` | (empty) | Comma-separated bootstrap peers |
| `SATORIP2P_ENABLE_RELAY` | `true` | Enable Circuit Relay |
| `SATORIP2P_ENABLE_DHT` | `true` | Enable DHT |
| `SATORIP2P_LOG_LEVEL` | `INFO` | Logging level |

### Example

```bash
export SATORIP2P_PORT=4002
export SATORIP2P_MODE=P2P_ONLY
export SATORIP2P_BOOTSTRAP="/ip4/1.2.3.4/tcp/4001/p2p/Qm..."
export SATORIP2P_LOG_LEVEL=DEBUG

python my_app.py
```

### Loading from Environment

```python
import os
from satorip2p import HybridPeers, HybridMode

mode_str = os.getenv("SATORIP2P_MODE", "HYBRID")
mode = HybridMode[mode_str]

peers = HybridPeers(
    identity=identity,
    mode=mode,
    listen_port=int(os.getenv("SATORIP2P_PORT", "4001")),
)
```

---

## Docker Configuration

### Environment Variables in Docker

```yaml
# docker-compose.yml
services:
  satori-node:
    image: satorip2p:latest
    environment:
      - SATORIP2P_PORT=4001
      - SATORIP2P_MODE=HYBRID
      - SATORIP2P_CENTRAL_URL=ws://pubsub.satorinet.io:24603
      - SATORIP2P_BOOTSTRAP=/ip4/bootstrap.example.com/tcp/4001/p2p/Qm...
    ports:
      - "4001:4001/tcp"
      - "4001:4001/udp"
```

### Docker Network Modes

**Bridge Mode (Recommended):**
```yaml
services:
  node:
    networks:
      - satori-net

networks:
  satori-net:
    driver: bridge
```

**Host Mode (Alternative):**
```yaml
services:
  node:
    network_mode: host
```

### Docker NAT Detection

satorip2p automatically detects Docker environments and adjusts NAT settings:

```python
from satorip2p.nat.docker import DockerNATDetector

detector = DockerNATDetector()
if detector.is_docker():
    # Automatically enable relay for NAT traversal
    pass
```

---

## Configuration Examples

### Production P2P Node

```python
from satorip2p import Peers
from satorilib.wallet.evrmore.identity import EvrmoreIdentity

identity = EvrmoreIdentity('/secure/path/wallet.yaml')

peers = Peers(
    identity=identity,
    port=4001,
    enable_relay=True,
    enable_dht=True,
    bootstrap_peers=[
        "/ip4/bootstrap1.satorinet.io/tcp/4001/p2p/Qm...",
        "/ip4/bootstrap2.satorinet.io/tcp/4001/p2p/Qm...",
    ],
)

await peers.start()
await peers.run_forever()
```

### High-Availability Hybrid

```python
from satorip2p import HybridPeers, HybridConfig, HybridMode
from satorip2p.hybrid.config import FailoverStrategy

config = HybridConfig(
    mode=HybridMode.HYBRID,
    failover=FailoverStrategy.PARALLEL,  # Send to both
    bridge_all_messages=True,
    sync_subscriptions=True,
    enable_health_checks=True,
    failover_on_disconnect=True,
    failover_threshold=3,
)

# Central server config
config.central.pubsub_url = "ws://pubsub.satorinet.io:24603"
config.central.reconnect_delay = 30.0
config.central.max_reconnect_attempts = 10

# P2P config
config.p2p.listen_port = 4001
config.p2p.enable_relay = True
config.p2p.bootstrap_peers = [
    "/ip4/bootstrap1.satorinet.io/tcp/4001/p2p/Qm...",
]

peers = HybridPeers(identity=identity, config=config)
await peers.start()
```

### Relay Node Configuration

For running a public relay node:

```python
from satorip2p import Peers

peers = Peers(
    identity=identity,
    port=4001,
    enable_relay=True,   # Must be True
    enable_dht=True,
)

# Ensure public IP is reachable
# Open ports 4001/tcp and 4001/udp in firewall
```

### Development/Testing

```python
from satorip2p import Peers

# Ephemeral identity for testing
peers = Peers(
    identity=None,  # Auto-generate
    port=0,         # Random available port
    enable_relay=False,
    enable_dht=False,
)

await peers.start()
print(f"Listening on {peers.multiaddrs}")
```

### Central-Only (Legacy Compatibility)

```python
from satorip2p import HybridPeers, HybridMode

peers = HybridPeers(
    identity=identity,
    mode=HybridMode.CENTRAL_ONLY,
    central_url="ws://pubsub.satorinet.io:24603",
)

# Works exactly like old SatoriPubSubConn
await peers.start()
await peers.subscribe("stream-uuid", callback)
```

---

## Configuration File

For complex deployments, use a YAML config file:

```yaml
# config.yaml
mode: HYBRID
failover: PREFER_P2P

p2p:
  port: 4001
  enable_relay: true
  enable_dht: true
  bootstrap_peers:
    - /ip4/bootstrap1.satorinet.io/tcp/4001/p2p/Qm...
    - /ip4/bootstrap2.satorinet.io/tcp/4001/p2p/Qm...

central:
  pubsub_url: ws://pubsub.satorinet.io:24603
  api_url: https://central.satorinet.io
  reconnect_delay: 60

health:
  enable_checks: true
  failover_on_disconnect: true
  failover_threshold: 3
```

Load with:

```python
import yaml
from satorip2p import HybridPeers, HybridConfig, HybridMode
from satorip2p.hybrid.config import FailoverStrategy

with open('config.yaml') as f:
    cfg = yaml.safe_load(f)

config = HybridConfig(
    mode=HybridMode[cfg['mode']],
    failover=FailoverStrategy[cfg['failover']],
)
config.p2p.listen_port = cfg['p2p']['port']
config.p2p.bootstrap_peers = cfg['p2p']['bootstrap_peers']
config.central.pubsub_url = cfg['central']['pubsub_url']

peers = HybridPeers(identity=identity, config=config)
```
