# Satori P2P Migration Guide

This guide explains how to migrate from the central Satori pubsub servers to the decentralized P2P network using `satorip2p`.

## Table of Contents

1. [Overview](#overview)
2. [Migration Modes](#migration-modes)
3. [Quick Start](#quick-start)
4. [Detailed Migration Steps](#detailed-migration-steps)
5. [Configuration Options](#configuration-options)
6. [API Compatibility](#api-compatibility)
7. [Troubleshooting](#troubleshooting)

---

## Overview

### Current Architecture (Central)

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────┐
│   Neuron    │────►│  Central Server │◄────│   Neuron    │
│  (Node A)   │     │ pubsub.satori   │     │  (Node B)   │
└─────────────┘     └─────────────────┘     └─────────────┘
```

- All messages flow through `pubsub.satorinet.io:24603`
- Single point of failure
- Requires central server uptime

### New Architecture (P2P)

```
┌─────────────┐                           ┌─────────────┐
│   Neuron    │◄─────── libp2p ──────────►│   Neuron    │
│  (Node A)   │      GossipSub + DHT      │  (Node B)   │
└─────────────┘                           └─────────────┘
        │                                         │
        └────────────► Bootstrap ◄────────────────┘
                        Peers
```

- Direct peer-to-peer communication
- No single point of failure
- Works even if central servers are down

### Hybrid Architecture (Transition)

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────┐
│   Legacy    │────►│    satorip2p    │◄────│    P2P      │
│   Neuron    │     │   Bridge Node   │     │   Neuron    │
└─────────────┘     └─────────────────┘     └─────────────┘
        │                   │                       │
        ▼                   ▼                       ▼
   Central Server      Both Networks           P2P Only
```

- Bridge nodes connect both networks
- Gradual migration without disruption
- Automatic failover

---

## Migration Modes

### Mode 1: P2P Only (`HybridMode.P2P_ONLY`)

Best for: New nodes, testing, or when central servers are unavailable.

```python
from satorip2p import HybridPeers, HybridMode

peers = HybridPeers(
    identity=identity,
    mode=HybridMode.P2P_ONLY,
)
```

### Mode 2: Hybrid (`HybridMode.HYBRID`)

Best for: Transition period, maximum reliability.

```python
from satorip2p import HybridPeers, HybridMode

peers = HybridPeers(
    identity=identity,
    mode=HybridMode.HYBRID,  # Default
)
```

Messages are sent to both P2P and Central, with automatic deduplication.

### Mode 3: Central Only (`HybridMode.CENTRAL_ONLY`)

Best for: Testing compatibility, fallback mode.

```python
from satorip2p import HybridPeers, HybridMode

peers = HybridPeers(
    identity=identity,
    mode=HybridMode.CENTRAL_ONLY,
)
```

---

## Quick Start

### Step 1: Install satorip2p

```bash
# Clone the repository
git clone https://github.com/rvnminers-A-and-N/satorip2p.git
cd satorip2p

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install
pip install -e .
```

### Step 2: Basic Usage

```python
import trio
from satorip2p import HybridPeers, HybridMode
from satorilib.wallet.evrmore.identity import EvrmoreIdentity

# Load your existing wallet
identity = EvrmoreIdentity('/path/to/wallet.yaml')

# Create hybrid peers (P2P + Central fallback)
peers = HybridPeers(
    identity=identity,
    mode=HybridMode.HYBRID,
)

async def main():
    await peers.start()

    # Subscribe to streams (works on both P2P and Central)
    def on_message(stream_id, data):
        print(f"Received: {data}")

    await peers.subscribe("stream-uuid", on_message)

    # Publish data (goes to both networks in HYBRID mode)
    await peers.publish("my-stream-uuid", {"value": 42.0})

    # Run forever
    await peers.run_forever()

trio.run(main)
```

---

## Detailed Migration Steps

### For Node Operators

#### 1. Update Dependencies

Add to your `requirements.txt`:
```
satorip2p>=1.0.0
```

Or install directly:
```bash
pip install git+https://github.com/rvnminers-A-and-N/satorip2p.git
```

#### 2. Replace PubSub Connection

**Before (Central only):**
```python
from satorilib.pubsub import SatoriPubSubConn

pubsub = SatoriPubSubConn(
    uid=wallet.address,
    payload=stream_id,
    url='ws://pubsub.satorinet.io:24603',
    router=my_callback,
)
```

**After (P2P with Central fallback):**
```python
from satorip2p import HybridPeers, HybridMode

peers = HybridPeers(
    identity=identity,
    mode=HybridMode.HYBRID,
)
await peers.start()
await peers.subscribe(stream_id, my_callback)
```

#### 3. Replace Server Client

**Before:**
```python
from satorilib.server import SatoriServerClient

server = SatoriServerClient(wallet=wallet)
details = server.checkin()
```

**After (P2P discovery):**
```python
from satorip2p import Peers

peers = Peers(identity=identity)
await peers.start()

# Discover publishers/subscribers via DHT
publishers = await peers.discover_publishers(stream_id)
subscribers = await peers.discover_subscribers(stream_id)
```

#### 4. Configure Failover Strategy

```python
from satorip2p import HybridPeers, HybridConfig, HybridMode
from satorip2p.hybrid.config import FailoverStrategy

config = HybridConfig(
    mode=HybridMode.HYBRID,
    failover=FailoverStrategy.PREFER_P2P,  # Try P2P first
    # failover=FailoverStrategy.PREFER_CENTRAL,  # Try Central first
    # failover=FailoverStrategy.PARALLEL,  # Send to both
)

peers = HybridPeers(identity=identity, config=config)
```

---

## Configuration Options

### HybridConfig Options

| Option | Default | Description |
|--------|---------|-------------|
| `mode` | `HYBRID` | Operating mode: P2P_ONLY, HYBRID, CENTRAL_ONLY |
| `failover` | `PREFER_P2P` | Failover strategy |
| `bridge_all_messages` | `True` | Bridge messages between modes |
| `sync_subscriptions` | `True` | Sync subscriptions across modes |
| `enable_health_checks` | `True` | Monitor connection health |
| `failover_on_disconnect` | `True` | Auto failover on disconnect |
| `failover_threshold` | `3` | Failures before failover |

### Central Server Config

| Option | Default | Description |
|--------|---------|-------------|
| `pubsub_url` | `ws://pubsub.satorinet.io:24603` | PubSub WebSocket URL |
| `api_url` | `https://central.satorinet.io` | REST API URL |
| `reconnect_delay` | `60.0` | Seconds between reconnects |
| `socket_timeout` | `3600.0` | Socket timeout (1 hour) |

### P2P Config

| Option | Default | Description |
|--------|---------|-------------|
| `listen_port` | `4001` | P2P listen port |
| `enable_relay` | `True` | Enable circuit relay |
| `enable_dht` | `True` | Enable Kademlia DHT |
| `bootstrap_peers` | `[]` | Bootstrap peer addresses |

---

## API Compatibility

### Drop-in Replacements

satorip2p provides compatibility layers that match the existing Satori APIs:

| Original Class | Compatibility Class | Notes |
|----------------|---------------------|-------|
| `SatoriPubSubConn` | `PubSubServer` | WebSocket server on port 24603 |
| `CentrifugoClient` | `CentrifugoServer` | JWT auth, REST publish |
| `SatoriServerClient` | `ServerAPI` | /checkin, /register/* endpoints |
| `DataClient/Server` | `DataManagerBridge` | PyArrow IPC serialization |

### Running Compatibility Servers

If you have existing code that connects to WebSocket servers, you can run compatibility servers:

```python
from satorip2p import Peers, PubSubServer, CentrifugoServer

peers = Peers(identity=identity)
await peers.start()

# Legacy SatoriPubSubConn clients can connect here
pubsub_server = PubSubServer(peers, port=24603)

# Centrifugo clients can connect here
centrifugo_server = CentrifugoServer(peers, port=8000)

async with trio.open_nursery() as nursery:
    nursery.start_soon(peers.run_forever)
    nursery.start_soon(pubsub_server.run_forever)
    nursery.start_soon(centrifugo_server.run_forever)
```

---

## Troubleshooting

### Common Issues

#### 1. "No peers found"

**Cause:** No bootstrap peers configured or network issues.

**Solution:**
```python
from satorip2p import HybridConfig

config = HybridConfig()
config.p2p.bootstrap_peers = [
    "/ip4/1.2.3.4/tcp/4001/p2p/QmBootstrapPeerId...",
]
```

#### 2. "Connection refused to central server"

**Cause:** Central server down or network issues.

**Solution:** Use HYBRID mode with PREFER_P2P failover:
```python
from satorip2p.hybrid.config import FailoverStrategy

config = HybridConfig(
    mode=HybridMode.HYBRID,
    failover=FailoverStrategy.PREFER_P2P,
)
```

#### 3. "Port 4001 already in use"

**Cause:** Another P2P instance or IPFS running.

**Solution:**
```python
peers = HybridPeers(
    identity=identity,
    listen_port=4002,  # Use different port
)
```

#### 4. "Messages not reaching peers behind NAT"

**Cause:** NAT traversal failing.

**Solution:** Ensure relay is enabled and bootstrap peers are reachable:
```python
config = HybridConfig()
config.p2p.enable_relay = True
```

### Debug Logging

Enable debug logging to troubleshoot:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("satorip2p").setLevel(logging.DEBUG)
```

### Health Check

Check connection status:

```python
stats = peers.get_stats()
print(f"P2P Connected: {stats['p2p_connected']}")
print(f"Central Connected: {stats['central_connected']}")
print(f"Subscriptions: {stats['subscriptions']}")
```

---

## Getting Help

- **GitHub Issues:** https://github.com/rvnminers-A-and-N/satorip2p/issues
- **Satori Discord:** [Join the community]
- **Documentation:** See `docs/` folder in the repository

---

## Version History

| Version | Changes |
|---------|---------|
| 1.0.0 | Initial release with full hybrid mode support |
| 0.1.0-alpha | Core P2P functionality |
