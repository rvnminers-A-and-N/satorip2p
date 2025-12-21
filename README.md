# satorip2p

Decentralized peer-to-peer networking module for the Satori Network.

Built on [py-libp2p](https://github.com/libp2p/py-libp2p), this module provides robust P2P capabilities including peer discovery, pub/sub messaging, NAT traversal, and offline message delivery.

## Features

- **Peer Discovery**: Kademlia DHT for finding peers across the network
- **Pub/Sub Messaging**: GossipSub protocol for topic-based communication
- **Stream Subscriptions**: Subscribe to and publish data streams by UUID
- **NAT Traversal**: AutoNAT, Circuit Relay v2, DCUtR (hole punching), and UPnP
- **Offline Messages**: Store-and-forward for peers that are temporarily offline
- **Docker Compatible**: Works in bridge mode without `--network host`
- **Evrmore Identity**: Seamless integration with Satori's Evrmore wallet identity

## Installation

```bash
pip install satorip2p
```

Or install from source:

```bash
git clone https://github.com/SatoriNetwork/satorip2p.git
cd satorip2p
pip install -e .
```

### Dependencies

- **Python 3.12 or 3.13** (required - see note below)
- libp2p >= 0.4.0
- trio >= 0.22.0
- miniupnpc >= 2.2.0

**Important**: Use `pymultihash` (not `py-multihash`) due to API compatibility with libp2p.

> **Python Version Note**: Python 3.14+ is not currently supported due to a protobuf deprecation issue in the upstream libp2p library. Ubuntu 24.04 LTS ships with Python 3.12 by default, which is recommended.

## Quick Start

```python
import asyncio
from satorip2p import Peers

# Mock identity for testing (use EvrmoreIdentity in production)
class MockIdentity:
    def __init__(self):
        self.address = "ETestAddress"
        self.pubkey = "02" + "ab" * 32
        self._entropy = bytes([42] * 32)

    def sign(self, msg): return b"signature"
    def verify(self, msg, sig, pubkey=None, address=None): return True
    def secret(self, pubkey): return b"sharedsecret" * 3

async def main():
    identity = MockIdentity()

    # Create and start peer
    peers = Peers(
        identity=identity,
        enable_dht=True,
        enable_pubsub=True,
        enable_relay=True,
    )
    await peers.start()

    print(f"Peer ID: {peers.peer_id}")

    # Subscribe to a stream
    def on_data(stream_id, data):
        print(f"Received on {stream_id}: {data}")

    peers.subscribe("my-stream-uuid", on_data)

    # Publish data
    await peers.broadcast("my-stream-uuid", b"Hello, network!")

    # Keep running
    await asyncio.sleep(60)
    await peers.stop()

asyncio.run(main())
```

## With Satori EvrmoreIdentity

```python
from satorilib.wallet.evrmore.identity import EvrmoreIdentity
from satorip2p import Peers

async def main():
    # Load real wallet identity
    identity = EvrmoreIdentity('/path/to/wallet.yaml')

    peers = Peers(identity=identity)
    await peers.start()

    # Your peer ID is derived from your Evrmore public key
    print(f"Peer ID: {peers.peer_id}")
    print(f"Evrmore Address: {peers.evrmore_address}")
```

## API Reference

### Peers Class

```python
peers = Peers(
    identity,                    # EvrmoreIdentity or compatible
    listen_port=24600,           # Port to listen on
    bootstrap_peers=[],          # List of bootstrap multiaddrs
    enable_upnp=True,            # Attempt UPnP port mapping
    enable_relay=True,           # Enable Circuit Relay v2
    enable_dht=True,             # Enable Kademlia DHT
    enable_pubsub=True,          # Enable GossipSub
    enable_rendezvous=True,      # Enable Rendezvous protocol
    enable_mdns=True,            # Enable mDNS local discovery
    enable_ping=True,            # Enable Ping protocol
    enable_autonat=True,         # Enable AutoNAT detection
    enable_identify=True,        # Enable Identify protocol
    enable_quic=False,           # Enable QUIC transport (experimental)
    enable_websocket=False,      # Enable WebSocket transport
    rendezvous_is_server=False,  # Run as rendezvous server (for relays)
)

# Lifecycle
await peers.start()
await peers.stop()
await peers.run_forever()               # Run with all background services

# Subscriptions
peers.subscribe(stream_id, callback)    # Subscribe to stream
await peers.subscribe_async(stream_id, callback)  # Subscribe with network registration
peers.unsubscribe(stream_id)            # Unsubscribe from stream
await peers.unsubscribe_async(stream_id)  # Unsubscribe with network unregistration
peers.get_my_subscriptions()            # List subscribed streams

# Publishing
await peers.publish(stream_id, data)    # Publish and register as publisher
await peers.broadcast(stream_id, data)  # Broadcast to subscribers
peers.get_my_publications()             # List published streams

# Network Info
peers.get_network_map()                 # Full network topology
peers.get_connected_peers()             # Currently connected peers
peers.get_subscribers(stream_id)        # Peers subscribed to stream
peers.get_publishers(stream_id)         # Peers publishing stream

# Peer Discovery
await peers.discover_peers()            # Discover peers via DHT
await peers.discover_publishers(stream_id)  # Find publishers for stream

# Direct Messaging
await peers.send(peer_id, message)      # Send to specific peer
await peers.request(peer_id, msg, timeout=30)  # Request/response pattern

# Connectivity & NAT Detection
await peers.ping_peer(peer_id, count=3) # Ping peer, returns latencies
await peers.check_reachability()        # Check if publicly reachable
await peers.get_public_addrs_autonat()  # Get public addresses via AutoNAT
peers.is_address_public(addr)           # Check if address is public

# Properties
peers.peer_id                           # This node's peer ID
peers.evrmore_address                   # This node's Evrmore address
peers.public_key                        # This node's public key
peers.is_connected                      # Whether connected to network
peers.nat_type                          # "PUBLIC", "PRIVATE", or "UNKNOWN"
peers.connected_peers                   # Number of connected peers
peers.public_addresses                  # List of public multiaddresses
peers.is_relay                          # Whether acting as relay node
```

### REST API

satorip2p includes an optional REST API for external integrations:

```python
from satorip2p import Peers
from satorip2p.api import PeersAPI

async def main():
    peers = Peers(identity=identity)
    await peers.start()

    # Start REST API on port 8080
    api = PeersAPI(peers, host="0.0.0.0", port=8080)
    await api.start()

    # API is now available at http://localhost:8080
    await peers.run_forever()
```

**Endpoints:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/status` | GET | Node status and info |
| `/peers` | GET | Connected peers list |
| `/peers/{peer_id}/ping` | POST | Ping a specific peer |
| `/network` | GET | Full network map |
| `/subscriptions` | GET | Current subscriptions |
| `/subscriptions` | POST | Subscribe to a stream |
| `/subscriptions/{stream_id}` | DELETE | Unsubscribe from stream |
| `/publish/{stream_id}` | POST | Publish data to stream |
| `/metrics` | GET | Prometheus metrics |

### Prometheus Metrics

Built-in Prometheus metrics for monitoring:

```python
from satorip2p import Peers
from satorip2p.metrics import MetricsCollector

async def main():
    peers = Peers(identity=identity)
    metrics = MetricsCollector(peers)

    await peers.start()
    # Metrics available at /metrics endpoint via REST API
```

**Available Metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `satorip2p_connected_peers` | Gauge | Number of connected peers |
| `satorip2p_known_peers` | Gauge | Number of known peers |
| `satorip2p_subscriptions_total` | Gauge | Number of active subscriptions |
| `satorip2p_publications_total` | Gauge | Number of active publications |
| `satorip2p_messages_sent_total` | Counter | Total messages sent |
| `satorip2p_messages_received_total` | Counter | Total messages received |
| `satorip2p_ping_latency_seconds` | Histogram | Ping latency distribution |
| `satorip2p_nat_type` | Gauge | NAT type (0=unknown, 1=public, 2=private) |
| `satorip2p_relay_enabled` | Gauge | Whether relay is enabled |
| `satorip2p_uptime_seconds` | Counter | Node uptime |

## Docker

Works in Docker **bridge mode** (no `--network host` required):

```bash
# Build
docker build -t satorip2p-test -f docker/Dockerfile .

# Run tests
docker run --rm satorip2p-test

# Multi-peer test
docker-compose -f docker/docker-compose.yml up --build
```

The module automatically detects Docker bridge mode and enables Circuit Relay for NAT traversal.

## Protocol Managers

satorip2p includes protocol managers for Satori Network operations:

### Staking & Delegation

```python
from satorip2p.protocol.lending import LendingManager
from satorip2p.protocol.delegation import DelegationManager

# Pool lending operations
lending = LendingManager(peers, wallet)
await lending.start()
await lending.register_pool()           # Open pool for lenders
await lending.lend_to_vault(address)    # Lend to a pool
pools = await lending.get_available_pools()

# Stake delegation
delegation = DelegationManager(peers, wallet)
await delegation.start()
await delegation.delegate_to(parent_address)
children = await delegation.get_proxy_children()
```

### Treasury & Rewards

```python
from satorip2p.protocol.donation import DonationManager
from satorip2p.protocol.referral import ReferralManager
from satorip2p.protocol.pricing import PricingManager

# Donations with tier rewards (Bronze → Diamond)
donation = DonationManager(peers, wallet)
stats = await donation.get_my_stats()

# Referral tracking
referral = ReferralManager(peers, wallet)
await referral.set_referrer(address)

# Real-time EVR/SATORI prices from SafeTrade
pricing = PricingManager()
evr_price = await pricing.get_evr_price()
satori_price = await pricing.get_satori_price()
```

### Consensus & Signing

```python
from satorip2p.protocol.consensus import ConsensusManager
from satorip2p.protocol.signer import SignerNode

# Stake-weighted consensus
consensus = ConsensusManager(peers, wallet)
await consensus.propose_round(round_id, merkle_root)
await consensus.vote(round_id, merkle_root)

# Multi-sig treasury (3-of-5 threshold)
signer = SignerNode(peers, wallet)
await signer.request_signatures(tx_data)
```

### Network Roles

| Role | Description | Protocol |
|------|-------------|----------|
| Predictor | Makes predictions on streams | `PredictionProtocol` |
| Oracle | Provides external data | `OracleNetwork` |
| Relay | Routes traffic for NAT traversal | `UptimeTracker` |
| Signer | Signs treasury transactions | `SignerNode` |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        satorip2p                            │
├─────────────────────────────────────────────────────────────┤
│  Peers API                                                  │
│  - subscribe/publish/broadcast                              │
│  - get_network_map/get_connected_peers                      │
├─────────────────────────────────────────────────────────────┤
│  Protocol Managers                                          │
│  - LendingManager / DelegationManager (staking)             │
│  - DonationManager / ReferralManager (treasury)             │
│  - ConsensusManager / SignerNode (governance)               │
│  - OracleNetwork / PredictionProtocol (data)                │
│  - PricingManager (SafeTrade prices)                        │
├─────────────────────────────────────────────────────────────┤
│  Core Protocol Layer                                        │
│  - SubscriptionManager (stream tracking)                    │
│  - MessageStore (offline delivery)                          │
│  - RendezvousManager (stream discovery)                     │
│  - UptimeTracker (heartbeats)                               │
├─────────────────────────────────────────────────────────────┤
│  Identity Layer                                             │
│  - EvrmoreIdentityBridge (wallet → libp2p)                  │
├─────────────────────────────────────────────────────────────┤
│  NAT Layer                                                  │
│  - Docker detection                                         │
│  - UPnP port mapping                                        │
├─────────────────────────────────────────────────────────────┤
│  py-libp2p                                                  │
│  - Kademlia DHT                                             │
│  - GossipSub                                                │
│  - AutoNAT / Circuit Relay / DCUtR                          │
│  - QUIC / TCP transports                                    │
└─────────────────────────────────────────────────────────────┘
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with timeout
pytest tests/ -v --timeout=60

# Run specific test file
pytest tests/test_integration.py -v
```

317 tests covering unit, integration, and Docker scenarios.

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SATORI_P2P_LISTEN_PORT` | Port to listen on | 24600 |
| `SATORI_P2P_BOOTSTRAP` | Bootstrap peer multiaddr | - |
| `SATORI_P2P_ENABLE_RELAY` | Enable Circuit Relay | true |

### Bootstrap Peers

To join the network, configure bootstrap peers:

```python
peers = Peers(
    identity=identity,
    bootstrap_peers=[
        "/ip4/1.2.3.4/tcp/4001/p2p/16Uiu2HAm...",
        "/ip4/5.6.7.8/tcp/4001/p2p/16Uiu2HAm...",
    ]
)
```

## Contributing

Contributions welcome! Please read the contributing guidelines and submit pull requests.

## License

MIT License - see [LICENSE](LICENSE) file.

## Related Projects

- [Satori Network](https://satorinet.io) - Decentralized data oracle network
- [py-libp2p](https://github.com/libp2p/py-libp2p) - Python libp2p implementation
- [satorilib](https://github.com/SatoriNetwork/Lib) - Satori utility library
