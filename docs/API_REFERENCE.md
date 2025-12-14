# satorip2p API Reference

Complete API documentation for all public classes and methods.

## Table of Contents

1. [Peers](#peers)
2. [HybridPeers](#hybridpeers)
3. [PeersAPI](#peersapi)
4. [SubscriptionManager](#subscriptionmanager)
5. [MessageStore](#messagestore)
6. [EvrmoreIdentityBridge](#evrmoreidentitybridge)
7. [Compatibility Classes](#compatibility-classes)
8. [Configuration Classes](#configuration-classes)

---

## Peers

The core P2P networking class.

```python
from satorip2p import Peers
```

### Constructor

```python
Peers(
    identity: Optional[EvrmoreIdentityBridge] = None,
    port: int = 4001,
    enable_relay: bool = True,
    enable_dht: bool = True,
    bootstrap_peers: Optional[List[str]] = None,
    message_handler: Optional[Callable] = None,
)
```

**Parameters:**
- `identity`: Evrmore identity for signing/verification. If None, generates ephemeral keys.
- `port`: Listen port for P2P connections (default: 4001)
- `enable_relay`: Enable Circuit Relay for NAT traversal (default: True)
- `enable_dht`: Enable Kademlia DHT for peer discovery (default: True)
- `bootstrap_peers`: List of bootstrap peer multiaddrs
- `message_handler`: Global message handler callback

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `peer_id` | `PeerID` | Our libp2p peer ID |
| `connected_peers` | `int` | Number of connected peers |
| `is_running` | `bool` | Whether the node is running |
| `multiaddrs` | `List[str]` | Our listening multiaddresses |

### Methods

#### start()

```python
async def start() -> bool
```

Start the P2P node. Initializes transports, connects to bootstrap peers, and starts background tasks.

**Returns:** `True` if started successfully

**Example:**
```python
peers = Peers(identity=identity)
success = await peers.start()
```

#### stop()

```python
async def stop() -> None
```

Gracefully stop the P2P node.

#### run_forever()

```python
async def run_forever() -> None
```

Run the node with all background tasks. Use with trio nursery.

**Example:**
```python
async with trio.open_nursery() as nursery:
    nursery.start_soon(peers.run_forever)
```

#### subscribe()

```python
def subscribe(
    stream_id: str,
    callback: Callable[[str, Any], None],
) -> None
```

Subscribe to a stream (synchronous version).

**Parameters:**
- `stream_id`: UUID of the stream to subscribe to
- `callback`: Function called with `(stream_id, data)` when messages arrive

#### subscribe_async()

```python
async def subscribe_async(
    stream_id: str,
    callback: Callable[[str, Any], None],
) -> None
```

Subscribe to a stream (async version with DHT announcement).

#### unsubscribe()

```python
def unsubscribe(stream_id: str) -> None
```

Unsubscribe from a stream.

#### publish()

```python
async def publish(
    stream_id: str,
    data: Any,
    sign: bool = True,
) -> None
```

Publish data to a stream.

**Parameters:**
- `stream_id`: UUID of the stream to publish to
- `data`: Data to publish (will be JSON serialized)
- `sign`: Whether to sign the message (default: True)

#### send()

```python
async def send(
    peer_id: str,
    message: bytes,
    protocol: str = "/satori/dm/1.0.0",
) -> bool
```

Send a direct message to a specific peer.

**Parameters:**
- `peer_id`: Target peer ID
- `message`: Message bytes to send
- `protocol`: Protocol ID (default: direct message protocol)

**Returns:** `True` if sent successfully

#### broadcast()

```python
async def broadcast(
    message: Any,
    topic: Optional[str] = None,
) -> int
```

Broadcast a message to all connected peers or specific topic.

**Returns:** Number of peers the message was sent to

#### discover_publishers()

```python
async def discover_publishers(stream_id: str) -> List[str]
```

Discover peers publishing to a stream via DHT/rendezvous.

**Returns:** List of peer IDs publishing to the stream

#### discover_subscribers()

```python
async def discover_subscribers(stream_id: str) -> List[str]
```

Discover peers subscribed to a stream.

**Returns:** List of peer IDs subscribed to the stream

#### get_my_subscriptions()

```python
def get_my_subscriptions() -> List[str]
```

Get list of streams we're subscribed to.

#### get_my_publications()

```python
def get_my_publications() -> List[str]
```

Get list of streams we're publishing to.

#### get_network_map()

```python
def get_network_map() -> Dict[str, Any]
```

Get a map of the network topology.

**Returns:** Dictionary with peer info, subscriptions, and connections

#### get_subscribers()

```python
def get_subscribers(stream_id: str) -> List[str]
```

Get known subscribers for a stream.

---

## HybridPeers

Extended Peers class supporting both P2P and Central server modes.

```python
from satorip2p import HybridPeers, HybridMode
```

### Constructor

```python
HybridPeers(
    identity: Optional[EvrmoreIdentityBridge] = None,
    mode: HybridMode = HybridMode.HYBRID,
    config: Optional[HybridConfig] = None,
    central_url: Optional[str] = None,
    listen_port: int = 4001,
    bootstrap_peers: Optional[List[str]] = None,
)
```

**Parameters:**
- `identity`: Evrmore identity for authentication
- `mode`: Operating mode (P2P_ONLY, HYBRID, CENTRAL_ONLY)
- `config`: Full HybridConfig (overrides other params)
- `central_url`: Central pubsub WebSocket URL
- `listen_port`: P2P listen port
- `bootstrap_peers`: P2P bootstrap peer addresses

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `mode` | `HybridMode` | Current operating mode |
| `p2p` | `Peers` | Underlying P2P instance |
| `connected_peers` | `int` | Number of P2P peers |
| `is_p2p_connected` | `bool` | Whether P2P is connected |
| `is_central_connected` | `bool` | Whether Central is connected |

### Methods

Inherits all methods from `Peers`, plus:

#### subscribe()

```python
async def subscribe(
    stream_id: str,
    callback: Callable[[str, Any], None],
) -> bool
```

Subscribe on both P2P and Central (in HYBRID mode).

#### publish()

```python
async def publish(
    stream_id: str,
    data: Any,
    observation_time: Optional[str] = None,
    observation_hash: Optional[str] = None,
) -> bool
```

Publish to both networks based on failover strategy.

#### get_stats()

```python
def get_stats() -> Dict[str, Any]
```

Get hybrid peers statistics including both networks.

---

## PeersAPI

REST API for external integrations.

```python
from satorip2p import PeersAPI
```

### Constructor

```python
PeersAPI(
    peers: Peers,
    host: str = "0.0.0.0",
    port: int = 8080,
)
```

### Methods

#### start()

```python
async def start() -> None
```

Start the REST API server.

#### stop()

```python
async def stop() -> None
```

Stop the REST API server.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/peers` | List connected peers |
| GET | `/subscriptions` | List subscriptions |
| POST | `/subscribe` | Subscribe to stream |
| POST | `/unsubscribe` | Unsubscribe from stream |
| POST | `/publish` | Publish message |
| GET | `/stats` | Get node statistics |

---

## SubscriptionManager

Manages stream subscriptions and callbacks.

```python
from satorip2p import SubscriptionManager
```

### Methods

#### add_subscription()

```python
def add_subscription(
    stream_id: str,
    callback: Callable[[str, Any], None],
) -> None
```

Add a subscription with callback.

#### remove_subscription()

```python
def remove_subscription(stream_id: str) -> None
```

Remove a subscription.

#### get_subscriptions()

```python
def get_subscriptions() -> List[str]
```

Get all subscribed stream IDs.

#### deliver_message()

```python
def deliver_message(stream_id: str, data: Any) -> None
```

Deliver a message to all callbacks for a stream.

---

## MessageStore

Stores messages for offline delivery.

```python
from satorip2p import MessageStore
```

### Constructor

```python
MessageStore(
    max_messages: int = 1000,
    ttl_seconds: int = 3600,
)
```

### Methods

#### store()

```python
def store(
    stream_id: str,
    message: Any,
    recipient: Optional[str] = None,
) -> str
```

Store a message. Returns message ID.

#### retrieve()

```python
def retrieve(
    stream_id: str,
    since: Optional[datetime] = None,
) -> List[Dict]
```

Retrieve stored messages for a stream.

#### retrieve_for_peer()

```python
def retrieve_for_peer(peer_id: str) -> List[Dict]
```

Retrieve messages stored for a specific peer.

#### cleanup()

```python
def cleanup() -> int
```

Remove expired messages. Returns count removed.

---

## EvrmoreIdentityBridge

Bridge between Evrmore wallet and libp2p identity.

```python
from satorip2p import EvrmoreIdentityBridge
```

### Constructor

```python
EvrmoreIdentityBridge(
    evrmore_identity: EvrmoreIdentity,
)
```

Or from wallet file:

```python
@classmethod
def from_wallet_file(cls, wallet_path: str) -> "EvrmoreIdentityBridge"
```

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `address` | `str` | Evrmore wallet address |
| `peer_id` | `PeerID` | Derived libp2p peer ID |
| `public_key` | `bytes` | Public key bytes |

### Methods

#### sign()

```python
def sign(message: bytes) -> bytes
```

Sign a message with the wallet private key.

#### verify()

```python
def verify(
    message: bytes,
    signature: bytes,
    peer_id: Optional[str] = None,
) -> bool
```

Verify a signature.

#### derive_shared_secret()

```python
def derive_shared_secret(peer_public_key: bytes) -> bytes
```

Derive ECDH shared secret for encrypted messaging.

---

## Compatibility Classes

### PubSubServer

WebSocket server compatible with SatoriPubSubConn.

```python
from satorip2p import PubSubServer

server = PubSubServer(peers, port=24603)
await server.start()
```

### CentrifugoServer

Server compatible with Centrifugo clients.

```python
from satorip2p import CentrifugoServer

server = CentrifugoServer(peers, port=8000, jwt_secret="secret")
await server.start()
```

### ServerAPI

REST API compatible with central server.

```python
from satorip2p import ServerAPI

api = ServerAPI(peers)
# Provides /checkin, /register/stream, /register/subscription
```

### DataManagerBridge

Bridge for DataClient/DataServer compatibility.

```python
from satorip2p import DataManagerBridge, DataManagerMessage

bridge = DataManagerBridge(peers, identity)
await bridge.send_data(peer_id, dataframe)
```

---

## Configuration Classes

### HybridMode

```python
from satorip2p import HybridMode

class HybridMode(Enum):
    P2P_ONLY = auto()      # P2P only, no central
    HYBRID = auto()        # Both P2P and central
    CENTRAL_ONLY = auto()  # Central only, no P2P
```

### HybridConfig

```python
from satorip2p import HybridConfig

config = HybridConfig(
    mode=HybridMode.HYBRID,
    failover=FailoverStrategy.PREFER_P2P,
)

# Or use factory methods
config = HybridConfig.p2p_only()
config = HybridConfig.central_only(pubsub_url="ws://...")
config = HybridConfig.hybrid(failover=FailoverStrategy.PARALLEL)
```

### FailoverStrategy

```python
from satorip2p.hybrid.config import FailoverStrategy

class FailoverStrategy(Enum):
    PREFER_P2P = auto()      # Try P2P first
    PREFER_CENTRAL = auto()  # Try Central first
    PARALLEL = auto()        # Send to both
```

---

## Type Definitions

### PeerInfo

```python
@dataclass
class PeerInfo:
    peer_id: str
    multiaddrs: List[str]
    connected: bool
    latency_ms: Optional[float]
```

### StreamInfo

```python
@dataclass
class StreamInfo:
    stream_id: str
    publishers: List[str]
    subscribers: List[str]
```

---

## Constants

```python
from satorip2p import DEFAULT_PORT, BOOTSTRAP_PEERS

DEFAULT_PORT = 4001
BOOTSTRAP_PEERS = [
    # Default bootstrap nodes (to be populated)
]
```

---

## Error Handling

All methods may raise:

- `ConnectionError`: Network connection issues
- `ValueError`: Invalid parameters
- `TimeoutError`: Operation timed out

**Example:**
```python
try:
    await peers.publish(stream_id, data)
except ConnectionError as e:
    logger.error(f"Failed to publish: {e}")
except TimeoutError:
    logger.warning("Publish timed out, message may not have been delivered")
```
