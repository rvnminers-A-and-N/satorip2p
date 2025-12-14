# satorip2p Examples

Code examples for common use cases.

## Table of Contents

1. [Basic Usage](#basic-usage)
2. [Pub/Sub Messaging](#pubsub-messaging)
3. [Peer Discovery](#peer-discovery)
4. [Hybrid Mode](#hybrid-mode)
5. [REST API](#rest-api)
6. [Direct Messages](#direct-messages)
7. [DataManager Compatibility](#datamanager-compatibility)
8. [Integration Patterns](#integration-patterns)

---

## Basic Usage

### Minimal P2P Node

```python
import trio
from satorip2p import Peers
from satorilib.wallet.evrmore.identity import EvrmoreIdentity

async def main():
    # Load wallet
    identity = EvrmoreIdentity('/path/to/wallet.yaml')

    # Create and start node
    peers = Peers(identity=identity)
    await peers.start()

    print(f"Node started!")
    print(f"PeerID: {peers.peer_id}")
    print(f"Listening: {peers.multiaddrs}")

    # Run forever
    await peers.run_forever()

if __name__ == "__main__":
    trio.run(main)
```

### Ephemeral Node (Testing)

```python
import trio
from satorip2p import Peers

async def main():
    # No identity = ephemeral keys
    peers = Peers(
        identity=None,
        port=0,  # Random port
    )
    await peers.start()

    print(f"Ephemeral PeerID: {peers.peer_id}")
    await trio.sleep(10)
    await peers.stop()

trio.run(main)
```

---

## Pub/Sub Messaging

### Publisher

```python
import trio
from satorip2p import Peers
from satorilib.wallet.evrmore.identity import EvrmoreIdentity

async def main():
    identity = EvrmoreIdentity('/path/to/wallet.yaml')
    peers = Peers(identity=identity)
    await peers.start()

    stream_id = "my-stream-uuid"

    # Publish messages every 5 seconds
    while True:
        data = {
            "value": 42.0,
            "timestamp": trio.current_time(),
        }

        await peers.publish(stream_id, data)
        print(f"Published: {data}")

        await trio.sleep(5)

trio.run(main)
```

### Subscriber

```python
import trio
from satorip2p import Peers
from satorilib.wallet.evrmore.identity import EvrmoreIdentity

def on_message(stream_id: str, data: dict):
    """Handle incoming messages."""
    print(f"Received on {stream_id}: {data}")

async def main():
    identity = EvrmoreIdentity('/path/to/wallet.yaml')
    peers = Peers(identity=identity)
    await peers.start()

    # Subscribe to stream
    stream_id = "my-stream-uuid"
    await peers.subscribe_async(stream_id, on_message)
    print(f"Subscribed to {stream_id}")

    # Run forever
    await peers.run_forever()

trio.run(main)
```

### Multi-Stream Subscriber

```python
import trio
from satorip2p import Peers

def create_handler(stream_name: str):
    """Create a handler for a specific stream."""
    def handler(stream_id: str, data: dict):
        print(f"[{stream_name}] {data}")
    return handler

async def main():
    peers = Peers()
    await peers.start()

    streams = [
        ("stream-1", "Temperature"),
        ("stream-2", "Humidity"),
        ("stream-3", "Pressure"),
    ]

    for stream_id, name in streams:
        await peers.subscribe_async(stream_id, create_handler(name))
        print(f"Subscribed to {name} ({stream_id})")

    await peers.run_forever()

trio.run(main)
```

---

## Peer Discovery

### Discover Publishers

```python
import trio
from satorip2p import Peers

async def main():
    peers = Peers()
    await peers.start()

    # Wait for DHT bootstrap
    await trio.sleep(5)

    stream_id = "target-stream-uuid"

    # Find who's publishing this stream
    publishers = await peers.discover_publishers(stream_id)
    print(f"Found {len(publishers)} publishers:")
    for pub in publishers:
        print(f"  - {pub}")

trio.run(main)
```

### Network Mapping

```python
import trio
import json
from satorip2p import Peers

async def main():
    peers = Peers()
    await peers.start()

    # Wait for network discovery
    await trio.sleep(10)

    # Get full network map
    network_map = peers.get_network_map()

    print("Network Map:")
    print(json.dumps(network_map, indent=2, default=str))

    # Specific queries
    print(f"\nConnected peers: {peers.connected_peers}")
    print(f"My subscriptions: {peers.get_my_subscriptions()}")
    print(f"My publications: {peers.get_my_publications()}")

trio.run(main)
```

---

## Hybrid Mode

### P2P with Central Fallback

```python
import trio
from satorip2p import HybridPeers, HybridMode
from satorilib.wallet.evrmore.identity import EvrmoreIdentity

def on_message(stream_id: str, data: dict):
    print(f"Received: {data}")

async def main():
    identity = EvrmoreIdentity('/path/to/wallet.yaml')

    # Hybrid mode - both P2P and Central
    peers = HybridPeers(
        identity=identity,
        mode=HybridMode.HYBRID,
    )
    await peers.start()

    print(f"Mode: {peers.mode.name}")
    print(f"P2P Connected: {peers.is_p2p_connected}")
    print(f"Central Connected: {peers.is_central_connected}")

    await peers.subscribe("stream-uuid", on_message)
    await peers.run_forever()

trio.run(main)
```

### Failover Strategy

```python
import trio
from satorip2p import HybridPeers, HybridConfig, HybridMode
from satorip2p.hybrid.config import FailoverStrategy

async def main():
    config = HybridConfig(
        mode=HybridMode.HYBRID,
        failover=FailoverStrategy.PREFER_P2P,  # P2P primary
        failover_on_disconnect=True,
        failover_threshold=3,  # Failures before switch
    )

    peers = HybridPeers(identity=identity, config=config)
    await peers.start()

    # Monitor health
    while True:
        stats = peers.get_stats()
        print(f"P2P: {stats['p2p_connected']}, Central: {stats['central_connected']}")
        await trio.sleep(10)

trio.run(main)
```

### Central-Only (Legacy Mode)

```python
import trio
from satorip2p import HybridPeers, HybridMode

async def main():
    # Use only central servers (like old SatoriPubSubConn)
    peers = HybridPeers(
        identity=identity,
        mode=HybridMode.CENTRAL_ONLY,
        central_url="ws://pubsub.satorinet.io:24603",
    )
    await peers.start()

    # Same API as before
    await peers.subscribe("stream-uuid", callback)
    await peers.publish("stream-uuid", {"value": 42})

trio.run(main)
```

---

## REST API

### Running API Server

```python
import trio
from satorip2p import Peers, PeersAPI

async def main():
    peers = Peers()
    await peers.start()

    api = PeersAPI(peers, host="0.0.0.0", port=8080)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(peers.run_forever)
        nursery.start_soon(api.start)

trio.run(main)
```

### Calling API from curl

```bash
# Health check
curl http://localhost:8080/health

# List peers
curl http://localhost:8080/peers

# Subscribe
curl -X POST http://localhost:8080/subscribe \
  -H "Content-Type: application/json" \
  -d '{"stream_id": "my-stream"}'

# Publish
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"stream_id": "my-stream", "data": {"value": 42}}'

# Get stats
curl http://localhost:8080/stats
```

### API Client (Python)

```python
import httpx

async def api_client():
    async with httpx.AsyncClient(base_url="http://localhost:8080") as client:
        # Subscribe
        response = await client.post("/subscribe", json={"stream_id": "my-stream"})
        print(f"Subscribe: {response.json()}")

        # Publish
        response = await client.post("/publish", json={
            "stream_id": "my-stream",
            "data": {"value": 42.0}
        })
        print(f"Publish: {response.json()}")

        # Get stats
        response = await client.get("/stats")
        print(f"Stats: {response.json()}")
```

---

## Direct Messages

### Encrypted Direct Message

```python
import trio
from satorip2p import Peers, EvrmoreIdentityBridge

async def main():
    identity = EvrmoreIdentityBridge.from_wallet_file('/path/to/wallet.yaml')
    peers = Peers(identity=identity)
    await peers.start()

    # Target peer
    target_peer_id = "12D3KooW..."

    # Derive shared secret for encryption
    # (Need target's public key from DHT or previous communication)
    # shared_secret = identity.derive_shared_secret(target_public_key)

    # Send encrypted message
    message = b"Hello, this is a private message!"
    success = await peers.send(target_peer_id, message)

    if success:
        print("Message sent!")
    else:
        print("Failed to send")

trio.run(main)
```

### Receiving Direct Messages

```python
import trio
from satorip2p import Peers

def dm_handler(peer_id: str, message: bytes):
    """Handle incoming direct messages."""
    print(f"DM from {peer_id}: {message.decode()}")

async def main():
    peers = Peers(
        identity=identity,
        message_handler=dm_handler,  # Set global handler
    )
    await peers.start()
    await peers.run_forever()

trio.run(main)
```

---

## DataManager Compatibility

### Sending DataFrames

```python
import trio
import pandas as pd
from satorip2p import Peers, DataManagerBridge

async def main():
    peers = Peers(identity=identity)
    await peers.start()

    bridge = DataManagerBridge(peers, identity)

    # Create DataFrame
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=10, freq="h"),
        "value": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
    })

    # Send to peer
    target_peer = "12D3KooW..."
    stream_id = "my-stream"

    success = await bridge.send_data(target_peer, stream_id, df)
    print(f"Sent: {success}")

trio.run(main)
```

### Receiving DataFrames

```python
import trio
from satorip2p import Peers, DataManagerBridge

def on_data(stream_id: str, df):
    """Handle incoming DataFrame."""
    print(f"Received DataFrame for {stream_id}:")
    print(df.head())

async def main():
    peers = Peers(identity=identity)
    await peers.start()

    bridge = DataManagerBridge(peers, identity, on_data_callback=on_data)
    await bridge.start()

    await peers.run_forever()

trio.run(main)
```

---

## Integration Patterns

### Satori Neuron Integration

```python
import trio
from satorip2p import HybridPeers, HybridMode
from satorilib.wallet.evrmore.identity import EvrmoreIdentity

class SatoriNeuron:
    """Example Neuron integration."""

    def __init__(self, wallet_path: str):
        self.identity = EvrmoreIdentity(wallet_path)
        self.peers = HybridPeers(
            identity=self.identity,
            mode=HybridMode.HYBRID,
        )
        self.subscriptions = {}

    async def start(self):
        await self.peers.start()
        print(f"Neuron started with address: {self.identity.address}")

    async def subscribe_to_stream(self, stream_id: str, callback):
        """Subscribe to a data stream."""
        self.subscriptions[stream_id] = callback
        await self.peers.subscribe(stream_id, callback)

    async def publish_observation(self, stream_id: str, value: float):
        """Publish an observation."""
        import time
        data = {
            "value": value,
            "timestamp": time.time(),
            "source": self.identity.address,
        }
        await self.peers.publish(stream_id, data)

    async def run(self):
        await self.peers.run_forever()

async def main():
    neuron = SatoriNeuron('/path/to/wallet.yaml')
    await neuron.start()

    # Subscribe to inputs
    await neuron.subscribe_to_stream(
        "input-stream",
        lambda sid, data: print(f"Input: {data}")
    )

    # Publish outputs
    async with trio.open_nursery() as nursery:
        nursery.start_soon(neuron.run)

        while True:
            await neuron.publish_observation("output-stream", 42.0)
            await trio.sleep(60)

trio.run(main)
```

### Background Worker

```python
import trio
from satorip2p import Peers

class BackgroundWorker:
    """Worker that processes P2P messages."""

    def __init__(self):
        self.peers = Peers()
        self.queue = []

    async def start(self):
        await self.peers.start()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.peers.run_forever)
            nursery.start_soon(self._process_queue)

    def on_message(self, stream_id: str, data: dict):
        """Queue message for processing."""
        self.queue.append((stream_id, data))

    async def _process_queue(self):
        """Process queued messages."""
        while True:
            if self.queue:
                stream_id, data = self.queue.pop(0)
                await self._handle_message(stream_id, data)
            await trio.sleep(0.1)

    async def _handle_message(self, stream_id: str, data: dict):
        """Handle a single message."""
        print(f"Processing: {stream_id} -> {data}")
        # Do work here

async def main():
    worker = BackgroundWorker()

    await worker.peers.start()
    await worker.peers.subscribe_async("work-stream", worker.on_message)

    await worker.start()

trio.run(main)
```

### Multi-Process Architecture

```python
import trio
import multiprocessing
from satorip2p import Peers

def p2p_worker(queue):
    """P2P worker process."""
    import trio

    async def run():
        peers = Peers()
        await peers.start()

        def on_message(stream_id, data):
            queue.put((stream_id, data))

        await peers.subscribe_async("data-stream", on_message)
        await peers.run_forever()

    trio.run(run)

def processor_worker(queue):
    """Message processor process."""
    while True:
        stream_id, data = queue.get()
        print(f"Processing: {data}")

if __name__ == "__main__":
    queue = multiprocessing.Queue()

    p2p_proc = multiprocessing.Process(target=p2p_worker, args=(queue,))
    proc_proc = multiprocessing.Process(target=processor_worker, args=(queue,))

    p2p_proc.start()
    proc_proc.start()

    p2p_proc.join()
    proc_proc.join()
```

---

## Error Handling

### Robust Publisher

```python
import trio
from satorip2p import Peers
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def publish_with_retry(peers, stream_id, data, max_retries=3):
    """Publish with retry logic."""
    for attempt in range(max_retries):
        try:
            await peers.publish(stream_id, data)
            return True
        except ConnectionError as e:
            logger.warning(f"Publish failed (attempt {attempt + 1}): {e}")
            await trio.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            break

    return False

async def main():
    peers = Peers()
    await peers.start()

    success = await publish_with_retry(
        peers,
        "my-stream",
        {"value": 42},
        max_retries=5
    )

    if not success:
        logger.error("Failed to publish after all retries")

trio.run(main)
```

### Connection Monitor

```python
import trio
from satorip2p import HybridPeers, HybridMode

async def monitor_connection(peers):
    """Monitor connection health."""
    while True:
        stats = peers.get_stats()

        if not stats["p2p_connected"] and not stats["central_connected"]:
            print("WARNING: No connectivity!")
        elif not stats["p2p_connected"]:
            print("P2P down, using Central")
        elif not stats["central_connected"]:
            print("Central down, using P2P")
        else:
            print("Both networks healthy")

        await trio.sleep(30)

async def main():
    peers = HybridPeers(mode=HybridMode.HYBRID)
    await peers.start()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(peers.run_forever)
        nursery.start_soon(monitor_connection, peers)

trio.run(main)
```
