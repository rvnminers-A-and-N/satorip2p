"""
satorip2p/examples/neuron_integration.py

Example integration of satorip2p with a Satori Neuron.

This shows how a Neuron uses satorip2p to:
1. Discover other neurons in the network
2. Subscribe to data streams from other neurons
3. Publish its own data streams (predictions, sensor readings)
4. Handle offline peers via store-and-forward
5. Work in Docker bridge mode (no --network host)

Usage:
    python examples/neuron_integration.py
"""

import asyncio
import logging
from typing import Callable, Optional
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [NEURON] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class DataStream:
    """Represents a data stream (subscription or publication)."""
    stream_id: str
    source: str  # Evrmore address of publisher
    description: str


class SatoriNeuron:
    """
    Example Satori Neuron with P2P integration.

    This demonstrates how satorip2p integrates with the Satori Neuron
    to provide robust peer-to-peer data sharing.
    """

    def __init__(
        self,
        wallet_path: str = None,
        enable_docker_relay: bool = True,
    ):
        """
        Initialize the Neuron.

        Args:
            wallet_path: Path to Evrmore wallet (uses mock if None)
            enable_docker_relay: Enable Circuit Relay for Docker bridge mode
        """
        self.wallet_path = wallet_path
        self.enable_docker_relay = enable_docker_relay

        # P2P layer
        self._peers = None

        # Data streams
        self._my_streams: dict[str, DataStream] = {}
        self._subscribed_streams: dict[str, DataStream] = {}

        # Callbacks
        self._data_callbacks: dict[str, list[Callable]] = {}

    async def start(self):
        """Start the Neuron and P2P layer."""
        from satorip2p import Peers
        from satorip2p.nat.docker import detect_docker_environment

        # Get identity (from wallet or mock)
        identity = self._load_identity()

        # Detect Docker environment
        docker_info = detect_docker_environment()
        if docker_info.in_container:
            logger.info(f"Running in Docker: mode={docker_info.network_mode}")
            if docker_info.needs_relay:
                logger.info("Bridge mode - enabling Circuit Relay for NAT traversal")

        # Initialize P2P layer
        self._peers = Peers(
            identity=identity,
            enable_dht=True,
            enable_pubsub=True,
            enable_rendezvous=True,
            enable_relay=self.enable_docker_relay,
            enable_upnp=not docker_info.in_container,  # UPnP only outside Docker
            bootstrap_peers=self._get_bootstrap_peers(),
        )

        # Start P2P
        await self._peers.start()

        logger.info(f"Neuron started: {self._peers.peer_id}")
        logger.info(f"Evrmore address: {identity.address}")

    async def stop(self):
        """Stop the Neuron and P2P layer."""
        if self._peers:
            await self._peers.stop()
            logger.info("Neuron stopped")

    def _load_identity(self):
        """Load Evrmore identity from wallet or use mock."""
        if self.wallet_path:
            try:
                from satorilib.wallet.evrmore.identity import EvrmoreIdentity
                return EvrmoreIdentity(self.wallet_path)
            except ImportError:
                logger.warning("satorilib not available, using mock identity")

        # Mock identity for testing
        class MockIdentity:
            def __init__(self):
                import os
                seed = int.from_bytes(os.urandom(4), 'big') % 1000000
                self.address = f"ENeuron{seed:06d}"
                self.pubkey = "02" + f"{seed:02x}" * 32
                self._entropy = os.urandom(32)

            def sign(self, msg: str) -> bytes:
                return b"mocksig"

            def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
                return True

            def secret(self, pubkey: str) -> bytes:
                return b"sharedsecret" * 3

        return MockIdentity()

    def _get_bootstrap_peers(self) -> list[str]:
        """Get bootstrap peer addresses."""
        # In production, these would come from configuration
        # or be hardcoded known bootstrap nodes
        return [
            # Example: "/ip4/bootstrap.satorinet.io/tcp/4001/p2p/16Uiu2HAm..."
        ]

    # ========== Publishing ==========

    async def publish_stream(self, stream_id: str, description: str = ""):
        """
        Register as a publisher for a data stream.

        Args:
            stream_id: Unique stream identifier (UUID)
            description: Human-readable description
        """
        identity = self._peers._bridge.identity
        stream = DataStream(
            stream_id=stream_id,
            source=identity.address,
            description=description
        )
        self._my_streams[stream_id] = stream

        # Register in rendezvous
        await self._peers._rendezvous.register_publisher(stream_id)

        logger.info(f"Publishing stream: {stream_id}")

    async def broadcast_data(self, stream_id: str, data: bytes):
        """
        Broadcast data to all subscribers of a stream.

        Args:
            stream_id: Stream to broadcast to
            data: Data payload (bytes)
        """
        if stream_id not in self._my_streams:
            raise ValueError(f"Not a publisher for stream: {stream_id}")

        await self._peers.broadcast(stream_id, data)
        logger.debug(f"Broadcast to {stream_id}: {len(data)} bytes")

    # ========== Subscribing ==========

    def subscribe(
        self,
        stream_id: str,
        callback: Callable[[str, bytes], None],
        source: str = None,
    ):
        """
        Subscribe to a data stream.

        Args:
            stream_id: Stream to subscribe to
            callback: Function called with (stream_id, data) on new data
            source: Expected publisher address (optional verification)
        """
        stream = DataStream(
            stream_id=stream_id,
            source=source or "unknown",
            description=""
        )
        self._subscribed_streams[stream_id] = stream

        # Register callback
        if stream_id not in self._data_callbacks:
            self._data_callbacks[stream_id] = []
        self._data_callbacks[stream_id].append(callback)

        # Subscribe via P2P
        self._peers.subscribe(stream_id, self._on_data_received)

        logger.info(f"Subscribed to stream: {stream_id}")

    def unsubscribe(self, stream_id: str):
        """Unsubscribe from a data stream."""
        if stream_id in self._subscribed_streams:
            del self._subscribed_streams[stream_id]

        if stream_id in self._data_callbacks:
            del self._data_callbacks[stream_id]

        self._peers.unsubscribe(stream_id)
        logger.info(f"Unsubscribed from: {stream_id}")

    def _on_data_received(self, stream_id: str, data: bytes):
        """Internal callback when data is received."""
        callbacks = self._data_callbacks.get(stream_id, [])
        for callback in callbacks:
            try:
                callback(stream_id, data)
            except Exception as e:
                logger.error(f"Callback error for {stream_id}: {e}")

    # ========== Discovery ==========

    async def discover_publishers(self, stream_id: str) -> list[str]:
        """
        Discover publishers for a stream.

        Args:
            stream_id: Stream to find publishers for

        Returns:
            List of peer IDs publishing this stream
        """
        return await self._peers._rendezvous.discover_publishers(stream_id)

    async def discover_subscribers(self, stream_id: str) -> list[str]:
        """
        Discover subscribers for a stream.

        Args:
            stream_id: Stream to find subscribers for

        Returns:
            List of peer IDs subscribed to this stream
        """
        return await self._peers._rendezvous.discover_subscribers(stream_id)

    # ========== Network Info ==========

    def get_network_map(self) -> dict:
        """Get the current network map."""
        return self._peers.get_network_map()

    def get_connected_peers(self) -> list[str]:
        """Get list of currently connected peer IDs."""
        return list(self._peers._connected_peers.keys())

    @property
    def peer_id(self) -> str:
        """Get this neuron's peer ID."""
        return self._peers.peer_id

    @property
    def evrmore_address(self) -> str:
        """Get this neuron's Evrmore address."""
        return self._peers._bridge.evrmore_address


# ========== Example Usage ==========

async def example_publisher():
    """Example: Neuron as a data publisher."""
    neuron = SatoriNeuron()
    await neuron.start()

    # Publish a weather data stream
    await neuron.publish_stream(
        stream_id="weather-sensor-001",
        description="Temperature readings from sensor 001"
    )

    # Broadcast data every 5 seconds
    counter = 0
    try:
        while True:
            counter += 1
            temperature = 20.0 + (counter % 10) * 0.5
            data = f'{{"temp": {temperature}, "unit": "C", "reading": {counter}}}'.encode()

            await neuron.broadcast_data("weather-sensor-001", data)
            logger.info(f"Published temperature: {temperature}°C")

            await asyncio.sleep(5)
    except asyncio.CancelledError:
        await neuron.stop()


async def example_subscriber():
    """Example: Neuron as a data subscriber."""
    neuron = SatoriNeuron()
    await neuron.start()

    # Callback for received data
    def on_weather_data(stream_id: str, data: bytes):
        import json
        try:
            reading = json.loads(data.decode())
            logger.info(f"Weather update: {reading['temp']}°{reading['unit']}")
        except Exception as e:
            logger.error(f"Parse error: {e}")

    # Subscribe to weather stream
    neuron.subscribe(
        stream_id="weather-sensor-001",
        callback=on_weather_data
    )

    # Run until cancelled
    try:
        while True:
            await asyncio.sleep(30)
            peers = neuron.get_connected_peers()
            logger.info(f"Connected to {len(peers)} peers")
    except asyncio.CancelledError:
        await neuron.stop()


async def example_full_neuron():
    """Example: Full Neuron that both publishes and subscribes."""
    neuron = SatoriNeuron()
    await neuron.start()

    # This neuron publishes predictions
    await neuron.publish_stream(
        stream_id="predictions-model-A",
        description="ML model predictions"
    )

    # And subscribes to input data
    def on_input_data(stream_id: str, data: bytes):
        logger.info(f"Received input data: {len(data)} bytes")

    neuron.subscribe("sensor-data-input", on_input_data)

    # Main loop
    counter = 0
    try:
        while True:
            await asyncio.sleep(10)
            counter += 1

            # Publish a prediction
            prediction = f'{{"value": {counter * 1.5}, "confidence": 0.95}}'.encode()
            await neuron.broadcast_data("predictions-model-A", prediction)

            # Log network status
            network_map = neuron.get_network_map()
            logger.info(
                f"Network: {len(network_map['connected_peers'])} connected, "
                f"{len(network_map['known_peers'])} known"
            )

    except asyncio.CancelledError:
        await neuron.stop()


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "full"

    if mode == "publisher":
        asyncio.run(example_publisher())
    elif mode == "subscriber":
        asyncio.run(example_subscriber())
    else:
        asyncio.run(example_full_neuron())
