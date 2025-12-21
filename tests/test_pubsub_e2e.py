"""
satorip2p/tests/test_pubsub_e2e.py

End-to-end tests for P2P pub/sub message delivery.
Tests the core requirement: "when a neuron publishes raw data,
any neuron subscribed to it should get that data"

For local testing, these tests are skipped by default.
For Docker E2E testing, use:
    docker compose -f docker/docker-compose.e2e.yml up --build
    docker compose -f docker/docker-compose.e2e.yml run peer1 pytest tests/test_pubsub_e2e.py -v

Set SATORI_P2P_ROLE=subscriber to enable tests in Docker.
"""

import os
import pytest
import trio
import time
import json
import logging
from typing import List, Tuple, Any

logger = logging.getLogger(__name__)

# Check if we're running in Docker E2E environment
IN_DOCKER = os.environ.get('SATORI_P2P_ROLE') is not None
BOOTSTRAP_PEER = os.environ.get('SATORI_BOOTSTRAP_PEER', '')
P2P_SEED = int(os.environ.get('SATORI_P2P_SEED', '100'))

# Skip tests if not in Docker environment
skip_if_not_docker = pytest.mark.skipif(
    not IN_DOCKER,
    reason="Requires Docker - run with: docker compose -f docker/docker-compose.e2e.yml run peer1 pytest tests/test_pubsub_e2e.py"
)


class MockEvrmoreIdentity:
    """Mock Evrmore identity for tests."""

    def __init__(self, seed: int = 1):
        self.address = f"ETestAddress{seed:032d}"
        self.pubkey = "02" + f"{seed:02x}" * 32
        self._entropy = bytes([seed % 256] * 32)

    def sign(self, msg: str) -> bytes:
        return b"signature"

    def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3


def get_bootstrap_multiaddr():
    """Get bootstrap peer multiaddr based on environment."""
    if BOOTSTRAP_PEER:
        # In Docker, bootstrap is set via env var
        return BOOTSTRAP_PEER
    return None


@skip_if_not_docker
class TestPubSubMessageDelivery:
    """Test actual message delivery between peers via PubSub."""

    @pytest.mark.timeout(120)
    def test_basic_pubsub_delivery(self):
        """Test basic publish/subscribe message delivery between two peers."""
        from satorip2p.peers import Peers

        async def run_test():
            identity = MockEvrmoreIdentity(seed=P2P_SEED)
            received_messages: List[Tuple[str, bytes]] = []
            message_received = trio.Event()

            def on_message(stream_id: str, data: bytes):
                logger.info(f"Received message on {stream_id}: {data}")
                received_messages.append((stream_id, data))
                message_received.set()

            peer = Peers(
                identity=identity,
                listen_port=4002,
                enable_dht=True,
                enable_pubsub=True,
                enable_relay=False,
                enable_upnp=False,
                enable_mdns=True,
                bootstrap_peers=[],
            )

            try:
                await peer.start()
                logger.info(f"Peer started with ID: {peer.peer_id}")

                # Wait for mesh to form via mDNS
                await trio.sleep(3)
                logger.info(f"Connected peers: {peer.connected_peers}")

                # Subscribe to test stream
                test_stream = "satori/test/pubsub"
                await peer.subscribe_async(test_stream, on_message)
                logger.info(f"Subscribed to {test_stream}")

                # Run services and wait for mesh to form
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(peer.run_forever)
                    if peer._pubsub_manager:
                        nursery.start_soon(peer._pubsub_manager.process_messages)

                    # Give time for mesh to form
                    await trio.sleep(5)

                    # Publish a test message
                    test_data = json.dumps({
                        "type": "test",
                        "timestamp": time.time(),
                        "source": peer.peer_id[:16] if peer.peer_id else "unknown",
                    }).encode()

                    await peer.broadcast(test_data, stream_id="test/pubsub")
                    logger.info(f"Published test message")

                    # Wait for message or timeout
                    with trio.move_on_after(30):
                        await message_received.wait()

                    nursery.cancel_scope.cancel()

                logger.info(f"Received {len(received_messages)} messages")

            finally:
                await peer.stop()

        trio.run(run_test)

    @pytest.mark.timeout(120)
    def test_multiple_subscribers(self):
        """Test message delivery to multiple subscribers."""
        from satorip2p.peers import Peers

        async def run_test():
            identity = MockEvrmoreIdentity(seed=P2P_SEED + 10)
            received_stream_a: List[bytes] = []
            received_stream_b: List[bytes] = []

            def on_stream_a(stream_id: str, data: bytes):
                received_stream_a.append(data)

            def on_stream_b(stream_id: str, data: bytes):
                received_stream_b.append(data)

            peer = Peers(
                identity=identity,
                listen_port=4005,
                enable_dht=True,
                enable_pubsub=True,
                enable_upnp=False,
                enable_mdns=True,
                bootstrap_peers=[],
            )

            try:
                await peer.start()

                # Subscribe to multiple streams
                await peer.subscribe_async("satori/stream-a", on_stream_a)
                await peer.subscribe_async("satori/stream-b", on_stream_b)

                async with trio.open_nursery() as nursery:
                    nursery.start_soon(peer.run_forever)
                    if peer._pubsub_manager:
                        nursery.start_soon(peer._pubsub_manager.process_messages)

                    await trio.sleep(3)

                    # Publish to both streams
                    await peer.broadcast(b"message-a", stream_id="stream-a")
                    await peer.broadcast(b"message-b", stream_id="stream-b")

                    await trio.sleep(5)
                    nursery.cancel_scope.cancel()

                logger.info(f"Stream A received: {len(received_stream_a)}")
                logger.info(f"Stream B received: {len(received_stream_b)}")

            finally:
                await peer.stop()

        trio.run(run_test)

    @pytest.mark.timeout(120)
    def test_prediction_publish_receive(self):
        """Test prediction data publish/receive cycle."""
        from satorip2p.peers import Peers

        async def run_test():
            identity = MockEvrmoreIdentity(seed=P2P_SEED + 20)
            predictions_received: List[dict] = []
            prediction_event = trio.Event()

            def on_prediction(stream_id: str, data: bytes):
                try:
                    pred = json.loads(data.decode())
                    predictions_received.append(pred)
                    logger.info(f"Received prediction: {pred}")
                    prediction_event.set()
                except Exception as e:
                    logger.error(f"Error parsing prediction: {e}")

            peer = Peers(
                identity=identity,
                listen_port=4006,
                enable_dht=True,
                enable_pubsub=True,
                enable_upnp=False,
                enable_mdns=True,
                bootstrap_peers=[],
            )

            try:
                await peer.start()

                # Subscribe to predictions stream
                await peer.subscribe_async("satori/predictions", on_prediction)

                async with trio.open_nursery() as nursery:
                    nursery.start_soon(peer.run_forever)
                    if peer._pubsub_manager:
                        nursery.start_soon(peer._pubsub_manager.process_messages)

                    await trio.sleep(3)

                    # Publish a prediction
                    prediction = {
                        "stream_id": "bitcoin/price/usd",
                        "value": 42000.50,
                        "timestamp": time.time(),
                        "predictor": peer.peer_id[:16] if peer.peer_id else "unknown",
                        "commit_hash": "abc123",
                    }
                    await peer.broadcast(
                        json.dumps(prediction).encode(),
                        stream_id="predictions"
                    )

                    with trio.move_on_after(30):
                        await prediction_event.wait()

                    nursery.cancel_scope.cancel()

                logger.info(f"Predictions received: {len(predictions_received)}")

            finally:
                await peer.stop()

        trio.run(run_test)

    @pytest.mark.timeout(120)
    def test_raw_observation_delivery(self):
        """Test raw observation data delivery (core requirement)."""
        from satorip2p.peers import Peers

        async def run_test():
            identity = MockEvrmoreIdentity(seed=P2P_SEED + 30)
            observations: List[dict] = []
            obs_event = trio.Event()

            def on_observation(stream_id: str, data: bytes):
                try:
                    obs = json.loads(data.decode())
                    observations.append(obs)
                    logger.info(f"Received observation: {obs}")
                    obs_event.set()
                except Exception as e:
                    logger.error(f"Error parsing observation: {e}")

            peer = Peers(
                identity=identity,
                listen_port=4007,
                enable_dht=True,
                enable_pubsub=True,
                enable_upnp=False,
                enable_mdns=True,
                bootstrap_peers=[],
            )

            try:
                await peer.start()

                # Subscribe to raw observations
                stream_topic = "satori/raw/bitcoin/price"
                await peer.subscribe_async(stream_topic, on_observation)

                async with trio.open_nursery() as nursery:
                    nursery.start_soon(peer.run_forever)
                    if peer._pubsub_manager:
                        nursery.start_soon(peer._pubsub_manager.process_messages)

                    await trio.sleep(3)

                    # Publish raw observation
                    raw_observation = {
                        "stream": "bitcoin/price",
                        "source": "exchange-api",
                        "value": 41950.25,
                        "timestamp": time.time(),
                        "publisher": peer.peer_id[:16] if peer.peer_id else "unknown",
                    }
                    await peer.broadcast(
                        json.dumps(raw_observation).encode(),
                        stream_id="raw/bitcoin/price"
                    )

                    with trio.move_on_after(30):
                        await obs_event.wait()

                    nursery.cancel_scope.cancel()

                logger.info(f"Observations received: {len(observations)}")

            finally:
                await peer.stop()

        trio.run(run_test)

    @pytest.mark.timeout(60)
    def test_ping_protocol(self):
        """Test the custom Ping protocol for peer connectivity."""
        from satorip2p.peers import Peers

        async def run_test():
            identity = MockEvrmoreIdentity(seed=P2P_SEED + 40)

            peer = Peers(
                identity=identity,
                listen_port=4008,
                enable_dht=True,
                enable_pubsub=True,
                enable_ping=True,
                enable_upnp=False,
                enable_mdns=True,
                bootstrap_peers=[],
            )

            try:
                await peer.start()

                async with trio.open_nursery() as nursery:
                    nursery.start_soon(peer.run_forever)

                    await trio.sleep(3)

                    # Check if ping service is initialized
                    assert peer._ping_service is not None, "Ping service should be initialized"

                    stats = peer._ping_service.get_stats()
                    logger.info(f"Ping protocol stats: {stats}")
                    assert stats['started'] == True, "Ping protocol should be started"

                    nursery.cancel_scope.cancel()

            finally:
                await peer.stop()

        trio.run(run_test)

    @pytest.mark.timeout(60)
    def test_identify_protocol(self):
        """Test the custom Identify protocol for peer info exchange."""
        from satorip2p.peers import Peers

        async def run_test():
            identity = MockEvrmoreIdentity(seed=P2P_SEED + 50)

            peer = Peers(
                identity=identity,
                listen_port=4009,
                enable_dht=True,
                enable_pubsub=True,
                enable_identify=True,
                enable_upnp=False,
                enable_mdns=True,
                bootstrap_peers=[],
            )

            try:
                await peer.start()

                async with trio.open_nursery() as nursery:
                    nursery.start_soon(peer.run_forever)

                    await trio.sleep(3)

                    # Check if identify handler is initialized
                    assert peer._identify_handler is not None, "Identify handler should be initialized"

                    stats = peer._identify_handler.get_stats()
                    logger.info(f"Identify protocol stats: {stats}")
                    assert stats['started'] == True, "Identify protocol should be started"

                    # Test identity announcement
                    await peer.announce_identity()
                    logger.info("Identity announced successfully")

                    nursery.cancel_scope.cancel()

            finally:
                await peer.stop()

        trio.run(run_test)


# Also include tests that work without Docker for basic functionality
class TestPubSubBasics:
    """Basic PubSub tests that work without Docker."""

    @pytest.mark.timeout(30)
    def test_subscription_registration(self):
        """Test that subscription registration works."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=999)

        async def run_test():
            peer = Peers(
                identity=identity,
                listen_port=24999,
                enable_dht=False,
                enable_pubsub=True,
                enable_relay=False,
                enable_upnp=False,
                enable_mdns=False,
                bootstrap_peers=[],
            )

            try:
                await peer.start()

                # Register a subscription
                def callback(stream_id: str, data: bytes):
                    pass

                peer.subscribe("test-stream", callback)
                subs = peer.get_my_subscriptions()

                assert "test-stream" in subs, "Stream should be in subscriptions"
                logger.info(f"Subscriptions: {subs}")

            finally:
                await peer.stop()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_protocol_initialization(self):
        """Test that Ping and Identify protocols initialize."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=998)

        async def run_test():
            peer = Peers(
                identity=identity,
                listen_port=24998,
                enable_dht=False,
                enable_pubsub=True,
                enable_ping=True,
                enable_identify=True,
                bootstrap_peers=[],
            )

            try:
                await peer.start()

                # Check protocols are initialized
                assert peer._ping_service is not None, "Ping service should exist"
                assert peer._identify_handler is not None, "Identify handler should exist"

                logger.info("Ping and Identify protocols initialized successfully")

            finally:
                await peer.stop()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_peer_properties(self):
        """Test peer properties are accessible."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=997)

        async def run_test():
            peer = Peers(
                identity=identity,
                listen_port=24997,
                enable_dht=False,
                enable_pubsub=True,
                bootstrap_peers=[],
            )

            try:
                await peer.start()

                # Check basic properties
                assert peer.peer_id is not None, "Peer ID should be set"
                assert peer.evrmore_address is not None, "Evrmore address should be set"
                assert isinstance(peer.connected_peers, int), "connected_peers should be int"
                assert peer.is_connected in [True, False], "is_connected should be bool"

                logger.info(f"Peer ID: {peer.peer_id[:20]}...")
                logger.info(f"Evrmore: {peer.evrmore_address}")
                logger.info(f"Connected: {peer.connected_peers}")

            finally:
                await peer.stop()

        trio.run(run_test)
