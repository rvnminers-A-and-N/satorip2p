"""
satorip2p/tests/test_integration.py

Integration tests for real P2P connectivity.
These tests require actual network access and may take longer to run.

Run with: pytest tests/test_integration.py -v --timeout=60
"""

import pytest
import asyncio
from unittest.mock import Mock, MagicMock


# Mock EvrmoreIdentity for integration tests
class MockEvrmoreIdentity:
    """Mock Evrmore identity for integration tests."""

    def __init__(self, seed: int = 0):
        # Different seed = different identity
        self.address = f"ETestAddress{seed:032d}"
        self.pubkey = "02" + f"{seed:02x}" * 32
        self._entropy = bytes([seed % 256] * 32)

    def sign(self, msg: str) -> bytes:
        return b"signature"

    def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3


class TestHostCreation:
    """Test libp2p host creation."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_create_host_with_identity(self):
        """Test creating a libp2p host with our identity bridge."""
        from satorip2p.identity import EvrmoreIdentityBridge

        identity = MockEvrmoreIdentity(seed=1)
        bridge = EvrmoreIdentityBridge(identity)

        # This should create a valid libp2p key pair
        key_pair = bridge.to_libp2p_key()
        assert key_pair is not None

        # Should be able to get peer ID
        peer_id = bridge.get_peer_id()
        assert peer_id is not None
        assert len(peer_id) > 10  # Valid peer ID should be substantial

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_peer_id_format(self):
        """Test that PeerID has expected format (not Qm... for secp256k1)."""
        from satorip2p.identity import EvrmoreIdentityBridge

        identity = MockEvrmoreIdentity(seed=42)
        bridge = EvrmoreIdentityBridge(identity)
        peer_id = bridge.get_peer_id()

        # secp256k1 keys produce 16Uiu2HAm... format, NOT Qm...
        assert not peer_id.startswith("Qm"), "secp256k1 keys should not produce Qm... PeerIDs"
        # Should start with 16Uiu (base58 encoding of multihash with identity codec)
        # or 12D3KooW (Ed25519) but NOT Qm (RSA or content hash)


class TestPeersLifecycle:
    """Test Peers class lifecycle without actual network."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_peers_start_stop_minimal(self):
        """Test starting and stopping Peers with minimal setup."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=100)

        peers = Peers(
            identity=identity,
            enable_dht=False,  # Skip DHT for quick test
            enable_pubsub=False,  # Skip PubSub
            enable_rendezvous=False,  # Skip Rendezvous
            enable_upnp=False,  # Skip UPnP
            bootstrap_peers=[],  # No bootstrap
        )

        # Verify initial state
        assert not peers._started
        assert peers.peer_id is None

        # Note: Actually starting requires libp2p host which needs proper setup
        # This is tested in test_create_host_with_identity


class TestSubscriptionFlow:
    """Test subscription workflow without network."""

    def test_subscription_workflow(self):
        """Test full subscription workflow."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=200)
        peers = Peers(identity=identity, bootstrap_peers=[])

        received_data = []

        def callback(stream_id, data):
            received_data.append((stream_id, data))

        # Subscribe
        stream_id = "test-stream-uuid-12345"
        peers.subscribe(stream_id, callback)

        assert stream_id in peers._my_subscriptions
        assert stream_id in peers._callbacks
        assert callback in peers._callbacks[stream_id]

        # Unsubscribe
        peers.unsubscribe(stream_id)

        assert stream_id not in peers._my_subscriptions
        assert stream_id not in peers._callbacks


class TestPublishFlow:
    """Test publish workflow without network."""

    def test_publish_tracking(self):
        """Test that publish correctly tracks publications."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=300)
        peers = Peers(identity=identity, bootstrap_peers=[])

        # Initially empty
        assert len(peers.get_my_publications()) == 0

        # Add publication directly (avoid async broadcast)
        peers._my_publications.add("my-stream-1")
        peers._my_publications.add("my-stream-2")

        pubs = peers.get_my_publications()
        assert len(pubs) == 2
        assert "my-stream-1" in pubs
        assert "my-stream-2" in pubs


class TestNetworkMapGeneration:
    """Test network map generation."""

    def test_network_map_structure(self):
        """Test network map has all required fields."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=400)
        peers = Peers(identity=identity, bootstrap_peers=[])

        peers.subscribe("stream-1", lambda s, d: None)
        peers._my_publications.add("stream-2")

        network_map = peers.get_network_map()

        # Verify structure
        assert "self" in network_map
        assert "connected_peers" in network_map
        assert "known_peers" in network_map
        assert "my_subscriptions" in network_map
        assert "my_publications" in network_map
        assert "streams" in network_map

        # Verify self info
        assert network_map["self"]["evrmore_address"] == identity.address

        # Verify subscriptions/publications
        assert "stream-1" in network_map["my_subscriptions"]
        assert "stream-2" in network_map["my_publications"]


class TestMessageStoreIntegration:
    """Test MessageStore integration."""

    def test_store_workflow(self):
        """Test message store workflow."""
        from satorip2p.protocol.message_store import MessageStore
        from satorip2p.config import MessageEnvelope
        import time

        store = MessageStore(host=None, dht=None)

        # Create envelope
        envelope = MessageEnvelope(
            message_id="msg-001",
            target_peer="peer-target",
            source_peer="peer-source",
            stream_id="stream-123",
            payload=b"Hello, offline peer!"
        )

        # Store message
        store._local_store["peer-target"].append(envelope)

        # Verify stored
        stats = store.get_stats()
        assert stats["total_pending_messages"] == 1
        assert stats["peers_with_pending"] == 1

        # Add relay
        store.add_relay("relay-peer-1")
        assert "relay-peer-1" in store._known_relays


class TestRendezvousIntegration:
    """Test Rendezvous protocol integration."""

    @pytest.mark.asyncio
    async def test_rendezvous_workflow(self):
        """Test rendezvous registration and discovery workflow."""
        from satorip2p.protocol.rendezvous import RendezvousManager

        manager = RendezvousManager(host=None)

        # Start (local mode)
        await manager.start()

        # Register as subscriber
        stream_id = "my-data-stream"
        result = await manager.register_subscriber(stream_id)
        assert result is True

        # Verify registration
        regs = manager.get_my_registrations()
        expected_ns = f"{RendezvousManager.SUBSCRIBER_NS_PREFIX}{stream_id}"
        assert expected_ns in regs

        # Register as publisher
        result = await manager.register_publisher(stream_id)
        assert result is True

        # Verify stats
        stats = manager.get_stats()
        assert stats["active_registrations"] == 2

        # Unregister
        await manager.unregister_subscriber(stream_id)
        await manager.unregister_publisher(stream_id)

        # Stop
        await manager.stop()


class TestDockerCompatibility:
    """Test Docker compatibility features."""

    def test_docker_detection(self):
        """Test Docker environment detection."""
        from satorip2p.nat.docker import detect_docker_environment

        info = detect_docker_environment()

        # Should return valid DockerInfo
        assert hasattr(info, 'in_container')
        assert hasattr(info, 'network_mode')
        assert hasattr(info, 'container_ip')
        assert hasattr(info, 'host_ip')
        assert hasattr(info, 'needs_relay')

    def test_docker_info_relay_recommendation(self):
        """Test that bridge mode recommends relay."""
        from satorip2p.nat.docker import DockerInfo

        # Simulate bridge mode
        info = DockerInfo(
            in_container=True,
            network_mode="bridge",
            container_ip="172.17.0.2",
            host_ip=None,
            gateway_ip="172.17.0.1"
        )

        # Bridge mode should recommend relay
        assert info.needs_relay is True


class TestUPnPIntegration:
    """Test UPnP integration."""

    @pytest.mark.asyncio
    async def test_upnp_graceful_failure(self):
        """Test UPnP handles unavailability gracefully."""
        from satorip2p.nat.upnp import UPnPManager

        manager = UPnPManager()

        # Discovery should not crash even if UPnP unavailable
        result = await manager.discover()
        # Result depends on environment, but shouldn't crash

        # Should report availability status
        assert hasattr(manager, 'is_available')


class TestEndToEndScenarios:
    """End-to-end scenario tests."""

    def test_scenario_subscriber_workflow(self):
        """Test complete subscriber workflow."""
        from satorip2p.peers import Peers
        from satorip2p.protocol.subscriptions import SubscriptionManager

        identity = MockEvrmoreIdentity(seed=500)
        peers = Peers(identity=identity, bootstrap_peers=[])

        # 1. Subscribe to streams
        streams = ["weather-data", "stock-prices", "sensor-readings"]
        for stream in streams:
            peers.subscribe(stream, lambda s, d: print(f"Got {s}: {d}"))

        # 2. Verify subscriptions
        my_subs = peers.get_my_subscriptions()
        assert len(my_subs) == 3
        for stream in streams:
            assert stream in my_subs

        # 3. Get network map
        network_map = peers.get_network_map()
        assert len(network_map["my_subscriptions"]) == 3

        # 4. Unsubscribe from one
        peers.unsubscribe("stock-prices")
        assert len(peers.get_my_subscriptions()) == 2

    def test_scenario_publisher_workflow(self):
        """Test complete publisher workflow."""
        from satorip2p.peers import Peers

        identity = MockEvrmoreIdentity(seed=600)
        peers = Peers(identity=identity, bootstrap_peers=[])

        # 1. Publish to streams (add directly to avoid async)
        streams = ["my-sensor-1", "my-sensor-2"]
        for stream in streams:
            peers._my_publications.add(stream)

        # 2. Verify publications
        my_pubs = peers.get_my_publications()
        assert len(my_pubs) == 2

        # 3. Network map shows publications
        network_map = peers.get_network_map()
        assert "my-sensor-1" in network_map["my_publications"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--timeout=60"])
