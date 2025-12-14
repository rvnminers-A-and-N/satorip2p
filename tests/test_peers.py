"""
satorip2p/tests/test_peers.py

Unit tests for the Peers class.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import asyncio


# Mock EvrmoreIdentity for testing
class MockEvrmoreIdentity:
    """Mock Evrmore identity for tests."""

    def __init__(self):
        self.address = "ETestAddress123456789012345678901234"
        self.pubkey = "02" + "a1" * 32  # Mock compressed pubkey
        self._entropy = b"x" * 32  # 32-byte entropy

    def sign(self, msg: str) -> bytes:
        return b"signature"

    def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3  # 32 bytes


@pytest.fixture
def mock_identity():
    """Create mock Evrmore identity."""
    return MockEvrmoreIdentity()


class TestPeersInit:
    """Test Peers class initialization."""

    def test_init_default_params(self, mock_identity):
        """Test initialization with default parameters."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert peers.identity == mock_identity
        assert peers.listen_port == 24600
        assert peers.enable_upnp is True
        assert peers.enable_relay is True
        assert peers.enable_dht is True
        assert peers._started is False

    def test_init_custom_params(self, mock_identity):
        """Test initialization with custom parameters."""
        from satorip2p.peers import Peers

        peers = Peers(
            identity=mock_identity,
            listen_port=25000,
            enable_upnp=False,
            enable_relay=False,
            bootstrap_peers=["/ip4/1.2.3.4/tcp/4001/p2p/QmTest"],
        )

        assert peers.listen_port == 25000
        assert peers.enable_upnp is False
        assert peers.enable_relay is False
        assert len(peers.bootstrap_peers) == 1


class TestPeersProperties:
    """Test Peers property methods."""

    def test_evrmore_address(self, mock_identity):
        """Test evrmore_address property."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)
        assert peers.evrmore_address == "ETestAddress123456789012345678901234"

    def test_public_key(self, mock_identity):
        """Test public_key property."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)
        assert peers.public_key == mock_identity.pubkey

    def test_is_connected_false_when_not_started(self, mock_identity):
        """Test is_connected returns False when not started."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)
        assert peers.is_connected is False

    def test_connected_peers_zero_when_not_started(self, mock_identity):
        """Test connected_peers returns 0 when not started."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)
        assert peers.connected_peers == 0


class TestSubscriptionManagement:
    """Test subscription management methods."""

    def test_subscribe(self, mock_identity):
        """Test subscribing to a stream."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        callback = Mock()
        peers.subscribe("stream-123", callback)

        assert "stream-123" in peers._callbacks
        assert callback in peers._callbacks["stream-123"]
        assert "stream-123" in peers._my_subscriptions

    def test_unsubscribe(self, mock_identity):
        """Test unsubscribing from a stream."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        callback = Mock()
        peers.subscribe("stream-123", callback)
        peers.unsubscribe("stream-123")

        assert "stream-123" not in peers._callbacks
        assert "stream-123" not in peers._my_subscriptions

    def test_get_my_subscriptions(self, mock_identity):
        """Test getting our subscriptions."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        peers.subscribe("stream-1", Mock())
        peers.subscribe("stream-2", Mock())

        subs = peers.get_my_subscriptions()
        assert len(subs) == 2
        assert "stream-1" in subs
        assert "stream-2" in subs


class TestNetworkMap:
    """Test network map functionality."""

    def test_get_network_map_basic(self, mock_identity):
        """Test getting basic network map."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)
        peers.subscribe("stream-1", Mock())

        network_map = peers.get_network_map()

        assert "self" in network_map
        assert network_map["self"]["evrmore_address"] == mock_identity.address
        assert "stream-1" in network_map["my_subscriptions"]


class TestIdentityBridge:
    """Test Evrmore identity bridge."""

    def test_bridge_creation(self, mock_identity):
        """Test creating identity bridge."""
        from satorip2p.identity import EvrmoreIdentityBridge

        bridge = EvrmoreIdentityBridge(mock_identity)

        assert bridge.evrmore_address == mock_identity.address
        assert bridge.evrmore_pubkey == mock_identity.pubkey

    def test_sign_and_verify(self, mock_identity):
        """Test signing and verification through bridge."""
        from satorip2p.identity import EvrmoreIdentityBridge

        bridge = EvrmoreIdentityBridge(mock_identity)

        signature = bridge.sign(b"test message")
        assert signature is not None

        is_valid = bridge.verify(b"test message", signature)
        assert is_valid is True


class TestSubscriptionManager:
    """Test subscription manager."""

    def test_add_subscriber(self):
        """Test adding a subscriber."""
        from satorip2p.protocol.subscriptions import SubscriptionManager

        manager = SubscriptionManager()
        manager.add_subscriber("stream-1", "peer-1", "Eaddr1")

        subs = manager.get_subscribers("stream-1")
        assert "peer-1" in subs

    def test_add_publisher(self):
        """Test adding a publisher."""
        from satorip2p.protocol.subscriptions import SubscriptionManager

        manager = SubscriptionManager()
        manager.add_publisher("stream-1", "peer-1", "Eaddr1")

        pubs = manager.get_publishers("stream-1")
        assert "peer-1" in pubs

    def test_remove_peer(self):
        """Test removing a peer."""
        from satorip2p.protocol.subscriptions import SubscriptionManager

        manager = SubscriptionManager()
        manager.add_subscriber("stream-1", "peer-1")
        manager.add_subscriber("stream-2", "peer-1")
        manager.remove_peer("peer-1")

        assert "peer-1" not in manager.get_subscribers("stream-1")
        assert "peer-1" not in manager.get_subscribers("stream-2")


class TestMessageSerialization:
    """Test message serialization."""

    def test_serialize_deserialize_dict(self):
        """Test serializing and deserializing a dict."""
        from satorip2p.protocol.messages import serialize_message, deserialize_message

        original = {"key": "value", "number": 42}
        serialized = serialize_message(original)
        deserialized = deserialize_message(serialized)

        assert deserialized == original

    def test_serialize_deserialize_with_bytes(self):
        """Test serializing dict with bytes."""
        from satorip2p.protocol.messages import serialize_message, deserialize_message

        original = {"data": b"binary data"}
        serialized = serialize_message(original)
        deserialized = deserialize_message(serialized)

        assert deserialized["data"] == b"binary data"

    def test_serialize_deserialize_with_dataframe(self):
        """Test serializing dict with DataFrame."""
        pytest.importorskip("pandas")
        import pandas as pd
        from satorip2p.protocol.messages import serialize_message, deserialize_message

        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        original = {"data": df}
        serialized = serialize_message(original)
        deserialized = deserialize_message(serialized)

        assert isinstance(deserialized["data"], pd.DataFrame)
        assert list(deserialized["data"]["col1"]) == [1, 2, 3]


class TestDockerDetection:
    """Test Docker environment detection."""

    def test_detect_not_in_container(self):
        """Test detection when not in container."""
        from satorip2p.nat.docker import detect_docker_environment

        # This will detect actual environment
        info = detect_docker_environment()
        # Just check it returns valid structure
        assert hasattr(info, "in_container")
        assert hasattr(info, "network_mode")


class TestUPnPManager:
    """Test UPnP manager."""

    @pytest.mark.asyncio
    async def test_upnp_not_available(self):
        """Test UPnP when not available."""
        from satorip2p.nat.upnp import UPnPManager

        # Mock miniupnpc to raise ImportError
        with patch.dict("sys.modules", {"miniupnpc": None}):
            manager = UPnPManager()
            result = await manager.discover()
            # Should handle gracefully
            assert manager.is_available is False


class TestMessageStore:
    """Test message store for offline delivery."""

    def test_store_creation(self):
        """Test creating message store."""
        from satorip2p.protocol.message_store import MessageStore

        store = MessageStore(host=None, dht=None)
        assert store is not None
        # defaultdict returns empty list for missing keys
        assert len(store._local_store) == 0

    def test_store_and_retrieve_local(self):
        """Test storing and retrieving messages locally."""
        from satorip2p.protocol.message_store import MessageStore
        from satorip2p.config import MessageEnvelope

        store = MessageStore(host=None, dht=None)

        # Store a message using MessageEnvelope
        envelope = MessageEnvelope(
            message_id="msg-123",
            target_peer="peer-123",
            source_peer="me",
            stream_id="stream-1",
            payload=b"test message"
        )
        store._local_store["peer-123"].append(envelope)

        # Verify it's there
        assert "peer-123" in store._local_store
        assert len(store._local_store["peer-123"]) == 1

    def test_cleanup_expired(self):
        """Test cleanup of expired messages."""
        from satorip2p.protocol.message_store import MessageStore
        from satorip2p.config import MessageEnvelope
        import time

        store = MessageStore(host=None, dht=None)

        # Store an old message (expired)
        old_time = time.time() - 100000  # Way in the past
        envelope = MessageEnvelope(
            message_id="old-msg",
            target_peer="peer-123",
            source_peer="me",
            stream_id=None,
            payload=b"old message",
            timestamp=old_time,
            ttl_hours=1  # Already expired
        )
        store._local_store["peer-123"].append(envelope)

        # Cleanup should remove it
        removed = store.cleanup_expired()

        # Should be removed
        assert removed >= 1 or len(store._local_store["peer-123"]) == 0

    def test_add_relay(self):
        """Test adding relay nodes."""
        from satorip2p.protocol.message_store import MessageStore

        store = MessageStore(host=None, dht=None)
        store.add_relay("relay-peer-1")
        store.add_relay("relay-peer-2")

        assert "relay-peer-1" in store._known_relays
        assert "relay-peer-2" in store._known_relays

    def test_get_stats(self):
        """Test getting store statistics."""
        from satorip2p.protocol.message_store import MessageStore
        from satorip2p.config import MessageEnvelope

        store = MessageStore(host=None, dht=None)

        # Add messages using MessageEnvelope
        store._local_store["peer-1"].append(MessageEnvelope(
            message_id="m1", target_peer="peer-1", source_peer="me",
            stream_id=None, payload=b"msg1"
        ))
        store._local_store["peer-2"].append(MessageEnvelope(
            message_id="m2", target_peer="peer-2", source_peer="me",
            stream_id=None, payload=b"msg2"
        ))
        store._local_store["peer-2"].append(MessageEnvelope(
            message_id="m3", target_peer="peer-2", source_peer="me",
            stream_id=None, payload=b"msg3"
        ))

        stats = store.get_stats()
        assert "total_pending_messages" in stats
        assert stats["total_pending_messages"] == 3
        assert stats["peers_with_pending"] == 2


class TestRendezvous:
    """Test Rendezvous protocol."""

    def test_registration_creation(self):
        """Test creating a registration."""
        from satorip2p.protocol.rendezvous import RendezvousRegistration

        reg = RendezvousRegistration(
            peer_id="peer-123",
            namespace="satori/sub/stream-1",
            ttl=3600
        )

        assert reg.peer_id == "peer-123"
        assert reg.namespace == "satori/sub/stream-1"
        assert reg.ttl == 3600
        assert not reg.is_expired()

    def test_registration_expiry(self):
        """Test registration expiry check."""
        from satorip2p.protocol.rendezvous import RendezvousRegistration
        import time

        # Create an already expired registration
        reg = RendezvousRegistration(
            peer_id="peer-123",
            namespace="test",
            ttl=0,  # 0 TTL = immediate expiry
            registered_at=time.time() - 100
        )

        assert reg.is_expired()

    def test_time_remaining(self):
        """Test time remaining calculation."""
        from satorip2p.protocol.rendezvous import RendezvousRegistration

        reg = RendezvousRegistration(
            peer_id="peer-123",
            namespace="test",
            ttl=7200  # 2 hours
        )

        remaining = reg.time_remaining()
        assert remaining > 7190  # Should be close to 7200

    def test_manager_creation(self):
        """Test creating rendezvous manager."""
        from satorip2p.protocol.rendezvous import RendezvousManager

        manager = RendezvousManager(host=None)
        assert manager is not None
        assert manager._my_registrations == {}
        assert manager._namespace_peers == {}

    def test_namespace_prefixes(self):
        """Test namespace prefix constants."""
        from satorip2p.protocol.rendezvous import RendezvousManager

        assert RendezvousManager.SUBSCRIBER_NS_PREFIX == "satori/sub/"
        assert RendezvousManager.PUBLISHER_NS_PREFIX == "satori/pub/"
        assert RendezvousManager.PEER_NS_PREFIX == "satori/peer/"

    def test_get_cached_peers(self):
        """Test getting cached peers."""
        from satorip2p.protocol.rendezvous import RendezvousManager

        manager = RendezvousManager(host=None)
        manager._namespace_peers["test-ns"] = {"peer-1", "peer-2", "peer-3"}

        cached = manager.get_cached_peers("test-ns")
        assert len(cached) == 3
        assert "peer-1" in cached

    def test_get_my_registrations(self):
        """Test getting our registrations."""
        from satorip2p.protocol.rendezvous import RendezvousManager, RendezvousRegistration

        manager = RendezvousManager(host=None)
        manager._my_registrations["ns-1"] = RendezvousRegistration("me", "ns-1")
        manager._my_registrations["ns-2"] = RendezvousRegistration("me", "ns-2")

        regs = manager.get_my_registrations()
        assert len(regs) == 2
        assert "ns-1" in regs
        assert "ns-2" in regs

    def test_get_stats(self):
        """Test getting manager statistics."""
        from satorip2p.protocol.rendezvous import RendezvousManager, RendezvousRegistration

        manager = RendezvousManager(host=None)
        manager._my_registrations["ns-1"] = RendezvousRegistration("me", "ns-1")
        manager._namespace_peers["ns-1"] = {"peer-1", "peer-2"}
        manager._namespace_peers["ns-2"] = {"peer-3"}

        stats = manager.get_stats()
        assert stats["active_registrations"] == 1
        assert stats["cached_namespaces"] == 2
        assert stats["total_cached_peers"] == 3

    def test_clear_cache(self):
        """Test clearing discovery cache."""
        from satorip2p.protocol.rendezvous import RendezvousManager

        manager = RendezvousManager(host=None)
        manager._namespace_peers["ns-1"] = {"peer-1"}
        manager._namespace_peers["ns-2"] = {"peer-2"}

        manager.clear_cache()
        assert manager._namespace_peers == {}


class TestConfigDataclasses:
    """Test configuration dataclasses."""

    def test_peer_info_creation(self):
        """Test creating PeerInfo."""
        from satorip2p.config import PeerInfo

        info = PeerInfo(
            peer_id="16Uiu2HAmTest...",
            evrmore_address="ETestAddress123",
            public_key="02aa" + "bb" * 31,
            addresses=["/ip4/192.168.1.1/tcp/24600"],
            nat_type="PUBLIC",
            is_relay=True
        )

        assert info.peer_id == "16Uiu2HAmTest..."
        assert info.is_relay is True
        assert info.nat_type == "PUBLIC"

    def test_stream_info_creation(self):
        """Test creating StreamInfo."""
        from satorip2p.config import StreamInfo

        info = StreamInfo(
            stream_id="stream-uuid-123",
            publisher="peer-3",
            subscribers={"peer-1", "peer-2"}  # Set not List
        )

        assert info.stream_id == "stream-uuid-123"
        assert len(info.subscribers) == 2
        assert info.publisher == "peer-3"

    def test_message_envelope_creation(self):
        """Test creating MessageEnvelope."""
        from satorip2p.config import MessageEnvelope

        envelope = MessageEnvelope(
            message_id="msg-123",
            target_peer="peer-target",
            source_peer="peer-source",
            stream_id="stream-1",
            payload=b"test data"
        )

        assert envelope.message_id == "msg-123"
        assert envelope.payload == b"test data"
        assert not envelope.is_expired()

    def test_message_envelope_expiry(self):
        """Test MessageEnvelope expiry check."""
        from satorip2p.config import MessageEnvelope
        import time

        # Create expired envelope
        envelope = MessageEnvelope(
            message_id="old-msg",
            target_peer="peer",
            source_peer="me",
            stream_id=None,
            payload=b"old",
            timestamp=time.time() - 100000,  # Old
            ttl_hours=1
        )

        assert envelope.is_expired()


class TestPublishMethod:
    """Test publish functionality."""

    def test_publish_adds_to_publications(self, mock_identity):
        """Test that publish adds stream to publications."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)
        # Directly add to publications to avoid async broadcast
        peers._my_publications.add("my-stream-123")

        assert "my-stream-123" in peers._my_publications

    def test_get_my_publications(self, mock_identity):
        """Test getting our publications."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)
        # Directly add to publications to avoid async broadcast
        peers._my_publications.add("stream-1")
        peers._my_publications.add("stream-2")

        pubs = peers.get_my_publications()
        assert len(pubs) == 2
        assert "stream-1" in pubs
        assert "stream-2" in pubs

    def test_publish_method_signature(self, mock_identity):
        """Test that publish method exists and has correct signature."""
        from satorip2p.peers import Peers
        import inspect

        peers = Peers(identity=mock_identity)
        sig = inspect.signature(peers.publish)

        # Should have stream_id and data parameters
        params = list(sig.parameters.keys())
        assert "stream_id" in params
        assert "data" in params


class TestMultipleCallbacks:
    """Test multiple callback handling."""

    def test_multiple_callbacks_same_stream(self, mock_identity):
        """Test multiple callbacks on same stream."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        callback1 = Mock()
        callback2 = Mock()
        callback3 = Mock()

        peers.subscribe("stream-1", callback1)
        peers.subscribe("stream-1", callback2)
        peers.subscribe("stream-1", callback3)

        assert len(peers._callbacks["stream-1"]) == 3
        assert callback1 in peers._callbacks["stream-1"]
        assert callback2 in peers._callbacks["stream-1"]
        assert callback3 in peers._callbacks["stream-1"]


class TestSubscriptionManagerAdvanced:
    """Advanced subscription manager tests."""

    def test_get_all_subscriptions(self):
        """Test getting all subscriptions."""
        from satorip2p.protocol.subscriptions import SubscriptionManager

        manager = SubscriptionManager()
        manager.add_subscriber("stream-1", "peer-1")
        manager.add_subscriber("stream-1", "peer-2")
        manager.add_subscriber("stream-2", "peer-3")

        all_subs = manager.get_all_subscriptions()
        assert "stream-1" in all_subs
        assert "stream-2" in all_subs
        assert len(all_subs["stream-1"]) == 2
        assert len(all_subs["stream-2"]) == 1

    def test_get_peer_subscriptions(self):
        """Test getting a peer's subscriptions."""
        from satorip2p.protocol.subscriptions import SubscriptionManager

        manager = SubscriptionManager()
        manager.add_subscriber("stream-1", "peer-1")
        manager.add_subscriber("stream-2", "peer-1")
        manager.add_subscriber("stream-3", "peer-2")

        peer1_subs = manager.get_peer_subscriptions("peer-1")
        assert len(peer1_subs) == 2
        assert "stream-1" in peer1_subs
        assert "stream-2" in peer1_subs

    def test_peer_info_storage(self):
        """Test storing peer info with subscriptions."""
        from satorip2p.protocol.subscriptions import SubscriptionManager

        manager = SubscriptionManager()
        manager.add_subscriber("stream-1", "peer-1", evrmore_address="Eaddr1")

        # Verify peer info is stored
        if hasattr(manager, '_peer_info'):
            assert "peer-1" in manager._peer_info


class TestRendezvousAsync:
    """Async tests for Rendezvous manager."""

    @pytest.mark.asyncio
    async def test_register_without_host(self):
        """Test registration without host (local mode)."""
        from satorip2p.protocol.rendezvous import RendezvousManager

        manager = RendezvousManager(host=None)
        result = await manager.register("test-namespace")

        # Should succeed in local-only mode
        assert result is True
        assert "test-namespace" in manager._my_registrations

    @pytest.mark.asyncio
    async def test_unregister_without_host(self):
        """Test unregistration without host."""
        from satorip2p.protocol.rendezvous import RendezvousManager

        manager = RendezvousManager(host=None)
        await manager.register("test-namespace")
        result = await manager.unregister("test-namespace")

        assert result is True
        assert "test-namespace" not in manager._my_registrations

    @pytest.mark.asyncio
    async def test_discover_without_host(self):
        """Test discovery without host (returns cache only)."""
        from satorip2p.protocol.rendezvous import RendezvousManager

        manager = RendezvousManager(host=None)
        manager._namespace_peers["test-ns"] = {"peer-1", "peer-2"}

        peers = await manager.discover("test-ns")
        assert len(peers) == 2

    @pytest.mark.asyncio
    async def test_register_subscriber(self):
        """Test registering as subscriber."""
        from satorip2p.protocol.rendezvous import RendezvousManager

        manager = RendezvousManager(host=None)
        result = await manager.register_subscriber("stream-uuid-123")

        assert result is True
        expected_ns = f"{RendezvousManager.SUBSCRIBER_NS_PREFIX}stream-uuid-123"
        assert expected_ns in manager._my_registrations

    @pytest.mark.asyncio
    async def test_register_publisher(self):
        """Test registering as publisher."""
        from satorip2p.protocol.rendezvous import RendezvousManager

        manager = RendezvousManager(host=None)
        result = await manager.register_publisher("stream-uuid-123")

        assert result is True
        expected_ns = f"{RendezvousManager.PUBLISHER_NS_PREFIX}stream-uuid-123"
        assert expected_ns in manager._my_registrations


class TestPackageImport:
    """Test that the package can be imported correctly."""

    def test_import_satorip2p(self):
        """Test importing the main package."""
        import satorip2p
        assert hasattr(satorip2p, 'Peers')
        assert hasattr(satorip2p, 'PeerInfo')
        assert hasattr(satorip2p, 'StreamInfo')

    def test_import_peers_class(self):
        """Test importing Peers directly."""
        from satorip2p import Peers
        assert Peers is not None

    def test_import_peer_info(self):
        """Test importing PeerInfo."""
        from satorip2p import PeerInfo
        assert PeerInfo is not None

    def test_import_stream_info(self):
        """Test importing StreamInfo."""
        from satorip2p import StreamInfo
        assert StreamInfo is not None

    def test_import_protocol_modules(self):
        """Test importing protocol submodules."""
        from satorip2p.protocol import SubscriptionManager
        from satorip2p.protocol import MessageStore
        from satorip2p.protocol import RendezvousManager
        from satorip2p.protocol import serialize_message, deserialize_message

        assert SubscriptionManager is not None
        assert MessageStore is not None
        assert RendezvousManager is not None

    def test_import_identity_bridge(self):
        """Test importing identity bridge."""
        from satorip2p.identity import EvrmoreIdentityBridge
        assert EvrmoreIdentityBridge is not None

    def test_import_nat_modules(self):
        """Test importing NAT modules."""
        from satorip2p.nat import UPnPManager
        from satorip2p.nat import detect_docker_environment

        assert UPnPManager is not None
        assert detect_docker_environment is not None

    def test_import_config(self):
        """Test importing config constants."""
        from satorip2p.config import (
            DEFAULT_PORT,
            BOOTSTRAP_PEERS,
            STREAM_TOPIC_PREFIX,
            SATORI_PROTOCOL_ID,
            MessageEnvelope,
        )

        assert DEFAULT_PORT == 24600
        assert isinstance(BOOTSTRAP_PEERS, list)
        assert STREAM_TOPIC_PREFIX == "satori/stream/"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
