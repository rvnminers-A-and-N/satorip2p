"""
satorip2p/tests/test_decentralization_protocols.py

Unit tests for decentralization protocols: PeerRegistry, StreamRegistry,
OracleNetwork, and PredictionProtocol.
"""

import pytest
import time
from unittest.mock import Mock, AsyncMock, MagicMock, patch


# ============================================================================
# Mock Classes for Testing
# ============================================================================

class MockEvrmoreIdentity:
    """Mock Evrmore identity for tests."""

    def __init__(self, seed: int = 1):
        self.evrmore_address = f"ETestAddress{seed:032d}"
        self.pubkey = "02" + f"{seed:02x}" * 32
        self._entropy = bytes([seed % 256] * 32)

    @property
    def address(self):
        return self.evrmore_address

    def sign(self, msg) -> bytes:
        if isinstance(msg, str):
            msg = msg.encode()
        return b"signature"

    def verify(self, msg, sig, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3


class MockPeers:
    """Mock Peers instance for tests."""

    def __init__(self, identity=None):
        self._identity_bridge = identity or MockEvrmoreIdentity()
        self.peer_id = "QmTestPeerId123456789"
        self._pubsub = Mock()
        self._rendezvous = Mock()
        self._dht = Mock()
        self._subscriptions = {}

    def get_listening_addresses(self):
        return ["/ip4/127.0.0.1/tcp/24680/p2p/QmTestPeerId"]

    async def subscribe(self, topic, callback):
        self._subscriptions[topic] = callback
        return True

    async def unsubscribe(self, topic):
        if topic in self._subscriptions:
            del self._subscriptions[topic]
        return True

    async def broadcast(self, topic, data):
        return True


@pytest.fixture
def mock_identity():
    """Create mock Evrmore identity."""
    return MockEvrmoreIdentity()


@pytest.fixture
def mock_peers(mock_identity):
    """Create mock Peers instance."""
    return MockPeers(mock_identity)


# ============================================================================
# Test PeerAnnouncement Dataclass
# ============================================================================

class TestPeerAnnouncement:
    """Tests for PeerAnnouncement dataclass."""

    def test_create_announcement(self):
        """Test creating a peer announcement."""
        from satorip2p.protocol.peer_registry import PeerAnnouncement

        announcement = PeerAnnouncement(
            peer_id="QmTestPeerId",
            evrmore_address="ETestAddress",
            multiaddrs=["/ip4/127.0.0.1/tcp/24680"],
            capabilities=["predictor"],
            timestamp=int(time.time()),
        )

        assert announcement.peer_id == "QmTestPeerId"
        assert announcement.evrmore_address == "ETestAddress"
        assert "predictor" in announcement.capabilities
        assert announcement.ttl == 3600  # Default TTL

    def test_is_expired_not_expired(self):
        """Test is_expired returns False for fresh announcement."""
        from satorip2p.protocol.peer_registry import PeerAnnouncement

        announcement = PeerAnnouncement(
            peer_id="QmTest",
            evrmore_address="ETest",
            multiaddrs=[],
            capabilities=[],
            timestamp=int(time.time()),
            ttl=3600,
        )

        assert announcement.is_expired() is False

    def test_is_expired_expired(self):
        """Test is_expired returns True for old announcement."""
        from satorip2p.protocol.peer_registry import PeerAnnouncement

        old_timestamp = int(time.time()) - 4000  # Older than TTL
        announcement = PeerAnnouncement(
            peer_id="QmTest",
            evrmore_address="ETest",
            multiaddrs=[],
            capabilities=[],
            timestamp=old_timestamp,
            ttl=3600,
        )

        assert announcement.is_expired() is True

    def test_time_remaining(self):
        """Test time_remaining calculation."""
        from satorip2p.protocol.peer_registry import PeerAnnouncement

        announcement = PeerAnnouncement(
            peer_id="QmTest",
            evrmore_address="ETest",
            multiaddrs=[],
            capabilities=[],
            timestamp=int(time.time()),
            ttl=3600,
        )

        remaining = announcement.time_remaining()
        assert 3590 < remaining <= 3600

    def test_time_remaining_expired(self):
        """Test time_remaining returns 0 for expired announcement."""
        from satorip2p.protocol.peer_registry import PeerAnnouncement

        old_timestamp = int(time.time()) - 5000
        announcement = PeerAnnouncement(
            peer_id="QmTest",
            evrmore_address="ETest",
            multiaddrs=[],
            capabilities=[],
            timestamp=old_timestamp,
            ttl=3600,
        )

        assert announcement.time_remaining() == 0

    def test_to_dict_from_dict(self):
        """Test serialization round-trip."""
        from satorip2p.protocol.peer_registry import PeerAnnouncement

        announcement = PeerAnnouncement(
            peer_id="QmTestPeerId",
            evrmore_address="ETestAddress",
            multiaddrs=["/ip4/127.0.0.1/tcp/24680"],
            capabilities=["predictor", "oracle"],
            timestamp=1234567890,
            signature="testsig",
            ttl=7200,
        )

        data = announcement.to_dict()
        restored = PeerAnnouncement.from_dict(data)

        assert restored.peer_id == announcement.peer_id
        assert restored.evrmore_address == announcement.evrmore_address
        assert restored.multiaddrs == announcement.multiaddrs
        assert restored.capabilities == announcement.capabilities
        assert restored.timestamp == announcement.timestamp
        assert restored.signature == announcement.signature
        assert restored.ttl == announcement.ttl

    def test_get_signing_message(self):
        """Test signing message generation."""
        from satorip2p.protocol.peer_registry import PeerAnnouncement

        announcement = PeerAnnouncement(
            peer_id="QmTest",
            evrmore_address="ETest",
            multiaddrs=[],
            capabilities=[],
            timestamp=1234567890,
        )

        message = announcement.get_signing_message()
        assert message == "QmTest:ETest:1234567890"


# ============================================================================
# Test PeerRegistry
# ============================================================================

class TestPeerRegistry:
    """Tests for PeerRegistry class."""

    def test_init(self, mock_peers):
        """Test PeerRegistry initialization."""
        from satorip2p.protocol.peer_registry import PeerRegistry

        registry = PeerRegistry(mock_peers)

        assert registry.peers == mock_peers
        assert registry._my_announcement is None
        assert registry._known_peers == {}
        assert registry._started is False

    def test_evrmore_address_property(self, mock_peers):
        """Test evrmore_address property."""
        from satorip2p.protocol.peer_registry import PeerRegistry

        registry = PeerRegistry(mock_peers)
        assert registry.evrmore_address == mock_peers._identity_bridge.evrmore_address

    def test_peer_id_property(self, mock_peers):
        """Test peer_id property."""
        from satorip2p.protocol.peer_registry import PeerRegistry

        registry = PeerRegistry(mock_peers)
        assert registry.peer_id == mock_peers.peer_id

    @pytest.mark.trio
    async def test_start(self, mock_peers):
        """Test starting the registry."""
        from satorip2p.protocol.peer_registry import PeerRegistry

        registry = PeerRegistry(mock_peers)
        result = await registry.start()

        assert result is True
        assert registry._started is True

    @pytest.mark.trio
    async def test_start_already_started(self, mock_peers):
        """Test start returns True if already started."""
        from satorip2p.protocol.peer_registry import PeerRegistry

        registry = PeerRegistry(mock_peers)
        registry._started = True

        result = await registry.start()
        assert result is True

    @pytest.mark.trio
    async def test_stop(self, mock_peers):
        """Test stopping the registry."""
        from satorip2p.protocol.peer_registry import PeerRegistry

        registry = PeerRegistry(mock_peers)
        registry._started = True

        await registry.stop()
        assert registry._started is False

    @pytest.mark.trio
    async def test_announce(self, mock_peers):
        """Test announcing peer presence."""
        from satorip2p.protocol.peer_registry import PeerRegistry

        registry = PeerRegistry(mock_peers)

        announcement = await registry.announce(capabilities=["predictor", "oracle"])

        assert announcement is not None
        assert announcement.peer_id == mock_peers.peer_id
        assert announcement.evrmore_address == mock_peers._identity_bridge.evrmore_address
        assert "predictor" in announcement.capabilities
        assert "oracle" in announcement.capabilities
        assert registry._my_announcement == announcement

    @pytest.mark.trio
    async def test_announce_no_identity(self, mock_peers):
        """Test announce returns None without identity."""
        from satorip2p.protocol.peer_registry import PeerRegistry

        mock_peers.peer_id = ""
        registry = PeerRegistry(mock_peers)

        announcement = await registry.announce()
        assert announcement is None

    @pytest.mark.trio
    async def test_get_all_peers(self, mock_peers):
        """Test getting all known peers."""
        from satorip2p.protocol.peer_registry import PeerRegistry, PeerAnnouncement

        registry = PeerRegistry(mock_peers)

        # Add some peers
        registry._known_peers["addr1"] = PeerAnnouncement(
            peer_id="peer1",
            evrmore_address="addr1",
            multiaddrs=[],
            capabilities=[],
            timestamp=int(time.time()),
        )
        registry._known_peers["addr2"] = PeerAnnouncement(
            peer_id="peer2",
            evrmore_address="addr2",
            multiaddrs=[],
            capabilities=[],
            timestamp=int(time.time()),
        )

        peers = await registry.get_all_peers()
        assert len(peers) == 2

    @pytest.mark.trio
    async def test_get_peers_by_capability(self, mock_peers):
        """Test filtering peers by capability."""
        from satorip2p.protocol.peer_registry import PeerRegistry, PeerAnnouncement

        registry = PeerRegistry(mock_peers)

        registry._known_peers["oracle1"] = PeerAnnouncement(
            peer_id="peer1",
            evrmore_address="oracle1",
            multiaddrs=[],
            capabilities=["oracle"],
            timestamp=int(time.time()),
        )
        registry._known_peers["predictor1"] = PeerAnnouncement(
            peer_id="peer2",
            evrmore_address="predictor1",
            multiaddrs=[],
            capabilities=["predictor"],
            timestamp=int(time.time()),
        )

        oracles = await registry.get_peers_by_capability("oracle")
        assert len(oracles) == 1
        assert oracles[0].evrmore_address == "oracle1"

    def test_get_stats(self, mock_peers):
        """Test getting registry statistics."""
        from satorip2p.protocol.peer_registry import PeerRegistry

        registry = PeerRegistry(mock_peers)
        stats = registry.get_stats()

        assert "known_peers" in stats
        assert "my_announcement" in stats
        assert "callbacks_registered" in stats
        assert "started" in stats


# ============================================================================
# Test StreamDefinition Dataclass
# ============================================================================

class TestStreamDefinition:
    """Tests for StreamDefinition dataclass."""

    def test_generate_stream_id(self):
        """Test stream ID generation is deterministic."""
        from satorip2p.protocol.stream_registry import StreamDefinition

        id1 = StreamDefinition.generate_stream_id("exchange/binance", "BTCUSDT", "close")
        id2 = StreamDefinition.generate_stream_id("exchange/binance", "BTCUSDT", "close")

        assert id1 == id2
        assert len(id1) == 32

    def test_different_inputs_different_ids(self):
        """Test different inputs produce different IDs."""
        from satorip2p.protocol.stream_registry import StreamDefinition

        id1 = StreamDefinition.generate_stream_id("exchange/binance", "BTCUSDT", "close")
        id2 = StreamDefinition.generate_stream_id("exchange/binance", "ETHUSDT", "close")

        assert id1 != id2

    def test_to_dict_from_dict(self):
        """Test serialization round-trip."""
        from satorip2p.protocol.stream_registry import StreamDefinition

        stream = StreamDefinition(
            stream_id="test123",
            source="exchange/binance",
            stream="BTCUSDT",
            target="close",
            datatype="price",
            cadence=60,
            predictor_slots=10,
            creator="ETestCreator",
            timestamp=1234567890,
            description="Test stream",
            tags=["crypto", "btc"],
        )

        data = stream.to_dict()
        restored = StreamDefinition.from_dict(data)

        assert restored.stream_id == stream.stream_id
        assert restored.source == stream.source
        assert restored.stream == stream.stream
        assert restored.target == stream.target
        assert restored.description == stream.description
        assert restored.tags == stream.tags


# ============================================================================
# Test StreamClaim Dataclass
# ============================================================================

class TestStreamClaim:
    """Tests for StreamClaim dataclass."""

    def test_create_claim(self):
        """Test creating a stream claim."""
        from satorip2p.protocol.stream_registry import StreamClaim

        claim = StreamClaim(
            stream_id="test123",
            slot_index=0,
            predictor="ETestPredictor",
            peer_id="QmTestPeer",
            timestamp=int(time.time()),
        )

        assert claim.stream_id == "test123"
        assert claim.slot_index == 0
        assert claim.predictor == "ETestPredictor"

    def test_is_expired_no_expiry(self):
        """Test is_expired returns False with no expiry."""
        from satorip2p.protocol.stream_registry import StreamClaim

        claim = StreamClaim(
            stream_id="test",
            slot_index=0,
            predictor="E",
            peer_id="Q",
            timestamp=int(time.time()),
            expires=0,  # No expiry
        )

        assert claim.is_expired() is False

    def test_is_expired_not_expired(self):
        """Test is_expired returns False for fresh claim."""
        from satorip2p.protocol.stream_registry import StreamClaim

        claim = StreamClaim(
            stream_id="test",
            slot_index=0,
            predictor="E",
            peer_id="Q",
            timestamp=int(time.time()),
            expires=int(time.time()) + 3600,
        )

        assert claim.is_expired() is False

    def test_is_expired_expired(self):
        """Test is_expired returns True for old claim."""
        from satorip2p.protocol.stream_registry import StreamClaim

        claim = StreamClaim(
            stream_id="test",
            slot_index=0,
            predictor="E",
            peer_id="Q",
            timestamp=int(time.time()) - 7200,
            expires=int(time.time()) - 3600,  # Expired an hour ago
        )

        assert claim.is_expired() is True

    def test_get_signing_message(self):
        """Test signing message generation."""
        from satorip2p.protocol.stream_registry import StreamClaim

        claim = StreamClaim(
            stream_id="stream1",
            slot_index=2,
            predictor="EPredictor",
            peer_id="QPeer",
            timestamp=1234567890,
        )

        message = claim.get_signing_message()
        assert message == "stream1:2:EPredictor:1234567890"


# ============================================================================
# Test StreamRegistry
# ============================================================================

class TestStreamRegistry:
    """Tests for StreamRegistry class."""

    def test_init(self, mock_peers):
        """Test StreamRegistry initialization."""
        from satorip2p.protocol.stream_registry import StreamRegistry

        registry = StreamRegistry(mock_peers)

        assert registry.peers == mock_peers
        assert registry._streams == {}
        assert registry._claims == {}
        assert registry._my_claims == {}
        assert registry._started is False

    @pytest.mark.trio
    async def test_start(self, mock_peers):
        """Test starting the registry."""
        from satorip2p.protocol.stream_registry import StreamRegistry

        registry = StreamRegistry(mock_peers)
        result = await registry.start()

        assert result is True
        assert registry._started is True

    @pytest.mark.trio
    async def test_register_stream(self, mock_peers):
        """Test registering a new stream."""
        from satorip2p.protocol.stream_registry import StreamRegistry

        registry = StreamRegistry(mock_peers)

        stream = await registry.register_stream(
            source="exchange/binance",
            stream="BTCUSDT",
            target="close",
            datatype="price",
            cadence=60,
            predictor_slots=10,
        )

        assert stream is not None
        assert stream.source == "exchange/binance"
        assert stream.stream == "BTCUSDT"
        assert stream.stream_id in registry._streams

    @pytest.mark.trio
    async def test_discover_streams(self, mock_peers):
        """Test discovering streams."""
        from satorip2p.protocol.stream_registry import StreamRegistry, StreamDefinition

        registry = StreamRegistry(mock_peers)

        # Add some streams
        registry._streams["id1"] = StreamDefinition(
            stream_id="id1",
            source="exchange/binance",
            stream="BTCUSDT",
            target="close",
            datatype="price",
            cadence=60,
            predictor_slots=10,
            creator="E",
            timestamp=int(time.time()),
        )
        registry._streams["id2"] = StreamDefinition(
            stream_id="id2",
            source="exchange/coinbase",
            stream="ETHUSDT",
            target="close",
            datatype="price",
            cadence=60,
            predictor_slots=10,
            creator="E",
            timestamp=int(time.time()),
        )

        # Discover all
        all_streams = await registry.discover_streams()
        assert len(all_streams) == 2

        # Filter by source
        binance_streams = await registry.discover_streams(source="exchange/binance")
        assert len(binance_streams) == 1
        assert binance_streams[0].source == "exchange/binance"

    @pytest.mark.trio
    async def test_claim_stream(self, mock_peers):
        """Test claiming a stream slot."""
        from satorip2p.protocol.stream_registry import StreamRegistry

        registry = StreamRegistry(mock_peers)

        claim = await registry.claim_stream("test_stream_id", slot_index=0)

        assert claim is not None
        assert claim.stream_id == "test_stream_id"
        assert claim.slot_index == 0
        assert claim.predictor == mock_peers._identity_bridge.evrmore_address
        assert "test_stream_id" in registry._my_claims

    @pytest.mark.trio
    async def test_release_claim(self, mock_peers):
        """Test releasing a stream claim."""
        from satorip2p.protocol.stream_registry import StreamRegistry

        registry = StreamRegistry(mock_peers)

        # First claim
        await registry.claim_stream("test_stream", slot_index=0)
        assert "test_stream" in registry._my_claims

        # Then release
        result = await registry.release_claim("test_stream")
        assert result is True
        assert "test_stream" not in registry._my_claims

    @pytest.mark.trio
    async def test_get_my_streams(self, mock_peers):
        """Test getting own claimed streams."""
        from satorip2p.protocol.stream_registry import StreamRegistry

        registry = StreamRegistry(mock_peers)

        await registry.claim_stream("stream1", slot_index=0)
        await registry.claim_stream("stream2", slot_index=0)

        my_claims = await registry.get_my_streams()
        assert len(my_claims) == 2

    def test_get_stats(self, mock_peers):
        """Test getting registry statistics."""
        from satorip2p.protocol.stream_registry import StreamRegistry

        registry = StreamRegistry(mock_peers)
        stats = registry.get_stats()

        assert "known_streams" in stats
        assert "my_claims" in stats
        assert "total_claims" in stats
        assert "started" in stats


# ============================================================================
# Test Observation Dataclass
# ============================================================================

class TestObservation:
    """Tests for Observation dataclass."""

    def test_create_observation(self):
        """Test creating an observation."""
        from satorip2p.protocol.oracle_network import Observation

        obs = Observation(
            stream_id="stream1",
            value=12345.67,
            timestamp=int(time.time()),
            oracle="EOracle",
        )

        assert obs.stream_id == "stream1"
        assert obs.value == 12345.67
        assert obs.oracle == "EOracle"
        assert obs.hash != ""  # Auto-generated

    def test_compute_hash_deterministic(self):
        """Test hash computation is deterministic."""
        from satorip2p.protocol.oracle_network import Observation

        obs1 = Observation(
            stream_id="stream1",
            value=100.0,
            timestamp=1234567890,
            oracle="EOracle",
        )
        obs2 = Observation(
            stream_id="stream1",
            value=100.0,
            timestamp=1234567890,
            oracle="EOracle",
        )

        assert obs1.hash == obs2.hash

    def test_to_dict_from_dict(self):
        """Test serialization round-trip."""
        from satorip2p.protocol.oracle_network import Observation

        obs = Observation(
            stream_id="stream1",
            value=100.0,
            timestamp=1234567890,
            oracle="EOracle",
            metadata={"source": "test"},
        )

        data = obs.to_dict()
        restored = Observation.from_dict(data)

        assert restored.stream_id == obs.stream_id
        assert restored.value == obs.value
        assert restored.timestamp == obs.timestamp
        assert restored.hash == obs.hash
        assert restored.metadata == obs.metadata


# ============================================================================
# Test OracleNetwork
# ============================================================================

class TestOracleNetwork:
    """Tests for OracleNetwork class."""

    def test_init(self, mock_peers):
        """Test OracleNetwork initialization."""
        from satorip2p.protocol.oracle_network import OracleNetwork

        network = OracleNetwork(mock_peers)

        assert network.peers == mock_peers
        assert network._subscribed_streams == {}
        assert network._oracle_registrations == {}
        assert network._started is False

    @pytest.mark.trio
    async def test_start(self, mock_peers):
        """Test starting the oracle network."""
        from satorip2p.protocol.oracle_network import OracleNetwork

        network = OracleNetwork(mock_peers)
        result = await network.start()

        assert result is True
        assert network._started is True

    @pytest.mark.trio
    async def test_register_as_oracle(self, mock_peers):
        """Test registering as an oracle."""
        from satorip2p.protocol.oracle_network import OracleNetwork

        network = OracleNetwork(mock_peers)

        registration = await network.register_as_oracle("stream1", is_primary=True)

        assert registration is not None
        assert registration.stream_id == "stream1"
        assert registration.is_primary is True
        assert registration.oracle == mock_peers._identity_bridge.evrmore_address
        assert "stream1" in network._my_registrations

    @pytest.mark.trio
    async def test_subscribe_to_stream(self, mock_peers):
        """Test subscribing to stream observations."""
        from satorip2p.protocol.oracle_network import OracleNetwork

        network = OracleNetwork(mock_peers)
        callback = Mock()

        result = await network.subscribe_to_stream("stream1", callback)

        assert result is True
        assert "stream1" in network._subscribed_streams
        assert callback in network._subscribed_streams["stream1"]

    @pytest.mark.trio
    async def test_unsubscribe_from_stream(self, mock_peers):
        """Test unsubscribing from stream."""
        from satorip2p.protocol.oracle_network import OracleNetwork

        network = OracleNetwork(mock_peers)

        await network.subscribe_to_stream("stream1", Mock())
        result = await network.unsubscribe_from_stream("stream1")

        assert result is True
        assert "stream1" not in network._subscribed_streams

    @pytest.mark.trio
    async def test_publish_observation(self, mock_peers):
        """Test publishing an observation."""
        from satorip2p.protocol.oracle_network import OracleNetwork

        network = OracleNetwork(mock_peers)
        await network.register_as_oracle("stream1")

        obs = await network.publish_observation("stream1", value=12345.67)

        assert obs is not None
        assert obs.stream_id == "stream1"
        assert obs.value == 12345.67
        assert obs.oracle == mock_peers._identity_bridge.evrmore_address

    def test_get_cached_observations(self, mock_peers):
        """Test getting cached observations."""
        from satorip2p.protocol.oracle_network import OracleNetwork, Observation

        network = OracleNetwork(mock_peers)

        # Add some observations to cache
        network._observation_cache["stream1"] = [
            Observation(stream_id="stream1", value=100.0, timestamp=1, oracle="E"),
            Observation(stream_id="stream1", value=101.0, timestamp=2, oracle="E"),
        ]

        observations = network.get_cached_observations("stream1")
        assert len(observations) == 2

    def test_get_latest_observation(self, mock_peers):
        """Test getting latest observation."""
        from satorip2p.protocol.oracle_network import OracleNetwork, Observation

        network = OracleNetwork(mock_peers)

        network._observation_cache["stream1"] = [
            Observation(stream_id="stream1", value=100.0, timestamp=1, oracle="E"),
            Observation(stream_id="stream1", value=101.0, timestamp=2, oracle="E"),
        ]

        latest = network.get_latest_observation("stream1")
        assert latest.value == 101.0

    def test_get_stats(self, mock_peers):
        """Test getting oracle network statistics."""
        from satorip2p.protocol.oracle_network import OracleNetwork

        network = OracleNetwork(mock_peers)
        stats = network.get_stats()

        assert "subscribed_streams" in stats
        assert "my_oracle_registrations" in stats
        assert "known_oracles" in stats
        assert "cached_observations" in stats
        assert "started" in stats


# ============================================================================
# Test Prediction Dataclass
# ============================================================================

class TestPrediction:
    """Tests for Prediction dataclass."""

    def test_create_prediction(self):
        """Test creating a prediction."""
        from satorip2p.protocol.prediction_protocol import Prediction

        prediction = Prediction(
            stream_id="stream1",
            value=12345.67,
            target_time=int(time.time()) + 3600,
            predictor="EPredictor",
            created_at=int(time.time()),
        )

        assert prediction.stream_id == "stream1"
        assert prediction.value == 12345.67
        assert prediction.predictor == "EPredictor"
        assert prediction.hash != ""  # Auto-generated

    def test_compute_hash_deterministic(self):
        """Test hash computation is deterministic."""
        from satorip2p.protocol.prediction_protocol import Prediction

        p1 = Prediction(
            stream_id="stream1",
            value=100.0,
            target_time=1234567890,
            predictor="E",
            created_at=1234567000,
        )
        p2 = Prediction(
            stream_id="stream1",
            value=100.0,
            target_time=1234567890,
            predictor="E",
            created_at=1234567000,
        )

        assert p1.hash == p2.hash

    def test_to_dict_from_dict(self):
        """Test serialization round-trip."""
        from satorip2p.protocol.prediction_protocol import Prediction

        prediction = Prediction(
            stream_id="stream1",
            value=100.0,
            target_time=1234567890,
            predictor="E",
            created_at=1234567000,
            confidence=0.85,
            metadata={"model": "xgb"},
        )

        data = prediction.to_dict()
        restored = Prediction.from_dict(data)

        assert restored.stream_id == prediction.stream_id
        assert restored.value == prediction.value
        assert restored.hash == prediction.hash
        assert restored.confidence == prediction.confidence
        assert restored.metadata == prediction.metadata


# ============================================================================
# Test PredictionScore Dataclass
# ============================================================================

class TestPredictionScore:
    """Tests for PredictionScore dataclass."""

    def test_create_score(self):
        """Test creating a prediction score."""
        from satorip2p.protocol.prediction_protocol import PredictionScore

        score = PredictionScore(
            prediction_hash="abc123",
            stream_id="stream1",
            predictor="EPredictor",
            predicted_value=100.0,
            actual_value=101.0,
            target_time=1234567890,
            score=0.95,
            scorer="EScorer",
            timestamp=int(time.time()),
        )

        assert score.prediction_hash == "abc123"
        assert score.score == 0.95
        assert score.scorer == "EScorer"

    def test_to_dict_from_dict(self):
        """Test serialization round-trip."""
        from satorip2p.protocol.prediction_protocol import PredictionScore

        score = PredictionScore(
            prediction_hash="abc123",
            stream_id="stream1",
            predictor="EPredictor",
            predicted_value=100.0,
            actual_value=101.0,
            target_time=1234567890,
            score=0.95,
            scorer="EScorer",
            timestamp=1234568000,
        )

        data = score.to_dict()
        restored = PredictionScore.from_dict(data)

        assert restored.prediction_hash == score.prediction_hash
        assert restored.score == score.score
        assert restored.scorer == score.scorer


# ============================================================================
# Test PredictionProtocol
# ============================================================================

class TestPredictionProtocol:
    """Tests for PredictionProtocol class."""

    def test_init(self, mock_peers):
        """Test PredictionProtocol initialization."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol

        protocol = PredictionProtocol(mock_peers)

        assert protocol.peers == mock_peers
        assert protocol._subscribed_streams == {}
        assert protocol._my_predictions == {}
        assert protocol._prediction_cache == {}
        assert protocol._started is False

    @pytest.mark.trio
    async def test_start(self, mock_peers):
        """Test starting the protocol."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol

        protocol = PredictionProtocol(mock_peers)
        result = await protocol.start()

        assert result is True
        assert protocol._started is True

    @pytest.mark.trio
    async def test_subscribe_to_predictions(self, mock_peers):
        """Test subscribing to predictions."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol

        protocol = PredictionProtocol(mock_peers)
        callback = Mock()

        result = await protocol.subscribe_to_predictions("stream1", callback)

        assert result is True
        assert "stream1" in protocol._subscribed_streams
        assert callback in protocol._subscribed_streams["stream1"]

    @pytest.mark.trio
    async def test_unsubscribe_from_predictions(self, mock_peers):
        """Test unsubscribing from predictions."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol

        protocol = PredictionProtocol(mock_peers)

        await protocol.subscribe_to_predictions("stream1", Mock())
        result = await protocol.unsubscribe_from_predictions("stream1")

        assert result is True
        assert "stream1" not in protocol._subscribed_streams

    @pytest.mark.trio
    async def test_publish_prediction(self, mock_peers):
        """Test publishing a prediction."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol

        protocol = PredictionProtocol(mock_peers)

        prediction = await protocol.publish_prediction(
            stream_id="stream1",
            value=12345.67,
            target_time=int(time.time()) + 3600,
            confidence=0.85,
        )

        assert prediction is not None
        assert prediction.stream_id == "stream1"
        assert prediction.value == 12345.67
        assert prediction.confidence == 0.85
        assert prediction.predictor == mock_peers._identity_bridge.evrmore_address
        assert "stream1" in protocol._my_predictions

    @pytest.mark.trio
    async def test_score_prediction(self, mock_peers):
        """Test scoring a prediction."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol, Prediction

        protocol = PredictionProtocol(mock_peers)

        prediction = Prediction(
            stream_id="stream1",
            value=100.0,
            target_time=int(time.time()),
            predictor="EPredictor",
            created_at=int(time.time()) - 3600,
        )

        score = await protocol.score_prediction(prediction, actual_value=101.0)

        assert score is not None
        assert score.prediction_hash == prediction.hash
        assert score.predicted_value == 100.0
        assert score.actual_value == 101.0
        assert 0 <= score.score <= 1

    def test_get_cached_predictions(self, mock_peers):
        """Test getting cached predictions."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol, Prediction

        protocol = PredictionProtocol(mock_peers)

        protocol._prediction_cache["stream1"] = [
            Prediction(stream_id="stream1", value=100.0, target_time=1, predictor="E", created_at=0),
            Prediction(stream_id="stream1", value=101.0, target_time=2, predictor="E", created_at=1),
        ]

        predictions = protocol.get_cached_predictions("stream1")
        assert len(predictions) == 2

    def test_get_my_predictions(self, mock_peers):
        """Test getting own predictions."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol, Prediction

        protocol = PredictionProtocol(mock_peers)

        protocol._my_predictions["stream1"] = [
            Prediction(stream_id="stream1", value=100.0, target_time=1, predictor="E", created_at=0),
        ]
        protocol._my_predictions["stream2"] = [
            Prediction(stream_id="stream2", value=200.0, target_time=1, predictor="E", created_at=0),
        ]

        # All predictions
        all_predictions = protocol.get_my_predictions()
        assert len(all_predictions) == 2

        # Filtered by stream
        stream1_predictions = protocol.get_my_predictions("stream1")
        assert len(stream1_predictions) == 1
        assert stream1_predictions[0].value == 100.0

    def test_get_predictor_average_score(self, mock_peers):
        """Test getting predictor average score."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol, PredictionScore

        protocol = PredictionProtocol(mock_peers)

        protocol._score_cache["stream1"] = [
            PredictionScore(
                prediction_hash="h1", stream_id="stream1", predictor="E1",
                predicted_value=100, actual_value=101, target_time=1,
                score=0.9, scorer="S", timestamp=1
            ),
            PredictionScore(
                prediction_hash="h2", stream_id="stream1", predictor="E1",
                predicted_value=100, actual_value=102, target_time=2,
                score=0.8, scorer="S", timestamp=2
            ),
        ]

        avg = protocol.get_predictor_average_score("E1")
        assert avg == pytest.approx(0.85)

    def test_get_predictor_average_score_no_scores(self, mock_peers):
        """Test average score returns 0 with no scores."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol

        protocol = PredictionProtocol(mock_peers)
        avg = protocol.get_predictor_average_score("NonExistent")
        assert avg == 0.0

    def test_get_stats(self, mock_peers):
        """Test getting protocol statistics."""
        from satorip2p.protocol.prediction_protocol import PredictionProtocol

        protocol = PredictionProtocol(mock_peers)
        stats = protocol.get_stats()

        assert "subscribed_streams" in stats
        assert "my_predictions" in stats
        assert "cached_predictions" in stats
        assert "cached_scores" in stats
        assert "started" in stats
