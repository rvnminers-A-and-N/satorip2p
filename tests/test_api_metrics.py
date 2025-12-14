"""
satorip2p/tests/test_api_metrics.py

Unit tests for REST API and Prometheus metrics.
"""

import pytest
import json
import time
from unittest.mock import Mock, AsyncMock, patch, MagicMock


# Mock EvrmoreIdentity for testing
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


@pytest.fixture
def mock_identity():
    """Create mock Evrmore identity."""
    return MockEvrmoreIdentity()


@pytest.fixture
def mock_peers(mock_identity):
    """Create a mock Peers instance for testing."""
    peers = Mock()
    peers.identity = mock_identity
    peers._started = True
    peers.is_connected = True
    peers.peer_id = "16Uiu2HAmTestPeerId12345"
    peers.evrmore_address = mock_identity.address
    peers.public_key = mock_identity.pubkey
    peers.public_addresses = ["/ip4/127.0.0.1/tcp/24600"]
    peers.nat_type = "PUBLIC"
    peers.is_relay = True
    peers.connected_peers = 5
    peers.enable_dht = True
    peers.enable_pubsub = True
    peers.enable_relay = True
    peers.enable_rendezvous = True
    peers.enable_mdns = True
    peers.enable_ping = True
    peers.enable_autonat = True
    peers.enable_identify = True
    peers.enable_quic = False
    peers.enable_websocket = False
    peers._peer_info = {"peer1": {}, "peer2": {}}
    peers._topic_subscriptions = {"topic1": None}
    peers._dht = Mock()
    peers._dht.get_routing_table_size.return_value = 10
    peers.get_my_subscriptions.return_value = ["stream1", "stream2"]
    peers.get_my_publications.return_value = ["stream1"]
    peers.get_connected_peers.return_value = ["peer1", "peer2", "peer3"]
    peers.get_subscribers.return_value = ["sub1", "sub2"]
    peers.get_publishers.return_value = ["pub1"]
    peers.get_peer_subscriptions.return_value = ["stream1"]
    peers.get_network_map.return_value = {
        "self": {"peer_id": "16Uiu2HAmTestPeerId12345"},
        "connected_peers": ["peer1", "peer2"],
    }
    peers.ping_peer = AsyncMock(return_value=[0.01, 0.02, 0.015])
    peers.subscribe_async = AsyncMock()
    peers.unsubscribe_async = AsyncMock()
    peers.publish = AsyncMock()
    return peers


class TestMetricsCollector:
    """Test MetricsCollector class."""

    def test_init(self, mock_peers):
        """Test MetricsCollector initialization."""
        from satorip2p.metrics import MetricsCollector

        metrics = MetricsCollector(mock_peers)

        assert metrics.peers == mock_peers
        assert metrics._messages_sent == 0
        assert metrics._messages_received == 0
        assert metrics._bytes_sent == 0
        assert metrics._bytes_received == 0

    def test_record_message_sent(self, mock_peers):
        """Test recording sent messages."""
        from satorip2p.metrics import MetricsCollector

        metrics = MetricsCollector(mock_peers)
        metrics.record_message_sent(100)
        metrics.record_message_sent(200)

        assert metrics._messages_sent == 2
        assert metrics._bytes_sent == 300

    def test_record_message_received(self, mock_peers):
        """Test recording received messages."""
        from satorip2p.metrics import MetricsCollector

        metrics = MetricsCollector(mock_peers)
        metrics.record_message_received(150)
        metrics.record_message_received(250)

        assert metrics._messages_received == 2
        assert metrics._bytes_received == 400

    def test_record_ping_latency(self, mock_peers):
        """Test recording ping latency."""
        from satorip2p.metrics import MetricsCollector

        metrics = MetricsCollector(mock_peers)
        metrics.record_ping_latency(0.05)
        metrics.record_ping_latency(0.10)

        assert metrics._latency_count == 2
        assert abs(metrics._latency_sum - 0.15) < 0.0001  # Float comparison

    def test_get_nat_type_value(self, mock_peers):
        """Test NAT type to numeric conversion."""
        from satorip2p.metrics import MetricsCollector

        metrics = MetricsCollector(mock_peers)

        mock_peers.nat_type = "PUBLIC"
        assert metrics._get_nat_type_value() == 1

        mock_peers.nat_type = "PRIVATE"
        assert metrics._get_nat_type_value() == 2

        mock_peers.nat_type = "UNKNOWN"
        assert metrics._get_nat_type_value() == 0

    def test_collect_prometheus_format(self, mock_peers):
        """Test collecting metrics in Prometheus format."""
        from satorip2p.metrics import MetricsCollector

        metrics = MetricsCollector(mock_peers)
        metrics.record_message_sent(100)
        metrics.record_ping_latency(0.05)

        output = metrics.collect()

        # Check format
        assert "# HELP satorip2p_connected_peers" in output
        assert "# TYPE satorip2p_connected_peers gauge" in output
        assert "satorip2p_connected_peers 5" in output
        assert "satorip2p_messages_sent_total 1" in output
        assert "satorip2p_nat_type 1" in output  # PUBLIC = 1
        assert "satorip2p_info" in output

    def test_get_stats(self, mock_peers):
        """Test getting stats as dictionary."""
        from satorip2p.metrics import MetricsCollector

        metrics = MetricsCollector(mock_peers)
        metrics.record_message_sent(100)

        stats = metrics.get_stats()

        assert stats["connected_peers"] == 5
        assert stats["messages_sent"] == 1
        assert stats["nat_type"] == "PUBLIC"
        assert stats["peer_id"] == "16Uiu2HAmTestPeerId12345"

    def test_reset_counters(self, mock_peers):
        """Test resetting counters."""
        from satorip2p.metrics import MetricsCollector

        metrics = MetricsCollector(mock_peers)
        metrics.record_message_sent(100)
        metrics.record_message_received(200)
        metrics.record_ping_latency(0.05)

        metrics.reset_counters()

        assert metrics._messages_sent == 0
        assert metrics._messages_received == 0
        assert metrics._bytes_sent == 0
        assert metrics._bytes_received == 0
        assert metrics._latency_count == 0


class TestPeersAPIRoutes:
    """Test PeersAPI route handlers."""

    def test_api_init(self, mock_peers):
        """Test PeersAPI initialization."""
        from satorip2p.api import PeersAPI

        api = PeersAPI(mock_peers, host="127.0.0.1", port=8080)

        assert api.peers == mock_peers
        assert api.host == "127.0.0.1"
        assert api.port == 8080
        assert api.enable_metrics is True
        assert api.metrics is not None

    def test_api_init_without_metrics(self, mock_peers):
        """Test PeersAPI initialization without metrics."""
        from satorip2p.api import PeersAPI

        api = PeersAPI(mock_peers, enable_metrics=False)

        assert api.metrics is None

    async def test_handle_health_healthy(self, mock_peers):
        """Test health endpoint when healthy."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        request = Request(method="GET", path="/health", query={}, headers={}, body=b"")

        response = await api._handle_health(request)

        assert response.status == 200
        data = json.loads(response.body)
        assert data["status"] == "healthy"
        assert data["started"] is True
        assert data["connected"] is True

    async def test_handle_health_unhealthy(self, mock_peers):
        """Test health endpoint when unhealthy."""
        from satorip2p.api import PeersAPI, Request

        mock_peers._started = False
        mock_peers.is_connected = False
        api = PeersAPI(mock_peers)
        request = Request(method="GET", path="/health", query={}, headers={}, body=b"")

        response = await api._handle_health(request)

        assert response.status == 503
        data = json.loads(response.body)
        assert data["status"] == "unhealthy"

    async def test_handle_status(self, mock_peers):
        """Test status endpoint."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        request = Request(method="GET", path="/status", query={}, headers={}, body=b"")

        response = await api._handle_status(request)

        assert response.status == 200
        data = json.loads(response.body)
        assert data["peer_id"] == "16Uiu2HAmTestPeerId12345"
        assert data["nat_type"] == "PUBLIC"
        assert data["connected_peers"] == 5
        assert "features" in data
        assert data["features"]["dht"] is True

    async def test_handle_get_peers(self, mock_peers):
        """Test get peers endpoint."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        request = Request(method="GET", path="/peers", query={}, headers={}, body=b"")

        response = await api._handle_get_peers(request)

        assert response.status == 200
        data = json.loads(response.body)
        assert data["count"] == 3
        assert len(data["peers"]) == 3

    async def test_handle_ping_peer(self, mock_peers):
        """Test ping peer endpoint."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        request = Request(
            method="POST",
            path="/peers/test-peer-id/ping",
            query={},
            headers={},
            body=b"",
        )
        request.path_params = {"peer_id": "test-peer-id"}

        response = await api._handle_ping_peer(request)

        assert response.status == 200
        data = json.loads(response.body)
        assert data["success"] is True
        assert data["count"] == 3
        assert len(data["latencies_ms"]) == 3

    async def test_handle_ping_peer_failed(self, mock_peers):
        """Test ping peer endpoint when ping fails."""
        from satorip2p.api import PeersAPI, Request

        mock_peers.ping_peer = AsyncMock(return_value=None)
        api = PeersAPI(mock_peers)
        request = Request(
            method="POST",
            path="/peers/test-peer-id/ping",
            query={},
            headers={},
            body=b"",
        )
        request.path_params = {"peer_id": "test-peer-id"}

        response = await api._handle_ping_peer(request)

        assert response.status == 404
        data = json.loads(response.body)
        assert data["success"] is False

    async def test_handle_network(self, mock_peers):
        """Test network map endpoint."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        request = Request(method="GET", path="/network", query={}, headers={}, body=b"")

        response = await api._handle_network(request)

        assert response.status == 200
        data = json.loads(response.body)
        assert "self" in data
        assert "connected_peers" in data

    async def test_handle_get_subscriptions(self, mock_peers):
        """Test get subscriptions endpoint."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        request = Request(
            method="GET", path="/subscriptions", query={}, headers={}, body=b""
        )

        response = await api._handle_get_subscriptions(request)

        assert response.status == 200
        data = json.loads(response.body)
        assert len(data["subscriptions"]) == 2
        assert len(data["publications"]) == 1

    async def test_handle_subscribe(self, mock_peers):
        """Test subscribe endpoint."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        body = json.dumps({"stream_id": "new-stream"}).encode()
        request = Request(
            method="POST", path="/subscriptions", query={}, headers={}, body=body
        )

        response = await api._handle_subscribe(request)

        assert response.status == 201
        data = json.loads(response.body)
        assert data["success"] is True
        assert data["stream_id"] == "new-stream"
        mock_peers.subscribe_async.assert_called_once()

    async def test_handle_subscribe_missing_body(self, mock_peers):
        """Test subscribe endpoint with missing body."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        request = Request(
            method="POST", path="/subscriptions", query={}, headers={}, body=b""
        )

        response = await api._handle_subscribe(request)

        assert response.status == 400

    async def test_handle_unsubscribe(self, mock_peers):
        """Test unsubscribe endpoint."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        request = Request(
            method="DELETE",
            path="/subscriptions/stream1",
            query={},
            headers={},
            body=b"",
        )
        request.path_params = {"stream_id": "stream1"}

        response = await api._handle_unsubscribe(request)

        assert response.status == 200
        data = json.loads(response.body)
        assert data["success"] is True
        mock_peers.unsubscribe_async.assert_called_once()

    async def test_handle_publish(self, mock_peers):
        """Test publish endpoint."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        body = json.dumps({"data": "test message"}).encode()
        request = Request(
            method="POST",
            path="/publish/stream1",
            query={},
            headers={},
            body=body,
        )
        request.path_params = {"stream_id": "stream1"}

        response = await api._handle_publish(request)

        assert response.status == 200
        data = json.loads(response.body)
        assert data["success"] is True
        mock_peers.publish.assert_called_once()

    async def test_handle_metrics(self, mock_peers):
        """Test metrics endpoint."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers)
        request = Request(method="GET", path="/metrics", query={}, headers={}, body=b"")

        response = await api._handle_metrics(request)

        assert response.status == 200
        assert b"satorip2p_connected_peers" in response.body
        assert response.headers["Content-Type"].startswith("text/plain")

    async def test_handle_metrics_disabled(self, mock_peers):
        """Test metrics endpoint when disabled."""
        from satorip2p.api import PeersAPI, Request

        api = PeersAPI(mock_peers, enable_metrics=False)
        request = Request(method="GET", path="/metrics", query={}, headers={}, body=b"")

        response = await api._handle_metrics(request)

        assert response.status == 404


class TestPeersAPIPathMatching:
    """Test path matching logic."""

    def test_match_exact_path(self, mock_peers):
        """Test exact path matching."""
        from satorip2p.api import PeersAPI

        api = PeersAPI(mock_peers)

        match, params = api._match_path("/health", "/health")
        assert match is True
        assert params == {}

    def test_match_path_with_param(self, mock_peers):
        """Test path matching with parameter."""
        from satorip2p.api import PeersAPI

        api = PeersAPI(mock_peers)

        match, params = api._match_path(
            "/peers/{peer_id}/ping", "/peers/abc123/ping"
        )
        assert match is True
        assert params == {"peer_id": "abc123"}

    def test_match_path_no_match(self, mock_peers):
        """Test path that doesn't match."""
        from satorip2p.api import PeersAPI

        api = PeersAPI(mock_peers)

        match, params = api._match_path("/health", "/status")
        assert match is False

    def test_match_path_different_length(self, mock_peers):
        """Test path with different segment count."""
        from satorip2p.api import PeersAPI

        api = PeersAPI(mock_peers)

        match, params = api._match_path("/peers/{id}", "/peers/abc/extra")
        assert match is False


class TestResponse:
    """Test Response class."""

    def test_json_response(self):
        """Test JSON response creation."""
        from satorip2p.api import Response

        response = Response.json({"key": "value"})

        assert response.status == 200
        assert response.headers["Content-Type"] == "application/json"
        assert b'"key"' in response.body

    def test_json_response_with_status(self):
        """Test JSON response with custom status."""
        from satorip2p.api import Response

        response = Response.json({"error": "not found"}, status=404)

        assert response.status == 404

    def test_text_response(self):
        """Test text response creation."""
        from satorip2p.api import Response

        response = Response.text("Hello, World!")

        assert response.status == 200
        assert response.headers["Content-Type"] == "text/plain"
        assert response.body == b"Hello, World!"

    def test_error_response(self):
        """Test error response creation."""
        from satorip2p.api import Response

        response = Response.error("Something went wrong", status=500)

        assert response.status == 500
        data = json.loads(response.body)
        assert data["error"] == "Something went wrong"
