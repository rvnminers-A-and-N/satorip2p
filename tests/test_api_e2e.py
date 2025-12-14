"""
satorip2p/tests/test_api_e2e.py

End-to-end tests for REST API with actual HTTP requests.

Run with: pytest tests/test_api_e2e.py -v --timeout=120
Skip with: pytest tests/ -v --ignore=tests/test_api_e2e.py
"""

import pytest
import trio
import json
import socket
from typing import Optional, Tuple
from unittest.mock import Mock, AsyncMock


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


def get_free_port() -> int:
    """Get a free port for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def http_request(
    host: str,
    port: int,
    method: str,
    path: str,
    body: Optional[bytes] = None,
    headers: Optional[dict] = None,
) -> Tuple[int, dict, bytes]:
    """Make an HTTP request and return (status, headers, body)."""
    headers = headers or {}

    # Build request
    lines = [f"{method} {path} HTTP/1.1"]
    lines.append(f"Host: {host}:{port}")

    if body:
        headers["Content-Length"] = str(len(body))
        headers["Content-Type"] = "application/json"

    for key, value in headers.items():
        lines.append(f"{key}: {value}")

    lines.append("")
    request = "\r\n".join(lines).encode() + b"\r\n"

    if body:
        request += body

    # Send request
    stream = await trio.open_tcp_stream(host, port)
    try:
        await stream.send_all(request)

        # Read response
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = await stream.receive_some(4096)
            if not chunk:
                break
            data += chunk

        # Parse response
        header_end = data.index(b"\r\n\r\n")
        header_data = data[:header_end].decode("utf-8")
        response_body = data[header_end + 4:]

        # Parse status line
        lines = header_data.split("\r\n")
        status_line = lines[0].split(" ", 2)
        status_code = int(status_line[1])

        # Parse headers
        response_headers = {}
        for line in lines[1:]:
            if ": " in line:
                key, value = line.split(": ", 1)
                response_headers[key.lower()] = value

        # Read remaining body if Content-Length specified
        content_length = int(response_headers.get("content-length", 0))
        while len(response_body) < content_length:
            chunk = await stream.receive_some(4096)
            if not chunk:
                break
            response_body += chunk

        return status_code, response_headers, response_body[:content_length]

    finally:
        await stream.aclose()


def create_mock_peers():
    """Create a fully mocked Peers instance."""
    mock_peers = Mock()
    mock_peers._started = True
    mock_peers.is_connected = True
    mock_peers.peer_id = "16Uiu2HAmTestPeerIdE2E12345"
    mock_peers.evrmore_address = "ETestAddressE2E"
    mock_peers.public_key = "02" + "e2" * 32
    mock_peers.public_addresses = ["/ip4/127.0.0.1/tcp/24600"]
    mock_peers.nat_type = "PUBLIC"
    mock_peers.is_relay = True
    mock_peers.connected_peers = 3
    mock_peers.enable_dht = True
    mock_peers.enable_pubsub = True
    mock_peers.enable_relay = True
    mock_peers.enable_rendezvous = True
    mock_peers.enable_mdns = True
    mock_peers.enable_ping = True
    mock_peers.enable_autonat = True
    mock_peers.enable_identify = True
    mock_peers.enable_quic = False
    mock_peers.enable_websocket = False
    mock_peers._peer_info = {"peer1": {}, "peer2": {}}
    mock_peers._topic_subscriptions = {}
    mock_peers._dht = Mock()
    mock_peers._dht.get_routing_table_size.return_value = 10
    mock_peers.get_my_subscriptions.return_value = ["stream-1", "stream-2"]
    mock_peers.get_my_publications.return_value = ["stream-1"]
    mock_peers.get_connected_peers.return_value = ["peer-1", "peer-2", "peer-3"]
    mock_peers.get_subscribers.return_value = ["sub-1", "sub-2"]
    mock_peers.get_publishers.return_value = ["pub-1"]
    mock_peers.get_peer_subscriptions.return_value = ["stream-1"]
    mock_peers.get_network_map.return_value = {
        "self": {
            "peer_id": "16Uiu2HAmTestPeerIdE2E12345",
            "evrmore_address": "ETestAddressE2E",
        },
        "connected_peers": ["peer-1", "peer-2", "peer-3"],
        "known_peers": 2,
        "my_subscriptions": ["stream-1", "stream-2"],
        "my_publications": ["stream-1"],
    }
    mock_peers.ping_peer = AsyncMock(return_value=[0.015, 0.012, 0.018])
    mock_peers.subscribe_async = AsyncMock()
    mock_peers.unsubscribe_async = AsyncMock()
    mock_peers.publish = AsyncMock()
    return mock_peers


class TestAPIEndToEnd:
    """End-to-end tests for REST API."""

    @pytest.mark.timeout(30)
    def test_health_endpoint_e2e(self):
        """Test /health endpoint with actual HTTP request."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                status, headers, body = await http_request(
                    "127.0.0.1", port, "GET", "/health"
                )

                assert status == 200
                data = json.loads(body)
                assert data["status"] == "healthy"
                assert data["started"] is True
                assert data["connected"] is True

                nursery.cancel_scope.cancel()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_status_endpoint_e2e(self):
        """Test /status endpoint with actual HTTP request."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                status, headers, body = await http_request(
                    "127.0.0.1", port, "GET", "/status"
                )

                assert status == 200
                data = json.loads(body)
                assert data["peer_id"] == "16Uiu2HAmTestPeerIdE2E12345"
                assert data["nat_type"] == "PUBLIC"
                assert "features" in data
                assert data["features"]["dht"] is True

                nursery.cancel_scope.cancel()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_peers_endpoint_e2e(self):
        """Test /peers endpoint with actual HTTP request."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                status, headers, body = await http_request(
                    "127.0.0.1", port, "GET", "/peers"
                )

                assert status == 200
                data = json.loads(body)
                assert data["count"] == 3
                assert len(data["peers"]) == 3

                nursery.cancel_scope.cancel()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_network_endpoint_e2e(self):
        """Test /network endpoint with actual HTTP request."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                status, headers, body = await http_request(
                    "127.0.0.1", port, "GET", "/network"
                )

                assert status == 200
                data = json.loads(body)
                assert "self" in data
                assert "connected_peers" in data

                nursery.cancel_scope.cancel()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_metrics_endpoint_e2e(self):
        """Test GET /metrics endpoint with actual HTTP request."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port, enable_metrics=True)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                status, headers, body = await http_request(
                    "127.0.0.1", port, "GET", "/metrics"
                )

                assert status == 200
                assert b"satorip2p_connected_peers" in body
                assert b"satorip2p_nat_type" in body
                assert headers.get("content-type", "").startswith("text/plain")

                nursery.cancel_scope.cancel()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_root_endpoint_e2e(self):
        """Test GET / endpoint with actual HTTP request."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                status, headers, body = await http_request(
                    "127.0.0.1", port, "GET", "/"
                )

                assert status == 200
                data = json.loads(body)
                assert data["name"] == "satorip2p"
                assert "endpoints" in data

                nursery.cancel_scope.cancel()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_404_endpoint_e2e(self):
        """Test 404 response for unknown endpoint."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                status, headers, body = await http_request(
                    "127.0.0.1", port, "GET", "/nonexistent"
                )

                assert status == 404

                nursery.cancel_scope.cancel()

        trio.run(run_test)


class TestAPIPostEndpoints:
    """Test POST endpoints."""

    @pytest.mark.timeout(30)
    def test_subscribe_endpoint_e2e(self):
        """Test POST /subscriptions endpoint."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                body = json.dumps({"stream_id": "new-stream"}).encode()
                status, headers, response_body = await http_request(
                    "127.0.0.1", port, "POST", "/subscriptions", body=body
                )

                assert status == 201
                data = json.loads(response_body)
                assert data["success"] is True
                assert data["stream_id"] == "new-stream"

                nursery.cancel_scope.cancel()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_publish_endpoint_e2e(self):
        """Test POST /publish/{stream_id} endpoint."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                body = json.dumps({"data": "test message"}).encode()
                status, headers, response_body = await http_request(
                    "127.0.0.1", port, "POST", "/publish/my-stream", body=body
                )

                assert status == 200
                data = json.loads(response_body)
                assert data["success"] is True

                nursery.cancel_scope.cancel()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_ping_endpoint_e2e(self):
        """Test POST /peers/{peer_id}/ping endpoint."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                status, headers, body = await http_request(
                    "127.0.0.1", port, "POST", "/peers/test-peer-id/ping"
                )

                assert status == 200
                data = json.loads(body)
                assert data["success"] is True
                assert len(data["latencies_ms"]) == 3
                assert data["avg_ms"] > 0

                nursery.cancel_scope.cancel()

        trio.run(run_test)


class TestAPIErrorHandling:
    """Test API error handling."""

    @pytest.mark.timeout(30)
    def test_invalid_json_body_e2e(self):
        """Test handling of invalid JSON body."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                status, headers, body = await http_request(
                    "127.0.0.1", port, "POST", "/subscriptions",
                    body=b"invalid json {"
                )

                assert status == 400

                nursery.cancel_scope.cancel()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_missing_required_field_e2e(self):
        """Test handling of missing required field."""
        from satorip2p.api import PeersAPI

        async def run_test():
            mock_peers = create_mock_peers()
            port = get_free_port()
            api = PeersAPI(mock_peers, host="127.0.0.1", port=port)

            async with trio.open_nursery() as nursery:
                nursery.start_soon(api.start)
                await trio.sleep(0.2)

                body = json.dumps({"wrong_field": "value"}).encode()
                status, headers, response_body = await http_request(
                    "127.0.0.1", port, "POST", "/subscriptions", body=body
                )

                assert status == 400

                nursery.cancel_scope.cancel()

        trio.run(run_test)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--timeout=120"])
