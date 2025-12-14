"""
Tests for satorip2p.compat module - Compatibility layer for legacy Satori protocols.

Tests both PubSubServer (SatoriPubSubConn compatible) and CentrifugoServer.
"""

import pytest
import trio
import json
from unittest.mock import AsyncMock, MagicMock, patch


class TestPubSubServer:
    """Tests for SatoriPubSubConn compatible WebSocket server."""

    def test_import(self):
        """Test that PubSubServer can be imported."""
        from satorip2p.compat import PubSubServer
        assert PubSubServer is not None

    def test_pubsub_client_creation(self):
        """Test PubSubClient initialization."""
        from satorip2p.compat.pubsub import PubSubClient

        async def run_test():
            send_channel, _ = trio.open_memory_channel(10)
            client = PubSubClient(
                uid="test-uid",
                websocket=MagicMock(),
                send_channel=send_channel,
            )
            assert client.uid == "test-uid"
            assert client.connected is True
            assert len(client.subscriptions) == 0

        trio.run(run_test)

    def test_pubsub_server_creation(self):
        """Test PubSubServer initialization."""
        from satorip2p.compat import PubSubServer

        mock_peers = MagicMock()
        server = PubSubServer(
            peers=mock_peers,
            host="127.0.0.1",
            port=24603,
        )
        assert server.host == "127.0.0.1"
        assert server.port == 24603
        assert server.client_count == 0
        assert server.stream_count == 0

    def test_pubsub_server_start(self):
        """Test PubSubServer start method."""
        from satorip2p.compat import PubSubServer

        async def run_test():
            mock_peers = MagicMock()
            server = PubSubServer(peers=mock_peers, port=24690)

            result = await server.start()
            assert result is True
            assert server._started is True

            # Double start should return True but warn
            result = await server.start()
            assert result is True

            await server.stop()
            assert server._started is False

        trio.run(run_test)

    def test_pubsub_server_stats(self):
        """Test PubSubServer stats reporting."""
        from satorip2p.compat import PubSubServer

        mock_peers = MagicMock()
        server = PubSubServer(peers=mock_peers, host="0.0.0.0", port=24603)

        stats = server.get_stats()
        assert stats["host"] == "0.0.0.0"
        assert stats["port"] == 24603
        assert stats["running"] is False
        assert stats["client_count"] == 0
        assert stats["stream_count"] == 0

    def test_pubsub_server_repr(self):
        """Test PubSubServer string representation."""
        from satorip2p.compat import PubSubServer

        mock_peers = MagicMock()
        server = PubSubServer(peers=mock_peers)

        assert "stopped" in repr(server)
        assert "clients=0" in repr(server)


class TestCentrifugoServer:
    """Tests for Centrifugo compatible WebSocket/REST server."""

    def test_import(self):
        """Test that CentrifugoServer can be imported."""
        from satorip2p.compat import CentrifugoServer
        assert CentrifugoServer is not None

    def test_centrifugo_client_creation(self):
        """Test CentrifugoClient initialization."""
        from satorip2p.compat.centrifugo import CentrifugoClient

        async def run_test():
            send_channel, _ = trio.open_memory_channel(10)
            client = CentrifugoClient(
                client_id="client-1",
                user_id="user-123",
                websocket=MagicMock(),
                send_channel=send_channel,
            )
            assert client.client_id == "client-1"
            assert client.user_id == "user-123"
            assert client.connected is True
            assert len(client.subscriptions) == 0

        trio.run(run_test)

    def test_centrifugo_server_creation(self):
        """Test CentrifugoServer initialization."""
        from satorip2p.compat import CentrifugoServer

        mock_peers = MagicMock()
        server = CentrifugoServer(
            peers=mock_peers,
            host="127.0.0.1",
            port=8000,
            jwt_secret="test-secret",
        )
        assert server.host == "127.0.0.1"
        assert server.port == 8000
        assert server.jwt_secret == "test-secret"
        assert server.client_count == 0

    def test_centrifugo_server_start(self):
        """Test CentrifugoServer start method."""
        from satorip2p.compat import CentrifugoServer

        async def run_test():
            mock_peers = MagicMock()
            server = CentrifugoServer(peers=mock_peers, port=8100)

            result = await server.start()
            assert result is True
            assert server._started is True

            await server.stop()
            assert server._started is False

        trio.run(run_test)

    def test_centrifugo_client_id_generation(self):
        """Test unique client ID generation."""
        from satorip2p.compat import CentrifugoServer

        mock_peers = MagicMock()
        server = CentrifugoServer(peers=mock_peers)

        id1 = server._generate_client_id()
        id2 = server._generate_client_id()

        assert id1 != id2
        assert id1.startswith("p2p-")
        assert id2.startswith("p2p-")

    def test_centrifugo_jwt_verify_invalid(self):
        """Test JWT verification with invalid token."""
        from satorip2p.compat import CentrifugoServer

        mock_peers = MagicMock()
        server = CentrifugoServer(
            peers=mock_peers,
            jwt_secret="test-secret",
        )

        # Invalid token format
        result = server._verify_jwt("invalid-token")
        assert result is None

        # Wrong number of parts
        result = server._verify_jwt("a.b")
        assert result is None

        # Invalid signature
        result = server._verify_jwt("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.wrong")
        assert result is None

    def test_centrifugo_jwt_verify_valid(self):
        """Test JWT verification with valid token."""
        import base64
        import hmac
        import hashlib
        import json
        from satorip2p.compat import CentrifugoServer

        mock_peers = MagicMock()
        secret = "test-secret"
        server = CentrifugoServer(peers=mock_peers, jwt_secret=secret)

        # Create valid JWT
        header = base64.urlsafe_b64encode(
            json.dumps({"alg": "HS256", "typ": "JWT"}).encode()
        ).rstrip(b"=").decode()

        payload_data = {"sub": "user-123"}
        payload = base64.urlsafe_b64encode(
            json.dumps(payload_data).encode()
        ).rstrip(b"=").decode()

        message = f"{header}.{payload}"
        signature = base64.urlsafe_b64encode(
            hmac.new(secret.encode(), message.encode(), hashlib.sha256).digest()
        ).rstrip(b"=").decode()

        token = f"{header}.{payload}.{signature}"

        result = server._verify_jwt(token)
        assert result is not None
        assert result["sub"] == "user-123"

    def test_centrifugo_server_stats(self):
        """Test CentrifugoServer stats reporting."""
        from satorip2p.compat import CentrifugoServer

        mock_peers = MagicMock()
        server = CentrifugoServer(peers=mock_peers, host="0.0.0.0", port=8000)

        stats = server.get_stats()
        assert stats["host"] == "0.0.0.0"
        assert stats["port"] == 8000
        assert stats["running"] is False
        assert stats["client_count"] == 0
        assert stats["channel_count"] == 0

    def test_centrifugo_server_repr(self):
        """Test CentrifugoServer string representation."""
        from satorip2p.compat import CentrifugoServer

        mock_peers = MagicMock()
        server = CentrifugoServer(peers=mock_peers)

        assert "stopped" in repr(server)
        assert "clients=0" in repr(server)


class TestMessageProcessing:
    """Tests for message processing logic."""

    def test_pubsub_parse_command_message(self):
        """Test parsing command:payload format messages."""
        # Test various message formats
        test_cases = [
            ("key:some-payload", "key", "some-payload"),
            ("subscribe:stream-uuid-123", "subscribe", "stream-uuid-123"),
            ("publish:{\"topic\":\"test\"}", "publish", "{\"topic\":\"test\"}"),
            ("notice:{\"topic\":\"connection\",\"data\":\"False\"}", "notice", "{\"topic\":\"connection\",\"data\":\"False\"}"),
        ]

        for message, expected_cmd, expected_payload in test_cases:
            colon_pos = message.index(":")
            cmd = message[:colon_pos].lower()
            payload = message[colon_pos + 1:]

            assert cmd == expected_cmd, f"Command mismatch for '{message}'"
            assert payload == expected_payload, f"Payload mismatch for '{message}'"

    def test_centrifugo_parse_subscribe_channel(self):
        """Test parsing Centrifugo channel format."""
        test_cases = [
            ("streams:abc-123-def", "abc-123-def"),
            ("streams:uuid-with-dashes", "uuid-with-dashes"),
            ("other-channel", "other-channel"),
        ]

        for channel, expected_stream_id in test_cases:
            if channel.startswith("streams:"):
                stream_id = channel[8:]
            else:
                stream_id = channel

            assert stream_id == expected_stream_id


class TestIntegration:
    """Integration tests for compat layer with Peers."""

    @pytest.mark.timeout(30)
    def test_pubsub_server_with_mock_peers(self):
        """Test PubSubServer integration with mock Peers."""
        from satorip2p.compat import PubSubServer

        async def run_test():
            # Create mock Peers
            mock_peers = MagicMock()
            mock_peers.subscribe_async = AsyncMock()
            mock_peers.publish = AsyncMock()

            # Create and start server
            server = PubSubServer(peers=mock_peers, port=24691)
            await server.start()

            # Verify server is ready
            assert server._started is True

            await server.stop()

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_centrifugo_server_with_mock_peers(self):
        """Test CentrifugoServer integration with mock Peers."""
        from satorip2p.compat import CentrifugoServer

        async def run_test():
            # Create mock Peers
            mock_peers = MagicMock()
            mock_peers.subscribe_async = AsyncMock()
            mock_peers.publish = AsyncMock()

            # Create and start server
            server = CentrifugoServer(
                peers=mock_peers,
                port=8101,
                skip_jwt_verify=True,
            )
            await server.start()

            # Verify server is ready
            assert server._started is True

            await server.stop()

        trio.run(run_test)


class TestMainModuleExports:
    """Test that compat classes are properly exported."""

    def test_compat_exports_from_main(self):
        """Test that compat classes can be imported from main module."""
        from satorip2p import PubSubServer, CentrifugoServer

        assert PubSubServer is not None
        assert CentrifugoServer is not None

    def test_compat_in_all(self):
        """Test that compat classes are in __all__."""
        import satorip2p

        assert "PubSubServer" in satorip2p.__all__
        assert "CentrifugoServer" in satorip2p.__all__


class TestHTTPResponses:
    """Test HTTP response formatting for Centrifugo REST API."""

    def test_json_response_format(self):
        """Test JSON response formatting."""
        from satorip2p.compat.centrifugo import CentrifugoServer

        mock_peers = MagicMock()
        server = CentrifugoServer(peers=mock_peers)

        response = server._json_response({"result": "ok"})

        assert "HTTP/1.1 200 OK" in response
        assert "Content-Type: application/json" in response
        assert "Access-Control-Allow-Origin: *" in response
        assert '"result": "ok"' in response

    def test_error_response_format(self):
        """Test error response formatting."""
        from satorip2p.compat.centrifugo import CentrifugoServer

        mock_peers = MagicMock()
        server = CentrifugoServer(peers=mock_peers)

        response = server._error_response(401, "Unauthorized")

        assert "HTTP/1.1 401" in response
        assert '"code": 401' in response
        assert '"message": "Unauthorized"' in response

    def test_cors_response_format(self):
        """Test CORS preflight response."""
        from satorip2p.compat.centrifugo import CentrifugoServer

        mock_peers = MagicMock()
        server = CentrifugoServer(peers=mock_peers)

        response = server._cors_response()

        assert "HTTP/1.1 204 No Content" in response
        assert "Access-Control-Allow-Origin: *" in response
        assert "Access-Control-Allow-Methods: POST, OPTIONS" in response


class TestServerAPI:
    """Tests for Central Server REST API compatibility."""

    def test_import(self):
        """Test that ServerAPI can be imported."""
        from satorip2p.compat import ServerAPI
        assert ServerAPI is not None

    def test_server_api_creation(self):
        """Test ServerAPI initialization."""
        from satorip2p.compat import ServerAPI

        mock_peers = MagicMock()
        server = ServerAPI(
            peers=mock_peers,
            host="127.0.0.1",
            port=8080,
        )
        assert server.host == "127.0.0.1"
        assert server.port == 8080
        assert server.jwt_secret is not None

    def test_checkin_details(self):
        """Test CheckinDetails creation."""
        from satorip2p.compat import CheckinDetails

        details = CheckinDetails(
            key="test-key",
            oracle_key="oracle-123",
            id_key="id-456",
            subscription_keys=["sub-1", "sub-2"],
            publication_keys=["pub-1"],
            subscriptions=[{"uuid": "stream-1"}],
            publications=[{"uuid": "stream-2"}],
        )

        d = details.to_dict()
        assert d["key"] == "test-key"
        assert d["oracleKey"] == "oracle-123"
        assert len(d["subscriptionKeys"]) == 2

    def test_stream_registration(self):
        """Test StreamRegistration creation."""
        from satorip2p.compat.server import StreamRegistration

        reg = StreamRegistration(
            stream_id="uuid-123",
            source="satori",
            author="wallet-addr",
            stream="test-stream",
            target="data.value",
        )

        d = reg.to_dict()
        assert d["uuid"] == "uuid-123"
        assert d["source"] == "satori"
        assert d["stream"] == "test-stream"

    def test_stream_uuid_generation(self):
        """Test stream UUID generation is deterministic."""
        from satorip2p.compat import ServerAPI

        mock_peers = MagicMock()
        server = ServerAPI(peers=mock_peers)

        uuid1 = server._generate_stream_uuid("satori", "author1", "stream1", "target")
        uuid2 = server._generate_stream_uuid("satori", "author1", "stream1", "target")
        uuid3 = server._generate_stream_uuid("satori", "author1", "stream2", "target")

        assert uuid1 == uuid2  # Same inputs = same UUID
        assert uuid1 != uuid3  # Different stream = different UUID

    def test_jwt_creation(self):
        """Test JWT token creation."""
        from satorip2p.compat import ServerAPI

        mock_peers = MagicMock()
        server = ServerAPI(peers=mock_peers, jwt_secret="test-secret")

        token = server._create_jwt("user-123")
        parts = token.split(".")
        assert len(parts) == 3  # header.payload.signature

    def test_server_api_stats(self):
        """Test ServerAPI stats reporting."""
        from satorip2p.compat import ServerAPI

        mock_peers = MagicMock()
        server = ServerAPI(peers=mock_peers)

        stats = server.get_stats()
        assert "host" in stats
        assert "port" in stats
        assert stats["registered_streams"] == 0


class TestDataManager:
    """Tests for DataManager compatibility layer."""

    def test_import(self):
        """Test that DataManager classes can be imported."""
        from satorip2p.compat import Message, DataManagerBridge, SecurityPolicy
        assert Message is not None
        assert DataManagerBridge is not None
        assert SecurityPolicy is not None

    def test_message_creation(self):
        """Test Message creation."""
        from satorip2p.compat.datamanager import Message

        msg = Message.create(
            method="stream/data/get",
            uuid="stream-123",
            status="success",
        )

        assert msg.method == "stream/data/get"
        assert msg.uuid == "stream-123"
        assert msg.status == "success"

    def test_message_to_dict(self):
        """Test Message to_dict conversion."""
        from satorip2p.compat.datamanager import Message

        msg = Message.create(
            method="stream/subscribe",
            uuid="test-uuid",
            is_subscription=True,
        )

        d = msg.to_dict()
        assert d["method"] == "stream/subscribe"
        assert d["params"]["uuid"] == "test-uuid"
        assert d["sub"] is True

    def test_message_to_json(self):
        """Test Message JSON serialization."""
        from satorip2p.compat.datamanager import Message

        msg = Message.create(
            method="test/method",
            uuid="test-uuid",
        )

        json_str = msg.to_json()
        assert "test/method" in json_str
        assert "test-uuid" in json_str

    def test_security_policy_local(self):
        """Test local security policy."""
        from satorip2p.compat.datamanager import SecurityPolicy

        policy = SecurityPolicy.local()
        assert policy.local_authentication is True
        assert policy.local_encryption is False

    def test_security_policy_peer(self):
        """Test peer security policy."""
        from satorip2p.compat.datamanager import SecurityPolicy

        policy = SecurityPolicy.peer()
        assert policy.remote_authentication is True
        assert policy.remote_encryption is True

    def test_datamanager_api_constants(self):
        """Test DataManager API constants."""
        from satorip2p.compat.datamanager import DataManagerAPI

        assert DataManagerAPI.SUBSCRIBE == "stream/subscribe"
        assert DataManagerAPI.DATA_GET == "stream/data/get"
        assert DataManagerAPI.DATA_INSERT == "stream/data/insert"

    def test_datamanager_bridge_creation(self):
        """Test DataManagerBridge initialization."""
        from satorip2p.compat import DataManagerBridge

        mock_peers = MagicMock()
        bridge = DataManagerBridge(peers=mock_peers)

        stats = bridge.get_stats()
        assert stats["subscriptions"] == 0


class TestDataManagerWithPandas:
    """Tests for DataManager with pandas (if available)."""

    def test_message_from_dataframe(self):
        """Test creating Message from DataFrame."""
        try:
            import pandas as pd
            from satorip2p.compat.datamanager import Message

            df = pd.DataFrame({"value": [1.0, 2.0, 3.0], "ts": ["2024-01-01", "2024-01-02", "2024-01-03"]})
            msg = Message.from_dataframe(df, stream_id="test-stream")

            assert msg.method == "stream/data/insert"
            assert msg.uuid == "test-stream"
            assert msg.data is not None

        except ImportError:
            pytest.skip("pandas not available")

    def test_message_get_dataframe(self):
        """Test getting DataFrame from Message."""
        try:
            import pandas as pd
            from satorip2p.compat.datamanager import Message

            df = pd.DataFrame({"value": [1.0, 2.0]})
            msg = Message.create(method="test", uuid="test", data=df)

            result = msg.get_dataframe()
            assert result is not None
            assert len(result) == 2

        except ImportError:
            pytest.skip("pandas not available")

    def test_message_bytes_roundtrip(self):
        """Test Message serialization/deserialization roundtrip."""
        try:
            import pandas as pd
            import pyarrow
            from satorip2p.compat.datamanager import Message

            df = pd.DataFrame({"x": [1, 2, 3], "y": [4.0, 5.0, 6.0]})
            original = Message.from_dataframe(df, stream_id="test")

            # Serialize and deserialize
            data = original.to_bytes()
            restored = Message.from_bytes(data)

            assert restored.method == original.method
            assert restored.uuid == original.uuid

            result_df = restored.get_dataframe()
            assert result_df is not None
            assert len(result_df) == 3

        except ImportError:
            pytest.skip("pandas or pyarrow not available")


class TestMainModuleNewExports:
    """Test new compat exports from main module."""

    def test_server_api_export(self):
        """Test ServerAPI export from main module."""
        from satorip2p import ServerAPI
        assert ServerAPI is not None

    def test_datamanager_bridge_export(self):
        """Test DataManagerBridge export from main module."""
        from satorip2p import DataManagerBridge
        assert DataManagerBridge is not None

    def test_datamanager_message_export(self):
        """Test DataManagerMessage export from main module."""
        from satorip2p import DataManagerMessage
        assert DataManagerMessage is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
