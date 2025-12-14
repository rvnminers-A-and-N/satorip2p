"""
Tests for satorip2p.integration.neuron module - Neuron Drop-in Replacement Classes.

These tests verify that the P2P replacement classes have compatible interfaces
with the original Satori networking classes.
"""

import pytest
import json
from unittest.mock import MagicMock, AsyncMock, patch


class TestP2PSatoriPubSubConnImport:
    """Test P2PSatoriPubSubConn import and basic creation."""

    def test_import(self):
        """Test that P2PSatoriPubSubConn can be imported."""
        from satorip2p.integration import P2PSatoriPubSubConn
        assert P2PSatoriPubSubConn is not None

    def test_import_from_main(self):
        """Test import from main package."""
        from satorip2p import P2PSatoriPubSubConn
        assert P2PSatoriPubSubConn is not None

    def test_creation_basic(self):
        """Test basic creation without starting."""
        from satorip2p.integration import P2PSatoriPubSubConn

        # Create without threaded mode to avoid starting
        conn = P2PSatoriPubSubConn(
            uid="test-wallet-address",
            payload="stream-uuid",
            threaded=False,
        )

        assert conn.uid == "test-wallet-address"
        assert conn.payload == "stream-uuid"
        assert conn.threaded is False

    def test_creation_with_router(self):
        """Test creation with router callback."""
        from satorip2p.integration import P2PSatoriPubSubConn

        def my_router(msg):
            pass

        conn = P2PSatoriPubSubConn(
            uid="test-uid",
            payload="stream-1",
            router=my_router,
            threaded=False,
        )

        assert conn.router == my_router

    def test_set_router(self):
        """Test setRouter method."""
        from satorip2p.integration import P2PSatoriPubSubConn

        conn = P2PSatoriPubSubConn(
            uid="test-uid",
            payload="stream-1",
            threaded=False,
        )

        def new_router(msg):
            pass

        conn.setRouter(new_router)
        assert conn.router == new_router

    def test_connected_property_initial(self):
        """Test connected property when not connected."""
        from satorip2p.integration import P2PSatoriPubSubConn

        conn = P2PSatoriPubSubConn(
            uid="test-uid",
            payload="stream-1",
            threaded=False,
        )

        # Should be False when not connected
        assert conn.connected is False

    def test_topic_time(self):
        """Test topic time tracking."""
        from satorip2p.integration import P2PSatoriPubSubConn
        import time

        conn = P2PSatoriPubSubConn(
            uid="test-uid",
            payload="stream-1",
            threaded=False,
        )

        conn.setTopicTime("test-topic")
        assert "test-topic" in conn.topicTime
        assert conn.topicTime["test-topic"] <= time.time()

    def test_ws_attribute_compatibility(self):
        """Test ws attribute exists for compatibility."""
        from satorip2p.integration import P2PSatoriPubSubConn

        conn = P2PSatoriPubSubConn(
            uid="test-uid",
            payload="stream-1",
            threaded=False,
        )

        # ws attribute should exist for code that checks ws.connected
        assert hasattr(conn, 'ws')
        assert conn.ws == conn  # Self-reference


class TestP2PSatoriServerClientImport:
    """Test P2PSatoriServerClient import and basic creation."""

    def test_import(self):
        """Test that P2PSatoriServerClient can be imported."""
        from satorip2p.integration import P2PSatoriServerClient
        assert P2PSatoriServerClient is not None

    def test_import_from_main(self):
        """Test import from main package."""
        from satorip2p import P2PSatoriServerClient
        assert P2PSatoriServerClient is not None

    def test_creation_basic(self):
        """Test basic creation."""
        from satorip2p.integration import P2PSatoriServerClient

        mock_wallet = MagicMock()
        mock_wallet.address = "ETestAddress123"

        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            enable_central_fallback=False,  # Don't try to import satorilib
        )

        assert client.wallet == mock_wallet
        assert client.url == "https://central.satorinet.io"

    def test_creation_with_urls(self):
        """Test creation with custom URLs."""
        from satorip2p.integration import P2PSatoriServerClient

        mock_wallet = MagicMock()

        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            url="https://custom.server.io",
            sendingUrl="https://custom.mundo.io",
            enable_central_fallback=False,
        )

        assert client.url == "https://custom.server.io"
        assert client.sendingUrl == "https://custom.mundo.io"

    def test_topic_time(self):
        """Test topic time tracking."""
        from satorip2p.integration import P2PSatoriServerClient
        import time

        mock_wallet = MagicMock()

        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            enable_central_fallback=False,
        )

        client.setTopicTime("test-topic")
        assert "test-topic" in client.topicTime

    def test_checkin_p2p_only(self):
        """Test checkin in P2P-only mode."""
        from satorip2p.integration import P2PSatoriServerClient

        mock_wallet = MagicMock()
        mock_wallet.address = "ETestAddress123"
        mock_wallet.pubkey = "testpubkey"

        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            enable_central_fallback=False,
        )

        result = client.checkin()

        assert isinstance(result, dict)
        assert result['key'] == "ETestAddress123"
        assert 'subscriptions' in result
        assert 'publications' in result

    def test_checkin_check(self):
        """Test checkinCheck returns False in P2P mode."""
        from satorip2p.integration import P2PSatoriServerClient

        mock_wallet = MagicMock()

        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            enable_central_fallback=False,
        )

        # P2P doesn't need periodic checkins
        assert client.checkinCheck() is False

    def test_register_stream(self):
        """Test registerStream stores locally."""
        from satorip2p.integration import P2PSatoriServerClient

        mock_wallet = MagicMock()

        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            enable_central_fallback=False,
        )

        stream = {'source': 'test', 'stream': 'stream1', 'target': 'target'}
        client.registerStream(stream)

        assert stream in client._local_streams

    def test_register_subscription(self):
        """Test registerSubscription stores locally."""
        from satorip2p.integration import P2PSatoriServerClient

        mock_wallet = MagicMock()

        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            enable_central_fallback=False,
        )

        sub = {'uuid': 'stream-uuid-123'}
        client.registerSubscription(sub)

        assert sub in client._local_subscriptions

    def test_get_balances_p2p(self):
        """Test getBalances in P2P mode."""
        from satorip2p.integration import P2PSatoriServerClient

        mock_wallet = MagicMock()

        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            enable_central_fallback=False,
        )

        success, balances = client.getBalances()
        assert success is True
        assert 'currency' in balances

    def test_get_centrifugo_token_p2p(self):
        """Test getCentrifugoToken returns empty in P2P mode."""
        from satorip2p.integration import P2PSatoriServerClient

        mock_wallet = MagicMock()

        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            enable_central_fallback=False,
        )

        token = client.getCentrifugoToken()
        assert token == {}


class TestP2PCentrifugoClientImport:
    """Test P2PCentrifugoClient import and basic creation."""

    def test_import(self):
        """Test that P2PCentrifugoClient can be imported."""
        from satorip2p.integration import P2PCentrifugoClient
        assert P2PCentrifugoClient is not None

    def test_import_from_main(self):
        """Test import from main package."""
        from satorip2p import P2PCentrifugoClient
        assert P2PCentrifugoClient is not None

    def test_creation_basic(self):
        """Test basic creation."""
        from satorip2p.integration import P2PCentrifugoClient

        client = P2PCentrifugoClient(
            ws_url="ws://test:8000/connection/websocket",
            token="test-token",
        )

        assert client.ws_url == "ws://test:8000/connection/websocket"
        assert client.token == "test-token"
        assert client.connected is False

    def test_creation_with_callbacks(self):
        """Test creation with callbacks."""
        from satorip2p.integration import P2PCentrifugoClient

        on_connect = MagicMock()
        on_disconnect = MagicMock()

        client = P2PCentrifugoClient(
            on_connected_callback=on_connect,
            on_disconnected_callback=on_disconnect,
        )

        assert client.on_connected == on_connect
        assert client.on_disconnected == on_disconnect


class TestP2PStartupDagMixinImport:
    """Test P2PStartupDagMixin import."""

    def test_import(self):
        """Test that P2PStartupDagMixin can be imported."""
        from satorip2p.integration import P2PStartupDagMixin
        assert P2PStartupDagMixin is not None

    def test_import_from_main(self):
        """Test import from main package."""
        from satorip2p import P2PStartupDagMixin
        assert P2PStartupDagMixin is not None

    def test_has_initialize_p2p(self):
        """Test mixin has initializeP2P method."""
        from satorip2p.integration import P2PStartupDagMixin

        assert hasattr(P2PStartupDagMixin, 'initializeP2P')

    def test_has_create_p2p_server_conn(self):
        """Test mixin has createP2PServerConn method."""
        from satorip2p.integration import P2PStartupDagMixin

        assert hasattr(P2PStartupDagMixin, 'createP2PServerConn')


class TestFactoryFunction:
    """Test factory function for Centrifugo client."""

    def test_create_p2p_centrifugo_client_import(self):
        """Test factory function can be imported."""
        from satorip2p.integration.neuron import create_p2p_centrifugo_client
        assert create_p2p_centrifugo_client is not None

    @pytest.mark.timeout(10)
    def test_create_p2p_centrifugo_client(self):
        """Test factory function creates client."""
        import trio
        from satorip2p.integration.neuron import create_p2p_centrifugo_client

        async def test():
            client = await create_p2p_centrifugo_client(
                ws_url="ws://test:8000",
                token="test-token",
            )
            assert client is not None
            assert hasattr(client, 'connect')
            assert hasattr(client, 'subscribe')

        trio.run(test)


class TestMainModuleIntegrationExports:
    """Test integration exports from main module."""

    def test_all_integration_exports(self):
        """Test all integration classes are in __all__."""
        import satorip2p

        assert "P2PSatoriPubSubConn" in satorip2p.__all__
        assert "P2PSatoriServerClient" in satorip2p.__all__
        assert "P2PCentrifugoClient" in satorip2p.__all__
        assert "P2PStartupDagMixin" in satorip2p.__all__

    def test_direct_import_pubsub(self):
        """Test direct import of P2PSatoriPubSubConn."""
        from satorip2p import P2PSatoriPubSubConn

        conn = P2PSatoriPubSubConn(
            uid="test",
            payload="stream",
            threaded=False,
        )
        assert conn is not None

    def test_direct_import_server(self):
        """Test direct import of P2PSatoriServerClient."""
        from satorip2p import P2PSatoriServerClient

        mock_wallet = MagicMock()
        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            enable_central_fallback=False,
        )
        assert client is not None


class TestInterfaceCompatibility:
    """Test that interfaces match original classes."""

    def test_pubsub_interface(self):
        """Test P2PSatoriPubSubConn has same interface as original."""
        from satorip2p.integration import P2PSatoriPubSubConn

        conn = P2PSatoriPubSubConn(
            uid="test",
            payload="stream",
            threaded=False,
        )

        # Check all expected attributes/methods exist
        assert hasattr(conn, 'uid')
        assert hasattr(conn, 'url')
        assert hasattr(conn, 'router')
        assert hasattr(conn, 'payload')
        assert hasattr(conn, 'command')
        assert hasattr(conn, 'topicTime')
        assert hasattr(conn, 'listening')
        assert hasattr(conn, 'threaded')
        assert hasattr(conn, 'shouldReconnect')
        assert hasattr(conn, 'ws')
        assert hasattr(conn, 'connected')

        # Methods
        assert callable(conn.connect)
        assert callable(conn.listen)
        assert callable(conn.send)
        assert callable(conn.publish)
        assert callable(conn.disconnect)
        assert callable(conn.setRouter)
        assert callable(conn.setTopicTime)
        assert callable(conn.restart)
        assert callable(conn.reestablish)
        assert callable(conn.connectThenListen)

    def test_server_client_interface(self):
        """Test P2PSatoriServerClient has same interface as original."""
        from satorip2p.integration import P2PSatoriServerClient

        mock_wallet = MagicMock()
        client = P2PSatoriServerClient(
            wallet=mock_wallet,
            enable_central_fallback=False,
        )

        # Check key methods exist
        assert callable(client.checkin)
        assert callable(client.checkinCheck)
        assert callable(client.registerWallet)
        assert callable(client.registerStream)
        assert callable(client.registerSubscription)
        assert callable(client.publish)
        assert callable(client.getStreamsSubscribers)
        assert callable(client.getStreamsPublishers)
        assert callable(client.getCentrifugoToken)
        assert callable(client.getBalances)
        assert callable(client.stakeCheck)
        assert callable(client.setMiningMode)
        assert callable(client.setRewardAddress)
        assert callable(client.setDataManagerPort)
        assert callable(client.loopbackCheck)
        assert callable(client.getPublicIp)
        assert callable(client.poolAccepting)

    def test_centrifugo_interface(self):
        """Test P2PCentrifugoClient has expected interface."""
        from satorip2p.integration import P2PCentrifugoClient

        client = P2PCentrifugoClient()

        # Check async methods exist
        assert hasattr(client, 'connect')
        assert hasattr(client, 'disconnect')
        assert hasattr(client, 'subscribe')
        assert hasattr(client, 'publish')
        assert hasattr(client, 'connected')


class TestDropInReplacementUsage:
    """Test drop-in replacement usage patterns."""

    def test_pubsub_as_drop_in(self):
        """Test using P2PSatoriPubSubConn as drop-in for SatoriPubSubConn."""
        # This is how it would be used in Neuron:
        # from satorip2p.integration import P2PSatoriPubSubConn as SatoriPubSubConn
        from satorip2p.integration import P2PSatoriPubSubConn as SatoriPubSubConn

        messages_received = []

        def router(msg):
            messages_received.append(msg)

        # Same usage as original
        conn = SatoriPubSubConn(
            uid="wallet-address",
            payload="stream-uuid",
            url="ws://pubsub.satorinet.io:24603",  # Ignored in P2P
            router=router,
            listening=True,
            threaded=False,  # Don't start thread for test
        )

        assert conn.uid == "wallet-address"
        assert conn.router == router

    def test_server_as_drop_in(self):
        """Test using P2PSatoriServerClient as drop-in for SatoriServerClient."""
        from satorip2p.integration import P2PSatoriServerClient as SatoriServerClient

        mock_wallet = MagicMock()
        mock_wallet.address = "ETestAddress"
        mock_wallet.pubkey = "pubkey123"

        # Same usage as original
        server = SatoriServerClient(
            wallet=mock_wallet,
            url="https://central.satorinet.io",  # Used for fallback
            sendingUrl="https://mundo.satorinet.io",
            enable_central_fallback=False,  # Pure P2P for test
        )

        # Should work the same way
        details = server.checkin()
        assert 'key' in details
        assert 'subscriptions' in details


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
