"""
Tests for satorip2p.hybrid module - Hybrid Mode for Central ↔ P2P Migration.
"""

import pytest
import trio
import json
from unittest.mock import MagicMock, AsyncMock, patch


class TestHybridConfig:
    """Tests for hybrid configuration."""

    def test_import(self):
        """Test that hybrid config can be imported."""
        from satorip2p.hybrid import HybridMode, HybridConfig
        assert HybridMode is not None
        assert HybridConfig is not None

    def test_hybrid_modes(self):
        """Test HybridMode enum values."""
        from satorip2p.hybrid.config import HybridMode

        assert HybridMode.P2P_ONLY is not None
        assert HybridMode.HYBRID is not None
        assert HybridMode.CENTRAL_ONLY is not None

    def test_failover_strategies(self):
        """Test FailoverStrategy enum values."""
        from satorip2p.hybrid.config import FailoverStrategy

        assert FailoverStrategy.PREFER_P2P is not None
        assert FailoverStrategy.PREFER_CENTRAL is not None
        assert FailoverStrategy.PARALLEL is not None

    def test_default_config(self):
        """Test default HybridConfig."""
        from satorip2p.hybrid import HybridConfig, HybridMode

        config = HybridConfig()
        assert config.mode == HybridMode.HYBRID
        assert config.central.pubsub_url == "ws://pubsub.satorinet.io:24603"
        assert config.p2p.listen_port == 4001

    def test_p2p_only_config(self):
        """Test P2P-only configuration."""
        from satorip2p.hybrid import HybridConfig, HybridMode

        config = HybridConfig.p2p_only()
        assert config.mode == HybridMode.P2P_ONLY
        assert config.is_p2p_enabled() is True
        assert config.is_central_enabled() is False

    def test_central_only_config(self):
        """Test Central-only configuration."""
        from satorip2p.hybrid import HybridConfig, HybridMode

        config = HybridConfig.central_only(pubsub_url="ws://custom:1234")
        assert config.mode == HybridMode.CENTRAL_ONLY
        assert config.is_p2p_enabled() is False
        assert config.is_central_enabled() is True
        assert config.central.pubsub_url == "ws://custom:1234"

    def test_hybrid_config(self):
        """Test Hybrid configuration."""
        from satorip2p.hybrid import HybridConfig, HybridMode
        from satorip2p.hybrid.config import FailoverStrategy

        config = HybridConfig.hybrid(failover=FailoverStrategy.PARALLEL)
        assert config.mode == HybridMode.HYBRID
        assert config.failover == FailoverStrategy.PARALLEL
        assert config.is_p2p_enabled() is True
        assert config.is_central_enabled() is True
        assert config.should_bridge() is True

    def test_central_server_config(self):
        """Test CentralServerConfig defaults."""
        from satorip2p.hybrid.config import CentralServerConfig

        config = CentralServerConfig()
        assert "pubsub.satorinet.io" in config.pubsub_url
        assert "central.satorinet.io" in config.api_url
        assert config.reconnect_delay == 60.0

    def test_p2p_config(self):
        """Test P2PConfig defaults."""
        from satorip2p.hybrid.config import P2PConfig

        config = P2PConfig()
        assert config.listen_port == 4001
        assert config.enable_relay is True
        assert config.enable_dht is True


class TestCentralPubSubClient:
    """Tests for Central PubSub client."""

    def test_import(self):
        """Test that CentralPubSubClient can be imported."""
        from satorip2p.hybrid import CentralPubSubClient
        assert CentralPubSubClient is not None

    def test_client_creation(self):
        """Test client initialization."""
        from satorip2p.hybrid import CentralPubSubClient

        client = CentralPubSubClient(
            uid="test-wallet-address",
            url="ws://localhost:24603",
        )
        assert client.uid == "test-wallet-address"
        assert client.url == "ws://localhost:24603"
        assert client.connected is False

    def test_client_stats(self):
        """Test client statistics."""
        from satorip2p.hybrid import CentralPubSubClient

        client = CentralPubSubClient(uid="test-uid")
        stats = client.get_stats()

        assert "connected" in stats
        assert "url" in stats
        assert "subscriptions" in stats
        assert stats["connected"] is False

    def test_extract_stream_id_json(self):
        """Test stream ID extraction from JSON message."""
        from satorip2p.hybrid import CentralPubSubClient

        client = CentralPubSubClient(uid="test")

        # JSON with topic
        msg = '{"topic": "stream-123", "data": "test"}'
        stream_id = client._extract_stream_id(msg)
        assert stream_id == "stream-123"

    def test_extract_stream_id_command(self):
        """Test stream ID extraction from command:payload format."""
        from satorip2p.hybrid import CentralPubSubClient

        client = CentralPubSubClient(uid="test")

        # publish:JSON format
        msg = 'publish:{"topic": "stream-456", "data": "test"}'
        stream_id = client._extract_stream_id(msg)
        assert stream_id == "stream-456"


class TestMessageDeduplicator:
    """Tests for message deduplication."""

    def test_import(self):
        """Test MessageDeduplicator import."""
        from satorip2p.hybrid.bridge import MessageDeduplicator
        assert MessageDeduplicator is not None

    def test_dedup_new_message(self):
        """Test that new messages are not duplicates."""
        from satorip2p.hybrid.bridge import MessageDeduplicator

        dedup = MessageDeduplicator()
        result = dedup.is_duplicate("stream-1", {"data": "test"})
        assert result is False

    def test_dedup_duplicate_message(self):
        """Test that repeated messages are detected as duplicates."""
        from satorip2p.hybrid.bridge import MessageDeduplicator

        dedup = MessageDeduplicator()

        # First time - not duplicate
        result1 = dedup.is_duplicate("stream-1", {"data": "test"})
        assert result1 is False

        # Second time - duplicate
        result2 = dedup.is_duplicate("stream-1", {"data": "test"})
        assert result2 is True

    def test_dedup_different_streams(self):
        """Test that same data on different streams are not duplicates."""
        from satorip2p.hybrid.bridge import MessageDeduplicator

        dedup = MessageDeduplicator()

        result1 = dedup.is_duplicate("stream-1", {"data": "test"})
        result2 = dedup.is_duplicate("stream-2", {"data": "test"})

        assert result1 is False
        assert result2 is False


class TestHybridBridge:
    """Tests for hybrid bridge."""

    def test_import(self):
        """Test HybridBridge import."""
        from satorip2p.hybrid import HybridBridge
        assert HybridBridge is not None

    def test_bridge_creation(self):
        """Test bridge initialization."""
        from satorip2p.hybrid import HybridBridge, HybridConfig

        mock_p2p = MagicMock()
        config = HybridConfig()

        bridge = HybridBridge(
            config=config,
            p2p=mock_p2p,
            uid="test-wallet",
        )

        assert bridge.config == config
        assert bridge.p2p == mock_p2p
        assert bridge.uid == "test-wallet"

    def test_bridge_stats(self):
        """Test bridge statistics."""
        from satorip2p.hybrid import HybridBridge, HybridConfig, HybridMode

        mock_p2p = MagicMock()
        config = HybridConfig(mode=HybridMode.HYBRID)

        bridge = HybridBridge(config=config, p2p=mock_p2p, uid="test")
        stats = bridge.get_stats()

        assert stats["mode"] == "HYBRID"
        assert "p2p_healthy" in stats
        assert "central_healthy" in stats

    def test_bridge_repr(self):
        """Test bridge string representation."""
        from satorip2p.hybrid import HybridBridge, HybridConfig

        mock_p2p = MagicMock()
        bridge = HybridBridge(
            config=HybridConfig(),
            p2p=mock_p2p,
            uid="test",
        )

        repr_str = repr(bridge)
        assert "HybridBridge" in repr_str
        assert "HYBRID" in repr_str


class TestHybridPeers:
    """Tests for HybridPeers class."""

    def test_import(self):
        """Test HybridPeers import."""
        from satorip2p.hybrid import HybridPeers
        assert HybridPeers is not None

    def test_hybrid_peers_creation_p2p_only(self):
        """Test HybridPeers creation in P2P_ONLY mode."""
        from satorip2p.hybrid import HybridPeers, HybridMode

        peers = HybridPeers(
            identity=None,
            mode=HybridMode.P2P_ONLY,
        )

        assert peers.mode == HybridMode.P2P_ONLY
        assert peers.config.is_p2p_enabled() is True
        assert peers.config.is_central_enabled() is False

    def test_hybrid_peers_creation_central_only(self):
        """Test HybridPeers creation in CENTRAL_ONLY mode."""
        from satorip2p.hybrid import HybridPeers, HybridMode

        peers = HybridPeers(
            identity=None,
            mode=HybridMode.CENTRAL_ONLY,
            central_url="ws://custom:1234",
        )

        assert peers.mode == HybridMode.CENTRAL_ONLY
        assert peers.config.central.pubsub_url == "ws://custom:1234"

    def test_hybrid_peers_creation_hybrid(self):
        """Test HybridPeers creation in HYBRID mode."""
        from satorip2p.hybrid import HybridPeers, HybridMode

        peers = HybridPeers(
            identity=None,
            mode=HybridMode.HYBRID,
        )

        assert peers.mode == HybridMode.HYBRID
        assert peers.config.is_p2p_enabled() is True
        assert peers.config.is_central_enabled() is True

    def test_hybrid_peers_with_config(self):
        """Test HybridPeers with full config."""
        from satorip2p.hybrid import HybridPeers, HybridConfig, HybridMode
        from satorip2p.hybrid.config import FailoverStrategy

        config = HybridConfig(
            mode=HybridMode.HYBRID,
            failover=FailoverStrategy.PARALLEL,
        )
        config.p2p.listen_port = 5001

        peers = HybridPeers(identity=None, config=config)

        assert peers.config.failover == FailoverStrategy.PARALLEL
        assert peers.config.p2p.listen_port == 5001

    def test_hybrid_peers_stats(self):
        """Test HybridPeers statistics."""
        from satorip2p.hybrid import HybridPeers, HybridMode

        peers = HybridPeers(identity=None, mode=HybridMode.P2P_ONLY)
        stats = peers.get_stats()

        assert stats["mode"] == "P2P_ONLY"
        assert "started" in stats
        assert "p2p_connected" in stats

    def test_hybrid_peers_repr(self):
        """Test HybridPeers string representation."""
        from satorip2p.hybrid import HybridPeers, HybridMode

        peers = HybridPeers(identity=None, mode=HybridMode.HYBRID)
        repr_str = repr(peers)

        assert "HybridPeers" in repr_str
        assert "HYBRID" in repr_str


class TestMainModuleHybridExports:
    """Test hybrid exports from main module."""

    def test_hybrid_peers_export(self):
        """Test HybridPeers export."""
        from satorip2p import HybridPeers
        assert HybridPeers is not None

    def test_hybrid_mode_export(self):
        """Test HybridMode export."""
        from satorip2p import HybridMode
        assert HybridMode is not None

    def test_hybrid_config_export(self):
        """Test HybridConfig export."""
        from satorip2p import HybridConfig
        assert HybridConfig is not None

    def test_hybrid_bridge_export(self):
        """Test HybridBridge export."""
        from satorip2p import HybridBridge
        assert HybridBridge is not None

    def test_all_hybrid_in_all(self):
        """Test that hybrid classes are in __all__."""
        import satorip2p

        assert "HybridPeers" in satorip2p.__all__
        assert "HybridMode" in satorip2p.__all__
        assert "HybridConfig" in satorip2p.__all__
        assert "HybridBridge" in satorip2p.__all__


class TestHybridIntegration:
    """Integration tests for hybrid mode."""

    @pytest.mark.timeout(30)
    def test_hybrid_peers_with_mock_p2p(self):
        """Test HybridPeers with mocked P2P."""
        from satorip2p.hybrid import HybridPeers, HybridMode

        async def run_test():
            peers = HybridPeers(
                identity=None,
                mode=HybridMode.P2P_ONLY,
            )

            # Don't actually start (would need real P2P)
            # Just verify creation works
            assert peers.mode == HybridMode.P2P_ONLY
            assert peers._started is False

        trio.run(run_test)

    @pytest.mark.timeout(30)
    def test_central_client_subscription_queue(self):
        """Test that subscriptions are queued when not connected."""
        from satorip2p.hybrid import CentralPubSubClient

        async def run_test():
            client = CentralPubSubClient(uid="test")

            # Subscribe while disconnected
            result = await client.subscribe("stream-1")
            assert result is True

            # Should be in pending queue
            assert "stream-1" in client._pending_subscriptions

        trio.run(run_test)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
