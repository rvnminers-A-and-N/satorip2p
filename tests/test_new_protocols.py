"""
satorip2p/tests/test_new_protocols.py

Unit tests for new protocol features: Ping, AutoNAT, Identify, QUIC, WebSocket.
"""

import pytest
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


class TestPeersNewParams:
    """Test new Peers parameters."""

    def test_init_with_new_protocol_params(self, mock_identity):
        """Test initialization with new protocol parameters."""
        from satorip2p.peers import Peers

        peers = Peers(
            identity=mock_identity,
            enable_ping=True,
            enable_autonat=True,
            enable_identify=True,
        )

        assert peers.enable_ping is True
        assert peers.enable_autonat is True
        assert peers.enable_identify is True

    def test_init_disable_new_protocols(self, mock_identity):
        """Test disabling new protocols."""
        from satorip2p.peers import Peers

        peers = Peers(
            identity=mock_identity,
            enable_ping=False,
            enable_autonat=False,
            enable_identify=False,
        )

        assert peers.enable_ping is False
        assert peers.enable_autonat is False
        assert peers.enable_identify is False

    def test_init_with_transport_params(self, mock_identity):
        """Test initialization with transport parameters."""
        from satorip2p.peers import Peers

        peers = Peers(
            identity=mock_identity,
            enable_quic=True,
            enable_websocket=True,
        )

        assert peers.enable_quic is True
        assert peers.enable_websocket is True

    def test_init_default_transport_params(self, mock_identity):
        """Test default transport parameters (disabled)."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert peers.enable_quic is False
        assert peers.enable_websocket is False


class TestPeersNewMethods:
    """Test new Peers methods."""

    def test_ping_peer_method_exists(self, mock_identity):
        """Test that ping_peer method exists."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert hasattr(peers, 'ping_peer')
        assert callable(getattr(peers, 'ping_peer'))

    def test_check_reachability_method_exists(self, mock_identity):
        """Test that check_reachability method exists."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert hasattr(peers, 'check_reachability')
        assert callable(getattr(peers, 'check_reachability'))

    def test_get_public_addrs_autonat_method_exists(self, mock_identity):
        """Test that get_public_addrs_autonat method exists."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert hasattr(peers, 'get_public_addrs_autonat')
        assert callable(getattr(peers, 'get_public_addrs_autonat'))

    def test_is_address_public_method_exists(self, mock_identity):
        """Test that is_address_public method exists."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert hasattr(peers, 'is_address_public')
        assert callable(getattr(peers, 'is_address_public'))


class TestPeersNewInitMethods:
    """Test new initialization methods."""

    def test_init_ping_method_exists(self, mock_identity):
        """Test that _init_ping method exists."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert hasattr(peers, '_init_ping')
        assert callable(getattr(peers, '_init_ping'))

    def test_init_autonat_method_exists(self, mock_identity):
        """Test that _init_autonat method exists."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert hasattr(peers, '_init_autonat')
        assert callable(getattr(peers, '_init_autonat'))

    def test_init_identify_method_exists(self, mock_identity):
        """Test that _init_identify method exists."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert hasattr(peers, '_init_identify')
        assert callable(getattr(peers, '_init_identify'))


class TestPeersNewInstanceVars:
    """Test new instance variables."""

    def test_ping_service_initialized(self, mock_identity):
        """Test _ping_service is initialized to None."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert hasattr(peers, '_ping_service')
        assert peers._ping_service is None

    def test_reachability_checker_initialized(self, mock_identity):
        """Test _reachability_checker is initialized to None."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert hasattr(peers, '_reachability_checker')
        assert peers._reachability_checker is None

    def test_identify_handler_initialized(self, mock_identity):
        """Test _identify_handler is initialized to None."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        assert hasattr(peers, '_identify_handler')
        assert peers._identify_handler is None


class TestPeersPingMethod:
    """Test ping_peer method behavior."""

    async def test_ping_peer_no_service(self, mock_identity):
        """Test ping_peer returns None when service not initialized."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)
        peers._ping_service = None

        result = await peers.ping_peer("test-peer-id")

        assert result is None


class TestPeersReachabilityMethod:
    """Test check_reachability method behavior."""

    async def test_check_reachability_no_checker(self, mock_identity):
        """Test check_reachability returns False when checker not initialized."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)
        peers._reachability_checker = None

        result = await peers.check_reachability()

        assert result is False


class TestPeersPublicAddrsMethod:
    """Test get_public_addrs_autonat method behavior."""

    async def test_get_public_addrs_no_checker(self, mock_identity):
        """Test get_public_addrs_autonat returns empty list when checker not initialized."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)
        peers._reachability_checker = None

        result = await peers.get_public_addrs_autonat()

        assert result == []


class TestPeersAddressPublicMethod:
    """Test is_address_public method behavior."""

    def test_is_address_public_private_ip(self, mock_identity):
        """Test is_address_public returns False for private IPs."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        # Test private ranges
        assert peers.is_address_public("/ip4/10.0.0.1/tcp/4001") is False
        assert peers.is_address_public("/ip4/172.16.0.1/tcp/4001") is False
        assert peers.is_address_public("/ip4/192.168.1.1/tcp/4001") is False
        assert peers.is_address_public("/ip4/127.0.0.1/tcp/4001") is False

    def test_is_address_public_public_ip(self, mock_identity):
        """Test is_address_public returns True for public IPs."""
        from satorip2p.peers import Peers

        peers = Peers(identity=mock_identity)

        # Test public IPs
        assert peers.is_address_public("/ip4/8.8.8.8/tcp/4001") is True
        assert peers.is_address_public("/ip4/1.1.1.1/tcp/4001") is True


class TestPeersAllParams:
    """Test Peers with all parameters."""

    def test_init_all_params(self, mock_identity):
        """Test initialization with all parameters."""
        from satorip2p.peers import Peers

        peers = Peers(
            identity=mock_identity,
            listen_port=25000,
            bootstrap_peers=["/ip4/1.2.3.4/tcp/4001/p2p/QmTest"],
            enable_upnp=False,
            enable_relay=True,
            enable_dht=True,
            enable_pubsub=True,
            enable_rendezvous=True,
            enable_mdns=True,
            enable_ping=True,
            enable_autonat=True,
            enable_identify=True,
            enable_quic=True,
            enable_websocket=True,
            rendezvous_is_server=True,
        )

        assert peers.listen_port == 25000
        assert peers.enable_upnp is False
        assert peers.enable_relay is True
        assert peers.enable_dht is True
        assert peers.enable_pubsub is True
        assert peers.enable_rendezvous is True
        assert peers.enable_mdns is True
        assert peers.enable_ping is True
        assert peers.enable_autonat is True
        assert peers.enable_identify is True
        assert peers.enable_quic is True
        assert peers.enable_websocket is True
        assert peers.rendezvous_is_server is True


class TestPeersDocstring:
    """Test Peers class has proper documentation."""

    def test_ping_peer_has_docstring(self, mock_identity):
        """Test ping_peer method has docstring."""
        from satorip2p.peers import Peers

        assert Peers.ping_peer.__doc__ is not None
        assert "Ping" in Peers.ping_peer.__doc__

    def test_check_reachability_has_docstring(self, mock_identity):
        """Test check_reachability method has docstring."""
        from satorip2p.peers import Peers

        assert Peers.check_reachability.__doc__ is not None
        assert "reachable" in Peers.check_reachability.__doc__.lower()
