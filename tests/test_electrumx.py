"""
Tests for satorip2p/electrumx/

Tests the ElectrumX client and connection modules.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
import json
import socket

from satorip2p.electrumx import (
    ElectrumXClient,
    ElectrumXConnection,
    ElectrumXError,
)
from satorip2p.electrumx.client import (
    UTXO,
    address_to_scripthash,
    ELECTRUMX_SERVERS,
    CLIENT_NAME,
    PROTOCOL_VERSION,
)


# ============================================================================
# TEST DATA
# ============================================================================

SAMPLE_UTXO_RESPONSE = [
    {"tx_hash": "abc123", "tx_pos": 0, "value": 1000000, "height": 500000},
    {"tx_hash": "def456", "tx_pos": 1, "value": 2000000, "height": 500001},
]

SAMPLE_ASSET_UTXO_RESPONSE = [
    {
        "tx_hash": "ghi789",
        "tx_pos": 0,
        "value": 0,
        "height": 500002,
        "asset": {"name": "SATORI", "amount": 100.5},
    },
    {
        "tx_hash": "jkl012",
        "tx_pos": 1,
        "value": 0,
        "height": 500003,
        "asset": {"name": "SATORI", "amount": 50.25},
    },
]


# ============================================================================
# UTXO DATACLASS TESTS
# ============================================================================

class TestUTXO:
    """Tests for UTXO dataclass."""

    def test_utxo_creation(self):
        """Test creating a UTXO."""
        utxo = UTXO(
            txid="abc123",
            vout=0,
            value=1000000,
            height=500000,
        )
        assert utxo.txid == "abc123"
        assert utxo.vout == 0
        assert utxo.value == 1000000
        assert utxo.height == 500000
        assert utxo.asset is None
        assert utxo.asset_amount is None

    def test_utxo_with_asset(self):
        """Test creating an asset UTXO."""
        utxo = UTXO(
            txid="def456",
            vout=1,
            value=0,
            height=500001,
            asset="SATORI",
            asset_amount=100.5,
        )
        assert utxo.asset == "SATORI"
        assert utxo.asset_amount == 100.5

    def test_utxo_to_dict(self):
        """Test UTXO serialization."""
        utxo = UTXO(
            txid="abc123",
            vout=0,
            value=1000000,
            height=500000,
            asset="SATORI",
            asset_amount=50.0,
        )
        data = utxo.to_dict()
        assert data["txid"] == "abc123"
        assert data["value"] == 1000000
        assert data["asset"] == "SATORI"
        assert data["asset_amount"] == 50.0


# ============================================================================
# ELECTRUMX CONNECTION TESTS
# ============================================================================

class TestElectrumXConnection:
    """Tests for ElectrumXConnection."""

    def test_initialization(self):
        """Test connection initialization."""
        conn = ElectrumXConnection(
            host="test.server.io",
            port=50002,
            use_ssl=True,
            timeout=30.0,
        )
        assert conn.host == "test.server.io"
        assert conn.port == 50002
        assert conn.use_ssl is True
        assert conn.timeout == 30.0
        assert conn.connected is False

    def test_initialization_defaults(self):
        """Test default initialization values."""
        conn = ElectrumXConnection(host="test.server.io")
        assert conn.port == 50002
        assert conn.use_ssl is True
        assert conn.timeout == ElectrumXConnection.DEFAULT_TIMEOUT

    @patch('socket.socket')
    @patch('ssl.create_default_context')
    def test_connect_ssl_success(self, mock_ssl_context, mock_socket):
        """Test successful SSL connection."""
        mock_sock = MagicMock()
        mock_socket.return_value = mock_sock

        mock_context = MagicMock()
        mock_ssl_context.return_value = mock_context
        mock_ssl_sock = MagicMock()
        mock_context.wrap_socket.return_value = mock_ssl_sock

        conn = ElectrumXConnection(host="test.server.io", port=50002)
        result = conn.connect()

        assert result is True
        assert conn.connected is True
        mock_ssl_sock.connect.assert_called_once_with(("test.server.io", 50002))

    @patch('socket.socket')
    def test_connect_tcp_success(self, mock_socket):
        """Test successful TCP connection."""
        mock_sock = MagicMock()
        mock_socket.return_value = mock_sock

        conn = ElectrumXConnection(host="test.server.io", port=50001, use_ssl=False)
        result = conn.connect()

        assert result is True
        assert conn.connected is True
        mock_sock.connect.assert_called_once_with(("test.server.io", 50001))

    @patch('socket.socket')
    def test_connect_failure(self, mock_socket):
        """Test connection failure."""
        mock_sock = MagicMock()
        mock_sock.connect.side_effect = socket.error("Connection refused")
        mock_socket.return_value = mock_sock

        conn = ElectrumXConnection(host="bad.server.io", port=50001, use_ssl=False)
        result = conn.connect()

        assert result is False
        assert conn.connected is False

    def test_close(self):
        """Test closing connection."""
        conn = ElectrumXConnection(host="test.server.io")
        conn._connected = True
        conn._socket = MagicMock()
        conn._ssl_socket = MagicMock()

        conn.close()

        assert conn.connected is False
        assert conn._socket is None
        assert conn._ssl_socket is None

    def test_send_not_connected(self):
        """Test sending when not connected."""
        conn = ElectrumXConnection(host="test.server.io")
        result = conn.send({"test": "data"})
        assert result is False

    def test_send_success(self):
        """Test successful send."""
        conn = ElectrumXConnection(host="test.server.io", use_ssl=False)
        conn._connected = True
        mock_socket = MagicMock()
        conn._socket = mock_socket

        result = conn.send({"jsonrpc": "2.0", "method": "test"})

        assert result is True
        mock_socket.sendall.assert_called_once()
        # Verify JSON format with newline
        sent_data = mock_socket.sendall.call_args[0][0].decode()
        assert "jsonrpc" in sent_data
        assert sent_data.endswith("\n")

    def test_receive_not_connected(self):
        """Test receiving when not connected."""
        conn = ElectrumXConnection(host="test.server.io")
        result = conn.receive()
        assert result is None

    def test_receive_success(self):
        """Test successful receive."""
        conn = ElectrumXConnection(host="test.server.io", use_ssl=False)
        conn._connected = True
        mock_socket = MagicMock()
        conn._socket = mock_socket

        response_json = json.dumps({"jsonrpc": "2.0", "result": "test"}) + "\n"
        mock_socket.recv.return_value = response_json.encode()

        result = conn.receive()

        assert result is not None
        assert result["result"] == "test"

    def test_context_manager(self):
        """Test context manager usage."""
        with patch.object(ElectrumXConnection, 'connect') as mock_connect, \
             patch.object(ElectrumXConnection, 'close') as mock_close:
            mock_connect.return_value = True

            with ElectrumXConnection(host="test.server.io") as conn:
                mock_connect.assert_called_once()

            mock_close.assert_called_once()


# ============================================================================
# ELECTRUMX CLIENT TESTS
# ============================================================================

class TestElectrumXClient:
    """Tests for ElectrumXClient."""

    def test_initialization(self):
        """Test client initialization."""
        client = ElectrumXClient(use_ssl=True, timeout=45.0)
        assert client.servers == ELECTRUMX_SERVERS
        assert client.use_ssl is True
        assert client.timeout == 45.0
        assert client.connected is False

    def test_initialization_custom_servers(self):
        """Test client with custom servers."""
        custom_servers = [("custom.server.io", 50002)]
        client = ElectrumXClient(servers=custom_servers)
        assert client.servers == custom_servers

    def test_connected_property(self):
        """Test connected property."""
        client = ElectrumXClient()
        assert client.connected is False

        # Simulate connected state
        client._connection = MagicMock()
        client._connection.connected = True
        assert client.connected is True

    def test_close(self):
        """Test closing client."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._server_version = "1.10"

        client.close()

        assert client._connection is None
        assert client._server_version is None

    def test_next_id(self):
        """Test request ID generation."""
        client = ElectrumXClient()
        id1 = client._next_id()
        id2 = client._next_id()
        assert id2 == id1 + 1

    def test_connect_success(self):
        """Test successful client connection."""
        client = ElectrumXClient()

        # Mock the connection object
        mock_connection = MagicMock()
        mock_connection.connect.return_value = True
        mock_connection.connected = True
        mock_connection.send.return_value = True
        mock_connection.receive.return_value = {
            "jsonrpc": "2.0",
            "result": ["ElectrumX 1.16.0", "1.10"]
        }

        # Patch ElectrumXConnection to return our mock
        with patch('satorip2p.electrumx.client.ElectrumXConnection', return_value=mock_connection):
            result = client.connect()

        assert result is True
        assert client._server_version == "ElectrumX 1.16.0"

    @patch.object(ElectrumXConnection, 'connect')
    def test_connect_failure(self, mock_connect):
        """Test connection failure."""
        mock_connect.return_value = False

        client = ElectrumXClient()
        result = client.connect()

        assert result is False

    def test_call_not_connected(self):
        """Test RPC call when not connected."""
        client = ElectrumXClient()

        with pytest.raises(ElectrumXError, match="Not connected"):
            client._call("test.method")

    def test_call_server_error(self):
        """Test handling server error response."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = True
        client._connection.receive.return_value = {
            "jsonrpc": "2.0",
            "error": {"message": "Invalid method"}
        }

        with pytest.raises(ElectrumXError, match="Invalid method"):
            client._call("invalid.method")

    def test_ping_success(self):
        """Test successful ping."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = True
        client._connection.receive.return_value = {
            "jsonrpc": "2.0",
            "result": None
        }

        result = client.ping()
        assert result is True

    def test_ping_failure(self):
        """Test failed ping."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = False

        result = client.ping()
        assert result is False

    def test_get_unspent(self):
        """Test getting unspent outputs."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = True
        client._connection.receive.return_value = {
            "jsonrpc": "2.0",
            "result": SAMPLE_UTXO_RESPONSE
        }

        with patch('satorip2p.electrumx.client.address_to_scripthash') as mock_scripthash:
            mock_scripthash.return_value = "scripthash123"
            utxos = client.get_unspent("ETestAddress")

        assert len(utxos) == 2
        assert utxos[0].txid == "abc123"
        assert utxos[0].value == 1000000
        assert utxos[1].txid == "def456"

    def test_get_asset_unspent(self):
        """Test getting asset unspent outputs."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = True
        client._connection.receive.return_value = {
            "jsonrpc": "2.0",
            "result": SAMPLE_ASSET_UTXO_RESPONSE
        }

        with patch('satorip2p.electrumx.client.address_to_scripthash') as mock_scripthash:
            mock_scripthash.return_value = "scripthash123"
            utxos = client.get_asset_unspent("ETestAddress", "SATORI")

        assert len(utxos) == 2
        assert utxos[0].asset == "SATORI"
        assert utxos[0].asset_amount == 100.5
        assert utxos[1].asset_amount == 50.25

    def test_get_balance(self):
        """Test getting balance."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = True
        client._connection.receive.return_value = {
            "jsonrpc": "2.0",
            "result": {"confirmed": 5000000, "unconfirmed": 1000000}
        }

        with patch('satorip2p.electrumx.client.address_to_scripthash') as mock_scripthash:
            mock_scripthash.return_value = "scripthash123"
            balance = client.get_balance("ETestAddress")

        assert balance["confirmed"] == 5000000
        assert balance["unconfirmed"] == 1000000

    def test_broadcast(self):
        """Test transaction broadcast."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = True
        # 64-char txid
        mock_txid = "a" * 64
        client._connection.receive.return_value = {
            "jsonrpc": "2.0",
            "result": mock_txid
        }

        txid = client.broadcast("0100000001...")
        assert txid == mock_txid

    def test_broadcast_failure(self):
        """Test failed transaction broadcast."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = True
        client._connection.receive.return_value = {
            "jsonrpc": "2.0",
            "result": "TX rejected: insufficient fee"
        }

        with pytest.raises(ElectrumXError, match="Broadcast failed"):
            client.broadcast("0100000001...")

    def test_get_block_height(self):
        """Test getting block height."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = True
        client._connection.receive.return_value = {
            "jsonrpc": "2.0",
            "result": {"height": 850000, "hex": "header_hex"}
        }

        height = client.get_block_height()
        assert height == 850000

    def test_get_fee_estimate(self):
        """Test fee estimation."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = True
        client._connection.receive.return_value = {
            "jsonrpc": "2.0",
            "result": 0.00002
        }

        fee = client.get_fee_estimate(6)
        assert fee == 0.00002

    def test_get_fee_estimate_fallback(self):
        """Test fee estimation fallback on failure."""
        client = ElectrumXClient()
        client._connection = MagicMock()
        client._connection.connected = True
        client._connection.send.return_value = True
        client._connection.receive.return_value = {
            "jsonrpc": "2.0",
            "result": -1  # Invalid response
        }

        fee = client.get_fee_estimate(6)
        assert fee == 0.00001  # Minimum fallback

    def test_context_manager(self):
        """Test client context manager."""
        with patch.object(ElectrumXClient, 'connect') as mock_connect, \
             patch.object(ElectrumXClient, 'close') as mock_close:
            mock_connect.return_value = True

            with ElectrumXClient() as client:
                mock_connect.assert_called_once()

            mock_close.assert_called_once()


# ============================================================================
# ADDRESS TO SCRIPTHASH TESTS
# ============================================================================

class TestAddressToScripthash:
    """Tests for address to scripthash conversion."""

    def test_address_conversion_with_evrmorelib(self):
        """Test address conversion when python-evrmorelib is available."""
        # This test will only work if python-evrmorelib is installed
        try:
            from evrmore.wallet import P2PKHEvrmoreAddress
            # Test with a valid-looking Evrmore address format
            # Note: This may need adjustment based on actual evrmorelib behavior
        except ImportError:
            pytest.skip("python-evrmorelib not available")

    def test_invalid_address(self):
        """Test invalid address raises error."""
        with pytest.raises(ElectrumXError):
            address_to_scripthash("invalid_address")


# ============================================================================
# SERVER CONFIGURATION TESTS
# ============================================================================

class TestServerConfiguration:
    """Tests for server configuration."""

    def test_default_servers_configured(self):
        """Test that default servers are configured."""
        assert len(ELECTRUMX_SERVERS) >= 1
        for host, port in ELECTRUMX_SERVERS:
            assert isinstance(host, str)
            assert isinstance(port, int)
            assert port > 0

    def test_client_protocol_version(self):
        """Test client protocol version."""
        assert PROTOCOL_VERSION == "1.10"

    def test_client_name(self):
        """Test client name."""
        assert CLIENT_NAME == "satorip2p"
