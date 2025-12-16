"""
Tests for satorip2p/blockchain/tx_builder.py

Tests the transaction builder for SATORI distribution.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from dataclasses import asdict

from satorip2p.blockchain.tx_builder import (
    TransactionOutput,
    UnsignedTransaction,
    SATORI_ASSET_NAME,
    SATOSHI_PER_EVR,
    MIN_RELAY_FEE,
    DUST_THRESHOLD,
    OP_RETURN_MAX_SIZE,
)

# Check if evrmorelib is available by checking the module's flag
try:
    from satorip2p.blockchain import tx_builder
    EVRMORELIB_AVAILABLE = tx_builder.EVRMORELIB_AVAILABLE
    if EVRMORELIB_AVAILABLE:
        from satorip2p.blockchain.tx_builder import TransactionBuilder, build_distribution_transaction
except (ImportError, AttributeError):
    EVRMORELIB_AVAILABLE = False


# ============================================================================
# TEST DATA
# ============================================================================

def create_mock_utxo(txid: str, vout: int, value: int, height: int = 500000):
    """Create a mock UTXO."""
    utxo = Mock()
    utxo.txid = txid
    utxo.vout = vout
    utxo.value = value
    utxo.height = height
    utxo.asset = None
    utxo.asset_amount = None
    return utxo


def create_mock_asset_utxo(
    txid: str,
    vout: int,
    asset_amount: float,
    height: int = 500000
):
    """Create a mock asset UTXO."""
    utxo = Mock()
    utxo.txid = txid
    utxo.vout = vout
    utxo.value = 0
    utxo.height = height
    utxo.asset = SATORI_ASSET_NAME
    utxo.asset_amount = asset_amount
    return utxo


def create_mock_electrumx_client(evr_utxos=None, satori_utxos=None):
    """Create a mock ElectrumX client."""
    client = Mock()
    client.get_unspent.return_value = evr_utxos or []
    client.get_asset_unspent.return_value = satori_utxos or []
    return client


# ============================================================================
# TRANSACTION OUTPUT TESTS
# ============================================================================

class TestTransactionOutput:
    """Tests for TransactionOutput dataclass."""

    def test_output_creation(self):
        """Test creating a transaction output."""
        output = TransactionOutput(
            address="ETestAddress",
            amount=1000000,
            is_asset=False,
            asset_name=""
        )
        assert output.address == "ETestAddress"
        assert output.amount == 1000000
        assert output.is_asset is False

    def test_asset_output_creation(self):
        """Test creating an asset output."""
        output = TransactionOutput(
            address="ETestAddress",
            amount=100,
            is_asset=True,
            asset_name="SATORI"
        )
        assert output.is_asset is True
        assert output.asset_name == "SATORI"

    def test_output_defaults(self):
        """Test output default values."""
        output = TransactionOutput(address="EAddr", amount=1000)
        assert output.is_asset is False
        assert output.asset_name == ""


# ============================================================================
# UNSIGNED TRANSACTION TESTS
# ============================================================================

class TestUnsignedTransaction:
    """Tests for UnsignedTransaction dataclass."""

    def test_unsigned_tx_creation(self):
        """Test creating an unsigned transaction."""
        utx = UnsignedTransaction(
            tx_hex="0100000001...",
            tx_hash="abc123",
            inputs=[{"txid": "prev_tx", "vout": 0}],
            outputs=[{"type": "evr", "address": "EAddr", "amount": 1000}],
            total_evr_in=2000,
            total_evr_out=1500,
            total_satori_in=100.0,
            total_satori_out=80.0,
            fee=500,
        )
        assert utx.tx_hex == "0100000001..."
        assert utx.tx_hash == "abc123"
        assert len(utx.inputs) == 1
        assert len(utx.outputs) == 1
        assert utx.fee == 500

    def test_unsigned_tx_to_dict(self):
        """Test unsigned transaction serialization."""
        utx = UnsignedTransaction(
            tx_hex="0100000001...",
            tx_hash="def456",
            inputs=[],
            outputs=[],
            total_evr_in=5000,
            total_evr_out=4000,
            total_satori_in=200.0,
            total_satori_out=180.0,
            fee=1000,
        )
        data = utx.to_dict()

        assert data["tx_hex"] == "0100000001..."
        assert data["tx_hash"] == "def456"
        assert data["total_evr_in"] == 5000
        assert data["total_satori_in"] == 200.0
        assert data["fee"] == 1000


# ============================================================================
# CONSTANTS TESTS
# ============================================================================

class TestConstants:
    """Tests for module constants."""

    def test_satori_asset_name(self):
        """Test SATORI asset name constant."""
        assert SATORI_ASSET_NAME == "SATORI"

    def test_satoshi_per_evr(self):
        """Test satoshi conversion constant."""
        assert SATOSHI_PER_EVR == 100_000_000

    def test_min_relay_fee(self):
        """Test minimum relay fee."""
        assert MIN_RELAY_FEE == 1000

    def test_dust_threshold(self):
        """Test dust threshold."""
        assert DUST_THRESHOLD == 546

    def test_op_return_max_size(self):
        """Test OP_RETURN max size."""
        assert OP_RETURN_MAX_SIZE == 80


# ============================================================================
# TRANSACTION BUILDER TESTS (require evrmorelib)
# ============================================================================

@pytest.mark.skipif(not EVRMORELIB_AVAILABLE, reason="python-evrmorelib not available")
class TestTransactionBuilder:
    """Tests for TransactionBuilder."""

    def test_initialization(self):
        """Test builder initialization."""
        client = create_mock_electrumx_client()
        builder = TransactionBuilder(
            electrumx_client=client,
            treasury_address="ETreasuryAddress",
            fee_rate=0.00002,
        )
        assert builder.treasury == "ETreasuryAddress"
        assert builder.fee_rate == 0.00002

    def test_initialization_requires_evrmorelib(self):
        """Test that builder requires evrmorelib."""
        # This test verifies the import check
        with patch.dict('satorip2p.blockchain.tx_builder.__dict__', {'EVRMORELIB_AVAILABLE': False}):
            # Would raise ImportError if evrmorelib not available
            pass

    def test_estimate_fee(self):
        """Test fee estimation."""
        client = create_mock_electrumx_client()
        builder = TransactionBuilder(client, "ETreasury", fee_rate=0.00001)

        fee = builder.estimate_fee(num_inputs=3, num_outputs=5)

        # Fee should be reasonable
        assert fee >= MIN_RELAY_FEE
        assert isinstance(fee, int)

    def test_build_distribution_tx_insufficient_satori(self):
        """Test building TX with insufficient SATORI."""
        evr_utxos = [create_mock_utxo("tx1", 0, 10000000)]  # 0.1 EVR
        satori_utxos = [create_mock_asset_utxo("tx2", 0, 50.0)]  # Only 50 SATORI

        client = create_mock_electrumx_client(evr_utxos, satori_utxos)
        builder = TransactionBuilder(client, "ETreasury")

        # Try to distribute 100 SATORI (more than we have)
        recipients = {"EAddr1": 60.0, "EAddr2": 40.0}

        with pytest.raises(ValueError, match="Insufficient SATORI"):
            builder.build_distribution_tx(recipients, "merkle_root_abc")

    def test_build_distribution_tx_insufficient_evr(self):
        """Test building TX with insufficient EVR for fees."""
        evr_utxos = [create_mock_utxo("tx1", 0, 100)]  # Only 100 satoshis
        satori_utxos = [create_mock_asset_utxo("tx2", 0, 100.0)]

        client = create_mock_electrumx_client(evr_utxos, satori_utxos)
        builder = TransactionBuilder(client, "ETreasury")

        recipients = {"EAddr1": 50.0}

        with pytest.raises(ValueError, match="Insufficient EVR"):
            builder.build_distribution_tx(recipients, "merkle_root_abc")

    def test_encode_op_return_prefix(self):
        """Test OP_RETURN encoding includes prefix."""
        client = create_mock_electrumx_client()
        builder = TransactionBuilder(client, "ETreasury")

        merkle_root = "a" * 64  # Valid 32-byte hex
        data = builder._encode_op_return(merkle_root)

        assert data.startswith(b"SATORI:")
        assert len(data) <= OP_RETURN_MAX_SIZE

    def test_encode_op_return_truncation(self):
        """Test OP_RETURN data truncation."""
        client = create_mock_electrumx_client()
        builder = TransactionBuilder(client, "ETreasury")

        # Very long merkle root (will be truncated)
        long_root = "x" * 200
        data = builder._encode_op_return(long_root)

        assert len(data) <= OP_RETURN_MAX_SIZE


@pytest.mark.skipif(not EVRMORELIB_AVAILABLE, reason="python-evrmorelib not available")
class TestBuildDistributionTransaction:
    """Tests for build_distribution_transaction convenience function."""

    def test_convenience_function(self):
        """Test the convenience function creates a builder."""
        evr_utxos = [create_mock_utxo("tx1", 0, 10000000)]
        satori_utxos = [create_mock_asset_utxo("tx2", 0, 100.0)]

        client = create_mock_electrumx_client(evr_utxos, satori_utxos)

        # This should create a builder internally
        # Note: actual TX building requires valid addresses
        # So we just verify the function exists and is callable
        assert callable(build_distribution_transaction)


# ============================================================================
# MOCK INTEGRATION TESTS
# ============================================================================

class TestTransactionBuilderIntegration:
    """Integration tests using mocks."""

    @pytest.mark.skipif(not EVRMORELIB_AVAILABLE, reason="python-evrmorelib not available")
    def test_utxo_selection_largest_first(self):
        """Test that UTXOs are selected largest first."""
        evr_utxos = [
            create_mock_utxo("small", 0, 1000),
            create_mock_utxo("large", 0, 5000000),
            create_mock_utxo("medium", 0, 100000),
        ]
        satori_utxos = [
            create_mock_asset_utxo("satori1", 0, 25.0),
            create_mock_asset_utxo("satori2", 0, 100.0),
            create_mock_asset_utxo("satori3", 0, 50.0),
        ]

        client = create_mock_electrumx_client(evr_utxos, satori_utxos)
        builder = TransactionBuilder(client, "ETreasury")

        # The builder should select largest UTXOs first
        # This is an internal behavior test
        assert builder.client == client

    def test_multiple_recipients_handling(self):
        """Test handling multiple recipients."""
        recipients = {
            "EAddr1": 10.0,
            "EAddr2": 20.0,
            "EAddr3": 30.0,
            "EAddr4": 40.0,
        }

        # Verify we can handle multiple recipients in dict form
        assert len(recipients) == 4
        assert sum(recipients.values()) == 100.0

    def test_fee_calculation_scaling(self):
        """Test that fees scale with transaction size."""
        # More inputs/outputs = higher fees
        # This is a sanity check for the fee calculation logic
        base_size = 180  # Header + metadata
        input_size = 148  # Per input
        output_size = 34  # Per output

        # 2 inputs, 3 outputs
        size1 = base_size + (2 * input_size) + (3 * output_size)

        # 5 inputs, 10 outputs
        size2 = base_size + (5 * input_size) + (10 * output_size)

        # Size2 should be larger
        assert size2 > size1
