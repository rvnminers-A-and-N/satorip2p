"""
Tests for satorip2p/protocol/signer.py

Tests the signer node for multi-sig reward distribution.
"""

import pytest
import time
from unittest.mock import Mock, MagicMock

from satorip2p.protocol.signer import (
    SignerNode,
    SignatureRequest,
    SignatureResponse,
    SigningResult,
    SigningPhase,
    MULTISIG_THRESHOLD,
    MULTISIG_TOTAL_SIGNERS,
    AUTHORIZED_SIGNERS,
    is_authorized_signer,
    get_signer_count,
    verify_combined_signature,
)


# ============================================================================
# TEST DATA
# ============================================================================

def create_mock_consensus_result(
    round_id: str = "2025-01-15",
    consensus_reached: bool = True,
    quorum_met: bool = True,
    merkle_root: str = "abc123def456",
) -> Mock:
    """Create a mock ConsensusResult."""
    result = Mock()
    result.round_id = round_id
    result.consensus_reached = consensus_reached
    result.quorum_met = quorum_met
    result.winning_merkle_root = merkle_root
    return result


def create_test_request(
    round_id: str = "2025-01-15",
    merkle_root: str = "abc123def456",
    distribution_tx_hash: str = "tx_hash_123",
    total_reward: float = 1000.0,
    num_recipients: int = 10,
) -> SignatureRequest:
    """Create a test signature request."""
    return SignatureRequest(
        round_id=round_id,
        merkle_root=merkle_root,
        distribution_tx_hash=distribution_tx_hash,
        total_reward=total_reward,
        num_recipients=num_recipients,
        timestamp=int(time.time()),
        requester_id="requester_node",
    )


# ============================================================================
# SIGNATURE REQUEST TESTS
# ============================================================================

class TestSignatureRequest:
    """Tests for SignatureRequest dataclass."""

    def test_request_creation(self):
        """Test creating a request."""
        request = create_test_request()
        assert request.round_id == "2025-01-15"
        assert request.merkle_root == "abc123def456"
        assert request.total_reward == 1000.0

    def test_request_to_dict(self):
        """Test request serialization."""
        request = create_test_request()
        data = request.to_dict()
        assert data['round_id'] == "2025-01-15"
        assert data['merkle_root'] == "abc123def456"

    def test_request_from_dict(self):
        """Test request deserialization."""
        data = {
            'round_id': '2025-01-16',
            'merkle_root': 'xyz789',
            'distribution_tx_hash': 'tx_456',
            'total_reward': 500.0,
            'num_recipients': 5,
            'timestamp': 1234567890,
            'requester_id': 'node123',
        }
        request = SignatureRequest.from_dict(data)
        assert request.round_id == '2025-01-16'
        assert request.total_reward == 500.0

    def test_signing_hash_deterministic(self):
        """Test that signing hash is deterministic."""
        request1 = create_test_request()
        request2 = create_test_request()
        assert request1.get_signing_hash() == request2.get_signing_hash()


# ============================================================================
# SIGNATURE RESPONSE TESTS
# ============================================================================

class TestSignatureResponse:
    """Tests for SignatureResponse dataclass."""

    def test_response_creation(self):
        """Test creating a response."""
        response = SignatureResponse(
            round_id="2025-01-15",
            signer_address="signer_addr_1",
            signature=b"test_signature",
            merkle_root="abc123",
            timestamp=int(time.time()),
        )
        assert response.round_id == "2025-01-15"
        assert response.signature == b"test_signature"

    def test_response_to_dict(self):
        """Test response serialization."""
        response = SignatureResponse(
            round_id="2025-01-15",
            signer_address="signer_addr_1",
            signature=b"test",
            merkle_root="abc123",
            timestamp=1234567890,
        )
        data = response.to_dict()
        assert data['signature'] == "74657374"  # "test" in hex

    def test_response_from_dict(self):
        """Test response deserialization."""
        data = {
            'round_id': '2025-01-15',
            'signer_address': 'addr_2',
            'signature': 'aabbccdd',
            'merkle_root': 'xyz',
            'timestamp': 1234567890,
        }
        response = SignatureResponse.from_dict(data)
        assert response.signer_address == 'addr_2'
        assert response.signature == bytes.fromhex('aabbccdd')


# ============================================================================
# SIGNER NODE BASIC TESTS
# ============================================================================

class TestSignerNodeBasic:
    """Basic tests for SignerNode."""

    def test_initialization_not_signer(self):
        """Test initialization as non-signer."""
        node = SignerNode(
            signer_address="some_address",
            is_authorized_signer=False,
        )
        assert node.is_authorized_signer is False
        assert node.get_current_phase() == SigningPhase.WAITING

    def test_initialization_as_signer(self):
        """Test initialization as authorized signer."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"test_private_key",
            is_authorized_signer=True,
        )
        assert node.is_authorized_signer is True
        assert node.signer_address == AUTHORIZED_SIGNERS[0]

    def test_start_signing_round_no_consensus(self):
        """Test starting a round without consensus."""
        node = SignerNode(signer_address="addr", is_authorized_signer=True)
        consensus = create_mock_consensus_result(consensus_reached=False)

        result = node.start_signing_round(
            round_id="2025-01-15",
            consensus_result=consensus,
            distribution_tx_hash="tx_hash",
            total_reward=1000.0,
            num_recipients=10,
        )

        assert result is None

    def test_start_signing_round_with_consensus(self):
        """Test starting a round with consensus."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"test_key",
            is_authorized_signer=True,
        )
        consensus = create_mock_consensus_result()

        request = node.start_signing_round(
            round_id="2025-01-15",
            consensus_result=consensus,
            distribution_tx_hash="tx_hash",
            total_reward=1000.0,
            num_recipients=10,
        )

        assert request is not None
        assert request.round_id == "2025-01-15"
        assert node.get_current_phase() == SigningPhase.COLLECTING


# ============================================================================
# SIGNATURE PROCESSING TESTS
# ============================================================================

class TestSignatureProcessing:
    """Tests for signature processing."""

    def test_non_signer_ignores_request(self):
        """Test that non-signers ignore signature requests."""
        node = SignerNode(
            signer_address="random_addr",
            is_authorized_signer=False,
        )
        request = create_test_request()

        result = node.process_signature_request(request)
        assert result is False

    def test_signer_processes_request(self):
        """Test that authorized signers process requests."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"test_key",
            is_authorized_signer=True,
        )
        # Start a round first
        consensus = create_mock_consensus_result()
        node.start_signing_round("2025-01-15", consensus, "tx", 1000, 10)

        request = create_test_request()
        result = node.process_signature_request(request)

        assert result is True
        # Should have our own signature stored
        assert AUTHORIZED_SIGNERS[0] in node.get_signatures()

    def test_receive_signature_unauthorized(self):
        """Test rejecting signatures from unauthorized signers."""
        node = SignerNode(signer_address="addr", is_authorized_signer=True)
        consensus = create_mock_consensus_result()
        node.start_signing_round("2025-01-15", consensus, "tx", 1000, 10)

        response = SignatureResponse(
            round_id="2025-01-15",
            signer_address="unauthorized_signer",
            signature=b"fake_sig",
            merkle_root="abc123def456",
            timestamp=int(time.time()),
        )

        result = node.receive_signature(response)
        assert result is False

    def test_receive_signature_authorized(self):
        """Test accepting signatures from authorized signers."""
        node = SignerNode(signer_address="addr", is_authorized_signer=True)
        consensus = create_mock_consensus_result()
        node.start_signing_round("2025-01-15", consensus, "tx", 1000, 10)

        response = SignatureResponse(
            round_id="2025-01-15",
            signer_address=AUTHORIZED_SIGNERS[1],
            signature=b"real_sig",
            merkle_root="abc123def456",
            timestamp=int(time.time()),
        )

        result = node.receive_signature(response)
        assert result is True
        assert AUTHORIZED_SIGNERS[1] in node.get_signatures()

    def test_receive_signature_wrong_round(self):
        """Test rejecting signatures for wrong round."""
        node = SignerNode(signer_address="addr", is_authorized_signer=True)
        consensus = create_mock_consensus_result()
        node.start_signing_round("2025-01-15", consensus, "tx", 1000, 10)

        response = SignatureResponse(
            round_id="2025-01-14",  # Wrong round
            signer_address=AUTHORIZED_SIGNERS[1],
            signature=b"sig",
            merkle_root="abc123def456",
            timestamp=int(time.time()),
        )

        result = node.receive_signature(response)
        assert result is False


# ============================================================================
# THRESHOLD TESTS
# ============================================================================

class TestThresholdCompletion:
    """Tests for 3-of-5 threshold."""

    def test_phase_complete_at_threshold(self):
        """Test that phase changes to COMPLETE when threshold met."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"key",
            is_authorized_signer=True,
        )
        consensus = create_mock_consensus_result()
        node.start_signing_round("2025-01-15", consensus, "tx", 1000, 10)

        # Add signatures from 3 signers (threshold)
        for i in range(MULTISIG_THRESHOLD):
            response = SignatureResponse(
                round_id="2025-01-15",
                signer_address=AUTHORIZED_SIGNERS[i],
                signature=f"sig_{i}".encode(),
                merkle_root="abc123def456",
                timestamp=int(time.time()),
            )
            node.receive_signature(response)

        assert node.get_current_phase() == SigningPhase.COMPLETE
        assert len(node.get_signatures()) >= MULTISIG_THRESHOLD

    def test_below_threshold_still_collecting(self):
        """Test that phase stays COLLECTING below threshold."""
        node = SignerNode(signer_address="addr", is_authorized_signer=True)
        consensus = create_mock_consensus_result()
        node.start_signing_round("2025-01-15", consensus, "tx", 1000, 10)

        # Add only 2 signatures (below threshold of 3)
        for i in range(MULTISIG_THRESHOLD - 1):
            response = SignatureResponse(
                round_id="2025-01-15",
                signer_address=AUTHORIZED_SIGNERS[i],
                signature=f"sig_{i}".encode(),
                merkle_root="abc123def456",
                timestamp=int(time.time()),
            )
            node.receive_signature(response)

        assert node.get_current_phase() == SigningPhase.COLLECTING


# ============================================================================
# SIGNING RESULT TESTS
# ============================================================================

class TestSigningResult:
    """Tests for SigningResult."""

    def test_check_signing_status(self):
        """Test getting signing status."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"key",
            is_authorized_signer=True,
        )
        consensus = create_mock_consensus_result()
        node.start_signing_round("2025-01-15", consensus, "tx", 1000, 10)

        status = node.check_signing_status()
        assert status.round_id == "2025-01-15"
        assert status.signatures_required == MULTISIG_THRESHOLD

    def test_result_to_dict(self):
        """Test result serialization."""
        result = SigningResult(
            round_id="2025-01-15",
            phase=SigningPhase.COMPLETE,
            merkle_root="abc123",
            signatures_collected=3,
            signatures_required=3,
            signers=["addr1", "addr2", "addr3"],
            combined_signature=b"combined",
            distribution_tx_hash="tx_hash",
            timestamp=1234567890,
        )
        data = result.to_dict()
        assert data['phase'] == "complete"
        assert data['signatures_collected'] == 3


# ============================================================================
# HELPER FUNCTION TESTS
# ============================================================================

class TestHelperFunctions:
    """Tests for helper functions."""

    def test_is_authorized_signer(self):
        """Test authorized signer check."""
        assert is_authorized_signer(AUTHORIZED_SIGNERS[0]) is True
        assert is_authorized_signer("random_address") is False

    def test_get_signer_count(self):
        """Test getting signer configuration."""
        threshold, total = get_signer_count()
        assert threshold == 3
        assert total == 5

    def test_verify_combined_signature_not_enough_signers(self):
        """Test verification fails with insufficient signers."""
        result = verify_combined_signature(
            merkle_root="abc",
            combined_signature=b"sig",
            signers=["addr1", "addr2"],  # Only 2, need 3
        )
        assert result is False

    def test_verify_combined_signature_unauthorized_signer(self):
        """Test verification fails with unauthorized signer."""
        result = verify_combined_signature(
            merkle_root="abc",
            combined_signature=b"sig",
            signers=[AUTHORIZED_SIGNERS[0], AUTHORIZED_SIGNERS[1], "unauthorized"],
        )
        assert result is False

    def test_verify_combined_signature_valid(self):
        """Test verification passes with valid signers."""
        result = verify_combined_signature(
            merkle_root="abc",
            combined_signature=b"sig",
            signers=[AUTHORIZED_SIGNERS[0], AUTHORIZED_SIGNERS[1], AUTHORIZED_SIGNERS[2]],
        )
        assert result is True


# ============================================================================
# CONSTANTS TESTS
# ============================================================================

class TestConstants:
    """Tests for module constants."""

    def test_threshold_value(self):
        """Test threshold is 3."""
        assert MULTISIG_THRESHOLD == 3

    def test_total_signers(self):
        """Test total signers is 5."""
        assert MULTISIG_TOTAL_SIGNERS == 5

    def test_authorized_signers_count(self):
        """Test we have 5 authorized signers."""
        assert len(AUTHORIZED_SIGNERS) == 5


# ============================================================================
# MERKLE ROOT VERIFICATION TESTS
# ============================================================================

class TestMerkleRootVerification:
    """Tests for merkle root verification."""

    def test_register_merkle_root(self):
        """Test registering a calculated merkle root."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            is_authorized_signer=True,
        )

        node.register_calculated_merkle_root("2025-01-15", "abc123def456")

        assert node.get_calculated_merkle_root("2025-01-15") == "abc123def456"

    def test_get_unregistered_merkle_root_returns_none(self):
        """Test getting unregistered merkle root returns None."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            is_authorized_signer=True,
        )

        assert node.get_calculated_merkle_root("unknown-round") is None

    def test_request_accepted_when_merkle_root_matches(self):
        """Test request accepted when merkle roots match."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"test_key",
            is_authorized_signer=True,
        )

        # Register our calculated merkle root
        node.register_calculated_merkle_root("2025-01-15", "abc123def456")

        # Request with matching merkle root
        request = create_test_request(
            round_id="2025-01-15",
            merkle_root="abc123def456",
        )

        result = node.process_signature_request(request)
        assert result is True

    def test_request_rejected_when_merkle_root_mismatch(self):
        """Test request rejected when merkle roots don't match."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"test_key",
            is_authorized_signer=True,
        )

        # Register our calculated merkle root
        node.register_calculated_merkle_root("2025-01-15", "our_calculated_root")

        # Request with different merkle root (potential malicious request)
        request = create_test_request(
            round_id="2025-01-15",
            merkle_root="different_root_from_attacker",
        )

        result = node.process_signature_request(request)
        assert result is False

    def test_request_accepted_when_no_local_merkle_root(self):
        """Test request accepted when we don't have a local calculation."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"test_key",
            is_authorized_signer=True,
        )

        # Don't register any merkle root (simulates joining mid-round)
        request = create_test_request(
            round_id="2025-01-15",
            merkle_root="some_merkle_root",
        )

        result = node.process_signature_request(request)
        assert result is True

    def test_merkle_root_cache_cleanup(self):
        """Test that old merkle roots are cleaned up."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            is_authorized_signer=True,
        )

        # Register 105 merkle roots (exceeds 100 limit)
        for i in range(105):
            node.register_calculated_merkle_root(f"round-{i:04d}", f"root-{i}")

        # Old rounds should be cleaned up, keeping only last 100
        assert node.get_calculated_merkle_root("round-0000") is None
        assert node.get_calculated_merkle_root("round-0004") is None
        assert node.get_calculated_merkle_root("round-0104") == "root-104"

    def test_multiple_rounds_tracked(self):
        """Test tracking multiple rounds simultaneously."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            is_authorized_signer=True,
        )

        node.register_calculated_merkle_root("2025-01-15", "root_a")
        node.register_calculated_merkle_root("2025-01-16", "root_b")
        node.register_calculated_merkle_root("2025-01-17", "root_c")

        assert node.get_calculated_merkle_root("2025-01-15") == "root_a"
        assert node.get_calculated_merkle_root("2025-01-16") == "root_b"
        assert node.get_calculated_merkle_root("2025-01-17") == "root_c"

    def test_request_rejected_with_old_timestamp(self):
        """Test request rejected when timestamp is too old."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"test_key",
            is_authorized_signer=True,
        )

        # Create request with old timestamp (11 minutes ago)
        request = SignatureRequest(
            round_id="2025-01-15",
            merkle_root="abc123",
            distribution_tx_hash="tx_hash",
            total_reward=1000.0,
            num_recipients=10,
            timestamp=int(time.time()) - 660,  # 11 minutes ago
            requester_id="requester",
        )

        result = node.process_signature_request(request)
        assert result is False

    def test_request_rejected_with_empty_merkle_root(self):
        """Test request rejected when merkle root is empty."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"test_key",
            is_authorized_signer=True,
        )

        request = SignatureRequest(
            round_id="2025-01-15",
            merkle_root="",  # Empty
            distribution_tx_hash="tx_hash",
            total_reward=1000.0,
            num_recipients=10,
            timestamp=int(time.time()),
            requester_id="requester",
        )

        result = node.process_signature_request(request)
        assert result is False


# ============================================================================
# MULTI-SIG HELPER FUNCTION TESTS
# ============================================================================

from satorip2p.protocol.signer import (
    create_multisig_redeem_script,
    create_multisig_scriptsig,
    parse_combined_signatures,
    get_treasury_address,
    TREASURY_MULTISIG_ADDRESS,
)


class TestMultisigRedeemScript:
    """Tests for create_multisig_redeem_script."""

    def test_create_3_of_5_redeem_script(self):
        """Test creating a 3-of-5 redeem script."""
        # Create 5 mock 33-byte compressed public keys
        pubkeys = [bytes([i] * 33) for i in range(1, 6)]

        script = create_multisig_redeem_script(pubkeys, threshold=3)

        # Script should start with OP_3 (0x53)
        assert script[0] == 0x53

        # Script should end with OP_5 (0x55) and OP_CHECKMULTISIG (0xae)
        assert script[-2] == 0x55
        assert script[-1] == 0xae

        # Script should contain all 5 pubkeys
        for pubkey in pubkeys:
            assert pubkey in script

    def test_create_2_of_3_redeem_script(self):
        """Test creating a 2-of-3 redeem script."""
        pubkeys = [bytes([i] * 33) for i in range(1, 4)]

        script = create_multisig_redeem_script(pubkeys, threshold=2)

        # Script should start with OP_2 (0x52)
        assert script[0] == 0x52
        # Script should have OP_3 (0x53) before OP_CHECKMULTISIG
        assert script[-2] == 0x53

    def test_insufficient_pubkeys_raises_error(self):
        """Test that insufficient pubkeys raises error."""
        pubkeys = [bytes([1] * 33), bytes([2] * 33)]  # Only 2 keys

        with pytest.raises(ValueError, match="Need at least 3 public keys"):
            create_multisig_redeem_script(pubkeys, threshold=3)

    def test_invalid_pubkey_length_raises_error(self):
        """Test that invalid pubkey length raises error."""
        # One pubkey is wrong length (32 bytes instead of 33)
        pubkeys = [
            bytes([1] * 33),
            bytes([2] * 32),  # Wrong length
            bytes([3] * 33),
        ]

        with pytest.raises(ValueError, match="33 bytes"):
            create_multisig_redeem_script(pubkeys, threshold=2)


class TestMultisigScriptsig:
    """Tests for create_multisig_scriptsig."""

    def test_create_scriptsig(self):
        """Test creating a P2SH multi-sig scriptSig."""
        # Mock signatures (DER encoded would be ~70 bytes)
        signatures = [
            bytes([0x30] + [i] * 70) for i in range(1, 4)
        ]

        # Mock redeem script (small enough for simple push)
        redeem_script = bytes([0x53] + [0x21] * 50 + [0x53, 0xae])

        scriptsig = create_multisig_scriptsig(signatures, redeem_script)

        # Should start with OP_0 (CHECKMULTISIG bug workaround)
        assert scriptsig[0] == 0x00

        # Should contain all signatures
        for sig in signatures:
            assert sig in scriptsig

        # Should end with redeem script
        assert scriptsig.endswith(redeem_script)

    def test_scriptsig_with_large_redeem_script(self):
        """Test scriptSig with large redeem script (needs OP_PUSHDATA1)."""
        signatures = [bytes([0x30] + [1] * 70)]

        # Create a redeem script > 75 bytes but < 256 bytes
        redeem_script = bytes([0x53] + [0x21] * 150 + [0x53, 0xae])

        scriptsig = create_multisig_scriptsig(signatures, redeem_script)

        # Should contain OP_PUSHDATA1 (0x4c) for large push
        assert 0x4c in scriptsig

        # Should contain the redeem script
        assert scriptsig.endswith(redeem_script)


class TestParseCombinedSignatures:
    """Tests for parse_combined_signatures."""

    def test_parse_multiple_signatures(self):
        """Test parsing combined signatures back to individual ones."""
        # Create individual signatures
        sig1 = bytes([0x30] + [1] * 64)
        sig2 = bytes([0x30] + [2] * 70)
        sig3 = bytes([0x30] + [3] * 68)

        # Combine them in our format: [num_sigs][sig1_len][sig1][sig2_len][sig2]...
        combined = bytes([3])  # 3 signatures
        combined += bytes([len(sig1)]) + sig1
        combined += bytes([len(sig2)]) + sig2
        combined += bytes([len(sig3)]) + sig3

        parsed = parse_combined_signatures(combined)

        assert len(parsed) == 3
        assert parsed[0] == sig1
        assert parsed[1] == sig2
        assert parsed[2] == sig3

    def test_parse_empty_combined(self):
        """Test parsing empty combined data."""
        parsed = parse_combined_signatures(b"")
        assert parsed == []

    def test_parse_too_short_combined(self):
        """Test parsing too-short combined data."""
        parsed = parse_combined_signatures(b"\x01")  # Claims 1 sig but no data
        assert parsed == []

    def test_parse_truncated_signature(self):
        """Test parsing truncated signature data."""
        # Claims sig is 10 bytes but only provides 5
        combined = bytes([1, 10, 1, 2, 3, 4, 5])
        parsed = parse_combined_signatures(combined)
        assert len(parsed) == 0  # Should fail gracefully


class TestGetTreasuryAddress:
    """Tests for get_treasury_address."""

    def test_get_treasury_address(self):
        """Test getting treasury address."""
        address = get_treasury_address()
        assert address == TREASURY_MULTISIG_ADDRESS
        assert isinstance(address, str)

    def test_treasury_address_is_placeholder(self):
        """Test that treasury address is currently a placeholder."""
        address = get_treasury_address()
        # Should be a placeholder until configured
        assert "PLACEHOLDER" in address or address.startswith("E")


class TestSignatureCombination:
    """Tests for signature combination in SignerNode."""

    def test_combine_signatures_below_threshold(self):
        """Test that combining fails below threshold."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"key",
            is_authorized_signer=True,
        )

        # Only add 2 signatures (below threshold of 3)
        node._signatures = {
            AUTHORIZED_SIGNERS[0]: b"sig_0",
            AUTHORIZED_SIGNERS[1]: b"sig_1",
        }

        combined = node._combine_signatures()
        assert combined is None

    def test_combine_signatures_at_threshold(self):
        """Test combining signatures at threshold."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"key",
            is_authorized_signer=True,
        )

        # Add exactly 3 signatures (threshold)
        node._signatures = {
            AUTHORIZED_SIGNERS[0]: b"sig_0",
            AUTHORIZED_SIGNERS[1]: b"sig_1",
            AUTHORIZED_SIGNERS[2]: b"sig_2",
        }

        combined = node._combine_signatures()
        assert combined is not None

        # Should start with count byte
        assert combined[0] == 3

    def test_combine_signatures_preserves_order(self):
        """Test that signatures are ordered by AUTHORIZED_SIGNERS order."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"key",
            is_authorized_signer=True,
        )

        # Add signatures in reverse order
        node._signatures = {
            AUTHORIZED_SIGNERS[4]: b"sig_4",
            AUTHORIZED_SIGNERS[2]: b"sig_2",
            AUTHORIZED_SIGNERS[0]: b"sig_0",
        }

        combined = node._combine_signatures()
        parsed = parse_combined_signatures(combined)

        # Should be in AUTHORIZED_SIGNERS order (0, 2, 4)
        assert parsed[0] == b"sig_0"
        assert parsed[1] == b"sig_2"
        assert parsed[2] == b"sig_4"

    def test_combine_more_than_threshold_selects_first(self):
        """Test that only threshold signatures are combined."""
        node = SignerNode(
            signer_address=AUTHORIZED_SIGNERS[0],
            private_key=b"key",
            is_authorized_signer=True,
        )

        # Add all 5 signatures
        node._signatures = {
            AUTHORIZED_SIGNERS[i]: f"sig_{i}".encode()
            for i in range(5)
        }

        combined = node._combine_signatures()
        parsed = parse_combined_signatures(combined)

        # Should only have 3 (threshold)
        assert len(parsed) == 3
        # Should be the first 3 in AUTHORIZED_SIGNERS order
        assert parsed[0] == b"sig_0"
        assert parsed[1] == b"sig_1"
        assert parsed[2] == b"sig_2"
