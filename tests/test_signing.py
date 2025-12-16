"""
Tests for satorip2p/signing.py

Tests the Evrmore message signing and verification using python-evrmorelib.
"""

import pytest
import base64
import os

from satorip2p.signing import (
    EvrmoreWallet,
    sign_message,
    verify_message,
    generate_address,
    recover_pubkey_from_signature,
)


# ============================================================================
# TEST DATA
# ============================================================================

# Fixed test entropy for reproducible tests
TEST_ENTROPY = bytes.fromhex(
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

# Another entropy for testing different wallets
TEST_ENTROPY_2 = bytes.fromhex(
    "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
)


# ============================================================================
# EVRMORE WALLET TESTS
# ============================================================================

class TestEvrmoreWallet:
    """Tests for EvrmoreWallet class."""

    def test_create_from_entropy(self):
        """Test creating wallet from entropy."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        assert wallet is not None
        assert wallet.address is not None
        assert wallet.address.startswith('E')  # Evrmore mainnet addresses start with E
        assert len(wallet.address) == 34  # Standard address length

    def test_create_from_entropy_deterministic(self):
        """Test that same entropy produces same wallet."""
        wallet1 = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        wallet2 = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        assert wallet1.address == wallet2.address
        assert wallet1.public_key == wallet2.public_key

    def test_different_entropy_different_wallet(self):
        """Test that different entropy produces different wallet."""
        wallet1 = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        wallet2 = EvrmoreWallet.from_entropy(TEST_ENTROPY_2)
        assert wallet1.address != wallet2.address

    def test_entropy_must_be_32_bytes(self):
        """Test that entropy must be exactly 32 bytes."""
        with pytest.raises(ValueError, match="32 bytes"):
            EvrmoreWallet.from_entropy(bytes(16))  # Too short

        with pytest.raises(ValueError, match="32 bytes"):
            EvrmoreWallet.from_entropy(bytes(64))  # Too long

    def test_public_key_format(self):
        """Test public key is returned as hex string."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        pubkey = wallet.public_key
        assert isinstance(pubkey, str)
        # Compressed pubkey is 33 bytes = 66 hex chars
        assert len(pubkey) == 66
        # Compressed pubkeys start with 02 or 03
        assert pubkey.startswith('02') or pubkey.startswith('03')

    def test_public_key_bytes(self):
        """Test public key bytes property."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        pubkey_bytes = wallet.public_key_bytes
        assert isinstance(pubkey_bytes, bytes)
        assert len(pubkey_bytes) == 33  # Compressed pubkey


# ============================================================================
# SIGNING TESTS
# ============================================================================

class TestSigning:
    """Tests for message signing."""

    def test_sign_message(self):
        """Test signing a message."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        message = "Hello, Satori!"
        signature = wallet.sign(message)

        assert signature is not None
        assert isinstance(signature, bytes)
        # Base64 encoded signature
        assert len(signature) > 0

    def test_sign_different_messages(self):
        """Test that different messages produce different signatures."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        sig1 = wallet.sign("Message 1")
        sig2 = wallet.sign("Message 2")
        assert sig1 != sig2

    def test_sign_same_message_both_valid(self):
        """Test that same message signed twice produces valid signatures."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        message = "Validity test"
        sig1 = wallet.sign(message)
        sig2 = wallet.sign(message)
        # Both signatures should be valid (may differ due to random k)
        assert verify_message(message, sig1, address=wallet.address) is True
        assert verify_message(message, sig2, address=wallet.address) is True

    def test_sign_empty_message(self):
        """Test signing an empty message."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        signature = wallet.sign("")
        assert signature is not None
        assert len(signature) > 0

    def test_sign_long_message(self):
        """Test signing a long message."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        message = "A" * 10000  # 10KB message
        signature = wallet.sign(message)
        assert signature is not None

    def test_sign_unicode_message(self):
        """Test signing a message with unicode characters."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        message = "Hello"
        signature = wallet.sign(message)
        assert signature is not None

    def test_sign_hex_function(self):
        """Test sign_hex returns hex string."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        sig_hex = wallet.sign_hex("Test message")
        assert isinstance(sig_hex, str)
        # Should be valid hex
        bytes.fromhex(sig_hex)


# ============================================================================
# VERIFICATION TESTS
# ============================================================================

class TestVerification:
    """Tests for signature verification."""

    def test_verify_valid_signature(self):
        """Test verifying a valid signature."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        message = "Verify me!"
        signature = wallet.sign(message)

        # Verify using wallet's verify method
        is_valid = wallet.verify(message, signature)
        assert is_valid is True

    def test_verify_with_address(self):
        """Test verifying using address."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        message = "Test verification"
        signature = wallet.sign(message)

        is_valid = verify_message(
            message=message,
            signature=signature,
            address=wallet.address,
        )
        assert is_valid is True

    def test_verify_wrong_message(self):
        """Test that verification fails with wrong message."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        signature = wallet.sign("Original message")

        is_valid = verify_message(
            message="Different message",
            signature=signature,
            address=wallet.address,
        )
        assert is_valid is False

    def test_verify_wrong_address(self):
        """Test that verification fails with wrong address."""
        wallet1 = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        wallet2 = EvrmoreWallet.from_entropy(TEST_ENTROPY_2)

        message = "Test message"
        signature = wallet1.sign(message)

        # Try to verify with wallet2's address
        is_valid = verify_message(
            message=message,
            signature=signature,
            address=wallet2.address,
        )
        assert is_valid is False

    def test_verify_signature_bytes(self):
        """Test verifying with signature as bytes."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        message = "Bytes test"
        signature = wallet.sign(message)

        # signature is already bytes (base64 encoded)
        is_valid = verify_message(
            message=message,
            signature=signature,
            address=wallet.address,
        )
        assert is_valid is True

    def test_verify_signature_string(self):
        """Test verifying with signature as string."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        message = "String test"
        signature = wallet.sign(message)

        # Convert to string
        sig_str = signature.decode('utf-8') if isinstance(signature, bytes) else signature

        is_valid = verify_message(
            message=message,
            signature=sig_str,
            address=wallet.address,
        )
        assert is_valid is True

    def test_verify_requires_address_or_pubkey(self):
        """Test that verify requires either address or pubkey."""
        with pytest.raises(ValueError, match="Must provide"):
            verify_message(
                message="test",
                signature=b"fake_sig",
            )


# ============================================================================
# ADDRESS GENERATION TESTS
# ============================================================================

class TestAddressGeneration:
    """Tests for address generation from public key."""

    def test_generate_address_from_hex(self):
        """Test generating address from hex pubkey."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        pubkey_hex = wallet.public_key

        address = generate_address(pubkey_hex)
        assert address == wallet.address

    def test_generate_address_from_bytes(self):
        """Test generating address from bytes pubkey."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        pubkey_bytes = wallet.public_key_bytes

        address = generate_address(pubkey_bytes)
        assert address == wallet.address

    def test_address_format(self):
        """Test that generated address has correct format."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        address = generate_address(wallet.public_key)

        assert address.startswith('E')
        assert len(address) == 34


# ============================================================================
# PUBLIC KEY RECOVERY TESTS
# ============================================================================

class TestPubkeyRecovery:
    """Tests for recovering public key from signature."""

    def test_recover_pubkey(self):
        """Test recovering public key from message and signature."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        message = "Recovery test"
        signature = wallet.sign(message)

        recovered = recover_pubkey_from_signature(message, signature)

        # The recovered pubkey should match
        assert recovered is not None
        assert recovered == wallet.public_key

    def test_recover_pubkey_generates_same_address(self):
        """Test that recovered pubkey generates same address."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        message = "Address test"
        signature = wallet.sign(message)

        recovered = recover_pubkey_from_signature(message, signature)
        recovered_address = generate_address(recovered)

        assert recovered_address == wallet.address

    def test_recover_pubkey_invalid_signature(self):
        """Test recovery with invalid signature returns None."""
        result = recover_pubkey_from_signature("test", b"invalid")
        assert result is None


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestSigningIntegration:
    """Integration tests for the signing workflow."""

    def test_full_signing_workflow(self):
        """Test complete sign -> verify -> recover workflow."""
        # Create wallet
        entropy = os.urandom(32)
        wallet = EvrmoreWallet.from_entropy(entropy)

        # Sign message
        message = "Full workflow test: " + wallet.address
        signature = wallet.sign(message)

        # Verify with address
        assert verify_message(message, signature, address=wallet.address) is True

        # Recover pubkey
        recovered = recover_pubkey_from_signature(message, signature)
        assert recovered == wallet.public_key

        # Verify recovered address matches
        assert generate_address(recovered) == wallet.address

    def test_cross_wallet_verification(self):
        """Test that one wallet can verify another's signature."""
        wallet1 = EvrmoreWallet.from_entropy(TEST_ENTROPY)
        wallet2 = EvrmoreWallet.from_entropy(TEST_ENTROPY_2)

        message = "Cross-wallet test"
        signature = wallet1.sign(message)

        # wallet2 verifies wallet1's signature
        is_valid = verify_message(
            message=message,
            signature=signature,
            address=wallet1.address,
        )
        assert is_valid is True

    def test_heartbeat_signing(self):
        """Test signing a heartbeat-like message."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)

        # Simulate heartbeat content
        heartbeat_content = (
            f"node1:{wallet.address}:peer123:2025-01-15:1234567890:"
            f"predictor,relay:50.0:active"
        )

        signature = wallet.sign(heartbeat_content)

        # Verify
        assert verify_message(
            message=heartbeat_content,
            signature=signature,
            address=wallet.address,
        ) is True

    def test_consensus_vote_signing(self):
        """Test signing a consensus vote-like message."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)

        # Simulate vote content
        vote_content = "node1:abcd1234merkleroot:100.0:2025-01-15:1234567890"

        signature = wallet.sign(vote_content)

        # Verify
        assert verify_message(
            message=vote_content,
            signature=signature,
            address=wallet.address,
        ) is True


# ============================================================================
# EDGE CASES
# ============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_special_characters_in_message(self):
        """Test signing messages with special characters."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)

        messages = [
            "Hello\nWorld",  # Newline
            "Tab\there",  # Tab
            "Quote\"test",  # Quote
            "Backslash\\test",  # Backslash
            '{"json": "data"}',  # JSON
        ]

        for msg in messages:
            sig = wallet.sign(msg)
            assert verify_message(msg, sig, address=wallet.address) is True

    def test_binary_like_message(self):
        """Test signing messages that look like binary data."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)

        # Hex string (looks binary)
        message = "0x" + "deadbeef" * 16
        sig = wallet.sign(message)
        assert verify_message(message, sig, address=wallet.address) is True

    def test_whitespace_only_message(self):
        """Test signing whitespace-only messages."""
        wallet = EvrmoreWallet.from_entropy(TEST_ENTROPY)

        messages = [" ", "  ", "\t", "\n", " \t\n "]

        for msg in messages:
            sig = wallet.sign(msg)
            assert verify_message(msg, sig, address=wallet.address) is True
