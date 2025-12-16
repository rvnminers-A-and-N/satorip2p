"""
satorip2p/signing.py

Evrmore message signing and verification using python-evrmorelib directly.

This module provides signing capabilities without requiring satorilib,
allowing satorip2p to be more standalone while maintaining compatibility
with the Satori ecosystem.

Usage:
    from satorip2p.signing import EvrmoreWallet, sign_message, verify_message

    # Create wallet from entropy or private key
    wallet = EvrmoreWallet.from_entropy(entropy_bytes)
    # or
    wallet = EvrmoreWallet.from_private_key(private_key_wif)

    # Sign a message
    signature = wallet.sign("Hello, world!")

    # Verify a signature
    is_valid = verify_message("Hello, world!", signature, address=wallet.address)
"""

import base64
from typing import Union, Optional

# Import from python-evrmorelib
from evrmore import SelectParams
from evrmore.wallet import CEvrmoreSecret, P2PKHEvrmoreAddress
from evrmore.signmessage import signMessage, verifyMessage, EvrmoreMessage
from evrmore.core.key import CPubKey


class EvrmoreWallet:
    """
    Lightweight Evrmore wallet for signing operations.

    This is a thin wrapper around python-evrmorelib that provides
    the signing/verification functionality needed by satorip2p
    without the full satorilib wallet infrastructure.
    """

    def __init__(self, private_key_obj: CEvrmoreSecret):
        """
        Initialize with a CEvrmoreSecret private key object.

        Use the factory methods from_entropy() or from_private_key() instead.
        """
        SelectParams('mainnet')
        self._private_key = private_key_obj
        self._address_obj = P2PKHEvrmoreAddress.from_pubkey(self._private_key.pub)

    @classmethod
    def from_entropy(cls, entropy: bytes, compressed: bool = True) -> "EvrmoreWallet":
        """
        Create wallet from 32 bytes of entropy.

        Args:
            entropy: 32 bytes of random entropy
            compressed: Whether to use compressed public key (default True)

        Returns:
            EvrmoreWallet instance
        """
        SelectParams('mainnet')
        if len(entropy) != 32:
            raise ValueError("Entropy must be exactly 32 bytes")
        private_key = CEvrmoreSecret.from_secret_bytes(entropy, compressed=compressed)
        return cls(private_key)

    @classmethod
    def from_private_key(cls, wif: str) -> "EvrmoreWallet":
        """
        Create wallet from WIF (Wallet Import Format) private key.

        Args:
            wif: Private key in WIF format (52 characters)

        Returns:
            EvrmoreWallet instance
        """
        SelectParams('mainnet')
        private_key = CEvrmoreSecret(wif)
        return cls(private_key)

    @property
    def address(self) -> str:
        """Get the Evrmore address."""
        return str(self._address_obj)

    @property
    def public_key(self) -> str:
        """Get the public key as hex string."""
        return self._private_key.pub.hex()

    @property
    def public_key_bytes(self) -> bytes:
        """Get the public key as bytes."""
        return bytes(self._private_key.pub)

    def sign(self, message: str) -> bytes:
        """
        Sign a message with the private key.

        Args:
            message: The message to sign

        Returns:
            Base64-encoded signature as bytes
        """
        msg = EvrmoreMessage(message)
        return signMessage(self._private_key, msg)

    def sign_hex(self, message: str) -> str:
        """
        Sign a message and return signature as hex string.

        Args:
            message: The message to sign

        Returns:
            Signature as hex string
        """
        sig = self.sign(message)
        # signMessage returns base64, decode then hex encode
        return base64.b64decode(sig).hex()

    def verify(
        self,
        message: str,
        signature: Union[bytes, str],
        address: Optional[str] = None,
    ) -> bool:
        """
        Verify a signature.

        Args:
            message: The original message
            signature: The signature (base64 bytes or string)
            address: Address to verify against (defaults to this wallet's address)

        Returns:
            True if signature is valid
        """
        return verify_message(
            message=message,
            signature=signature,
            address=address or self.address,
        )


def sign_message(private_key: CEvrmoreSecret, message: str) -> bytes:
    """
    Sign a message with an Evrmore private key.

    Args:
        private_key: CEvrmoreSecret private key object
        message: The message to sign

    Returns:
        Base64-encoded signature as bytes
    """
    msg = EvrmoreMessage(message)
    return signMessage(private_key, msg)


def verify_message(
    message: str,
    signature: Union[bytes, str],
    pubkey: Optional[Union[str, bytes]] = None,
    address: Optional[str] = None,
) -> bool:
    """
    Verify a message signature.

    Args:
        message: The original message
        signature: The signature (base64 encoded)
        pubkey: Public key to verify against (hex string or bytes)
        address: Address to verify against

    Returns:
        True if signature is valid

    Note:
        Must provide either pubkey or address (or both)
    """
    if pubkey is None and address is None:
        raise ValueError("Must provide either pubkey or address")

    msg = EvrmoreMessage(message)

    # Handle signature format
    if isinstance(signature, bytes):
        # Could be raw bytes or base64-encoded bytes
        try:
            # Try to decode as base64 first
            signature = signature.decode('utf-8')
        except UnicodeDecodeError:
            # Raw bytes - encode to base64 string
            signature = base64.b64encode(signature).decode('utf-8')

    # Handle pubkey format
    if isinstance(pubkey, bytes):
        pubkey = pubkey.hex()

    return verifyMessage(
        message=msg,
        signature=signature,
        pubkey=pubkey,
        address=address,
    )


def generate_address(pubkey: Union[bytes, str]) -> str:
    """
    Generate an Evrmore address from a public key.

    Args:
        pubkey: Public key as bytes or hex string

    Returns:
        Evrmore address string
    """
    SelectParams('mainnet')
    if isinstance(pubkey, str):
        pubkey = bytes.fromhex(pubkey)
    return str(P2PKHEvrmoreAddress.from_pubkey(pubkey))


def recover_pubkey_from_signature(message: str, signature: Union[bytes, str]) -> Optional[str]:
    """
    Recover the public key from a message and signature.

    Args:
        message: The original message
        signature: The signature (base64 encoded)

    Returns:
        Public key as hex string, or None if recovery fails
    """
    try:
        msg = EvrmoreMessage(message)

        # Handle signature format
        if isinstance(signature, bytes):
            try:
                signature = signature.decode('utf-8')
            except UnicodeDecodeError:
                signature = base64.b64encode(signature).decode('utf-8')

        # Recover public key
        pub = CPubKey.recover_compact(
            hash=msg.GetHash(),
            sig=base64.b64decode(signature)
        )
        return pub.hex() if pub else None
    except Exception:
        return None
