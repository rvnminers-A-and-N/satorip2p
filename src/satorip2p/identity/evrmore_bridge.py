"""
satorip2p/identity/evrmore_bridge.py

Bridge between Evrmore wallet identity and libp2p peer identity.
Both use secp256k1 keys, so conversion is straightforward.
"""

from typing import TYPE_CHECKING, Optional
import logging

if TYPE_CHECKING:
    from satorilib.wallet.evrmore.identity import EvrmoreIdentity

logger = logging.getLogger("satorip2p.identity")


class EvrmoreIdentityBridge:
    """
    Converts Evrmore wallet identity to libp2p peer identity.

    Both Evrmore and libp2p use secp256k1 keys, so the conversion
    is straightforward. The peer ID is derived from the public key,
    maintaining cryptographic identity across both systems.

    Usage:
        from satorilib.wallet.evrmore.identity import EvrmoreIdentity
        from satorip2p.identity import EvrmoreIdentityBridge

        identity = EvrmoreIdentity('/path/to/wallet.yaml')
        bridge = EvrmoreIdentityBridge(identity)

        libp2p_key = bridge.to_libp2p_key()
        peer_id = bridge.get_peer_id()
    """

    def __init__(self, identity: "EvrmoreIdentity"):
        """
        Initialize bridge with Evrmore identity.

        Args:
            identity: EvrmoreIdentity instance with loaded wallet
        """
        self.identity = identity
        self._libp2p_private_key = None
        self._libp2p_public_key = None
        self._peer_id = None

    @property
    def evrmore_address(self) -> str:
        """Get the Evrmore address."""
        return self.identity.address

    @property
    def evrmore_pubkey(self) -> str:
        """Get the Evrmore public key (hex)."""
        return self.identity.pubkey

    def to_libp2p_key(self):
        """
        Convert Evrmore private key to libp2p KeyPair.

        Both Evrmore and libp2p use secp256k1, so we extract
        the raw entropy bytes and create a libp2p key pair.

        Returns:
            KeyPair (with private_key and public_key) for use with libp2p
        """
        if self._libp2p_private_key is not None:
            return self._libp2p_private_key

        try:
            from libp2p.crypto.secp256k1 import create_new_key_pair

            # Get raw 32-byte entropy from Evrmore identity
            # The identity stores entropy as base64-encoded bytes
            if self.identity._entropy is None:
                raise ValueError("Identity has no entropy (watch-only wallet?)")

            entropy_bytes = self.identity._entropy
            if len(entropy_bytes) != 32:
                raise ValueError(f"Expected 32-byte entropy, got {len(entropy_bytes)}")

            # Create libp2p key pair from raw bytes
            self._libp2p_private_key = create_new_key_pair(entropy_bytes)
            logger.debug(f"Created libp2p key for {self.evrmore_address[:16]}...")

            return self._libp2p_private_key

        except ImportError:
            logger.error("libp2p not installed. Install with: pip install libp2p")
            raise
        except Exception as e:
            logger.error(f"Failed to convert identity to libp2p key: {e}")
            raise

    def get_public_key(self):
        """
        Get libp2p public key.

        Returns:
            Secp256k1PublicKey
        """
        if self._libp2p_public_key is not None:
            return self._libp2p_public_key

        key_pair = self.to_libp2p_key()
        # KeyPair has public_key attribute
        self._libp2p_public_key = key_pair.public_key
        return self._libp2p_public_key

    def get_peer_id(self) -> str:
        """
        Get libp2p PeerID derived from Evrmore public key.

        The PeerID is a hash of the public key, providing a
        unique identifier for this node in the libp2p network.

        Returns:
            PeerID as string (base58 encoded)
        """
        if self._peer_id is not None:
            return self._peer_id

        try:
            from libp2p.peer.id import ID as PeerID

            public_key = self.get_public_key()
            peer_id = PeerID.from_pubkey(public_key)
            self._peer_id = str(peer_id)

            logger.debug(f"PeerID for {self.evrmore_address}: {self._peer_id[:16]}...")
            return self._peer_id

        except Exception as e:
            logger.error(f"Failed to generate PeerID: {e}")
            raise

    def sign(self, message: bytes) -> bytes:
        """
        Sign message using Evrmore identity.

        This uses the Evrmore signing method, which is compatible
        with Bitcoin message signing.

        Args:
            message: Bytes to sign

        Returns:
            Signature bytes
        """
        if isinstance(message, bytes):
            message = message.decode("utf-8")
        return self.identity.sign(message)

    def verify(
        self,
        message: bytes,
        signature: bytes,
        public_key: Optional[str] = None
    ) -> bool:
        """
        Verify signature using Evrmore verification.

        Args:
            message: Original message bytes
            signature: Signature to verify
            public_key: Public key (hex), uses own key if not provided

        Returns:
            True if signature is valid
        """
        if isinstance(message, bytes):
            message = message.decode("utf-8")
        return self.identity.verify(
            msg=message,
            sig=signature,
            pubkey=public_key
        )

    def derive_shared_secret(self, peer_pubkey: str) -> bytes:
        """
        Derive shared secret with another peer using ECDH.

        Uses the Evrmore identity's secret() method which performs
        ECDH key exchange.

        Args:
            peer_pubkey: Peer's public key (hex string)

        Returns:
            Shared secret bytes (32 bytes)
        """
        return self.identity.secret(peer_pubkey)

    def encrypt(self, shared_secret: bytes, plaintext: bytes) -> bytes:
        """
        Encrypt message using shared secret (AES-GCM).

        Args:
            shared_secret: ECDH shared secret
            plaintext: Data to encrypt

        Returns:
            Encrypted data (nonce + ciphertext)
        """
        from satorilib.wallet.identity import IdentityBase
        return IdentityBase.encrypt(shared_secret, plaintext)

    def decrypt(self, shared_secret: bytes, ciphertext: bytes) -> bytes:
        """
        Decrypt message using shared secret (AES-GCM).

        Args:
            shared_secret: ECDH shared secret
            ciphertext: Encrypted data (nonce + ciphertext)

        Returns:
            Decrypted plaintext
        """
        from satorilib.wallet.identity import IdentityBase
        return IdentityBase.decrypt(shared_secret, ciphertext)

    def __repr__(self) -> str:
        return (
            f"EvrmoreIdentityBridge("
            f"address={self.evrmore_address}, "
            f"peer_id={self._peer_id or 'not generated'})"
        )
