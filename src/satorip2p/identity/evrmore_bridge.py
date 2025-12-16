"""
satorip2p/identity/evrmore_bridge.py

Bridge between Evrmore wallet identity and libp2p peer identity.
Both use secp256k1 keys, so conversion is straightforward.

This module now supports two modes:
1. Using satorilib's EvrmoreIdentity (for full Neuron integration)
2. Using satorip2p's EvrmoreWallet (standalone, uses python-evrmorelib directly)
"""

from typing import TYPE_CHECKING, Optional, Union
import logging
import os

# Import our signing module (uses python-evrmorelib directly)
from satorip2p.signing import (
    EvrmoreWallet,
    sign_message,
    verify_message,
    generate_address,
)

if TYPE_CHECKING:
    from satorilib.wallet.evrmore.identity import EvrmoreIdentity

logger = logging.getLogger("satorip2p.identity")


class EvrmoreIdentityBridge:
    """
    Converts Evrmore wallet identity to libp2p peer identity.

    Both Evrmore and libp2p use secp256k1 keys, so the conversion
    is straightforward. The peer ID is derived from the public key,
    maintaining cryptographic identity across both systems.

    Supports two modes:
    1. With satorilib EvrmoreIdentity (full wallet features)
    2. With satorip2p EvrmoreWallet (standalone signing)

    Usage with satorilib:
        from satorilib.wallet.evrmore.identity import EvrmoreIdentity
        identity = EvrmoreIdentity('/path/to/wallet.yaml')
        bridge = EvrmoreIdentityBridge(identity)

    Usage standalone (python-evrmorelib only):
        from satorip2p.signing import EvrmoreWallet
        wallet = EvrmoreWallet.from_entropy(entropy_bytes)
        bridge = EvrmoreIdentityBridge(wallet)

    Or create directly from entropy:
        bridge = EvrmoreIdentityBridge.from_entropy(entropy_bytes)
    """

    def __init__(self, identity: Union["EvrmoreIdentity", EvrmoreWallet]):
        """
        Initialize bridge with Evrmore identity or wallet.

        Args:
            identity: EvrmoreIdentity (satorilib) or EvrmoreWallet (satorip2p)
        """
        self._identity = identity
        self._is_satorilib = not isinstance(identity, EvrmoreWallet)
        self._entropy: Optional[bytes] = None
        self._libp2p_private_key = None
        self._libp2p_public_key = None
        self._peer_id = None

        # Extract entropy for libp2p key generation
        if self._is_satorilib:
            # satorilib EvrmoreIdentity has _entropy attribute
            self._entropy = getattr(identity, '_entropy', None)
        else:
            # Our EvrmoreWallet - we need to store entropy separately
            # Note: EvrmoreWallet doesn't expose entropy after creation
            pass

    @classmethod
    def from_entropy(cls, entropy: bytes) -> "EvrmoreIdentityBridge":
        """
        Create bridge directly from entropy (standalone mode).

        Args:
            entropy: 32 bytes of entropy

        Returns:
            EvrmoreIdentityBridge instance
        """
        wallet = EvrmoreWallet.from_entropy(entropy)
        bridge = cls(wallet)
        bridge._entropy = entropy  # Store for libp2p key generation
        return bridge

    @classmethod
    def from_private_key(cls, wif: str, entropy: Optional[bytes] = None) -> "EvrmoreIdentityBridge":
        """
        Create bridge from WIF private key.

        Args:
            wif: Private key in WIF format
            entropy: Optional entropy for libp2p key generation

        Returns:
            EvrmoreIdentityBridge instance
        """
        wallet = EvrmoreWallet.from_private_key(wif)
        bridge = cls(wallet)
        bridge._entropy = entropy
        return bridge

    # Backward compatibility property
    @property
    def identity(self):
        """Get underlying identity (for backward compatibility)."""
        return self._identity

    @property
    def evrmore_address(self) -> str:
        """Get the Evrmore address."""
        if self._is_satorilib:
            return self._identity.address
        else:
            return self._identity.address  # EvrmoreWallet also has .address

    @property
    def evrmore_pubkey(self) -> str:
        """Get the Evrmore public key (hex)."""
        if self._is_satorilib:
            return self._identity.pubkey
        else:
            return self._identity.public_key  # EvrmoreWallet uses public_key

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

            # Get raw 32-byte entropy
            if self._entropy is None:
                raise ValueError("No entropy available (watch-only wallet or entropy not stored)")

            if len(self._entropy) != 32:
                raise ValueError(f"Expected 32-byte entropy, got {len(self._entropy)}")

            # Create libp2p key pair from raw bytes
            self._libp2p_private_key = create_new_key_pair(self._entropy)
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

    def sign(self, message: Union[bytes, str]) -> bytes:
        """
        Sign message using Evrmore identity.

        This uses the Evrmore signing method, which is compatible
        with Bitcoin message signing.

        Args:
            message: Message to sign (bytes or string)

        Returns:
            Signature bytes (base64 encoded)
        """
        if isinstance(message, bytes):
            message = message.decode("utf-8")

        # Use our EvrmoreWallet's sign method (python-evrmorelib directly)
        if isinstance(self._identity, EvrmoreWallet):
            return self._identity.sign(message)
        else:
            # satorilib EvrmoreIdentity
            return self._identity.sign(message)

    def verify(
        self,
        message: Union[bytes, str],
        signature: bytes,
        public_key: Optional[str] = None,
        address: Optional[str] = None,
    ) -> bool:
        """
        Verify signature using Evrmore verification.

        Args:
            message: Original message (bytes or string)
            signature: Signature to verify
            public_key: Public key (hex), uses own key if not provided
            address: Address to verify against

        Returns:
            True if signature is valid
        """
        if isinstance(message, bytes):
            message = message.decode("utf-8")

        # For satorilib identity (or mock), delegate to its verify method
        if self._is_satorilib and hasattr(self._identity, 'verify'):
            return self._identity.verify(message, signature, public_key, address)

        # Use python-evrmorelib directly via our verify_message
        return verify_message(
            message=message,
            signature=signature,
            pubkey=public_key or self.evrmore_pubkey,
            address=address or self.evrmore_address,
        )

    def derive_shared_secret(self, peer_pubkey: str) -> bytes:
        """
        Derive shared secret with another peer using ECDH.

        Args:
            peer_pubkey: Peer's public key (hex string)

        Returns:
            Shared secret bytes (32 bytes)
        """
        if self._is_satorilib and hasattr(self._identity, 'secret'):
            # Use satorilib's method if available
            return self._identity.secret(peer_pubkey)
        else:
            # Implement ECDH directly using cryptography library
            return self._ecdh_derive_secret(peer_pubkey)

    def _ecdh_derive_secret(self, peer_pubkey: str) -> bytes:
        """
        Derive ECDH shared secret using cryptography library.

        Args:
            peer_pubkey: Peer's public key (hex string)

        Returns:
            Shared secret bytes
        """
        from cryptography.hazmat.primitives.asymmetric import ec

        if self._entropy is None:
            raise ValueError("No entropy available for ECDH")

        # Create our private key
        my_ec_private_key = ec.derive_private_key(
            private_value=int.from_bytes(self._entropy, 'big'),
            curve=ec.SECP256K1()
        )

        # Parse peer's public key
        peer_pubkey_bytes = bytes.fromhex(peer_pubkey)
        peer_ec_public_key = ec.EllipticCurvePublicKey.from_encoded_point(
            curve=ec.SECP256K1(),
            data=peer_pubkey_bytes
        )

        # Perform ECDH
        shared_secret = my_ec_private_key.exchange(
            algorithm=ec.ECDH(),
            peer_public_key=peer_ec_public_key
        )

        return shared_secret

    def encrypt(self, shared_secret: bytes, plaintext: Union[bytes, str]) -> bytes:
        """
        Encrypt message using shared secret (AES-GCM).

        Args:
            shared_secret: ECDH shared secret
            plaintext: Data to encrypt

        Returns:
            Encrypted data (nonce + ciphertext)
        """
        # Try satorilib first for compatibility
        try:
            from satorilib.wallet.identity import IdentityBase
            return IdentityBase.encrypt(shared_secret, plaintext)
        except ImportError:
            # Fall back to direct implementation
            return self._aes_gcm_encrypt(shared_secret, plaintext)

    def _aes_gcm_encrypt(self, shared_secret: bytes, plaintext: Union[bytes, str]) -> bytes:
        """
        Encrypt using AES-GCM directly (no satorilib dependency).
        """
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        if isinstance(plaintext, str):
            plaintext = plaintext.encode('utf-8')

        # Derive AES key from shared secret
        aes_key = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=b'evrmore-ecdh'
        ).derive(shared_secret)

        # Encrypt
        nonce = os.urandom(12)
        aesgcm = AESGCM(aes_key)
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)

        return nonce + ciphertext

    def decrypt(self, shared_secret: bytes, ciphertext: bytes) -> bytes:
        """
        Decrypt message using shared secret (AES-GCM).

        Args:
            shared_secret: ECDH shared secret
            ciphertext: Encrypted data (nonce + ciphertext)

        Returns:
            Decrypted plaintext
        """
        # Try satorilib first for compatibility
        try:
            from satorilib.wallet.identity import IdentityBase
            return IdentityBase.decrypt(shared_secret, ciphertext)
        except ImportError:
            # Fall back to direct implementation
            return self._aes_gcm_decrypt(shared_secret, ciphertext)

    def _aes_gcm_decrypt(self, shared_secret: bytes, ciphertext: bytes) -> bytes:
        """
        Decrypt using AES-GCM directly (no satorilib dependency).
        """
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        # Derive AES key from shared secret
        aes_key = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=b'evrmore-ecdh'
        ).derive(shared_secret)

        # Extract nonce and ciphertext
        nonce = ciphertext[:12]
        encrypted_data = ciphertext[12:]

        # Decrypt
        aesgcm = AESGCM(aes_key)
        plaintext = aesgcm.decrypt(nonce, encrypted_data, None)

        return plaintext

    def __repr__(self) -> str:
        mode = "satorilib" if self._is_satorilib else "standalone"
        return (
            f"EvrmoreIdentityBridge("
            f"address={self.evrmore_address}, "
            f"mode={mode}, "
            f"peer_id={self._peer_id or 'not generated'})"
        )
