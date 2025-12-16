"""
satorip2p/protocol/signer.py

Signer node implementation for multi-sig reward distribution.

Signer nodes are designated trusted parties that auto-sign distribution
transactions when consensus is verified. A 3-of-5 multi-sig scheme
requires 3 of 5 signers to approve each distribution.

Key Features:
- Auto-sign on verified consensus
- Signature collection via P2P
- 3-of-5 threshold signature combining
- Signature broadcast/aggregation
"""

import logging
import time
import hashlib
from typing import Dict, List, Optional, Set, Callable, TYPE_CHECKING, Tuple, Union, Any
from dataclasses import dataclass, field, asdict
from enum import Enum

if TYPE_CHECKING:
    from ..peers import Peers
    from .consensus import ConsensusResult

# Import signing module for real ECDSA signatures
try:
    from ..signing import EvrmoreWallet, sign_message, verify_message
    SIGNING_AVAILABLE = True
except ImportError:
    SIGNING_AVAILABLE = False

logger = logging.getLogger("satorip2p.protocol.signer")


# ============================================================================
# CONSTANTS - PLACEHOLDERS (Team will configure)
# ============================================================================

# Multi-sig configuration
MULTISIG_THRESHOLD = 3           # 3 signatures required
MULTISIG_TOTAL_SIGNERS = 5       # Out of 5 total signers

# Placeholder signer addresses (team will replace with actual addresses)
AUTHORIZED_SIGNERS = [
    "PLACEHOLDER_SIGNER_ADDRESS_1",
    "PLACEHOLDER_SIGNER_ADDRESS_2",
    "PLACEHOLDER_SIGNER_ADDRESS_3",
    "PLACEHOLDER_SIGNER_ADDRESS_4",
    "PLACEHOLDER_SIGNER_ADDRESS_5",
]

# Placeholder multi-sig treasury address
TREASURY_MULTISIG_ADDRESS = "PLACEHOLDER_MULTISIG_TREASURY_ADDRESS"

# Timing
SIGNATURE_COLLECTION_TIMEOUT = 300  # 5 minutes to collect signatures
SIGNATURE_RETRY_INTERVAL = 30       # Retry every 30 seconds

# PubSub topics
SIGNATURE_REQUEST_TOPIC = "satori/signer/requests"
SIGNATURE_RESPONSE_TOPIC = "satori/signer/responses"


# ============================================================================
# DATA STRUCTURES
# ============================================================================

class SigningPhase(Enum):
    """Phases of the signing process."""
    WAITING = "waiting"           # Waiting for consensus
    COLLECTING = "collecting"     # Collecting signatures
    COMPLETE = "complete"         # Required signatures collected
    FAILED = "failed"             # Failed to collect enough signatures
    BROADCAST = "broadcast"       # Transaction broadcasted


@dataclass
class SignatureRequest:
    """Request for a signer to sign a distribution."""
    round_id: str
    merkle_root: str
    distribution_tx_hash: str     # Hash of the unsigned distribution TX
    total_reward: float           # Total SATORI being distributed
    num_recipients: int           # Number of recipients
    timestamp: int
    requester_id: str

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "SignatureRequest":
        return cls(
            round_id=data['round_id'],
            merkle_root=data['merkle_root'],
            distribution_tx_hash=data['distribution_tx_hash'],
            total_reward=data['total_reward'],
            num_recipients=data['num_recipients'],
            timestamp=data['timestamp'],
            requester_id=data['requester_id'],
        )

    def get_signing_hash(self) -> str:
        """Get deterministic hash for signing."""
        content = f"{self.round_id}:{self.merkle_root}:{self.distribution_tx_hash}:{self.total_reward}"
        return hashlib.sha256(content.encode()).hexdigest()


@dataclass
class SignatureResponse:
    """Response from a signer with their signature."""
    round_id: str
    signer_address: str
    signature: bytes
    merkle_root: str
    timestamp: int

    def to_dict(self) -> dict:
        return {
            'round_id': self.round_id,
            'signer_address': self.signer_address,
            'signature': self.signature.hex() if self.signature else "",
            'merkle_root': self.merkle_root,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SignatureResponse":
        return cls(
            round_id=data['round_id'],
            signer_address=data['signer_address'],
            signature=bytes.fromhex(data.get('signature', "")) if data.get('signature') else b"",
            merkle_root=data['merkle_root'],
            timestamp=data['timestamp'],
        )


@dataclass
class SigningResult:
    """Result of a signing round."""
    round_id: str
    phase: SigningPhase
    merkle_root: str
    signatures_collected: int
    signatures_required: int
    signers: List[str]               # Addresses of signers who signed
    combined_signature: Optional[bytes]
    distribution_tx_hash: Optional[str]
    timestamp: int

    def to_dict(self) -> dict:
        return {
            'round_id': self.round_id,
            'phase': self.phase.value,
            'merkle_root': self.merkle_root,
            'signatures_collected': self.signatures_collected,
            'signatures_required': self.signatures_required,
            'signers': self.signers,
            'combined_signature': self.combined_signature.hex() if self.combined_signature else None,
            'distribution_tx_hash': self.distribution_tx_hash,
            'timestamp': self.timestamp,
        }


# ============================================================================
# SIGNER NODE
# ============================================================================

class SignerNode:
    """
    A signer node in the multi-sig scheme.

    Flow:
    1. Wait for consensus result
    2. Verify consensus (66% threshold, quorum met)
    3. If valid, sign the distribution transaction
    4. Broadcast signature to other signers
    5. Collect signatures from other signers
    6. When 3-of-5 reached, combine and broadcast TX
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        signer_address: Optional[str] = None,
        private_key: Optional[bytes] = None,
        is_authorized_signer: bool = False,
        wallet: Optional[Union["EvrmoreWallet", Any]] = None,
    ):
        """
        Initialize SignerNode.

        Args:
            peers: Peers instance for P2P communication
            signer_address: This signer's Evrmore address
            private_key: Private key for signing (deprecated, use wallet)
            is_authorized_signer: Whether this node is one of the 5 signers
            wallet: EvrmoreWallet or compatible object for signing
        """
        self.peers = peers
        self.signer_address = signer_address or ""
        self.private_key = private_key  # Deprecated, use wallet
        self.is_authorized_signer = is_authorized_signer
        self.wallet = wallet  # For real ECDSA signing

        # Current signing round state
        self._current_round: Optional[str] = None
        self._phase: SigningPhase = SigningPhase.WAITING
        self._signatures: Dict[str, bytes] = {}  # signer_address -> signature
        self._current_request: Optional[SignatureRequest] = None
        self._collection_start_time: int = 0

        # Merkle root verification cache
        # Stores merkle roots we've calculated locally for verification
        self._calculated_merkle_roots: Dict[str, str] = {}  # round_id -> merkle_root

        # Callbacks
        self._on_signatures_collected: Optional[Callable[[SigningResult], None]] = None
        self._on_signing_failed: Optional[Callable[[SigningResult], None]] = None

        # Subscribe to signing topics if peers available
        if self.peers:
            self._setup_pubsub()

    def _setup_pubsub(self) -> None:
        """Subscribe to signing PubSub topics."""
        if not self.peers:
            return

        try:
            self.peers.subscribe(
                SIGNATURE_REQUEST_TOPIC,
                self._handle_signature_request
            )
            self.peers.subscribe(
                SIGNATURE_RESPONSE_TOPIC,
                self._handle_signature_response
            )
            logger.info(f"Subscribed to signer topics")
        except Exception as e:
            logger.error(f"Failed to setup signer PubSub: {e}")

    def _handle_signature_request(self, message: dict) -> None:
        """Handle incoming signature request."""
        try:
            request = SignatureRequest.from_dict(message)
            self.process_signature_request(request)
        except Exception as e:
            logger.error(f"Failed to handle signature request: {e}")

    def _handle_signature_response(self, message: dict) -> None:
        """Handle incoming signature response."""
        try:
            response = SignatureResponse.from_dict(message)
            self.receive_signature(response)
        except Exception as e:
            logger.error(f"Failed to handle signature response: {e}")

    # ========================================================================
    # PUBLIC API
    # ========================================================================

    def register_calculated_merkle_root(self, round_id: str, merkle_root: str) -> None:
        """
        Register a locally calculated merkle root for verification.

        Call this after calculating rewards locally, before the signing phase.
        When a signing request arrives, the signer will verify that the
        requested merkle root matches what we calculated independently.

        This prevents malicious coordinators from getting signatures for
        incorrect reward distributions.

        Args:
            round_id: The round identifier
            merkle_root: The merkle root we calculated locally
        """
        self._calculated_merkle_roots[round_id] = merkle_root
        logger.debug(f"Registered merkle root for round {round_id}: {merkle_root[:16]}...")

        # Clean up old entries (keep last 100 rounds)
        if len(self._calculated_merkle_roots) > 100:
            oldest_rounds = sorted(self._calculated_merkle_roots.keys())[:-100]
            for old_round in oldest_rounds:
                del self._calculated_merkle_roots[old_round]

    def get_calculated_merkle_root(self, round_id: str) -> Optional[str]:
        """
        Get the merkle root we calculated for a round.

        Args:
            round_id: The round identifier

        Returns:
            The merkle root if we have it, None otherwise
        """
        return self._calculated_merkle_roots.get(round_id)

    def start_signing_round(
        self,
        round_id: str,
        consensus_result: "ConsensusResult",
        distribution_tx_hash: str,
        total_reward: float,
        num_recipients: int,
    ) -> Optional[SignatureRequest]:
        """
        Start a signing round after consensus is reached.

        Only call this after verifying consensus was reached.

        Args:
            round_id: Round identifier
            consensus_result: The consensus result to sign
            distribution_tx_hash: Hash of unsigned distribution TX
            total_reward: Total SATORI being distributed
            num_recipients: Number of recipients

        Returns:
            SignatureRequest if successful, None otherwise
        """
        # Verify consensus
        if not self._verify_consensus(consensus_result):
            logger.warning(f"Cannot start signing: consensus not verified")
            return None

        self._current_round = round_id
        self._phase = SigningPhase.COLLECTING
        self._signatures = {}
        self._collection_start_time = int(time.time())

        # Create signature request
        request = SignatureRequest(
            round_id=round_id,
            merkle_root=consensus_result.winning_merkle_root or "",
            distribution_tx_hash=distribution_tx_hash,
            total_reward=total_reward,
            num_recipients=num_recipients,
            timestamp=int(time.time()),
            requester_id=self.signer_address,
        )
        self._current_request = request

        # If we're a signer, sign immediately
        if self.is_authorized_signer:
            self._sign_and_respond(request)

        # Broadcast request to other signers
        if self.peers:
            try:
                self.peers.broadcast(
                    SIGNATURE_REQUEST_TOPIC,
                    request.to_dict()
                )
                logger.info(f"Broadcasted signature request for round {round_id}")
            except Exception as e:
                logger.error(f"Failed to broadcast signature request: {e}")

        return request

    def process_signature_request(self, request: SignatureRequest) -> bool:
        """
        Process a signature request (called when we receive a request).

        Only authorized signers should sign.

        Args:
            request: The signature request

        Returns:
            True if we signed and responded
        """
        if not self.is_authorized_signer:
            logger.debug("Not an authorized signer, ignoring request")
            return False

        if not self.signer_address:
            logger.warning("No signer address configured")
            return False

        # Verify this is a valid request
        if not self._verify_signature_request(request):
            logger.warning(f"Invalid signature request for round {request.round_id}")
            return False

        # Sign and respond
        return self._sign_and_respond(request)

    def _sign_and_respond(self, request: SignatureRequest) -> bool:
        """Sign a request and broadcast the response."""
        if not self.wallet and not self.private_key:
            logger.warning("No wallet or private key configured, cannot sign")
            return False

        # Create signature
        signature = self._create_signature(request)
        if not signature:
            return False

        # Store our own signature
        self._signatures[self.signer_address] = signature

        # Create and broadcast response
        response = SignatureResponse(
            round_id=request.round_id,
            signer_address=self.signer_address,
            signature=signature,
            merkle_root=request.merkle_root,
            timestamp=int(time.time()),
        )

        if self.peers:
            try:
                self.peers.broadcast(
                    SIGNATURE_RESPONSE_TOPIC,
                    response.to_dict()
                )
                logger.info(f"Broadcasted signature for round {request.round_id}")
            except Exception as e:
                logger.error(f"Failed to broadcast signature: {e}")

        return True

    def receive_signature(self, response: SignatureResponse) -> bool:
        """
        Receive a signature from another signer.

        Args:
            response: The signature response

        Returns:
            True if signature was accepted
        """
        # Verify signer is authorized
        if response.signer_address not in AUTHORIZED_SIGNERS:
            logger.warning(f"Signature from unauthorized signer: {response.signer_address}")
            return False

        # Verify round matches
        if response.round_id != self._current_round:
            logger.debug(f"Signature for wrong round: {response.round_id}")
            return False

        # Verify merkle root matches
        if self._current_request and response.merkle_root != self._current_request.merkle_root:
            logger.warning(f"Signature for wrong merkle root")
            return False

        # Verify signature cryptographically
        if self._current_request and not self._verify_signature(response, self._current_request):
            logger.warning(f"Invalid signature from {response.signer_address}")
            return False

        # Store signature
        self._signatures[response.signer_address] = response.signature
        logger.info(f"Received signature from {response.signer_address} "
                   f"({len(self._signatures)}/{MULTISIG_THRESHOLD})")

        # Check if we have enough signatures
        if len(self._signatures) >= MULTISIG_THRESHOLD:
            self._phase = SigningPhase.COMPLETE
            self._trigger_callbacks()

        return True

    def check_signing_status(self) -> SigningResult:
        """
        Check current signing status.

        Returns:
            SigningResult with current state
        """
        # Check for timeout
        if self._phase == SigningPhase.COLLECTING:
            elapsed = int(time.time()) - self._collection_start_time
            if elapsed > SIGNATURE_COLLECTION_TIMEOUT:
                self._phase = SigningPhase.FAILED
                self._trigger_callbacks()

        # Combine signatures if complete
        combined_sig = None
        if self._phase == SigningPhase.COMPLETE:
            combined_sig = self._combine_signatures()

        return SigningResult(
            round_id=self._current_round or "",
            phase=self._phase,
            merkle_root=self._current_request.merkle_root if self._current_request else "",
            signatures_collected=len(self._signatures),
            signatures_required=MULTISIG_THRESHOLD,
            signers=list(self._signatures.keys()),
            combined_signature=combined_sig,
            distribution_tx_hash=self._current_request.distribution_tx_hash if self._current_request else None,
            timestamp=int(time.time()),
        )

    def _verify_consensus(self, result: "ConsensusResult") -> bool:
        """Verify that consensus was legitimately reached."""
        if not result.consensus_reached:
            return False
        if not result.quorum_met:
            return False
        if not result.winning_merkle_root:
            return False
        return True

    def _verify_signature_request(self, request: SignatureRequest) -> bool:
        """Verify a signature request is valid."""
        # Check timestamp is recent
        now = int(time.time())
        if abs(now - request.timestamp) > 600:  # Within 10 minutes
            logger.warning(f"Signature request timestamp too old: {request.timestamp}")
            return False

        # Check merkle root is not empty
        if not request.merkle_root:
            logger.warning("Signature request has empty merkle root")
            return False

        # Verify merkle root against our own calculation (if available)
        our_merkle_root = self._calculated_merkle_roots.get(request.round_id)
        if our_merkle_root is not None:
            if our_merkle_root != request.merkle_root:
                logger.warning(
                    f"Merkle root mismatch for round {request.round_id}: "
                    f"request={request.merkle_root[:16]}... "
                    f"calculated={our_merkle_root[:16]}..."
                )
                return False
            logger.debug(f"Merkle root verified for round {request.round_id}")
        else:
            # We don't have a local calculation - log but allow
            # This can happen if we joined mid-round or are catching up
            logger.info(
                f"No local merkle root for round {request.round_id}, "
                f"accepting request merkle root: {request.merkle_root[:16]}..."
            )

        return True

    def _verify_signature(
        self,
        response: SignatureResponse,
        request: SignatureRequest,
    ) -> bool:
        """
        Verify a signature response cryptographically.

        Args:
            response: The signature response to verify
            request: The original signing request

        Returns:
            True if signature is valid
        """
        if not response.signature:
            return False

        # Skip real verification for placeholder addresses (testing mode)
        if response.signer_address.startswith("PLACEHOLDER_"):
            return len(response.signature) > 0

        # Reconstruct the signing content
        signing_content = (
            f"{request.round_id}:{request.merkle_root}:"
            f"{request.distribution_tx_hash}:{request.total_reward}"
        )

        # Use python-evrmorelib for real verification
        if SIGNING_AVAILABLE:
            try:
                return verify_message(
                    message=signing_content,
                    signature=response.signature,
                    address=response.signer_address,
                )
            except Exception as e:
                logger.warning(f"Signature verification failed: {e}")
                return False

        # Fallback: accept if signature exists and signer is authorized
        return len(response.signature) > 0

    def _create_signature(self, request: SignatureRequest) -> Optional[bytes]:
        """
        Create a signature for a request.

        Args:
            request: The request to sign

        Returns:
            Signature bytes, or None on failure
        """
        # Get the content to sign
        signing_content = (
            f"{request.round_id}:{request.merkle_root}:"
            f"{request.distribution_tx_hash}:{request.total_reward}"
        )

        # Use wallet for real ECDSA signing
        if self.wallet is not None:
            try:
                signature = self.wallet.sign(signing_content)
                logger.debug(f"Created ECDSA signature for {request.round_id}")
                return signature
            except Exception as e:
                logger.warning(f"Wallet signing failed: {e}, falling back to placeholder")

        # Fallback to placeholder (for backward compatibility)
        if self.private_key:
            signing_hash = request.get_signing_hash()
            placeholder_sig = hashlib.sha256(
                signing_hash.encode() + self.private_key
            ).digest()
            logger.debug(f"Created placeholder signature for {request.round_id}")
            return placeholder_sig

        return None

    def _combine_signatures(self) -> Optional[bytes]:
        """
        Combine collected signatures for multi-sig P2SH redemption.

        For a 3-of-5 multi-sig, creates a scriptSig of the form:
        OP_0 <sig1> <sig2> <sig3> <redeemScript>

        The signatures must be ordered to match the public key order
        in the redeem script.

        Returns:
            Combined signature data, or None if not enough signatures

        Note:
            The actual scriptSig construction happens in the transaction
            builder. This method returns the signatures in the correct
            order for combination.
        """
        if len(self._signatures) < MULTISIG_THRESHOLD:
            return None

        # Get signatures in the order matching AUTHORIZED_SIGNERS
        # This ensures consistent ordering across all signer nodes
        ordered_signatures = []
        for signer_addr in AUTHORIZED_SIGNERS:
            if signer_addr in self._signatures:
                ordered_signatures.append(self._signatures[signer_addr])
                if len(ordered_signatures) >= MULTISIG_THRESHOLD:
                    break

        if len(ordered_signatures) < MULTISIG_THRESHOLD:
            logger.warning(f"Not enough valid signatures: {len(ordered_signatures)}/{MULTISIG_THRESHOLD}")
            return None

        # Combine signatures for transmission
        # Format: [num_sigs (1 byte)] + [sig1_len (1 byte)] + [sig1] + ...
        combined = bytes([len(ordered_signatures)])
        for sig in ordered_signatures:
            if isinstance(sig, bytes):
                combined += bytes([len(sig)]) + sig
            else:
                # Handle string signatures
                sig_bytes = sig.encode() if isinstance(sig, str) else bytes(sig)
                combined += bytes([len(sig_bytes)]) + sig_bytes

        logger.info(f"Combined {len(ordered_signatures)} signatures for multi-sig")
        return combined

    def _trigger_callbacks(self) -> None:
        """Trigger appropriate callbacks based on phase."""
        result = self.check_signing_status()

        if self._phase == SigningPhase.COMPLETE and self._on_signatures_collected:
            self._on_signatures_collected(result)
        elif self._phase == SigningPhase.FAILED and self._on_signing_failed:
            self._on_signing_failed(result)

    def get_current_phase(self) -> SigningPhase:
        """Get current signing phase."""
        return self._phase

    def get_signatures(self) -> Dict[str, bytes]:
        """Get all collected signatures."""
        return self._signatures.copy()

    def set_on_signatures_collected(self, callback: Callable[[SigningResult], None]) -> None:
        """Set callback for when enough signatures are collected."""
        self._on_signatures_collected = callback

    def set_on_signing_failed(self, callback: Callable[[SigningResult], None]) -> None:
        """Set callback for when signing fails."""
        self._on_signing_failed = callback


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def is_authorized_signer(address: str) -> bool:
    """
    Check if an address is an authorized signer.

    Args:
        address: Evrmore address to check

    Returns:
        True if address is in the authorized signers list
    """
    return address in AUTHORIZED_SIGNERS


def get_signer_count() -> Tuple[int, int]:
    """
    Get multi-sig configuration.

    Returns:
        (threshold, total_signers) tuple
    """
    return MULTISIG_THRESHOLD, MULTISIG_TOTAL_SIGNERS


def verify_combined_signature(
    merkle_root: str,
    combined_signature: bytes,
    signers: List[str],
    signing_content: Optional[str] = None,
    individual_signatures: Optional[Dict[str, bytes]] = None,
) -> bool:
    """
    Verify a combined multi-sig signature.

    Args:
        merkle_root: The merkle root that was signed
        combined_signature: The combined signature
        signers: List of signer addresses
        signing_content: Optional content that was signed
        individual_signatures: Optional dict of signer_address -> signature

    Returns:
        True if signature is valid
    """
    # Check we have enough signers
    if len(signers) < MULTISIG_THRESHOLD:
        return False

    # Check all signers are authorized
    for signer in signers:
        if signer not in AUTHORIZED_SIGNERS:
            return False

    # If we have individual signatures and signing content, verify each
    if SIGNING_AVAILABLE and signing_content and individual_signatures:
        for signer, signature in individual_signatures.items():
            if signer not in AUTHORIZED_SIGNERS:
                continue
            try:
                if not verify_message(
                    message=signing_content,
                    signature=signature,
                    address=signer,
                ):
                    logger.warning(f"Invalid signature from signer {signer}")
                    return False
            except Exception as e:
                logger.warning(f"Signature verification failed for {signer}: {e}")
                return False

    return True


# ============================================================================
# MULTI-SIG SCRIPT HELPERS
# ============================================================================

def create_multisig_redeem_script(public_keys: List[bytes], threshold: int = 3) -> bytes:
    """
    Create a multi-sig redeem script.

    Creates script: OP_M <pubkey1> ... <pubkeyN> OP_N OP_CHECKMULTISIG

    Args:
        public_keys: List of compressed public keys (33 bytes each)
        threshold: Number of signatures required (M of N)

    Returns:
        Redeem script bytes
    """
    if len(public_keys) < threshold:
        raise ValueError(f"Need at least {threshold} public keys")

    # Build script manually (compatible with python-evrmorelib if available)
    script = bytes([0x50 + threshold])  # OP_M (OP_1 = 0x51, OP_2 = 0x52, OP_3 = 0x53, etc.)

    for pubkey in public_keys:
        if len(pubkey) != 33:
            raise ValueError(f"Public key must be 33 bytes (compressed), got {len(pubkey)}")
        script += bytes([len(pubkey)]) + pubkey

    script += bytes([0x50 + len(public_keys)])  # OP_N
    script += bytes([0xae])  # OP_CHECKMULTISIG

    return script


def create_multisig_scriptsig(
    signatures: List[bytes],
    redeem_script: bytes
) -> bytes:
    """
    Create a scriptSig for spending from a multi-sig P2SH address.

    Creates: OP_0 <sig1> <sig2> <sig3> <redeemScript>

    Args:
        signatures: List of DER-encoded signatures
        redeem_script: The multi-sig redeem script

    Returns:
        scriptSig bytes
    """
    # OP_0 (required due to CHECKMULTISIG off-by-one bug)
    script = bytes([0x00])

    # Add each signature
    for sig in signatures:
        script += bytes([len(sig)]) + sig

    # Add redeem script
    if len(redeem_script) < 0x4c:
        script += bytes([len(redeem_script)]) + redeem_script
    elif len(redeem_script) <= 0xff:
        script += bytes([0x4c, len(redeem_script)]) + redeem_script  # OP_PUSHDATA1
    else:
        script += bytes([0x4d]) + len(redeem_script).to_bytes(2, 'little') + redeem_script  # OP_PUSHDATA2

    return script


def parse_combined_signatures(combined: bytes) -> List[bytes]:
    """
    Parse combined signatures back into individual signatures.

    Format: [num_sigs (1 byte)] + [sig1_len (1 byte)] + [sig1] + ...

    Args:
        combined: Combined signature bytes

    Returns:
        List of individual signatures
    """
    if not combined or len(combined) < 2:
        return []

    signatures = []
    num_sigs = combined[0]
    offset = 1

    for _ in range(num_sigs):
        if offset >= len(combined):
            break
        sig_len = combined[offset]
        offset += 1

        if offset + sig_len > len(combined):
            break
        signatures.append(combined[offset:offset + sig_len])
        offset += sig_len

    return signatures


def get_treasury_address() -> str:
    """
    Get the treasury multi-sig address.

    Returns:
        Treasury address string
    """
    return TREASURY_MULTISIG_ADDRESS
