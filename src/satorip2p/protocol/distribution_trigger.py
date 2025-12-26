"""
satorip2p/protocol/distribution_trigger.py

Automatic distribution trigger when consensus is reached.

This module coordinates the flow from consensus to actual reward distribution:
1. Listen for consensus results
2. When consensus reached, trigger signer nodes
3. Collect signatures (3-of-5)
4. Build and broadcast distribution transaction
5. Record on-chain merkle root

Key Features:
- Auto-trigger on consensus
- Coordinates with signer nodes
- Handles timeout/failure cases
- Integrates with reward distributor
"""

import logging
import time
from typing import Dict, List, Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass, asdict
from enum import Enum

if TYPE_CHECKING:
    from ..peers import Peers
    from .consensus import ConsensusResult, ConsensusManager
    from .signer import SignerNode, SigningResult
    from .rewards import RoundSummary, RewardCalculator
    from ..electrumx.client import ElectrumXClient
    from ..blockchain.tx_builder import UnsignedTransaction

# Import ElectrumX and TX builder (optional - for real blockchain interaction)
try:
    from ..electrumx.client import ElectrumXClient, create_client, ElectrumXError
    from ..blockchain.tx_builder import TransactionBuilder, UnsignedTransaction
    BLOCKCHAIN_AVAILABLE = True
except ImportError:
    BLOCKCHAIN_AVAILABLE = False

logger = logging.getLogger("satorip2p.protocol.distribution_trigger")


# ============================================================================
# CONSTANTS - PLACEHOLDERS (Team will configure)
# ============================================================================

# Distribution timing
POST_CONSENSUS_DELAY = 10        # Seconds to wait after consensus before triggering
DISTRIBUTION_TIMEOUT = 600       # 10 minutes max for distribution process
RETRY_DELAY = 60                 # Retry after 1 minute on failure

# PubSub topics
DISTRIBUTION_TRIGGER_TOPIC = "satori/distribution/trigger"
DISTRIBUTION_STATUS_TOPIC = "satori/distribution/status"

# NOTE: We use ElectrumX for all blockchain interaction (no RPC needed)
# ElectrumX servers are configured in satorip2p/electrumx/client.py


# ============================================================================
# DATA STRUCTURES
# ============================================================================

class DistributionPhase(Enum):
    """Phases of the distribution process."""
    IDLE = "idle"                      # Waiting for consensus
    CONSENSUS_REACHED = "consensus"    # Consensus confirmed
    WAITING_SIGNATURES = "signing"     # Collecting signatures
    BROADCASTING = "broadcasting"      # Broadcasting TX
    CONFIRMED = "confirmed"            # TX confirmed on-chain
    FAILED = "failed"                  # Distribution failed


@dataclass
class DistributionStatus:
    """Status of a distribution attempt."""
    round_id: str
    phase: DistributionPhase
    merkle_root: str
    total_reward: float
    num_recipients: int
    signatures_collected: int
    signatures_required: int
    tx_hash: Optional[str]
    error_message: Optional[str]
    timestamp: int

    def to_dict(self) -> dict:
        return {
            'round_id': self.round_id,
            'phase': self.phase.value,
            'merkle_root': self.merkle_root,
            'total_reward': self.total_reward,
            'num_recipients': self.num_recipients,
            'signatures_collected': self.signatures_collected,
            'signatures_required': self.signatures_required,
            'tx_hash': self.tx_hash,
            'error_message': self.error_message,
            'timestamp': self.timestamp,
        }


# ============================================================================
# DISTRIBUTION TRIGGER
# ============================================================================

class DistributionTrigger:
    """
    Coordinates automatic reward distribution after consensus.

    Flow:
    1. Consensus manager reports consensus reached
    2. Trigger builds unsigned distribution TX
    3. Request signatures from signer nodes
    4. Combine signatures and broadcast TX
    5. Confirm TX and update status
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        signer_node: Optional["SignerNode"] = None,
        consensus_manager: Optional["ConsensusManager"] = None,
        electrumx_client: Optional["ElectrumXClient"] = None,
        treasury_address: Optional[str] = None,
    ):
        """
        Initialize DistributionTrigger.

        Args:
            peers: Peers instance for P2P communication
            signer_node: SignerNode for multi-sig coordination
            consensus_manager: ConsensusManager for consensus events
            electrumx_client: Optional ElectrumX client for blockchain interaction
            treasury_address: Treasury address for SATORI distribution
        """
        self.peers = peers
        self.signer_node = signer_node
        self.consensus_manager = consensus_manager
        self.electrumx_client = electrumx_client
        self.treasury_address = treasury_address

        # Current distribution state
        self._current_round: Optional[str] = None
        self._phase: DistributionPhase = DistributionPhase.IDLE
        self._round_summary: Optional["RoundSummary"] = None
        self._consensus_result: Optional["ConsensusResult"] = None
        self._start_time: int = 0
        self._tx_hash: Optional[str] = None
        self._error_message: Optional[str] = None
        self._unsigned_tx: Optional["UnsignedTransaction"] = None

        # Callbacks
        self._on_distribution_complete: Optional[Callable[[DistributionStatus], None]] = None
        self._on_distribution_failed: Optional[Callable[[DistributionStatus], None]] = None

        # Wire up consensus callback if manager provided
        if self.consensus_manager:
            self.consensus_manager.set_on_consensus_reached(self._on_consensus_reached)

        # Wire up signer callback if signer provided
        if self.signer_node:
            self.signer_node.set_on_signatures_collected(self._on_signatures_collected)
            self.signer_node.set_on_signing_failed(self._on_signing_failed)

    # ========================================================================
    # PUBLIC API
    # ========================================================================

    def trigger_distribution(
        self,
        round_summary: "RoundSummary",
        consensus_result: "ConsensusResult",
    ) -> DistributionStatus:
        """
        Manually trigger a distribution.

        Usually called automatically when consensus is reached,
        but can be called manually for recovery/retry.

        Args:
            round_summary: The calculated rewards for this round
            consensus_result: The consensus result

        Returns:
            Current distribution status
        """
        logger.info(f"Triggering distribution for round {round_summary.round_id}")

        # Verify consensus
        if not consensus_result.consensus_reached:
            self._phase = DistributionPhase.FAILED
            self._error_message = "Consensus not reached"
            return self.get_status()

        if consensus_result.winning_merkle_root != round_summary.merkle_root:
            self._phase = DistributionPhase.FAILED
            self._error_message = "Merkle root mismatch"
            return self.get_status()

        # Initialize state
        self._current_round = round_summary.round_id
        self._phase = DistributionPhase.CONSENSUS_REACHED
        self._round_summary = round_summary
        self._consensus_result = consensus_result
        self._start_time = int(time.time())
        self._tx_hash = None
        self._error_message = None

        # Build unsigned distribution transaction
        unsigned_tx_hash = self._build_unsigned_tx(round_summary)
        if not unsigned_tx_hash:
            self._phase = DistributionPhase.FAILED
            self._error_message = "Failed to build transaction"
            return self.get_status()

        # Request signatures from signer nodes
        self._phase = DistributionPhase.WAITING_SIGNATURES
        self._request_signatures(round_summary, consensus_result, unsigned_tx_hash)

        return self.get_status()

    def get_status(self) -> DistributionStatus:
        """
        Get current distribution status.

        Returns:
            DistributionStatus with current state
        """
        return DistributionStatus(
            round_id=self._current_round or "",
            phase=self._phase,
            merkle_root=self._round_summary.merkle_root if self._round_summary else "",
            total_reward=self._round_summary.total_reward_pool if self._round_summary else 0.0,
            num_recipients=len(self._round_summary.rewards) if self._round_summary else 0,
            signatures_collected=len(self.signer_node.get_signatures()) if self.signer_node else 0,
            signatures_required=3,  # MULTISIG_THRESHOLD
            tx_hash=self._tx_hash,
            error_message=self._error_message,
            timestamp=int(time.time()),
        )

    def check_timeout(self) -> bool:
        """
        Check if distribution has timed out.

        Returns:
            True if timed out
        """
        if self._phase == DistributionPhase.IDLE:
            return False

        if self._start_time == 0:
            return False

        elapsed = int(time.time()) - self._start_time
        if elapsed > DISTRIBUTION_TIMEOUT:
            self._phase = DistributionPhase.FAILED
            self._error_message = "Distribution timed out"
            if self._on_distribution_failed:
                self._on_distribution_failed(self.get_status())
            return True

        return False

    # ========================================================================
    # INTERNAL METHODS
    # ========================================================================

    def _on_consensus_reached(self, result: "ConsensusResult") -> None:
        """Called when consensus is reached."""
        logger.info(f"Consensus reached for round {result.round_id}")

        # Wait a short delay before triggering
        # (In production, would use async/scheduler)
        time.sleep(POST_CONSENSUS_DELAY)

        # We need the round summary to trigger distribution
        # This should be passed or retrieved from RoundDataStore
        logger.info("Distribution trigger: waiting for round summary")
        # In practice, this would be retrieved from DHT or passed separately

    def _on_signatures_collected(self, signing_result: "SigningResult") -> None:
        """Called when enough signatures are collected."""
        logger.info(f"Signatures collected for round {signing_result.round_id}")

        # Build and broadcast the signed transaction
        self._phase = DistributionPhase.BROADCASTING
        success = self._broadcast_distribution_tx(signing_result)

        if success:
            self._phase = DistributionPhase.CONFIRMED
            if self._on_distribution_complete:
                self._on_distribution_complete(self.get_status())
        else:
            self._phase = DistributionPhase.FAILED
            self._error_message = "Failed to broadcast transaction"
            if self._on_distribution_failed:
                self._on_distribution_failed(self.get_status())

    def _on_signing_failed(self, signing_result: "SigningResult") -> None:
        """Called when signing fails."""
        logger.error(f"Signing failed for round {signing_result.round_id}")
        self._phase = DistributionPhase.FAILED
        self._error_message = f"Failed to collect signatures ({signing_result.signatures_collected}/{signing_result.signatures_required})"
        if self._on_distribution_failed:
            self._on_distribution_failed(self.get_status())

    def _build_unsigned_tx(self, round_summary: "RoundSummary") -> Optional[str]:
        """
        Build an unsigned distribution transaction.

        Uses ElectrumX for UTXO queries and python-evrmorelib for
        transaction construction.

        Args:
            round_summary: The round summary with rewards

        Returns:
            Transaction hash (for signing) or None on failure
        """
        logger.info(f"Building unsigned TX for {len(round_summary.rewards)} recipients, "
                   f"total {round_summary.total_reward_pool} SATORI")

        # Check if blockchain interaction is available
        if not BLOCKCHAIN_AVAILABLE:
            logger.warning("Blockchain modules not available, using placeholder TX")
            import hashlib
            tx_hash = hashlib.sha256(round_summary.merkle_root.encode()).hexdigest()
            return tx_hash

        # Check if we have ElectrumX client and treasury configured
        if not self.electrumx_client or not self.treasury_address:
            logger.warning("ElectrumX client or treasury not configured, using placeholder TX")
            import hashlib
            tx_hash = hashlib.sha256(round_summary.merkle_root.encode()).hexdigest()
            return tx_hash

        try:
            # Build recipient map from rewards
            recipients: Dict[str, float] = {}
            for reward in round_summary.rewards:
                if reward.amount > 0:
                    addr = reward.address
                    recipients[addr] = recipients.get(addr, 0) + reward.amount

            if not recipients:
                logger.error("No recipients with positive rewards")
                return None

            # Ensure ElectrumX is connected
            if not self.electrumx_client.connected:
                logger.info("Connecting to ElectrumX...")
                if not self.electrumx_client.connect():
                    logger.error("Failed to connect to ElectrumX")
                    return None

            # Build unsigned transaction
            builder = TransactionBuilder(
                electrumx_client=self.electrumx_client,
                treasury_address=self.treasury_address,
            )

            self._unsigned_tx = builder.build_distribution_tx(
                recipients=recipients,
                merkle_root=round_summary.merkle_root,
                round_id=round_summary.round_id,
            )

            logger.info(f"Built unsigned TX: {self._unsigned_tx.tx_hash} "
                       f"({len(recipients)} recipients, fee: {self._unsigned_tx.fee} sat)")

            return self._unsigned_tx.tx_hash

        except Exception as e:
            logger.error(f"Failed to build transaction: {e}")
            self._error_message = str(e)
            return None

    def _request_signatures(
        self,
        round_summary: "RoundSummary",
        consensus_result: "ConsensusResult",
        unsigned_tx_hash: str,
    ) -> None:
        """Request signatures from signer nodes."""
        if not self.signer_node:
            logger.error("No signer node configured")
            return

        self.signer_node.start_signing_round(
            round_id=round_summary.round_id,
            consensus_result=consensus_result,
            distribution_tx_hash=unsigned_tx_hash,
            total_reward=round_summary.total_reward_pool,
            num_recipients=len(round_summary.rewards),
        )

    def _broadcast_distribution_tx(self, signing_result: "SigningResult") -> bool:
        """
        Broadcast the signed distribution transaction.

        Uses ElectrumX to broadcast the fully-signed transaction to the
        Evrmore network.

        Args:
            signing_result: The signing result with combined signature

        Returns:
            True if broadcast successful
        """
        if not signing_result.combined_signature:
            logger.error("No combined signature available")
            return False

        logger.info(f"Broadcasting distribution TX with {signing_result.signatures_collected} signatures")

        # Check if we have ElectrumX client
        if not self.electrumx_client:
            logger.warning("No ElectrumX client configured, cannot broadcast")
            # Still mark as successful for testing without blockchain
            self._tx_hash = signing_result.distribution_tx_hash
            return True

        # Check if we have the unsigned transaction
        if not self._unsigned_tx:
            logger.error("No unsigned transaction available")
            return False

        try:
            # Ensure connected
            if not self.electrumx_client.connected:
                if not self.electrumx_client.connect():
                    logger.error("Failed to connect to ElectrumX for broadcast")
                    return False

            # The signed transaction hex should be constructed by combining
            # the unsigned transaction with the multi-sig signatures.
            # For now, we use the combined_signature from the signing result.
            #
            # In a full implementation, this would:
            # 1. Take the unsigned TX from self._unsigned_tx.tx_hex
            # 2. Add the scriptSig with multi-sig redemption script + signatures
            # 3. Serialize the complete signed transaction
            #
            # For the MVP, the signer nodes would provide the fully signed TX hex
            # after combining their signatures.

            signed_tx_hex = self._build_signed_tx(signing_result)
            if not signed_tx_hex:
                logger.error("Failed to build signed transaction")
                return False

            # Broadcast via ElectrumX
            txid = self.electrumx_client.broadcast(signed_tx_hex)

            logger.info(f"Transaction broadcast successful: {txid}")
            self._tx_hash = txid

            return True

        except Exception as e:
            logger.error(f"Failed to broadcast transaction: {e}")
            self._error_message = str(e)
            return False

    def _build_signed_tx(self, signing_result: "SigningResult") -> Optional[str]:
        """
        Build the fully signed transaction from unsigned TX and signatures.

        For multi-sig P2SH, this combines the partial signatures into
        a complete scriptSig.

        Args:
            signing_result: The signing result with signatures

        Returns:
            Hex-encoded signed transaction, or None on failure
        """
        if not self._unsigned_tx:
            return None

        # This is where multi-sig combination happens
        # For a 3-of-5 multi-sig, we need:
        # 1. The redeem script (OP_3 <pubkey1> ... <pubkey5> OP_5 OP_CHECKMULTISIG)
        # 2. At least 3 signatures
        # 3. scriptSig = OP_0 <sig1> <sig2> <sig3> <redeemScript>

        # For MVP, if we have a combined_signature from the signer node,
        # assume it's the fully signed transaction hex
        if signing_result.combined_signature:
            # Check if combined_signature looks like a full TX hex
            sig_hex = signing_result.combined_signature.hex() if isinstance(
                signing_result.combined_signature, bytes
            ) else str(signing_result.combined_signature)

            # If it's long enough to be a transaction, use it
            if len(sig_hex) > 100:
                return sig_hex

        # Otherwise, return the unsigned TX (for testing without full multi-sig)
        logger.warning("Using unsigned TX for broadcast (multi-sig not fully implemented)")
        return self._unsigned_tx.tx_hex

    # ========================================================================
    # CALLBACKS
    # ========================================================================

    def set_on_distribution_complete(self, callback: Callable[[DistributionStatus], None]) -> None:
        """Set callback for when distribution completes successfully."""
        self._on_distribution_complete = callback

    def set_on_distribution_failed(self, callback: Callable[[DistributionStatus], None]) -> None:
        """Set callback for when distribution fails."""
        self._on_distribution_failed = callback


# ============================================================================
# COORDINATOR
# ============================================================================

class RewardCoordinator:
    """
    High-level coordinator for the entire reward distribution flow.

    Manages:
    - Round timing (00:00 UTC)
    - Score calculation
    - Consensus voting
    - Signature collection
    - Distribution trigger

    This is the main entry point for Neuron/satori-lite integration.
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        node_id: Optional[str] = None,
        signer_address: Optional[str] = None,
        is_signer: bool = False,
    ):
        """
        Initialize RewardCoordinator.

        Args:
            peers: Peers instance
            node_id: This node's ID
            signer_address: Signer address if this is a signer node
            is_signer: Whether this node is an authorized signer
        """
        self.peers = peers
        self.node_id = node_id or "unknown"

        # Initialize components
        from .consensus import ConsensusManager
        from .signer import SignerNode

        self.consensus_manager = ConsensusManager(
            peers=peers,
            node_id=node_id,
        )

        self.signer_node = SignerNode(
            peers=peers,
            signer_address=signer_address,
            is_authorized_signer=is_signer,
        ) if is_signer else None

        self.distribution_trigger = DistributionTrigger(
            peers=peers,
            signer_node=self.signer_node,
            consensus_manager=self.consensus_manager,
        )

        # Current round state
        self._current_round_id: Optional[str] = None
        self._round_summary: Optional["RoundSummary"] = None

    def start_round(self, round_id: str) -> None:
        """
        Start a new reward round.

        Called at 00:00 UTC when the prediction round ends.

        Args:
            round_id: Unique round identifier (e.g., "2025-01-15")
        """
        logger.info(f"Starting reward round: {round_id}")
        self._current_round_id = round_id

        # Start consensus voting
        self.consensus_manager.start_round(round_id)

    def submit_calculated_rewards(
        self,
        round_summary: "RoundSummary",
    ) -> bool:
        """
        Submit our calculated rewards and vote in consensus.

        Called after local score calculation is complete.

        Args:
            round_summary: Our calculated round summary

        Returns:
            True if vote was submitted
        """
        self._round_summary = round_summary

        # Submit our vote with the merkle root
        vote = self.consensus_manager.submit_vote(round_summary.merkle_root)
        if vote:
            logger.info(f"Submitted consensus vote for merkle_root={round_summary.merkle_root}")
            return True

        return False

    def check_and_trigger_distribution(self) -> Optional[DistributionStatus]:
        """
        Check consensus and trigger distribution if ready.

        Should be called periodically during voting phase.

        Returns:
            Distribution status if triggered, None otherwise
        """
        result = self.consensus_manager.check_consensus()

        if result.consensus_reached and self._round_summary:
            # Verify our merkle root matches consensus
            if result.winning_merkle_root == self._round_summary.merkle_root:
                return self.distribution_trigger.trigger_distribution(
                    self._round_summary,
                    result,
                )
            else:
                logger.warning(
                    f"Our merkle root doesn't match consensus: "
                    f"ours={self._round_summary.merkle_root} "
                    f"consensus={result.winning_merkle_root if result.winning_merkle_root else 'None'}"
                )

        return None

    def get_consensus_status(self) -> Dict:
        """Get current consensus status."""
        result = self.consensus_manager.check_consensus()
        return result.to_dict()

    def get_distribution_status(self) -> Dict:
        """Get current distribution status."""
        return self.distribution_trigger.get_status().to_dict()
