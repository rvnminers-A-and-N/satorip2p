"""
satorip2p/protocol/rewards.py

Decentralized reward scoring and calculation protocol.

Implements the Satori Hybrid MCP/Continuous scoring model:
- Phase 1: MCP Inhibitory Gate (binary check - any inhibitor = disqualification)
- Phase 2: Continuous Weighted Scoring (sigmoid activation on weighted factors)

The scoring algorithm is deterministic - any node can verify scores independently.

Reward distribution is blockchain-agnostic:
- Currently uses Evrmore (SATORI is an EVR asset)
- Abstracted for future SAT chain or other backends

Round boundaries: 00:00 UTC to 23:59:59 UTC daily
Distribution time: 00:00 UTC (after round ends)

Usage:
    from satorip2p.protocol.rewards import SatoriScorer, RewardCalculator

    scorer = SatoriScorer()
    breakdown = scorer.calculate_score(prediction_input)

    calculator = RewardCalculator(scorer)
    rewards = calculator.calculate_round_rewards(predictions, reward_pool)

Reference:
    McCulloch & Pitts (1943), "A logical calculus of the ideas
    immanent in nervous activity"
"""

import logging
import time
import math
import json
import hashlib
from typing import Dict, List, Optional, Set, Tuple, Any, TYPE_CHECKING
from dataclasses import dataclass, field, asdict
from enum import Enum
from abc import ABC, abstractmethod
from datetime import datetime, timezone

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.rewards")


# ============================================================================
# ROLE MULTIPLIER CONSTANTS
# ============================================================================

# Role bonus multipliers (added to base 1.0x)
ROLE_BONUS_RELAY = 0.05     # +5% for relay nodes (≥95% uptime)
ROLE_BONUS_ORACLE = 0.10    # +10% for oracles (observation used + matched)
ROLE_BONUS_SIGNER = 0.10    # +10% for signers (signature included)
ROLE_MULTIPLIER_CAP = 1.25  # Maximum multiplier (25% bonus cap)

# Relay uptime threshold
RELAY_UPTIME_THRESHOLD = 0.95  # 95% uptime required for bonus


@dataclass
class NodeRoles:
    """
    Role qualification data for a node.

    Used to calculate reward multipliers based on active roles.
    """
    node_id: str
    is_predictor: bool = True  # Must be true to earn rewards
    is_relay: bool = False
    is_oracle: bool = False
    is_signer: bool = False

    # Qualification flags (did they actually perform the role this round?)
    relay_qualified: bool = False   # Met 95% uptime
    oracle_qualified: bool = False  # Observation used and matched consensus
    signer_qualified: bool = False  # Signature included in distribution

    # Supporting data
    uptime_percentage: float = 0.0  # 0.0 to 1.0

    def get_multiplier(self) -> float:
        """
        Calculate reward multiplier based on qualified roles.

        Returns:
            Multiplier between 1.0 and 1.25
        """
        multiplier = 1.0

        if self.relay_qualified:
            multiplier += ROLE_BONUS_RELAY

        if self.oracle_qualified:
            multiplier += ROLE_BONUS_ORACLE

        if self.signer_qualified:
            multiplier += ROLE_BONUS_SIGNER

        return min(multiplier, ROLE_MULTIPLIER_CAP)


def get_role_multiplier(
    node_id: str,
    role_data: Optional[Dict[str, NodeRoles]] = None
) -> float:
    """
    Get reward multiplier for a node based on their roles.

    Args:
        node_id: Node identifier (address)
        role_data: Dict mapping node_id to NodeRoles

    Returns:
        Multiplier between 1.0 and 1.25
    """
    if not role_data or node_id not in role_data:
        return 1.0

    return role_data[node_id].get_multiplier()


def check_relay_qualified(uptime: float) -> bool:
    """Check if relay uptime meets threshold."""
    return uptime >= RELAY_UPTIME_THRESHOLD


def check_oracle_qualified(
    node_observation: Optional[float],
    consensus_observation: float,
    tolerance: float = 0.01
) -> bool:
    """
    Check if oracle observation qualified for bonus.

    Args:
        node_observation: Value this node provided
        consensus_observation: Consensus observation value
        tolerance: Allowed deviation (default 1%)

    Returns:
        True if observation was used and matched consensus
    """
    if node_observation is None:
        return False

    if consensus_observation == 0:
        return node_observation == 0

    deviation = abs(node_observation - consensus_observation) / abs(consensus_observation)
    return deviation <= tolerance


def check_signer_qualified(
    node_id: str,
    included_signers: List[str]
) -> bool:
    """Check if signer's signature was included in distribution."""
    return node_id in included_signers


# ============================================================================
# DATA STRUCTURES
# ============================================================================

class ScoringResult(Enum):
    """Outcome of scoring attempt."""
    SUCCESS = "success"
    INHIBITED = "inhibited"  # Failed Phase 1 (MCP inhibitor fired)


@dataclass
class PredictionInput:
    """
    All data needed to score a prediction.

    This is the input to the scoring algorithm.
    """
    # Core prediction data
    predicted_value: float
    actual_value: float

    # Timing
    commit_time: int          # Unix timestamp of commit
    round_start: int          # Unix timestamp (00:00 UTC)
    deadline: int             # Unix timestamp (23:59:59 UTC)

    # Predictor metadata
    stated_confidence: float  # 0.0 to 1.0
    predictor_reputation: float  # 0.0 to 1.0
    predictor_address: str
    stake: float

    # Validation data
    signature: bytes
    stream_id: str
    round_id: str

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        # Convert bytes to hex string for JSON serialization
        result['signature'] = self.signature.hex() if self.signature else ""
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "PredictionInput":
        """Create from dictionary."""
        # Convert hex string back to bytes
        if isinstance(data.get('signature'), str):
            data['signature'] = bytes.fromhex(data['signature']) if data['signature'] else b""
        return cls(**data)


@dataclass
class InhibitorResult:
    """Result of a single inhibitor check."""
    name: str
    fired: bool
    reason: str

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class ScoreBreakdown:
    """Detailed breakdown of score components."""
    # Phase 1 results
    inhibitor_results: List[InhibitorResult]
    passed_phase1: bool

    # Phase 2 results (only if passed Phase 1)
    accuracy: Optional[float] = None
    timing: Optional[float] = None
    calibration: Optional[float] = None
    reputation: Optional[float] = None
    weighted_sum: Optional[float] = None
    final_score: float = 0.0

    # Metadata
    result: ScoringResult = ScoringResult.INHIBITED

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "inhibitor_results": [r.to_dict() for r in self.inhibitor_results],
            "passed_phase1": self.passed_phase1,
            "accuracy": self.accuracy,
            "timing": self.timing,
            "calibration": self.calibration,
            "reputation": self.reputation,
            "weighted_sum": self.weighted_sum,
            "final_score": self.final_score,
            "result": self.result.value,
        }


@dataclass
class RewardEntry:
    """A single reward allocation."""
    address: str
    amount: float
    score: float
    rank: int
    prediction_hash: str = ""
    multiplier: float = 1.0  # Role multiplier applied

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class RoundSummary:
    """Complete summary of a scoring round."""
    round_id: str
    stream_id: str
    epoch: int
    round_start: int          # Unix timestamp (00:00 UTC)
    round_end: int            # Unix timestamp (23:59:59 UTC)
    observation_value: float
    observation_time: int
    total_reward_pool: float
    num_predictions: int
    num_eligible: int
    rewards: List[RewardEntry]
    merkle_root: str = ""
    merkle_tree: List[str] = field(default_factory=list)
    evrmore_tx_hash: str = ""
    dht_key: str = ""
    created_at: int = field(default_factory=lambda: int(time.time()))

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        result['rewards'] = [r.to_dict() if hasattr(r, 'to_dict') else r for r in self.rewards]
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "RoundSummary":
        """Create from dictionary."""
        if 'rewards' in data:
            data['rewards'] = [
                RewardEntry(**r) if isinstance(r, dict) else r
                for r in data['rewards']
            ]
        return cls(**data)


# ============================================================================
# SCORING ENGINE
# ============================================================================

class SatoriScorer:
    """
    Satori prediction scoring using Hybrid MCP/Continuous neuron model.

    Phase 1: MCP Inhibitory Check
        - Binary gate: ANY inhibitor fires -> score = 0
        - Based on McCulloch-Pitts model of inhibitory synapses

    Phase 2: Continuous Weighted Scoring
        - score = sigmoid(g * (sum(w_i * x_i) - 0.5))
        - Sigmoid activation on weighted sum of factors

    Reference: McCulloch & Pitts (1943), "A logical calculus of the ideas
    immanent in nervous activity"
    """

    # =========== CONFIGURATION ===========

    # Default weights (must sum to 1.0)
    DEFAULT_WEIGHTS = {
        'accuracy': 0.50,      # Most important - core purpose
        'timing': 0.20,        # Rewards conviction & early commitment
        'calibration': 0.15,   # Encourages honest confidence reporting
        'reputation': 0.15,    # Rewards consistent performers
    }

    # Inhibitor configuration
    MIN_STAKE = 50             # Minimum stake to participate (Satori network requirement)
    BLACKLIST: Set[str] = set()  # Blacklisted addresses

    # Scoring parameters
    MINIMUM_SCALE = 0.0001     # Prevent division by zero
    SIGMOID_GAIN = 6           # Steepness of final activation
    ACCURACY_SHIFT = 3         # Sigmoid shift for accuracy calc
    TIMING_EXPONENT = 0.8      # Curve for timing reward

    # Precision for deterministic scoring
    PRECISION = 6              # Decimal places for rounding

    # =========== INITIALIZATION ===========

    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        stream_registry: Optional[Set[str]] = None,
        current_round_id: Optional[str] = None,
        blacklist: Optional[Set[str]] = None,
        min_stake: Optional[float] = None,
    ):
        """
        Initialize SatoriScorer.

        Args:
            weights: Custom weights for scoring factors (must sum to 1.0)
            stream_registry: Set of valid stream IDs
            current_round_id: Current round identifier
            blacklist: Set of blacklisted addresses
            min_stake: Override minimum stake requirement
        """
        self.weights = weights or self.DEFAULT_WEIGHTS.copy()
        self.stream_registry = stream_registry or set()
        self.current_round_id = current_round_id
        self.blacklist = blacklist or self.BLACKLIST.copy()
        if min_stake is not None:
            self.MIN_STAKE = min_stake

        # Validate weights
        weight_sum = sum(self.weights.values())
        if abs(weight_sum - 1.0) > 0.001:
            raise ValueError(f"Weights must sum to 1.0, got {weight_sum}")

    # =========== PHASE 1: INHIBITORY CHECK ===========

    def check_inhibitors(
        self,
        prediction: PredictionInput
    ) -> Tuple[bool, List[InhibitorResult]]:
        """
        Phase 1: MCP Inhibitory Gate

        Checks all inhibitory conditions. If ANY fires, the prediction
        is disqualified (score = 0) regardless of other factors.

        This mirrors biological inhibitory synapses which can veto
        neuron firing regardless of excitatory input.

        Returns:
            (passes: bool, results: list of InhibitorResult)
        """
        results = []

        # Inhibitor 1: Late submission
        late = prediction.commit_time > prediction.deadline
        results.append(InhibitorResult(
            name='late_submission',
            fired=late,
            reason=f"Committed at {prediction.commit_time}, deadline was {prediction.deadline}" if late else "On time"
        ))

        # Inhibitor 2: Invalid signature
        sig_valid = self._verify_signature(prediction)
        results.append(InhibitorResult(
            name='invalid_signature',
            fired=not sig_valid,
            reason="Signature verification failed" if not sig_valid else "Valid signature"
        ))

        # Inhibitor 3: Sybil detection (placeholder - would use actual detection)
        sybil = self._is_sybil(prediction.predictor_address)
        results.append(InhibitorResult(
            name='sybil_detected',
            fired=sybil,
            reason="Address flagged as potential Sybil attack" if sybil else "Not flagged"
        ))

        # Inhibitor 4: Prediction copying
        copied = self._is_duplicate(prediction)
        results.append(InhibitorResult(
            name='prediction_copied',
            fired=copied,
            reason="Prediction appears copied from another predictor" if copied else "Original prediction"
        ))

        # Inhibitor 5: Below minimum stake
        below_min = prediction.stake < self.MIN_STAKE
        results.append(InhibitorResult(
            name='below_minimum_stake',
            fired=below_min,
            reason=f"Stake {prediction.stake} < minimum {self.MIN_STAKE} SATORI" if below_min else "Sufficient stake"
        ))

        # Inhibitor 6: Blacklisted address
        blacklisted = prediction.predictor_address in self.blacklist
        results.append(InhibitorResult(
            name='blacklisted_address',
            fired=blacklisted,
            reason="Address is blacklisted" if blacklisted else "Not blacklisted"
        ))

        # Inhibitor 7: Invalid stream
        invalid_stream = (
            len(self.stream_registry) > 0 and
            prediction.stream_id not in self.stream_registry
        )
        results.append(InhibitorResult(
            name='invalid_stream',
            fired=invalid_stream,
            reason=f"Stream {prediction.stream_id} not registered" if invalid_stream else "Valid stream"
        ))

        # Inhibitor 8: Round mismatch
        wrong_round = (
            self.current_round_id is not None and
            prediction.round_id != self.current_round_id
        )
        results.append(InhibitorResult(
            name='round_mismatch',
            fired=wrong_round,
            reason=f"Round {prediction.round_id} != current {self.current_round_id}" if wrong_round else "Correct round"
        ))

        # MCP Logic: OR gate - ANY inhibitor fires -> VETO
        any_fired = any(r.fired for r in results)
        passes = not any_fired

        return passes, results

    def _verify_signature(self, prediction: PredictionInput) -> bool:
        """
        Verify prediction signature.

        Override this method with actual cryptographic verification.
        """
        # Placeholder - would verify cryptographic signature
        # In production, this would use the Evrmore identity bridge
        return prediction.signature is not None and len(prediction.signature) > 0

    def _is_sybil(self, address: str) -> bool:
        """
        Check if address is flagged as Sybil.

        Override this method with actual Sybil detection logic.
        """
        # Placeholder - would use actual Sybil detection
        # Could check for: same IP, same patterns, linked addresses, etc.
        return False

    def _is_duplicate(self, prediction: PredictionInput) -> bool:
        """
        Check if prediction is copied from another predictor.

        Override this method with actual duplicate detection.
        """
        # Placeholder - would compare against other predictions in round
        # Check for: identical values, suspiciously similar timing, etc.
        return False

    # =========== PHASE 2: CONTINUOUS SCORING ===========

    @staticmethod
    def sigmoid(x: float) -> float:
        """
        Standard logistic sigmoid activation function.

        sigma(x) = 1 / (1 + e^(-x))

        Properties:
        - Range: (0, 1)
        - sigma(0) = 0.5
        - sigma'(x) = sigma(x)(1 - sigma(x))
        - Monotonically increasing
        - C-infinity continuous (infinitely differentiable)
        """
        # Clamp to prevent overflow
        x = max(-500, min(500, x))
        return 1 / (1 + math.exp(-x))

    def calculate_accuracy(self, predicted: float, actual: float) -> float:
        """
        Calculate accuracy score from prediction error.

        Uses sigmoid on normalized error for smooth decay.
        Perfect prediction ~= 0.95, terrible prediction -> ~0.05.

        Formula: accuracy = sigma(-epsilon/s + SHIFT)
        Where epsilon = |predicted - actual|, s = scale factor
        """
        error = abs(predicted - actual)
        scale = max(abs(actual) * 0.1, self.MINIMUM_SCALE)
        normalized_error = error / scale

        # Sigmoid with shift so 0 error gives high score
        return self.sigmoid(-normalized_error + self.ACCURACY_SHIFT)

    def calculate_timing(self, commit_time: int, round_start: int, deadline: int) -> float:
        """
        Calculate timing score based on when prediction was committed.

        Earlier commits score higher (more conviction, less information available).
        Uses power curve to slightly reward very early commits.

        Formula: timing = (1 - t/T)^EXPONENT
        Where t = time elapsed, T = round duration
        """
        if commit_time <= round_start:
            return 1.0  # Committed before round started (maximum)
        if commit_time >= deadline:
            return 0.0  # At or after deadline (should be caught by inhibitor)

        round_duration = deadline - round_start
        if round_duration <= 0:
            return 0.5  # Edge case: invalid round duration

        time_elapsed = commit_time - round_start
        timing_ratio = time_elapsed / round_duration

        # Power curve rewards early commits
        return (1 - timing_ratio) ** self.TIMING_EXPONENT

    def calculate_calibration(self, stated_confidence: float, accuracy: float) -> float:
        """
        Calculate confidence calibration score.

        Rewards predictors whose stated confidence matches their accuracy.
        Punishes overconfidence on wrong predictions.

        Formula: calibration = 1 - |confidence - accuracy|
        """
        confidence = max(0.0, min(1.0, stated_confidence))
        calibration_error = abs(confidence - accuracy)
        return 1 - calibration_error

    # =========== MAIN SCORING FUNCTION ===========

    def calculate_score(self, prediction: PredictionInput) -> ScoreBreakdown:
        """
        Calculate complete score for a prediction using hybrid model.

        Phase 1: Check inhibitory conditions (MCP gate)
        Phase 2: If passed, calculate weighted continuous score

        Returns:
            ScoreBreakdown with all components and final score
        """
        # ===== PHASE 1: INHIBITORY CHECK =====
        passed_phase1, inhibitor_results = self.check_inhibitors(prediction)

        if not passed_phase1:
            # MCP inhibitor fired - immediate VETO
            return ScoreBreakdown(
                inhibitor_results=inhibitor_results,
                passed_phase1=False,
                final_score=0.0,
                result=ScoringResult.INHIBITED
            )

        # ===== PHASE 2: CONTINUOUS SCORING =====

        # Calculate individual factors (all in [0, 1])
        accuracy = self.calculate_accuracy(
            prediction.predicted_value,
            prediction.actual_value
        )

        timing = self.calculate_timing(
            prediction.commit_time,
            prediction.round_start,
            prediction.deadline
        )

        calibration = self.calculate_calibration(
            prediction.stated_confidence,
            accuracy
        )

        reputation = prediction.predictor_reputation

        # Weighted sum (neuron's pre-activation value)
        weighted_sum = (
            self.weights['accuracy'] * accuracy +
            self.weights['timing'] * timing +
            self.weights['calibration'] * calibration +
            self.weights['reputation'] * reputation
        )

        # Final activation (sigmoid centered at 0.5)
        centered = weighted_sum - 0.5
        final_score = self.sigmoid(self.SIGMOID_GAIN * centered)

        # Round for determinism
        final_score = round(final_score, self.PRECISION)

        return ScoreBreakdown(
            inhibitor_results=inhibitor_results,
            passed_phase1=True,
            accuracy=round(accuracy, self.PRECISION),
            timing=round(timing, self.PRECISION),
            calibration=round(calibration, self.PRECISION),
            reputation=round(reputation, self.PRECISION),
            weighted_sum=round(weighted_sum, self.PRECISION),
            final_score=final_score,
            result=ScoringResult.SUCCESS
        )


# ============================================================================
# REWARD CALCULATOR
# ============================================================================

class RewardCalculator:
    """
    Calculate reward distributions from prediction scores.

    Takes scores from SatoriScorer and distributes reward pool
    proportionally to scores.
    """

    def __init__(
        self,
        scorer: Optional[SatoriScorer] = None,
        min_score_threshold: float = 0.0
    ):
        """
        Initialize RewardCalculator.

        Args:
            scorer: SatoriScorer instance (creates default if None)
            min_score_threshold: Minimum score to receive rewards
        """
        self.scorer = scorer or SatoriScorer()
        self.min_score_threshold = min_score_threshold

    def calculate_round_rewards(
        self,
        predictions: List[PredictionInput],
        reward_pool: float,
        actual_value: float,
        stream_id: str,
        round_id: str,
        round_start: int,
        round_end: int,
        epoch: int = 0,
        node_roles: Optional[Dict[str, "NodeRoles"]] = None,
    ) -> RoundSummary:
        """
        Calculate rewards for all predictions in a round.

        Args:
            predictions: List of predictions to score
            reward_pool: Total SATORI to distribute
            actual_value: Actual observed value
            stream_id: Stream identifier
            round_id: Round identifier
            round_start: Round start timestamp (00:00 UTC)
            round_end: Round end timestamp (23:59:59 UTC)
            epoch: Epoch number
            node_roles: Optional {address: NodeRoles} for role multipliers

        Returns:
            RoundSummary with all rewards calculated
        """
        # Score all predictions
        scores: Dict[str, Tuple[float, PredictionInput]] = {}
        for pred in predictions:
            # Update actual value for scoring
            pred.actual_value = actual_value
            breakdown = self.scorer.calculate_score(pred)
            if breakdown.final_score >= self.min_score_threshold:
                scores[pred.predictor_address] = (breakdown.final_score, pred)

        # Calculate reward shares with role multipliers
        rewards = self._distribute_rewards(scores, reward_pool, node_roles)

        # Build reward entries with ranking
        reward_entries = []
        sorted_rewards = sorted(rewards.items(), key=lambda x: x[1], reverse=True)
        for rank, (address, amount) in enumerate(sorted_rewards, 1):
            score, pred = scores[address]
            # Get multiplier for this address
            multiplier = 1.0
            if node_roles and address in node_roles:
                multiplier = node_roles[address].get_multiplier()
            reward_entries.append(RewardEntry(
                address=address,
                amount=round(amount, 8),  # SATORI precision
                score=score,
                rank=rank,
                prediction_hash=hashlib.sha256(
                    f"{pred.stream_id}:{pred.predicted_value}:{pred.predictor_address}".encode()
                ).hexdigest()[:32],
                multiplier=multiplier,
            ))

        # Build merkle tree
        merkle_root, merkle_tree = self._build_merkle_tree(reward_entries)

        return RoundSummary(
            round_id=round_id,
            stream_id=stream_id,
            epoch=epoch,
            round_start=round_start,
            round_end=round_end,
            observation_value=actual_value,
            observation_time=round_end,
            total_reward_pool=reward_pool,
            num_predictions=len(predictions),
            num_eligible=len(scores),
            rewards=reward_entries,
            merkle_root=merkle_root,
            merkle_tree=merkle_tree,
        )

    def _distribute_rewards(
        self,
        scores: Dict[str, Tuple[float, PredictionInput]],
        reward_pool: float,
        node_roles: Optional[Dict[str, "NodeRoles"]] = None
    ) -> Dict[str, float]:
        """
        Distribute reward pool proportionally to scores with role multipliers.

        The formula is:
        Your Reward = (Score × Multiplier) / Sum(All Scores × Multipliers) × Pool

        Args:
            scores: {address: (score, prediction)}
            reward_pool: Total SATORI to distribute
            node_roles: Optional {address: NodeRoles} for role multipliers

        Returns:
            {address: reward_amount}
        """
        if not scores:
            return {}

        # Calculate weighted scores (score × multiplier)
        weighted_scores: Dict[str, float] = {}
        for address, (score, _) in scores.items():
            multiplier = 1.0
            if node_roles and address in node_roles:
                multiplier = node_roles[address].get_multiplier()
            weighted_scores[address] = score * multiplier

        total_weighted = sum(weighted_scores.values())

        if total_weighted == 0:
            return {}

        rewards = {}
        for address, weighted_score in weighted_scores.items():
            share = weighted_score / total_weighted
            rewards[address] = share * reward_pool

        return rewards

    def _build_merkle_tree(
        self,
        rewards: List[RewardEntry]
    ) -> Tuple[str, List[str]]:
        """
        Build merkle tree from reward entries.

        Returns:
            (merkle_root, list of tree nodes)
        """
        if not rewards:
            return "", []

        # Create leaves
        leaves = []
        for r in rewards:
            leaf_data = f"{r.address}:{r.amount:.8f}:{r.score:.6f}:{r.rank}"
            leaf_hash = hashlib.sha256(leaf_data.encode()).hexdigest()
            leaves.append(leaf_hash)

        # Build tree bottom-up
        tree = list(leaves)
        current_level = leaves

        while len(current_level) > 1:
            next_level = []
            for i in range(0, len(current_level), 2):
                left = current_level[i]
                right = current_level[i + 1] if i + 1 < len(current_level) else left
                combined = hashlib.sha256((left + right).encode()).hexdigest()
                next_level.append(combined)
                tree.append(combined)
            current_level = next_level

        merkle_root = current_level[0] if current_level else ""
        return merkle_root, tree


# ============================================================================
# ROUND DATA STORE (DHT)
# ============================================================================

class RoundDataStore:
    """
    Store and retrieve round data using satorip2p DHT.

    Uses the P2P network's DHT for decentralized storage
    and PubSub for notifications.
    """

    DHT_KEY_PREFIX = "satori:round:"
    PUBSUB_TOPIC_PREFIX = "satori/rewards/"

    def __init__(self, peers: Optional["Peers"] = None):
        """
        Initialize RoundDataStore.

        Args:
            peers: Peers instance for P2P operations (optional for testing)
        """
        self.peers = peers
        self._local_cache: Dict[str, RoundSummary] = {}

    async def store_round_data(self, round_summary: RoundSummary) -> str:
        """
        Store complete round data in DHT.

        DHT Key: satori:round:{round_id}
        DHT Value: JSON-encoded round summary with merkle tree

        Returns:
            DHT key where data was stored
        """
        round_id = round_summary.round_id
        dht_key = f"{self.DHT_KEY_PREFIX}{round_id}"

        # Store locally
        self._local_cache[dht_key] = round_summary

        # Store in DHT if peers available
        if self.peers and hasattr(self.peers, '_dht') and self.peers._dht:
            try:
                data = json.dumps(round_summary.to_dict())
                await self.peers._dht.put(
                    key=dht_key,
                    value=data.encode(),
                    ttl=0  # Indefinite - permanent like blockchain
                )
                logger.debug(f"Stored round data in DHT: {dht_key}")
            except Exception as e:
                logger.warning(f"Failed to store round data in DHT: {e}")

        round_summary.dht_key = dht_key
        return dht_key

    async def get_round_data(self, round_id: str) -> Optional[RoundSummary]:
        """
        Retrieve round data from DHT.

        Args:
            round_id: Round identifier

        Returns:
            RoundSummary if found, None otherwise
        """
        dht_key = f"{self.DHT_KEY_PREFIX}{round_id}"

        # Check local cache first
        if dht_key in self._local_cache:
            return self._local_cache[dht_key]

        # Query DHT if peers available
        if self.peers and hasattr(self.peers, '_dht') and self.peers._dht:
            try:
                data = await self.peers._dht.get(dht_key)
                if data:
                    summary = RoundSummary.from_dict(json.loads(data.decode()))
                    self._local_cache[dht_key] = summary
                    return summary
            except Exception as e:
                logger.debug(f"Failed to get round data from DHT: {e}")

        return None

    async def broadcast_round_complete(self, round_summary: RoundSummary) -> bool:
        """
        Broadcast round completion via PubSub.

        Topic: satori/rewards/{stream_id}
        Message: Compact notification with round_id and merkle_root

        Returns:
            True if broadcast successful
        """
        stream_id = round_summary.stream_id
        topic = f"{self.PUBSUB_TOPIC_PREFIX}{stream_id}"

        notification = {
            'type': 'round_complete',
            'round_id': round_summary.round_id,
            'epoch': round_summary.epoch,
            'merkle_root': round_summary.merkle_root,
            'total_rewards': round_summary.total_reward_pool,
            'num_predictors': len(round_summary.rewards),
            'tx_hash': round_summary.evrmore_tx_hash,
            'dht_key': round_summary.dht_key,
            'timestamp': int(time.time()),
        }

        if self.peers and self.peers._pubsub:
            try:
                await self.peers.broadcast(topic, notification)
                logger.debug(f"Broadcast round complete: {round_summary.round_id}")
                return True
            except Exception as e:
                logger.warning(f"Failed to broadcast round complete: {e}")
                return False

        return False

    async def subscribe_to_rewards(
        self,
        stream_id: str,
        callback
    ) -> bool:
        """
        Subscribe to reward notifications for a stream.

        Args:
            stream_id: Stream to subscribe to
            callback: Function called with notification dict

        Returns:
            True if subscribed successfully
        """
        topic = f"{self.PUBSUB_TOPIC_PREFIX}{stream_id}"

        if self.peers and self.peers._pubsub:
            try:
                await self.peers.subscribe(topic, callback)
                logger.debug(f"Subscribed to rewards for {stream_id}")
                return True
            except Exception as e:
                logger.warning(f"Failed to subscribe to rewards: {e}")
                return False

        return False


# ============================================================================
# VERIFICATION UTILITIES
# ============================================================================

def verify_score(
    prediction: PredictionInput,
    claimed_score: float,
    tolerance: float = 0.0001,
    scorer: Optional[SatoriScorer] = None
) -> bool:
    """
    Verify a claimed score is correct.

    Used by nodes to validate scores calculated by others.
    Essential for decentralized consensus on rewards.

    Args:
        prediction: The prediction that was scored
        claimed_score: The score being verified
        tolerance: Acceptable difference due to float precision
        scorer: Optional scorer with custom config

    Returns:
        True if claimed score matches calculated score
    """
    scorer = scorer or SatoriScorer()
    breakdown = scorer.calculate_score(prediction)
    return abs(breakdown.final_score - claimed_score) < tolerance


def verify_reward_claim(
    address: str,
    amount: float,
    score: float,
    rank: int,
    merkle_root: str,
    merkle_proof: List[Tuple[str, str]]
) -> bool:
    """
    Verify a reward was part of a round using merkle proof.

    1. Reconstruct leaf hash from claim data
    2. Walk up merkle tree using proof
    3. Compare final hash to merkle_root from OP_RETURN

    Args:
        address: Claimer's address
        amount: Claimed reward amount
        score: Claimed score
        rank: Claimed rank
        merkle_root: Root from on-chain OP_RETURN
        merkle_proof: List of (direction, sibling_hash) tuples

    Returns:
        True if claim is valid
    """
    # Reconstruct leaf
    leaf_data = f"{address}:{amount:.8f}:{score:.6f}:{rank}"
    current_hash = hashlib.sha256(leaf_data.encode()).hexdigest()

    # Walk up tree
    for direction, sibling_hash in merkle_proof:
        if direction == 'left':
            combined = sibling_hash + current_hash
        else:
            combined = current_hash + sibling_hash
        current_hash = hashlib.sha256(combined.encode()).hexdigest()

    return current_hash == merkle_root


# ============================================================================
# ROUND UTILITIES
# ============================================================================

def get_round_boundaries(timestamp: Optional[int] = None) -> Tuple[int, int, str]:
    """
    Get round start and end timestamps for a given time.

    Rounds are daily, 00:00 UTC to 23:59:59 UTC.

    Args:
        timestamp: Unix timestamp (defaults to now)

    Returns:
        (round_start, round_end, round_id)
    """
    if timestamp is None:
        timestamp = int(time.time())

    # Get UTC date
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")

    # Round start: 00:00:00 UTC
    round_start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    round_start = int(round_start_dt.timestamp())

    # Round end: 23:59:59 UTC
    round_end = round_start + 86400 - 1  # 24 hours minus 1 second

    # Round ID: stream_date format
    round_id = date_str

    return round_start, round_end, round_id


def get_epoch_from_timestamp(timestamp: int, epoch_start: int = 0) -> int:
    """
    Calculate epoch number from timestamp.

    Epochs are counted from a starting point (network genesis).

    Args:
        timestamp: Current Unix timestamp
        epoch_start: Unix timestamp of epoch 0

    Returns:
        Epoch number
    """
    if timestamp < epoch_start:
        return 0

    days_since_start = (timestamp - epoch_start) // 86400
    return days_since_start


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def score_prediction(
    predicted_value: float,
    actual_value: float,
    commit_time: int,
    round_start: int,
    deadline: int,
    stated_confidence: float = 0.5,
    predictor_reputation: float = 0.5,
    predictor_address: str = "",
    stake: float = 50,
    signature: bytes = b"valid",
    stream_id: str = "default",
    round_id: str = "current",
    weights: Optional[Dict[str, float]] = None
) -> float:
    """
    Convenience function to score a single prediction.

    Returns final score between 0 and 1.
    """
    scorer = SatoriScorer(
        weights=weights,
        stream_registry={stream_id},
        current_round_id=round_id
    )
    prediction = PredictionInput(
        predicted_value=predicted_value,
        actual_value=actual_value,
        commit_time=commit_time,
        round_start=round_start,
        deadline=deadline,
        stated_confidence=stated_confidence,
        predictor_reputation=predictor_reputation,
        predictor_address=predictor_address,
        stake=stake,
        signature=signature,
        stream_id=stream_id,
        round_id=round_id
    )
    breakdown = scorer.calculate_score(prediction)
    return breakdown.final_score
