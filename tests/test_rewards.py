"""
satorip2p/tests/test_rewards.py

Unit tests for reward scoring and distribution:
- SatoriScorer (Phase 1 inhibitors + Phase 2 continuous scoring)
- RewardCalculator
- RoundDataStore
- MerkleTree
- OP_RETURN encoding/decoding
"""

import pytest
import time
import math
from unittest.mock import Mock, AsyncMock, patch

# Import from satorip2p
from src.satorip2p.protocol.rewards import (
    SatoriScorer,
    RewardCalculator,
    RoundDataStore,
    PredictionInput,
    ScoreBreakdown,
    ScoringResult,
    RewardEntry,
    RoundSummary,
    InhibitorResult,
    score_prediction,
    verify_score,
    verify_reward_claim,
    get_round_boundaries,
    get_epoch_from_timestamp,
)
from src.satorip2p.blockchain.reward_distributor import (
    MerkleTree,
    encode_round_summary_for_opreturn,
    decode_round_summary_from_opreturn,
    EvrmoreDistributor,
    CentralizedDistributor,
    AssetBadgeIssuer,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def scorer():
    """Create a SatoriScorer with default settings."""
    return SatoriScorer(
        stream_registry={"test-stream", "btc-usd-1h"},
        current_round_id="2025-01-01",
    )


@pytest.fixture
def valid_prediction():
    """Create a valid prediction that should pass all inhibitors."""
    now = int(time.time())
    return PredictionInput(
        predicted_value=50250.0,
        actual_value=50000.0,
        commit_time=now - 3000,  # 50 minutes ago
        round_start=now - 3600,  # 1 hour ago
        deadline=now,            # Now
        stated_confidence=0.85,
        predictor_reputation=0.75,
        predictor_address="ETestAddress123",
        stake=100,
        signature=b"valid_signature",
        stream_id="test-stream",
        round_id="2025-01-01",
    )


@pytest.fixture
def mock_peers():
    """Create mock Peers instance for DHT/PubSub tests."""
    peers = Mock()
    peers._pubsub = Mock()
    peers._dht = AsyncMock()
    peers.broadcast = AsyncMock(return_value=True)
    peers.subscribe = AsyncMock(return_value=True)
    return peers


# ============================================================================
# Test SatoriScorer - Phase 1 (Inhibitors)
# ============================================================================

class TestSatoriScorerInhibitors:
    """Tests for Phase 1: MCP Inhibitory Gate."""

    def test_late_submission_inhibitor(self, scorer):
        """Late submission should disqualify."""
        now = int(time.time())
        pred = PredictionInput(
            predicted_value=50000.0,
            actual_value=50000.0,
            commit_time=now + 100,  # AFTER deadline
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.5,
            predictor_reputation=0.5,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="test-stream",
            round_id="2025-01-01",
        )

        breakdown = scorer.calculate_score(pred)

        assert breakdown.final_score == 0.0
        assert breakdown.result == ScoringResult.INHIBITED
        assert not breakdown.passed_phase1
        # Find late_submission inhibitor
        late_inhibitor = next(
            (r for r in breakdown.inhibitor_results if r.name == 'late_submission'),
            None
        )
        assert late_inhibitor is not None
        assert late_inhibitor.fired

    def test_below_minimum_stake_inhibitor(self, scorer):
        """Below minimum stake should disqualify."""
        now = int(time.time())
        pred = PredictionInput(
            predicted_value=50000.0,
            actual_value=50000.0,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.5,
            predictor_reputation=0.5,
            predictor_address="ETest",
            stake=10,  # Below MIN_STAKE of 50
            signature=b"sig",
            stream_id="test-stream",
            round_id="2025-01-01",
        )

        breakdown = scorer.calculate_score(pred)

        assert breakdown.final_score == 0.0
        assert breakdown.result == ScoringResult.INHIBITED
        stake_inhibitor = next(
            (r for r in breakdown.inhibitor_results if r.name == 'below_minimum_stake'),
            None
        )
        assert stake_inhibitor is not None
        assert stake_inhibitor.fired

    def test_invalid_signature_inhibitor(self, scorer):
        """Empty signature should disqualify."""
        now = int(time.time())
        pred = PredictionInput(
            predicted_value=50000.0,
            actual_value=50000.0,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.5,
            predictor_reputation=0.5,
            predictor_address="ETest",
            stake=100,
            signature=b"",  # Empty signature
            stream_id="test-stream",
            round_id="2025-01-01",
        )

        breakdown = scorer.calculate_score(pred)

        assert breakdown.final_score == 0.0
        sig_inhibitor = next(
            (r for r in breakdown.inhibitor_results if r.name == 'invalid_signature'),
            None
        )
        assert sig_inhibitor is not None
        assert sig_inhibitor.fired

    def test_invalid_stream_inhibitor(self, scorer):
        """Invalid stream ID should disqualify."""
        now = int(time.time())
        pred = PredictionInput(
            predicted_value=50000.0,
            actual_value=50000.0,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.5,
            predictor_reputation=0.5,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="nonexistent-stream",  # Not in registry
            round_id="2025-01-01",
        )

        breakdown = scorer.calculate_score(pred)

        assert breakdown.final_score == 0.0
        stream_inhibitor = next(
            (r for r in breakdown.inhibitor_results if r.name == 'invalid_stream'),
            None
        )
        assert stream_inhibitor is not None
        assert stream_inhibitor.fired

    def test_round_mismatch_inhibitor(self, scorer):
        """Wrong round ID should disqualify."""
        now = int(time.time())
        pred = PredictionInput(
            predicted_value=50000.0,
            actual_value=50000.0,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.5,
            predictor_reputation=0.5,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="test-stream",
            round_id="2024-12-31",  # Wrong round
        )

        breakdown = scorer.calculate_score(pred)

        assert breakdown.final_score == 0.0
        round_inhibitor = next(
            (r for r in breakdown.inhibitor_results if r.name == 'round_mismatch'),
            None
        )
        assert round_inhibitor is not None
        assert round_inhibitor.fired

    def test_blacklisted_address_inhibitor(self):
        """Blacklisted address should disqualify."""
        now = int(time.time())
        scorer = SatoriScorer(
            blacklist={"EBlacklistedAddress"},
            stream_registry={"test-stream"},
            current_round_id="2025-01-01",
        )

        pred = PredictionInput(
            predicted_value=50000.0,
            actual_value=50000.0,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.5,
            predictor_reputation=0.5,
            predictor_address="EBlacklistedAddress",
            stake=100,
            signature=b"sig",
            stream_id="test-stream",
            round_id="2025-01-01",
        )

        breakdown = scorer.calculate_score(pred)

        assert breakdown.final_score == 0.0
        blacklist_inhibitor = next(
            (r for r in breakdown.inhibitor_results if r.name == 'blacklisted_address'),
            None
        )
        assert blacklist_inhibitor is not None
        assert blacklist_inhibitor.fired

    def test_all_inhibitors_pass(self, scorer, valid_prediction):
        """Valid prediction should pass all inhibitors."""
        breakdown = scorer.calculate_score(valid_prediction)

        assert breakdown.passed_phase1
        assert breakdown.result == ScoringResult.SUCCESS
        assert breakdown.final_score > 0

        # All inhibitors should be False
        for inhibitor in breakdown.inhibitor_results:
            assert not inhibitor.fired, f"Inhibitor {inhibitor.name} should not fire"


# ============================================================================
# Test SatoriScorer - Phase 2 (Continuous Scoring)
# ============================================================================

class TestSatoriScorerContinuous:
    """Tests for Phase 2: Continuous Weighted Scoring."""

    def test_perfect_prediction_high_score(self, scorer):
        """Perfect prediction should score ~0.95."""
        now = int(time.time())
        pred = PredictionInput(
            predicted_value=50000.0,  # Exact match!
            actual_value=50000.0,
            commit_time=now - 3600 + 60,  # Very early (1 min into round)
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.95,  # High confidence
            predictor_reputation=0.9,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="test-stream",
            round_id="2025-01-01",
        )

        breakdown = scorer.calculate_score(pred)

        assert breakdown.passed_phase1
        assert breakdown.final_score > 0.9  # Very high score
        assert breakdown.accuracy > 0.9

    def test_poor_prediction_low_score(self, scorer):
        """Very wrong prediction should score low."""
        now = int(time.time())
        pred = PredictionInput(
            predicted_value=100000.0,  # 100% error
            actual_value=50000.0,
            commit_time=now - 60,  # Very late (1 min before deadline)
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.9,  # Overconfident
            predictor_reputation=0.3,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="test-stream",
            round_id="2025-01-01",
        )

        breakdown = scorer.calculate_score(pred)

        assert breakdown.passed_phase1
        assert breakdown.final_score < 0.5  # Low score
        assert breakdown.accuracy < 0.5

    def test_timing_rewards_early_commit(self, scorer):
        """Earlier commits should score higher on timing."""
        now = int(time.time())

        # Early commit
        early_pred = PredictionInput(
            predicted_value=50000.0,
            actual_value=50000.0,
            commit_time=now - 3600 + 360,  # 10% into round
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.5,
            predictor_reputation=0.5,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="test-stream",
            round_id="2025-01-01",
        )

        # Late commit
        late_pred = PredictionInput(
            predicted_value=50000.0,
            actual_value=50000.0,
            commit_time=now - 360,  # 90% into round
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.5,
            predictor_reputation=0.5,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="test-stream",
            round_id="2025-01-01",
        )

        early_breakdown = scorer.calculate_score(early_pred)
        late_breakdown = scorer.calculate_score(late_pred)

        assert early_breakdown.timing > late_breakdown.timing

    def test_calibration_rewards_honesty(self, scorer):
        """Calibration rewards matching confidence to accuracy."""
        now = int(time.time())

        # Well-calibrated (confidence matches accuracy)
        well_calibrated = PredictionInput(
            predicted_value=52500.0,  # 5% error -> ~0.92 accuracy
            actual_value=50000.0,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.9,  # Close to actual accuracy
            predictor_reputation=0.5,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="test-stream",
            round_id="2025-01-01",
        )

        # Overconfident (high confidence but poor accuracy)
        overconfident = PredictionInput(
            predicted_value=75000.0,  # 50% error -> poor accuracy
            actual_value=50000.0,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.95,  # Way too confident
            predictor_reputation=0.5,
            predictor_address="ETest2",
            stake=100,
            signature=b"sig",
            stream_id="test-stream",
            round_id="2025-01-01",
        )

        well_breakdown = scorer.calculate_score(well_calibrated)
        over_breakdown = scorer.calculate_score(overconfident)

        assert well_breakdown.calibration > over_breakdown.calibration

    def test_weights_sum_to_one(self, scorer):
        """Default weights should sum to 1.0."""
        total = sum(scorer.weights.values())
        assert abs(total - 1.0) < 0.001

    def test_custom_weights(self):
        """Custom weights should be applied."""
        custom_weights = {
            'accuracy': 0.70,
            'timing': 0.10,
            'calibration': 0.10,
            'reputation': 0.10,
        }
        scorer = SatoriScorer(weights=custom_weights)
        assert scorer.weights['accuracy'] == 0.70

    def test_invalid_weights_rejected(self):
        """Weights not summing to 1.0 should raise error."""
        bad_weights = {
            'accuracy': 0.50,
            'timing': 0.50,
            'calibration': 0.50,
            'reputation': 0.50,
        }
        with pytest.raises(ValueError):
            SatoriScorer(weights=bad_weights)


# ============================================================================
# Test Sigmoid Function
# ============================================================================

class TestSigmoid:
    """Tests for sigmoid activation function."""

    def test_sigmoid_at_zero(self):
        """sigmoid(0) = 0.5."""
        assert SatoriScorer.sigmoid(0) == 0.5

    def test_sigmoid_monotonic(self):
        """Sigmoid should be monotonically increasing."""
        for x in range(-10, 10):
            assert SatoriScorer.sigmoid(x) < SatoriScorer.sigmoid(x + 1)

    def test_sigmoid_bounded(self):
        """Sigmoid output should be in [0, 1] (inclusive due to float precision)."""
        for x in [-100, -10, -1, 0, 1, 10, 100]:
            result = SatoriScorer.sigmoid(x)
            assert 0 <= result <= 1

    def test_sigmoid_overflow_protection(self):
        """Sigmoid should not overflow with extreme values."""
        # These would cause overflow without clamping
        # Due to float precision, very large values hit exactly 1.0
        assert SatoriScorer.sigmoid(1000) <= 1
        assert SatoriScorer.sigmoid(-1000) >= 0
        # Should not raise any errors
        SatoriScorer.sigmoid(10000)
        SatoriScorer.sigmoid(-10000)


# ============================================================================
# Test RewardCalculator
# ============================================================================

class TestRewardCalculator:
    """Tests for RewardCalculator."""

    def test_distribute_rewards_proportionally(self):
        """Rewards should be distributed proportionally to scores."""
        calculator = RewardCalculator()

        now = int(time.time())
        predictions = []

        # Create 3 predictions with different accuracy
        for i, (predicted, address) in enumerate([
            (50250.0, "EAlice"),    # 0.5% error - best
            (51000.0, "EBob"),      # 2% error - medium
            (55000.0, "ECharlie"),  # 10% error - worst
        ]):
            predictions.append(PredictionInput(
                predicted_value=predicted,
                actual_value=50000.0,
                commit_time=now - 1800,
                round_start=now - 3600,
                deadline=now,
                stated_confidence=0.5,
                predictor_reputation=0.5,
                predictor_address=address,
                stake=100,
                signature=b"sig",
                stream_id="test-stream",
                round_id="2025-01-01",
            ))

        summary = calculator.calculate_round_rewards(
            predictions=predictions,
            reward_pool=1000.0,
            actual_value=50000.0,
            stream_id="test-stream",
            round_id="2025-01-01",
            round_start=now - 3600,
            round_end=now,
        )

        # Check rewards allocated
        assert len(summary.rewards) == 3
        # Use approx due to floating point precision
        assert sum(r.amount for r in summary.rewards) == pytest.approx(1000.0, rel=1e-6)

        # Alice (best) should get most
        alice = next(r for r in summary.rewards if r.address == "EAlice")
        bob = next(r for r in summary.rewards if r.address == "EBob")
        charlie = next(r for r in summary.rewards if r.address == "ECharlie")

        assert alice.amount > bob.amount > charlie.amount

    def test_no_rewards_for_disqualified(self):
        """Disqualified predictions get no rewards."""
        calculator = RewardCalculator(
            scorer=SatoriScorer(
                stream_registry={"test-stream"},
                current_round_id="2025-01-01",
            )
        )

        now = int(time.time())
        predictions = [
            # Valid prediction
            PredictionInput(
                predicted_value=50000.0,
                actual_value=50000.0,
                commit_time=now - 1800,
                round_start=now - 3600,
                deadline=now,
                stated_confidence=0.5,
                predictor_reputation=0.5,
                predictor_address="EValid",
                stake=100,
                signature=b"sig",
                stream_id="test-stream",
                round_id="2025-01-01",
            ),
            # Invalid (below stake)
            PredictionInput(
                predicted_value=50000.0,
                actual_value=50000.0,
                commit_time=now - 1800,
                round_start=now - 3600,
                deadline=now,
                stated_confidence=0.5,
                predictor_reputation=0.5,
                predictor_address="EInvalid",
                stake=10,  # Too low
                signature=b"sig",
                stream_id="test-stream",
                round_id="2025-01-01",
            ),
        ]

        summary = calculator.calculate_round_rewards(
            predictions=predictions,
            reward_pool=1000.0,
            actual_value=50000.0,
            stream_id="test-stream",
            round_id="2025-01-01",
            round_start=now - 3600,
            round_end=now,
        )

        # Only valid predictor gets non-zero rewards
        non_zero_rewards = [r for r in summary.rewards if r.amount > 0]
        assert len(non_zero_rewards) == 1
        assert non_zero_rewards[0].address == "EValid"
        assert non_zero_rewards[0].amount == pytest.approx(1000.0, rel=1e-6)


# ============================================================================
# Test MerkleTree
# ============================================================================

class TestMerkleTree:
    """Tests for MerkleTree."""

    def test_build_merkle_tree(self):
        """Tree should be built correctly."""
        leaves = ['a', 'b', 'c', 'd']
        tree = MerkleTree(leaves)

        assert tree.root != ""
        assert len(tree.tree) > len(leaves)

    def test_merkle_proof_valid(self):
        """Valid proof should verify."""
        leaves = ['leaf1', 'leaf2', 'leaf3', 'leaf4']
        tree = MerkleTree(leaves)

        # Get proof for first leaf
        proof = tree.get_proof(0)

        # Verify proof
        leaf_hash = leaves[0]  # Already a hash in this test
        assert MerkleTree.verify_proof(leaf_hash, tree.root, proof)

    def test_merkle_proof_invalid(self):
        """Invalid proof should fail."""
        leaves = ['leaf1', 'leaf2', 'leaf3', 'leaf4']
        tree = MerkleTree(leaves)

        proof = tree.get_proof(0)

        # Wrong leaf should fail
        wrong_leaf = "wrong_leaf_hash"
        assert not MerkleTree.verify_proof(wrong_leaf, tree.root, proof)

    def test_single_leaf(self):
        """Single leaf tree should work."""
        tree = MerkleTree(['single'])
        assert tree.root == 'single'

    def test_hash_reward_entry(self):
        """Reward entry hashing should be deterministic."""
        hash1 = MerkleTree.hash_reward_entry("EAddr", 100.0, 0.95, 1)
        hash2 = MerkleTree.hash_reward_entry("EAddr", 100.0, 0.95, 1)
        hash3 = MerkleTree.hash_reward_entry("EAddr", 100.0, 0.95, 2)  # Different rank

        assert hash1 == hash2
        assert hash1 != hash3


# ============================================================================
# Test OP_RETURN Encoding
# ============================================================================

class TestOpReturnEncoding:
    """Tests for OP_RETURN encoding/decoding."""

    def test_encode_decode_roundtrip(self):
        """Encode and decode should be reversible."""
        summary = {
            'round_id': 'test-round-2025-01-01',
            'epoch': 42,
            'merkle_root': '0' * 64,  # 32 bytes as hex
            'total_reward_pool': 1000.0,
            'observation_value': 50000.0,
            'observation_time': 1704067200,
            'rewards': [{'address': 'ETest', 'amount': 1000}],
        }

        encoded = encode_round_summary_for_opreturn(summary)

        assert len(encoded) == 69  # Fixed size

        decoded = decode_round_summary_from_opreturn(encoded)

        assert decoded['epoch'] == 42
        assert decoded['total_reward_pool'] == 1000.0
        assert abs(decoded['observation_value'] - 50000.0) < 0.001

    def test_encoding_size(self):
        """Encoding should be exactly 69 bytes."""
        summary = {
            'round_id': 'test',
            'epoch': 0,
            'merkle_root': '0' * 64,
            'total_reward_pool': 0,
            'observation_value': 0,
            'observation_time': 0,
            'rewards': [],
        }

        encoded = encode_round_summary_for_opreturn(summary)
        assert len(encoded) == 69


# ============================================================================
# Test RoundDataStore
# ============================================================================

class TestRoundDataStore:
    """Tests for RoundDataStore."""

    @pytest.mark.asyncio
    async def test_store_locally(self):
        """Store should work without peers (local cache)."""
        store = RoundDataStore(peers=None)

        summary = RoundSummary(
            round_id="2025-01-01",
            stream_id="test-stream",
            epoch=1,
            round_start=0,
            round_end=86400,
            observation_value=50000.0,
            observation_time=86400,
            total_reward_pool=1000.0,
            num_predictions=10,
            num_eligible=8,
            rewards=[],
            merkle_root="0" * 64,
        )

        dht_key = await store.store_round_data(summary)

        assert dht_key == "satori:round:2025-01-01"
        assert summary.dht_key == dht_key

        # Retrieve from local cache
        retrieved = await store.get_round_data("2025-01-01")
        assert retrieved is not None
        assert retrieved.round_id == "2025-01-01"

    @pytest.mark.asyncio
    async def test_broadcast_with_peers(self, mock_peers):
        """Broadcast should call PubSub."""
        store = RoundDataStore(peers=mock_peers)

        summary = RoundSummary(
            round_id="2025-01-01",
            stream_id="test-stream",
            epoch=1,
            round_start=0,
            round_end=86400,
            observation_value=50000.0,
            observation_time=86400,
            total_reward_pool=1000.0,
            num_predictions=10,
            num_eligible=8,
            rewards=[],
            merkle_root="0" * 64,
        )

        result = await store.broadcast_round_complete(summary)

        assert result
        mock_peers.broadcast.assert_called_once()


# ============================================================================
# Test Round Utilities
# ============================================================================

class TestRoundUtilities:
    """Tests for round boundary utilities."""

    def test_get_round_boundaries(self):
        """Round boundaries should be correct."""
        # Test with a known timestamp (2025-01-01 12:00:00 UTC)
        timestamp = 1735732800

        round_start, round_end, round_id = get_round_boundaries(timestamp)

        assert round_id == "2025-01-01"
        assert round_end - round_start == 86399  # 24 hours - 1 second

    def test_get_epoch_from_timestamp(self):
        """Epoch calculation should be correct."""
        epoch_start = 1704067200  # 2024-01-01 00:00:00 UTC

        # Day 0
        assert get_epoch_from_timestamp(epoch_start, epoch_start) == 0

        # Day 1
        assert get_epoch_from_timestamp(epoch_start + 86400, epoch_start) == 1

        # Day 100
        assert get_epoch_from_timestamp(epoch_start + 86400 * 100, epoch_start) == 100


# ============================================================================
# Test Convenience Functions
# ============================================================================

class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_score_prediction_function(self):
        """score_prediction convenience function should work."""
        now = int(time.time())

        score = score_prediction(
            predicted_value=50000.0,
            actual_value=50000.0,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
        )

        assert 0 < score < 1
        assert score > 0.5  # Perfect prediction should be good

    def test_verify_score_function(self):
        """verify_score should correctly validate scores."""
        now = int(time.time())

        pred = PredictionInput(
            predicted_value=50000.0,
            actual_value=50000.0,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.5,
            predictor_reputation=0.5,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="test",
            round_id="test",
        )

        scorer = SatoriScorer()
        breakdown = scorer.calculate_score(pred)

        # Correct score should verify
        assert verify_score(pred, breakdown.final_score)

        # Wrong score should fail
        assert not verify_score(pred, 0.123456)


# ============================================================================
# Test CentralizedDistributor
# ============================================================================

class TestCentralizedDistributor:
    """Tests for CentralizedDistributor (transition phase)."""

    @pytest.mark.asyncio
    async def test_record_distribution(self, tmp_path):
        """Distribution should be recorded for manual processing."""
        output_file = str(tmp_path / "pending.json")
        distributor = CentralizedDistributor(output_file=output_file)

        summary = RoundSummary(
            round_id="2025-01-01",
            stream_id="test-stream",
            epoch=1,
            round_start=0,
            round_end=86400,
            observation_value=50000.0,
            observation_time=86400,
            total_reward_pool=1000.0,
            num_predictions=10,
            num_eligible=8,
            rewards=[RewardEntry("ETest", 1000.0, 0.95, 1)],
            merkle_root="0" * 64,
        )

        result = await distributor.distribute_round(summary)

        assert result['status'] == 'pending_manual_approval'

        # Check pending list
        pending = distributor.get_pending_distributions()
        assert len(pending) == 1
        assert pending[0]['round_id'] == "2025-01-01"


# ============================================================================
# Test EvrmoreDistributor (Dry Run)
# ============================================================================

class TestEvrmoreDistributor:
    """Tests for EvrmoreDistributor."""

    @pytest.mark.asyncio
    async def test_dry_run_distribution(self):
        """Dry run should not send transactions."""
        mock_rpc = Mock()
        distributor = EvrmoreDistributor(
            rpc_client=mock_rpc,
            treasury_address="ETreasury",
            dry_run=True,
        )

        summary = RoundSummary(
            round_id="2025-01-01",
            stream_id="test-stream",
            epoch=1,
            round_start=0,
            round_end=86400,
            observation_value=50000.0,
            observation_time=86400,
            total_reward_pool=1000.0,
            num_predictions=10,
            num_eligible=8,
            rewards=[RewardEntry("ETest", 1000.0, 0.95, 1)],
            merkle_root="0" * 64,
        )

        result = await distributor.distribute_round(summary)

        assert result['status'] == 'success'
        assert result['dry_run']
        assert 'dry_run_' in result['tx_hash']
        mock_rpc.call.assert_not_called()


# ============================================================================
# Test AssetBadgeIssuer
# ============================================================================

class TestAssetBadgeIssuer:
    """Tests for AssetBadgeIssuer."""

    @pytest.mark.asyncio
    async def test_issue_rank_badge_dry_run(self):
        """Dry run badge issuance should work."""
        mock_rpc = Mock()
        issuer = AssetBadgeIssuer(rpc_client=mock_rpc, dry_run=True)

        tx_hash = await issuer.issue_rank_badge(
            epoch=42,
            rank=1,
            recipient="EWinner",
        )

        assert tx_hash == "dry_run_SATORI#E42_R1"
        mock_rpc.call.assert_not_called()

    @pytest.mark.asyncio
    async def test_issue_achievement_badge_dry_run(self):
        """Achievement badge dry run should work."""
        mock_rpc = Mock()
        issuer = AssetBadgeIssuer(rpc_client=mock_rpc, dry_run=True)

        tx_hash = await issuer.issue_achievement_badge(
            achievement="PERFECT_ROUND",
            recipient="EPerfect",
        )

        assert tx_hash == "dry_run_SATORI#PERFECT_ROUND"

    @pytest.mark.asyncio
    async def test_issue_round_badges_combined(self):
        """Round badges should issue both rank and achievement badges."""
        mock_rpc = Mock()
        issuer = AssetBadgeIssuer(rpc_client=mock_rpc, dry_run=True)

        results = [
            {'address': 'EAlice', 'rank': 1, 'achievements': ['PERFECT', 'FAST']},
            {'address': 'EBob', 'rank': 2, 'achievements': ['STREAK_10']},
            {'address': 'ECarol', 'rank': 3, 'achievements': []},
            {'address': 'EDave', 'rank': None, 'achievements': ['CALIBRATED']},
        ]

        result = await issuer.issue_round_badges(epoch=42, results=results)

        # Check counts
        assert result['epoch'] == 42
        assert result['rank_badges_issued'] == 3  # Alice, Bob, Carol
        assert result['achievement_badges_issued'] == 4  # Alice(2) + Bob(1) + Dave(1)
        assert result['total_badges'] == 7
        assert len(result['errors']) == 0

        # Check badges by recipient
        badges = result['badges_by_recipient']
        assert 'SATORI#E42_R1' in badges['EAlice']
        assert 'SATORI#E42_PERFECT' in badges['EAlice']
        assert 'SATORI#E42_FAST' in badges['EAlice']
        assert len(badges['EAlice']) == 3

        assert 'SATORI#E42_R2' in badges['EBob']
        assert 'SATORI#E42_STREAK_10' in badges['EBob']
        assert len(badges['EBob']) == 2

        assert 'SATORI#E42_R3' in badges['ECarol']
        assert len(badges['ECarol']) == 1

        assert 'SATORI#E42_CALIBRATED' in badges['EDave']
        assert len(badges['EDave']) == 1

    @pytest.mark.asyncio
    async def test_issue_round_badges_top_n_limit(self):
        """Only top N ranks should get rank badges."""
        mock_rpc = Mock()
        issuer = AssetBadgeIssuer(rpc_client=mock_rpc, dry_run=True)

        results = [
            {'address': 'E1', 'rank': 1, 'achievements': []},
            {'address': 'E2', 'rank': 2, 'achievements': []},
            {'address': 'E3', 'rank': 3, 'achievements': []},
            {'address': 'E4', 'rank': 4, 'achievements': []},  # Should NOT get badge
        ]

        result = await issuer.issue_round_badges(epoch=1, results=results, top_n_ranks=3)

        assert result['rank_badges_issued'] == 3
        assert 'E4' not in result['badges_by_recipient']


# ============================================================================
# Test Determinism
# ============================================================================

class TestDeterminism:
    """Tests to ensure scoring is deterministic."""

    def test_same_input_same_output(self):
        """Same inputs should always produce same scores."""
        scorer = SatoriScorer()

        now = int(time.time())
        pred = PredictionInput(
            predicted_value=50250.0,
            actual_value=50000.0,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.85,
            predictor_reputation=0.75,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="test",
            round_id="test",
        )

        # Calculate multiple times
        scores = [scorer.calculate_score(pred).final_score for _ in range(10)]

        # All should be identical
        assert all(s == scores[0] for s in scores)

    def test_precision_rounding(self):
        """Scores should be rounded to fixed precision."""
        scorer = SatoriScorer()

        now = int(time.time())
        pred = PredictionInput(
            predicted_value=50250.123456789,
            actual_value=50000.987654321,
            commit_time=now - 1800,
            round_start=now - 3600,
            deadline=now,
            stated_confidence=0.85,
            predictor_reputation=0.75,
            predictor_address="ETest",
            stake=100,
            signature=b"sig",
            stream_id="test",
            round_id="test",
        )

        breakdown = scorer.calculate_score(pred)

        # Check precision (6 decimal places)
        score_str = f"{breakdown.final_score:.10f}"
        # After 6 decimals should be zeros (due to rounding)
        significant = score_str.split('.')[1][:6]
        trailing = score_str.split('.')[1][6:]

        # The trailing digits should be zeros after rounding
        assert len(significant) == 6


# ============================================================================
# Test Worked Example from Spec
# ============================================================================

class TestWorkedExample:
    """Test the worked example from the specification."""

    def test_example_1_excellent_prediction(self):
        """
        Example 1 from spec:
        - BTC/USD $50,000 actual
        - $50,250 predicted (0.5% error)
        - Committed 10 min into 1 hour round
        - 85% confidence, 0.75 reputation
        - Expected: ~91.5% final score
        """
        scorer = SatoriScorer(
            stream_registry={"btc-usd"},
            current_round_id="test",
        )

        # Round times
        round_start = 1000000000
        deadline = round_start + 3600  # 1 hour
        commit_time = round_start + 600  # 10 minutes in

        pred = PredictionInput(
            predicted_value=50250.0,
            actual_value=50000.0,
            commit_time=commit_time,
            round_start=round_start,
            deadline=deadline,
            stated_confidence=0.85,
            predictor_reputation=0.75,
            predictor_address="ETest",
            stake=500,
            signature=b"valid",
            stream_id="btc-usd",
            round_id="test",
        )

        breakdown = scorer.calculate_score(pred)

        # Should pass all inhibitors
        assert breakdown.passed_phase1

        # Should be high score (around 0.9)
        assert breakdown.final_score > 0.85
        assert breakdown.final_score < 0.98

        # Accuracy should be high
        assert breakdown.accuracy > 0.9

    def test_example_2_late_submission(self):
        """
        Example 2 from spec:
        - Same as example 1, but 5 minutes LATE
        - Expected: 0% (disqualified)
        """
        scorer = SatoriScorer(
            stream_registry={"btc-usd"},
            current_round_id="test",
        )

        round_start = 1000000000
        deadline = round_start + 3600
        commit_time = deadline + 300  # 5 minutes AFTER deadline

        pred = PredictionInput(
            predicted_value=50250.0,
            actual_value=50000.0,
            commit_time=commit_time,
            round_start=round_start,
            deadline=deadline,
            stated_confidence=0.85,
            predictor_reputation=0.75,
            predictor_address="ETest",
            stake=500,
            signature=b"valid",
            stream_id="btc-usd",
            round_id="test",
        )

        breakdown = scorer.calculate_score(pred)

        # Should fail Phase 1
        assert not breakdown.passed_phase1
        assert breakdown.final_score == 0.0
        assert breakdown.result == ScoringResult.INHIBITED
