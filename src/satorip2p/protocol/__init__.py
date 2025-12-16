"""
satorip2p/protocol/

Protocol implementations for Satori P2P networking.
"""

from .subscriptions import SubscriptionManager
from .message_store import MessageStore
from .messages import serialize_message, deserialize_message
from .rendezvous import RendezvousManager, RendezvousRegistration
from .peer_registry import PeerRegistry, PeerAnnouncement
from .stream_registry import StreamRegistry, StreamDefinition, StreamClaim
from .oracle_network import OracleNetwork, Observation, OracleRegistration
from .prediction_protocol import PredictionProtocol, Prediction, PredictionScore
from .rewards import (
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
)

__all__ = [
    "SubscriptionManager",
    "MessageStore",
    "serialize_message",
    "deserialize_message",
    "RendezvousManager",
    "RendezvousRegistration",
    "PeerRegistry",
    "PeerAnnouncement",
    "StreamRegistry",
    "StreamDefinition",
    "StreamClaim",
    "OracleNetwork",
    "Observation",
    "OracleRegistration",
    "PredictionProtocol",
    "Prediction",
    "PredictionScore",
    # Rewards (Phase 5)
    "SatoriScorer",
    "RewardCalculator",
    "RoundDataStore",
    "PredictionInput",
    "ScoreBreakdown",
    "ScoringResult",
    "RewardEntry",
    "RoundSummary",
    "InhibitorResult",
    "score_prediction",
    "verify_score",
    "verify_reward_claim",
    "get_round_boundaries",
]
