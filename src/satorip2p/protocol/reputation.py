"""
Peer Reputation Score (Trust Score) System

Tracks peer behavior over time with a visual "trust meter" - good peers vs bad peers.

Score Ranges:
    90-100: Trusted (Blue, full) - Priority routing, full rewards
    70-89:  Good (Blue, high) - Normal rewards
    50-69:  Neutral (Gray) - Normal rewards
    30-49:  Suspect (Red, low) - Reduced rewards, monitored
    0-29:   Untrusted (Red, full) - No rewards, may be isolated

Positive Behaviors (increase score):
    - Consistent uptime (heartbeats match expected)
    - Honest data reporting (stats match DHT consensus)
    - Helpful peer behavior (serving data to others)
    - Long uptime streaks
    - Participating in consensus votes
    - Successful role verification challenges

Negative Behaviors (decrease score):
    - Stat manipulation (local stats don't match DHT consensus)
    - Inconsistent data (claims heartbeats but no peers witnessed them)
    - Downtime without explanation
    - Missing consensus rounds
    - Invalid signatures or bad data
    - Failed role verification challenges
    - Slashing events
"""

import logging
import time
import json
import hashlib
from typing import Dict, List, Optional, Set, Tuple, Any, TYPE_CHECKING
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path

if TYPE_CHECKING:
    from satorip2p.peers import Peers

logger = logging.getLogger(__name__)

# Score thresholds
SCORE_TRUSTED_MIN = 90
SCORE_GOOD_MIN = 70
SCORE_NEUTRAL_MIN = 50
SCORE_SUSPECT_MIN = 30
SCORE_UNTRUSTED_MAX = 29

# Starting score for new peers
DEFAULT_SCORE = 50

# Score change amounts
SCORE_CHANGE_HEARTBEAT = 0.1        # Per successful heartbeat witnessed
SCORE_CHANGE_UPTIME_STREAK = 1.0    # Per day of continuous uptime
SCORE_CHANGE_CONSENSUS_VOTE = 0.5   # Per consensus participation
SCORE_CHANGE_DATA_SERVED = 0.2      # Per data request served to peer
SCORE_CHANGE_ARCHIVE_VERIFIED = 1.0 # Per verified archive integrity

SCORE_PENALTY_MISSED_HEARTBEAT = -0.5   # Per missed expected heartbeat
SCORE_PENALTY_STAT_MISMATCH = -5.0      # Stats don't match DHT consensus
SCORE_PENALTY_MISSED_CONSENSUS = -1.0   # Missed consensus round
SCORE_PENALTY_INVALID_SIGNATURE = -10.0 # Submitted invalid signature
SCORE_PENALTY_DOUBLE_SIGN = -50.0       # Double-signing (severe)
SCORE_PENALTY_ARCHIVE_CORRUPTION = -20.0 # Archive data corrupted
SCORE_PENALTY_ROLE_CLAIM_FAILED = -5.0  # Claimed role but failed verification

# Score bounds
SCORE_MIN = 0
SCORE_MAX = 100

# DHT key prefix for reputation data
DHT_REPUTATION_PREFIX = "satori:reputation:"

# How often to sync reputation to DHT (seconds)
REPUTATION_SYNC_INTERVAL = 300  # 5 minutes

# Decay rate - scores slowly move toward neutral over time
SCORE_DECAY_RATE = 0.01  # Per hour, move 1% toward 50


class TrustLevel(Enum):
    """Trust level classification based on score."""
    TRUSTED = "trusted"
    GOOD = "good"
    NEUTRAL = "neutral"
    SUSPECT = "suspect"
    UNTRUSTED = "untrusted"


class ReputationEventType(Enum):
    """Types of events that affect reputation."""
    # Positive events
    HEARTBEAT_WITNESSED = "heartbeat_witnessed"
    UPTIME_STREAK = "uptime_streak"
    CONSENSUS_PARTICIPATED = "consensus_participated"
    DATA_SERVED = "data_served"
    ARCHIVE_VERIFIED = "archive_verified"
    ROLE_VERIFIED = "role_verified"

    # Negative events
    HEARTBEAT_MISSED = "heartbeat_missed"
    STAT_MISMATCH = "stat_mismatch"
    CONSENSUS_MISSED = "consensus_missed"
    INVALID_SIGNATURE = "invalid_signature"
    DOUBLE_SIGN = "double_sign"
    ARCHIVE_CORRUPTED = "archive_corrupted"
    ROLE_CLAIM_FAILED = "role_claim_failed"

    # Neutral
    SCORE_DECAY = "score_decay"


@dataclass
class ReputationEvent:
    """A single reputation-affecting event."""
    event_type: ReputationEventType
    peer_id: str
    score_change: float
    timestamp: int
    details: str = ""
    witnesses: List[str] = field(default_factory=list)  # Peers who witnessed this

    def to_dict(self) -> dict:
        return {
            'event_type': self.event_type.value,
            'peer_id': self.peer_id,
            'score_change': self.score_change,
            'timestamp': self.timestamp,
            'details': self.details,
            'witnesses': self.witnesses,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ReputationEvent":
        return cls(
            event_type=ReputationEventType(data['event_type']),
            peer_id=data['peer_id'],
            score_change=data['score_change'],
            timestamp=data['timestamp'],
            details=data.get('details', ''),
            witnesses=data.get('witnesses', []),
        )


@dataclass
class PeerReputation:
    """Reputation data for a single peer."""
    peer_id: str
    address: str  # Evrmore address
    score: float = DEFAULT_SCORE
    trust_level: TrustLevel = TrustLevel.NEUTRAL

    # Tracking
    total_positive_events: int = 0
    total_negative_events: int = 0
    last_positive_event: int = 0
    last_negative_event: int = 0
    last_updated: int = 0

    # Recent event history (last 100)
    recent_events: List[ReputationEvent] = field(default_factory=list)

    # Specific counters
    heartbeats_witnessed: int = 0
    consensus_participations: int = 0
    data_requests_served: int = 0
    archives_verified: int = 0
    roles_verified: int = 0

    heartbeats_missed: int = 0
    stat_mismatches: int = 0
    consensus_missed: int = 0
    invalid_signatures: int = 0
    double_signs: int = 0
    archives_corrupted: int = 0
    role_claims_failed: int = 0

    def to_dict(self) -> dict:
        return {
            'peer_id': self.peer_id,
            'address': self.address,
            'score': self.score,
            'trust_level': self.trust_level.value,
            'total_positive_events': self.total_positive_events,
            'total_negative_events': self.total_negative_events,
            'last_positive_event': self.last_positive_event,
            'last_negative_event': self.last_negative_event,
            'last_updated': self.last_updated,
            'recent_events': [e.to_dict() for e in self.recent_events[-100:]],
            'heartbeats_witnessed': self.heartbeats_witnessed,
            'consensus_participations': self.consensus_participations,
            'data_requests_served': self.data_requests_served,
            'archives_verified': self.archives_verified,
            'roles_verified': self.roles_verified,
            'heartbeats_missed': self.heartbeats_missed,
            'stat_mismatches': self.stat_mismatches,
            'consensus_missed': self.consensus_missed,
            'invalid_signatures': self.invalid_signatures,
            'double_signs': self.double_signs,
            'archives_corrupted': self.archives_corrupted,
            'role_claims_failed': self.role_claims_failed,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PeerReputation":
        rep = cls(
            peer_id=data['peer_id'],
            address=data.get('address', ''),
            score=data.get('score', DEFAULT_SCORE),
            trust_level=TrustLevel(data.get('trust_level', 'neutral')),
            total_positive_events=data.get('total_positive_events', 0),
            total_negative_events=data.get('total_negative_events', 0),
            last_positive_event=data.get('last_positive_event', 0),
            last_negative_event=data.get('last_negative_event', 0),
            last_updated=data.get('last_updated', 0),
            heartbeats_witnessed=data.get('heartbeats_witnessed', 0),
            consensus_participations=data.get('consensus_participations', 0),
            data_requests_served=data.get('data_requests_served', 0),
            archives_verified=data.get('archives_verified', 0),
            roles_verified=data.get('roles_verified', 0),
            heartbeats_missed=data.get('heartbeats_missed', 0),
            stat_mismatches=data.get('stat_mismatches', 0),
            consensus_missed=data.get('consensus_missed', 0),
            invalid_signatures=data.get('invalid_signatures', 0),
            double_signs=data.get('double_signs', 0),
            archives_corrupted=data.get('archives_corrupted', 0),
            role_claims_failed=data.get('role_claims_failed', 0),
        )
        # Parse recent events
        rep.recent_events = [
            ReputationEvent.from_dict(e) for e in data.get('recent_events', [])
        ]
        return rep


class ReputationManager:
    """
    Manages peer reputation scores across the network.

    Reputation is:
    - Stored locally with periodic DHT sync
    - Verified by multiple witnesses for important events
    - Decays slowly toward neutral over time
    - Affects reward multipliers and peer prioritization
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        storage_path: Optional[Path] = None,
    ):
        """
        Initialize the reputation manager.

        Args:
            peers: Peers instance for DHT/network operations
            storage_path: Path for local reputation storage
        """
        self._peers = peers
        self._storage_path = storage_path or Path.home() / ".satori" / "reputation"
        self._storage_path.mkdir(parents=True, exist_ok=True)

        # Reputation data: peer_id -> PeerReputation
        self._reputations: Dict[str, PeerReputation] = {}

        # My own peer ID (set on start)
        self._my_peer_id: str = ""
        self._my_address: str = ""

        # Last sync time
        self._last_dht_sync = 0

        # Event callbacks
        self._on_score_change_callbacks: List[callable] = []
        self._on_trust_level_change_callbacks: List[callable] = []

        # Load local data
        self._load_local_reputations()

        self._started = False

    async def start(self) -> None:
        """Start the reputation manager."""
        if self._started:
            return

        if self._peers:
            self._my_peer_id = self._peers.peer_id or ""
            if hasattr(self._peers, '_identity_bridge'):
                self._my_address = getattr(
                    self._peers._identity_bridge, 'address', ''
                ) or ""

        self._started = True
        logger.info("Reputation manager started")

    async def stop(self) -> None:
        """Stop the reputation manager."""
        if not self._started:
            return

        # Final save
        self._save_local_reputations()

        self._started = False
        logger.info("Reputation manager stopped")

    def _load_local_reputations(self) -> None:
        """Load reputation data from local storage."""
        try:
            rep_file = self._storage_path / "reputations.json"
            if rep_file.exists():
                with open(rep_file, 'r') as f:
                    data = json.load(f)
                for peer_id, rep_data in data.items():
                    self._reputations[peer_id] = PeerReputation.from_dict(rep_data)
                logger.debug(f"Loaded {len(self._reputations)} peer reputations")
        except Exception as e:
            logger.warning(f"Failed to load reputations: {e}")

    def _save_local_reputations(self) -> None:
        """Save reputation data to local storage."""
        try:
            rep_file = self._storage_path / "reputations.json"
            data = {
                peer_id: rep.to_dict()
                for peer_id, rep in self._reputations.items()
            }
            with open(rep_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save reputations: {e}")

    def _get_or_create_reputation(
        self,
        peer_id: str,
        address: str = ""
    ) -> PeerReputation:
        """Get or create reputation record for a peer."""
        if peer_id not in self._reputations:
            self._reputations[peer_id] = PeerReputation(
                peer_id=peer_id,
                address=address,
                last_updated=int(time.time()),
            )
        elif address and not self._reputations[peer_id].address:
            self._reputations[peer_id].address = address
        return self._reputations[peer_id]

    def _update_trust_level(self, rep: PeerReputation) -> bool:
        """
        Update trust level based on score.

        Returns True if trust level changed.
        """
        old_level = rep.trust_level

        if rep.score >= SCORE_TRUSTED_MIN:
            rep.trust_level = TrustLevel.TRUSTED
        elif rep.score >= SCORE_GOOD_MIN:
            rep.trust_level = TrustLevel.GOOD
        elif rep.score >= SCORE_NEUTRAL_MIN:
            rep.trust_level = TrustLevel.NEUTRAL
        elif rep.score >= SCORE_SUSPECT_MIN:
            rep.trust_level = TrustLevel.SUSPECT
        else:
            rep.trust_level = TrustLevel.UNTRUSTED

        return old_level != rep.trust_level

    def _clamp_score(self, score: float) -> float:
        """Clamp score to valid range."""
        return max(SCORE_MIN, min(SCORE_MAX, score))

    def record_event(
        self,
        peer_id: str,
        event_type: ReputationEventType,
        details: str = "",
        witnesses: Optional[List[str]] = None,
        address: str = "",
    ) -> float:
        """
        Record a reputation event for a peer.

        Args:
            peer_id: The peer's ID
            event_type: Type of event
            details: Optional details about the event
            witnesses: Optional list of peer IDs who witnessed this
            address: Optional Evrmore address

        Returns:
            The new score after this event
        """
        rep = self._get_or_create_reputation(peer_id, address)
        old_score = rep.score
        old_level = rep.trust_level

        # Determine score change
        score_change = self._get_score_change(event_type)

        # Create event record
        event = ReputationEvent(
            event_type=event_type,
            peer_id=peer_id,
            score_change=score_change,
            timestamp=int(time.time()),
            details=details,
            witnesses=witnesses or [],
        )

        # Apply score change
        rep.score = self._clamp_score(rep.score + score_change)
        rep.last_updated = event.timestamp

        # Update counters
        if score_change > 0:
            rep.total_positive_events += 1
            rep.last_positive_event = event.timestamp
        elif score_change < 0:
            rep.total_negative_events += 1
            rep.last_negative_event = event.timestamp

        # Update specific counters
        self._update_event_counters(rep, event_type)

        # Add to recent events
        rep.recent_events.append(event)
        if len(rep.recent_events) > 100:
            rep.recent_events = rep.recent_events[-100:]

        # Update trust level
        level_changed = self._update_trust_level(rep)

        # Callbacks
        if rep.score != old_score:
            for callback in self._on_score_change_callbacks:
                try:
                    callback(peer_id, old_score, rep.score)
                except Exception as e:
                    logger.warning(f"Score change callback error: {e}")

        if level_changed:
            for callback in self._on_trust_level_change_callbacks:
                try:
                    callback(peer_id, old_level, rep.trust_level)
                except Exception as e:
                    logger.warning(f"Trust level callback error: {e}")

        # Log significant events
        if abs(score_change) >= 5.0:
            logger.info(
                f"Reputation event: {peer_id[:16]}... {event_type.value} "
                f"({score_change:+.1f}) -> {rep.score:.1f} ({rep.trust_level.value})"
            )

        # Auto-save
        self._save_local_reputations()

        return rep.score

    def _get_score_change(self, event_type: ReputationEventType) -> float:
        """Get the score change for an event type."""
        changes = {
            # Positive
            ReputationEventType.HEARTBEAT_WITNESSED: SCORE_CHANGE_HEARTBEAT,
            ReputationEventType.UPTIME_STREAK: SCORE_CHANGE_UPTIME_STREAK,
            ReputationEventType.CONSENSUS_PARTICIPATED: SCORE_CHANGE_CONSENSUS_VOTE,
            ReputationEventType.DATA_SERVED: SCORE_CHANGE_DATA_SERVED,
            ReputationEventType.ARCHIVE_VERIFIED: SCORE_CHANGE_ARCHIVE_VERIFIED,
            ReputationEventType.ROLE_VERIFIED: SCORE_CHANGE_ARCHIVE_VERIFIED,
            # Negative
            ReputationEventType.HEARTBEAT_MISSED: SCORE_PENALTY_MISSED_HEARTBEAT,
            ReputationEventType.STAT_MISMATCH: SCORE_PENALTY_STAT_MISMATCH,
            ReputationEventType.CONSENSUS_MISSED: SCORE_PENALTY_MISSED_CONSENSUS,
            ReputationEventType.INVALID_SIGNATURE: SCORE_PENALTY_INVALID_SIGNATURE,
            ReputationEventType.DOUBLE_SIGN: SCORE_PENALTY_DOUBLE_SIGN,
            ReputationEventType.ARCHIVE_CORRUPTED: SCORE_PENALTY_ARCHIVE_CORRUPTION,
            ReputationEventType.ROLE_CLAIM_FAILED: SCORE_PENALTY_ROLE_CLAIM_FAILED,
            # Neutral
            ReputationEventType.SCORE_DECAY: 0,
        }
        return changes.get(event_type, 0)

    def _update_event_counters(
        self,
        rep: PeerReputation,
        event_type: ReputationEventType
    ) -> None:
        """Update specific event counters."""
        counter_map = {
            ReputationEventType.HEARTBEAT_WITNESSED: 'heartbeats_witnessed',
            ReputationEventType.CONSENSUS_PARTICIPATED: 'consensus_participations',
            ReputationEventType.DATA_SERVED: 'data_requests_served',
            ReputationEventType.ARCHIVE_VERIFIED: 'archives_verified',
            ReputationEventType.ROLE_VERIFIED: 'roles_verified',
            ReputationEventType.HEARTBEAT_MISSED: 'heartbeats_missed',
            ReputationEventType.STAT_MISMATCH: 'stat_mismatches',
            ReputationEventType.CONSENSUS_MISSED: 'consensus_missed',
            ReputationEventType.INVALID_SIGNATURE: 'invalid_signatures',
            ReputationEventType.DOUBLE_SIGN: 'double_signs',
            ReputationEventType.ARCHIVE_CORRUPTED: 'archives_corrupted',
            ReputationEventType.ROLE_CLAIM_FAILED: 'role_claims_failed',
        }
        attr = counter_map.get(event_type)
        if attr:
            setattr(rep, attr, getattr(rep, attr) + 1)

    def get_reputation(self, peer_id: str) -> Optional[PeerReputation]:
        """Get reputation for a peer."""
        return self._reputations.get(peer_id)

    def get_score(self, peer_id: str) -> float:
        """Get score for a peer (returns default if unknown)."""
        rep = self._reputations.get(peer_id)
        return rep.score if rep else DEFAULT_SCORE

    def get_trust_level(self, peer_id: str) -> TrustLevel:
        """Get trust level for a peer (returns neutral if unknown)."""
        rep = self._reputations.get(peer_id)
        return rep.trust_level if rep else TrustLevel.NEUTRAL

    def is_trusted(self, peer_id: str) -> bool:
        """Check if peer is trusted (score >= 70)."""
        return self.get_score(peer_id) >= SCORE_GOOD_MIN

    def is_suspect(self, peer_id: str) -> bool:
        """Check if peer is suspect (score < 50)."""
        return self.get_score(peer_id) < SCORE_NEUTRAL_MIN

    def get_all_reputations(self) -> Dict[str, PeerReputation]:
        """Get all reputation records."""
        return dict(self._reputations)

    def get_trusted_peers(self) -> List[str]:
        """Get list of trusted peer IDs (score >= 70)."""
        return [
            peer_id for peer_id, rep in self._reputations.items()
            if rep.score >= SCORE_GOOD_MIN
        ]

    def get_suspect_peers(self) -> List[str]:
        """Get list of suspect peer IDs (score < 50)."""
        return [
            peer_id for peer_id, rep in self._reputations.items()
            if rep.score < SCORE_NEUTRAL_MIN
        ]

    def get_leaderboard(self, limit: int = 20) -> List[PeerReputation]:
        """Get top peers by reputation score."""
        sorted_reps = sorted(
            self._reputations.values(),
            key=lambda r: r.score,
            reverse=True
        )
        return sorted_reps[:limit]

    def get_reward_multiplier(self, peer_id: str) -> float:
        """
        Get reward multiplier based on reputation.

        Returns:
            Multiplier from 0.0 (untrusted) to 1.1 (trusted bonus)
        """
        score = self.get_score(peer_id)

        if score >= SCORE_TRUSTED_MIN:
            return 1.10  # +10% bonus for trusted
        elif score >= SCORE_GOOD_MIN:
            return 1.0   # Normal rewards
        elif score >= SCORE_NEUTRAL_MIN:
            return 1.0   # Normal rewards
        elif score >= SCORE_SUSPECT_MIN:
            return 0.75  # -25% for suspect
        else:
            return 0.0   # No rewards for untrusted

    def apply_decay(self) -> None:
        """
        Apply score decay - slowly move scores toward neutral.

        Should be called periodically (e.g., hourly).
        """
        now = int(time.time())
        for peer_id, rep in self._reputations.items():
            if rep.score != DEFAULT_SCORE:
                # Calculate decay toward 50
                diff = rep.score - DEFAULT_SCORE
                decay = diff * SCORE_DECAY_RATE
                rep.score = self._clamp_score(rep.score - decay)
                rep.last_updated = now

        self._save_local_reputations()

    async def sync_to_dht(self) -> bool:
        """
        Sync my peer's reputation to DHT for network consensus.

        Returns:
            True if sync successful
        """
        if not self._peers or not self._my_peer_id:
            return False

        my_rep = self._reputations.get(self._my_peer_id)
        if not my_rep:
            return False

        try:
            key = f"{DHT_REPUTATION_PREFIX}{self._my_peer_id}"
            value = json.dumps(my_rep.to_dict())

            # Store in DHT (implementation depends on Peers interface)
            if hasattr(self._peers, 'put_dht'):
                await self._peers.put_dht(key, value.encode())

            self._last_dht_sync = int(time.time())
            logger.debug(f"Synced reputation to DHT: score={my_rep.score:.1f}")
            return True
        except Exception as e:
            logger.warning(f"Failed to sync reputation to DHT: {e}")
            return False

    async def fetch_from_dht(self, peer_id: str) -> Optional[PeerReputation]:
        """
        Fetch a peer's reputation from DHT.

        Args:
            peer_id: The peer's ID

        Returns:
            PeerReputation if found
        """
        if not self._peers:
            return None

        try:
            key = f"{DHT_REPUTATION_PREFIX}{peer_id}"

            # Fetch from DHT
            if hasattr(self._peers, 'get_dht'):
                value = await self._peers.get_dht(key)
                if value:
                    data = json.loads(value.decode())
                    return PeerReputation.from_dict(data)
        except Exception as e:
            logger.debug(f"Failed to fetch reputation from DHT: {e}")

        return None

    def get_status(self) -> Dict[str, Any]:
        """Get overall reputation system status."""
        total_peers = len(self._reputations)
        trusted = len(self.get_trusted_peers())
        suspect = len(self.get_suspect_peers())

        return {
            'total_peers': total_peers,
            'trusted_peers': trusted,
            'good_peers': sum(
                1 for r in self._reputations.values()
                if SCORE_GOOD_MIN <= r.score < SCORE_TRUSTED_MIN
            ),
            'neutral_peers': sum(
                1 for r in self._reputations.values()
                if SCORE_NEUTRAL_MIN <= r.score < SCORE_GOOD_MIN
            ),
            'suspect_peers': sum(
                1 for r in self._reputations.values()
                if SCORE_SUSPECT_MIN <= r.score < SCORE_NEUTRAL_MIN
            ),
            'untrusted_peers': sum(
                1 for r in self._reputations.values()
                if r.score < SCORE_SUSPECT_MIN
            ),
            'my_score': self.get_score(self._my_peer_id) if self._my_peer_id else None,
            'my_trust_level': self.get_trust_level(self._my_peer_id).value if self._my_peer_id else None,
            'last_dht_sync': self._last_dht_sync,
        }

    def on_score_change(self, callback: callable) -> None:
        """Register callback for score changes: callback(peer_id, old_score, new_score)."""
        self._on_score_change_callbacks.append(callback)

    def on_trust_level_change(self, callback: callable) -> None:
        """Register callback for trust level changes: callback(peer_id, old_level, new_level)."""
        self._on_trust_level_change_callbacks.append(callback)


# Convenience functions for recording common events

def record_heartbeat_witnessed(
    manager: ReputationManager,
    peer_id: str,
    witnesses: Optional[List[str]] = None
) -> float:
    """Record that we witnessed a peer's heartbeat."""
    return manager.record_event(
        peer_id,
        ReputationEventType.HEARTBEAT_WITNESSED,
        witnesses=witnesses
    )


def record_consensus_participation(
    manager: ReputationManager,
    peer_id: str,
    round_id: str
) -> float:
    """Record that a peer participated in consensus."""
    return manager.record_event(
        peer_id,
        ReputationEventType.CONSENSUS_PARTICIPATED,
        details=f"round:{round_id}"
    )


def record_double_sign(
    manager: ReputationManager,
    peer_id: str,
    round_id: str,
    witnesses: List[str]
) -> float:
    """Record a double-signing event (severe penalty)."""
    return manager.record_event(
        peer_id,
        ReputationEventType.DOUBLE_SIGN,
        details=f"round:{round_id}",
        witnesses=witnesses
    )


def record_invalid_signature(
    manager: ReputationManager,
    peer_id: str,
    details: str = ""
) -> float:
    """Record an invalid signature event."""
    return manager.record_event(
        peer_id,
        ReputationEventType.INVALID_SIGNATURE,
        details=details
    )


def record_role_verification(
    manager: ReputationManager,
    peer_id: str,
    role: str,
    success: bool
) -> float:
    """Record result of a role verification challenge."""
    if success:
        return manager.record_event(
            peer_id,
            ReputationEventType.ROLE_VERIFIED,
            details=f"role:{role}"
        )
    else:
        return manager.record_event(
            peer_id,
            ReputationEventType.ROLE_CLAIM_FAILED,
            details=f"role:{role}"
        )
