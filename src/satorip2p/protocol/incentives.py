"""
Incentive Coordination for Satori P2P Network

Ensures nodes run required managers (not just heartbeats) through role verification
challenges and activity-based bonuses. This coordinates the incentive mechanisms
across reputation, slashing, and rewards.

Key Features:
    - Role verification via periodic challenges
    - Activity-based reward bonuses
    - Uptime tracking integration
    - Multi-role coordination
    - Lazy node detection (heartbeat-only nodes)

Role Verification:
    Nodes claiming roles (Oracle, Relay, Signer) must prove they're actually
    running those services, not just responding to heartbeats.
"""

import logging
import time
import json
import hashlib
import random
from typing import Dict, List, Optional, Set, Tuple, Any, TYPE_CHECKING
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path

if TYPE_CHECKING:
    from satorip2p.peers import Peers
    from satorip2p.protocol.reputation import ReputationManager
    from satorip2p.protocol.slashing import SlashingManager

logger = logging.getLogger(__name__)

# Challenge configuration
CHALLENGE_INTERVAL = 3600           # Issue challenges every hour
CHALLENGE_TIMEOUT = 300             # 5 minutes to respond
CHALLENGE_HISTORY_SIZE = 100        # Keep last 100 challenges per peer

# Activity windows
ACTIVITY_WINDOW_SHORT = 3600        # 1 hour for recent activity
ACTIVITY_WINDOW_MEDIUM = 86400      # 24 hours for daily activity
ACTIVITY_WINDOW_LONG = 604800       # 7 days for weekly activity

# =============================================================================
# ACTIVITY BONUSES (Verification-Based, Non-Redundant)
# =============================================================================
# These bonuses are ONLY given for activities not already covered by other
# multipliers. They require passing verification challenges, not just claiming.
#
# Existing bonuses (NOT duplicated here):
# - Uptime: Already covered by uptime_streak_bonus in rewards.py
# - Oracle role: Already covered by role_multiplier (+10%)
# - Relay role: Already covered by role_multiplier (+5%)
# - Governance: Already covered by governance_bonus in rewards.py
#
# NEW unique activities rewarded here:
# - Challenge Response: Passing role verification challenges
# - Peer Mentoring: Successfully helping new nodes bootstrap
# - Data Availability: Providing requested historical data to peers
# - Network Health: Contributing to network stability metrics

# Challenge response bonus (proving you're actually running services)
CHALLENGE_PASS_BONUS = 0.05             # +5% for passing role challenges

# Peer mentoring bonus (helping new nodes)
MENTOR_BONUS_THRESHOLD = 3              # Help at least 3 new nodes
MENTOR_BONUS = 0.03                     # +3% for active mentoring

# Data availability bonus (serving historical data requests)
DATA_AVAILABILITY_BONUS_THRESHOLD = 0.95  # 95% request fulfillment rate
DATA_AVAILABILITY_BONUS = 0.04          # +4% for high data availability

# Network health contribution bonus
NETWORK_HEALTH_BONUS = 0.03             # +3% for contributing to network metrics

# Maximum total activity bonus (all unique activities combined)
MAX_ACTIVITY_BONUS = 0.15               # +15% maximum (5+3+4+3)

# Legacy constants (kept for backwards compatibility, but values set to 0)
# These are now handled by their respective systems, not double-counted
UPTIME_BONUS_THRESHOLD_HIGH = 0.99
UPTIME_BONUS_THRESHOLD_MEDIUM = 0.95
UPTIME_BONUS_THRESHOLD_LOW = 0.90
UPTIME_BONUS_HIGH = 0.0                 # Now 0 - handled by uptime_streak_bonus
UPTIME_BONUS_MEDIUM = 0.0
UPTIME_BONUS_LOW = 0.0
ACTIVITY_BONUS_ORACLE = 0.0             # Now 0 - handled by role_multiplier
ACTIVITY_BONUS_RELAY = 0.0              # Now 0 - handled by role_multiplier
ACTIVITY_BONUS_CONSENSUS = 0.0          # Now 0 - signers get role bonus
ACTIVITY_BONUS_GOVERNANCE = 0.0         # Now 0 - handled by governance_bonus

# DHT and PubSub
DHT_INCENTIVE_PREFIX = "satori:incentive:"
CHALLENGE_TOPIC = "satori/incentive/challenges"
RESPONSE_TOPIC = "satori/incentive/responses"


class RoleType(Enum):
    """Types of roles that can be verified."""
    ORACLE = "oracle"
    RELAY = "relay"
    SIGNER = "signer"
    ARCHIVER = "archiver"
    CURATOR = "curator"


class ChallengeType(Enum):
    """Types of verification challenges."""
    ORACLE_DATA = "oracle_data"           # Request recent oracle observation
    RELAY_FORWARD = "relay_forward"       # Request message forwarding proof
    SIGNER_PARTIAL = "signer_partial"     # Request partial signature
    ARCHIVE_PROOF = "archive_proof"       # Request archived data proof
    CURATOR_STATUS = "curator_status"     # Request curation activity


class ChallengeStatus(Enum):
    """Status of a challenge."""
    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass
class RoleChallenge:
    """A challenge issued to verify role activity."""
    challenge_id: str
    peer_id: str
    role: RoleType
    challenge_type: ChallengeType
    challenge_data: Dict[str, Any]
    status: ChallengeStatus = ChallengeStatus.PENDING
    issued_at: int = 0
    expires_at: int = 0
    responded_at: int = 0
    response_data: Dict[str, Any] = field(default_factory=dict)
    verified: bool = False

    def to_dict(self) -> dict:
        return {
            'challenge_id': self.challenge_id,
            'peer_id': self.peer_id,
            'role': self.role.value,
            'challenge_type': self.challenge_type.value,
            'challenge_data': self.challenge_data,
            'status': self.status.value,
            'issued_at': self.issued_at,
            'expires_at': self.expires_at,
            'responded_at': self.responded_at,
            'response_data': self.response_data,
            'verified': self.verified,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RoleChallenge":
        return cls(
            challenge_id=data['challenge_id'],
            peer_id=data['peer_id'],
            role=RoleType(data['role']),
            challenge_type=ChallengeType(data['challenge_type']),
            challenge_data=data.get('challenge_data', {}),
            status=ChallengeStatus(data.get('status', 'pending')),
            issued_at=data.get('issued_at', 0),
            expires_at=data.get('expires_at', 0),
            responded_at=data.get('responded_at', 0),
            response_data=data.get('response_data', {}),
            verified=data.get('verified', False),
        )


@dataclass
class ActivityRecord:
    """Record of a peer's activity across roles."""
    peer_id: str

    # Activity counts in current window
    oracle_observations: int = 0
    messages_relayed: int = 0
    consensus_participations: int = 0
    signatures_contributed: int = 0
    archives_served: int = 0
    curations_submitted: int = 0
    governance_votes: int = 0

    # Challenge history
    challenges_received: int = 0
    challenges_passed: int = 0
    challenges_failed: int = 0

    # Uptime metrics
    uptime_samples: int = 0
    online_samples: int = 0

    # Timing
    first_activity_at: int = 0
    last_activity_at: int = 0
    window_start: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ActivityRecord":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def get_uptime_percent(self) -> float:
        """Get uptime percentage."""
        if self.uptime_samples == 0:
            return 0.0
        return self.online_samples / self.uptime_samples

    def get_challenge_pass_rate(self) -> float:
        """Get challenge pass rate."""
        if self.challenges_received == 0:
            return 1.0  # No challenges yet
        return self.challenges_passed / self.challenges_received

    def reset_window(self) -> None:
        """Reset activity counts for new window."""
        self.oracle_observations = 0
        self.messages_relayed = 0
        self.consensus_participations = 0
        self.signatures_contributed = 0
        self.archives_served = 0
        self.curations_submitted = 0
        self.governance_votes = 0
        self.window_start = int(time.time())


@dataclass
class IncentiveProfile:
    """Complete incentive profile for a peer."""
    peer_id: str
    address: str = ""

    # Claimed roles
    claimed_roles: List[str] = field(default_factory=list)

    # Verified roles (passed challenge within window)
    verified_roles: List[str] = field(default_factory=list)

    # Activity record
    activity: Optional[ActivityRecord] = None

    # Current bonuses
    uptime_bonus: float = 0.0
    activity_bonus: float = 0.0
    total_bonus: float = 0.0

    # Lazy node detection
    is_lazy_node: bool = False        # Only heartbeats, no real work
    lazy_warning_count: int = 0

    # Last verification
    last_challenge_at: int = 0
    next_challenge_at: int = 0

    def to_dict(self) -> dict:
        return {
            'peer_id': self.peer_id,
            'address': self.address,
            'claimed_roles': self.claimed_roles,
            'verified_roles': self.verified_roles,
            'activity': self.activity.to_dict() if self.activity else None,
            'uptime_bonus': self.uptime_bonus,
            'activity_bonus': self.activity_bonus,
            'total_bonus': self.total_bonus,
            'is_lazy_node': self.is_lazy_node,
            'lazy_warning_count': self.lazy_warning_count,
            'last_challenge_at': self.last_challenge_at,
            'next_challenge_at': self.next_challenge_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "IncentiveProfile":
        profile = cls(
            peer_id=data['peer_id'],
            address=data.get('address', ''),
            claimed_roles=data.get('claimed_roles', []),
            verified_roles=data.get('verified_roles', []),
            uptime_bonus=data.get('uptime_bonus', 0.0),
            activity_bonus=data.get('activity_bonus', 0.0),
            total_bonus=data.get('total_bonus', 0.0),
            is_lazy_node=data.get('is_lazy_node', False),
            lazy_warning_count=data.get('lazy_warning_count', 0),
            last_challenge_at=data.get('last_challenge_at', 0),
            next_challenge_at=data.get('next_challenge_at', 0),
        )
        if data.get('activity'):
            profile.activity = ActivityRecord.from_dict(data['activity'])
        return profile


class IncentiveCoordinator:
    """
    Coordinates incentive mechanisms across the network.

    Ensures nodes are actually performing their claimed roles, not just
    responding to heartbeats. Works with ReputationManager and SlashingManager.
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        reputation_manager: Optional["ReputationManager"] = None,
        slashing_manager: Optional["SlashingManager"] = None,
        storage_path: Optional[Path] = None,
    ):
        """
        Initialize the incentive coordinator.

        Args:
            peers: Peers instance for network operations
            reputation_manager: ReputationManager for reputation updates
            slashing_manager: SlashingManager for penalty coordination
            storage_path: Path for local storage
        """
        self._peers = peers
        self._reputation_manager = reputation_manager
        self._slashing_manager = slashing_manager
        self._storage_path = storage_path or Path.home() / ".satori" / "incentives"
        self._storage_path.mkdir(parents=True, exist_ok=True)

        # Peer profiles: peer_id -> IncentiveProfile
        self._profiles: Dict[str, IncentiveProfile] = {}

        # Active challenges: challenge_id -> RoleChallenge
        self._challenges: Dict[str, RoleChallenge] = {}

        # Challenge history per peer: peer_id -> List[RoleChallenge]
        self._challenge_history: Dict[str, List[RoleChallenge]] = {}

        # Callbacks
        self._on_lazy_node_callbacks: List[callable] = []
        self._on_role_verified_callbacks: List[callable] = []

        # Load local data
        self._load_local_data()

        self._started = False

    async def start(self) -> None:
        """Start the incentive coordinator."""
        if self._started:
            return

        if self._peers:
            await self._peers.subscribe_async(CHALLENGE_TOPIC, self._on_challenge_message)
            await self._peers.subscribe_async(RESPONSE_TOPIC, self._on_response_message)

        self._started = True
        logger.info("Incentive coordinator started")

    async def stop(self) -> None:
        """Stop the incentive coordinator."""
        if not self._started:
            return

        self._save_local_data()

        if self._peers:
            self._peers.unsubscribe(CHALLENGE_TOPIC)
            self._peers.unsubscribe(RESPONSE_TOPIC)

        self._started = False
        logger.info("Incentive coordinator stopped")

    def _load_local_data(self) -> None:
        """Load data from local storage."""
        try:
            profiles_file = self._storage_path / "profiles.json"
            if profiles_file.exists():
                with open(profiles_file, 'r') as f:
                    data = json.load(f)
                for peer_id, profile_data in data.items():
                    self._profiles[peer_id] = IncentiveProfile.from_dict(profile_data)

            logger.debug(f"Loaded {len(self._profiles)} incentive profiles")
        except Exception as e:
            logger.warning(f"Failed to load incentive data: {e}")

    def _save_local_data(self) -> None:
        """Save data to local storage."""
        try:
            profiles_file = self._storage_path / "profiles.json"
            profiles_data = {pid: p.to_dict() for pid, p in self._profiles.items()}
            with open(profiles_file, 'w') as f:
                json.dump(profiles_data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save incentive data: {e}")

    def _get_or_create_profile(self, peer_id: str, address: str = "") -> IncentiveProfile:
        """Get or create incentive profile."""
        if peer_id not in self._profiles:
            self._profiles[peer_id] = IncentiveProfile(
                peer_id=peer_id,
                address=address,
                activity=ActivityRecord(peer_id=peer_id, window_start=int(time.time())),
            )
        elif address and not self._profiles[peer_id].address:
            self._profiles[peer_id].address = address
        return self._profiles[peer_id]

    def _generate_challenge_id(self, peer_id: str, role: RoleType) -> str:
        """Generate unique challenge ID."""
        content = f"{peer_id}:{role.value}:{time.time()}:{random.random()}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    # -------------------------------------------------------------------------
    # Activity Recording
    # -------------------------------------------------------------------------

    def record_oracle_observation(self, peer_id: str, stream_id: str = "") -> None:
        """Record an oracle observation from a peer."""
        profile = self._get_or_create_profile(peer_id)
        if profile.activity:
            profile.activity.oracle_observations += 1
            profile.activity.last_activity_at = int(time.time())
            if not profile.activity.first_activity_at:
                profile.activity.first_activity_at = int(time.time())
        self._update_bonuses(profile)

    def record_message_relayed(self, peer_id: str, message_count: int = 1) -> None:
        """Record messages relayed by a peer."""
        profile = self._get_or_create_profile(peer_id)
        if profile.activity:
            profile.activity.messages_relayed += message_count
            profile.activity.last_activity_at = int(time.time())
        self._update_bonuses(profile)

    def record_consensus_participation(self, peer_id: str, round_id: str = "") -> None:
        """Record consensus participation from a peer."""
        profile = self._get_or_create_profile(peer_id)
        if profile.activity:
            profile.activity.consensus_participations += 1
            profile.activity.last_activity_at = int(time.time())
        self._update_bonuses(profile)

    def record_signature_contribution(self, peer_id: str, round_id: str = "") -> None:
        """Record signature contribution from a signer peer."""
        profile = self._get_or_create_profile(peer_id)
        if profile.activity:
            profile.activity.signatures_contributed += 1
            profile.activity.last_activity_at = int(time.time())
        self._update_bonuses(profile)

    def record_archive_served(self, peer_id: str, round_id: str = "") -> None:
        """Record archive data served by a peer."""
        profile = self._get_or_create_profile(peer_id)
        if profile.activity:
            profile.activity.archives_served += 1
            profile.activity.last_activity_at = int(time.time())
        self._update_bonuses(profile)

    def record_curation_submitted(self, peer_id: str, stream_id: str = "") -> None:
        """Record curation activity from a peer."""
        profile = self._get_or_create_profile(peer_id)
        if profile.activity:
            profile.activity.curations_submitted += 1
            profile.activity.last_activity_at = int(time.time())
        self._update_bonuses(profile)

    def record_governance_vote(self, peer_id: str, proposal_id: str = "") -> None:
        """Record governance vote from a peer."""
        profile = self._get_or_create_profile(peer_id)
        if profile.activity:
            profile.activity.governance_votes += 1
            profile.activity.last_activity_at = int(time.time())
        self._update_bonuses(profile)

    def record_uptime_sample(self, peer_id: str, is_online: bool) -> None:
        """Record an uptime sample for a peer."""
        profile = self._get_or_create_profile(peer_id)
        if profile.activity:
            profile.activity.uptime_samples += 1
            if is_online:
                profile.activity.online_samples += 1
        self._update_bonuses(profile)

    # -------------------------------------------------------------------------
    # Role Claiming and Verification
    # -------------------------------------------------------------------------

    def claim_role(self, peer_id: str, role: RoleType) -> None:
        """Register that a peer claims to perform a role."""
        profile = self._get_or_create_profile(peer_id)
        if role.value not in profile.claimed_roles:
            profile.claimed_roles.append(role.value)
            logger.debug(f"Peer {peer_id[:16]}... claimed role: {role.value}")

    def unclaim_role(self, peer_id: str, role: RoleType) -> None:
        """Remove a role claim from a peer."""
        profile = self._get_or_create_profile(peer_id)
        if role.value in profile.claimed_roles:
            profile.claimed_roles.remove(role.value)
        if role.value in profile.verified_roles:
            profile.verified_roles.remove(role.value)

    def get_claimed_roles(self, peer_id: str) -> List[RoleType]:
        """Get roles claimed by a peer."""
        profile = self._profiles.get(peer_id)
        if not profile:
            return []
        return [RoleType(r) for r in profile.claimed_roles]

    def get_verified_roles(self, peer_id: str) -> List[RoleType]:
        """Get roles verified for a peer."""
        profile = self._profiles.get(peer_id)
        if not profile:
            return []
        return [RoleType(r) for r in profile.verified_roles]

    def is_role_verified(self, peer_id: str, role: RoleType) -> bool:
        """Check if a role is verified for a peer."""
        profile = self._profiles.get(peer_id)
        if not profile:
            return False
        return role.value in profile.verified_roles

    # -------------------------------------------------------------------------
    # Challenge System
    # -------------------------------------------------------------------------

    def create_challenge(
        self,
        peer_id: str,
        role: RoleType,
    ) -> RoleChallenge:
        """
        Create a challenge for a peer to verify their role.

        Args:
            peer_id: The peer to challenge
            role: The role to verify

        Returns:
            The created RoleChallenge
        """
        now = int(time.time())
        challenge_id = self._generate_challenge_id(peer_id, role)

        # Generate challenge data based on role
        challenge_type, challenge_data = self._generate_challenge_data(role)

        challenge = RoleChallenge(
            challenge_id=challenge_id,
            peer_id=peer_id,
            role=role,
            challenge_type=challenge_type,
            challenge_data=challenge_data,
            status=ChallengeStatus.PENDING,
            issued_at=now,
            expires_at=now + CHALLENGE_TIMEOUT,
        )

        self._challenges[challenge_id] = challenge

        # Add to history
        if peer_id not in self._challenge_history:
            self._challenge_history[peer_id] = []
        self._challenge_history[peer_id].append(challenge)

        # Trim history
        if len(self._challenge_history[peer_id]) > CHALLENGE_HISTORY_SIZE:
            self._challenge_history[peer_id] = self._challenge_history[peer_id][-CHALLENGE_HISTORY_SIZE:]

        # Update profile
        profile = self._get_or_create_profile(peer_id)
        profile.last_challenge_at = now
        profile.next_challenge_at = now + CHALLENGE_INTERVAL
        if profile.activity:
            profile.activity.challenges_received += 1

        logger.debug(f"Created challenge {challenge_id} for {peer_id[:16]}... role={role.value}")

        return challenge

    def _generate_challenge_data(self, role: RoleType) -> Tuple[ChallengeType, Dict[str, Any]]:
        """Generate challenge data based on role."""
        now = int(time.time())

        if role == RoleType.ORACLE:
            return ChallengeType.ORACLE_DATA, {
                'request': 'latest_observation',
                'max_age': 3600,  # Within last hour
                'nonce': hashlib.sha256(str(now + random.random()).encode()).hexdigest()[:16],
            }

        elif role == RoleType.RELAY:
            return ChallengeType.RELAY_FORWARD, {
                'request': 'relay_stats',
                'window': 3600,  # Last hour stats
                'nonce': hashlib.sha256(str(now + random.random()).encode()).hexdigest()[:16],
            }

        elif role == RoleType.SIGNER:
            return ChallengeType.SIGNER_PARTIAL, {
                'request': 'signing_status',
                'nonce': hashlib.sha256(str(now + random.random()).encode()).hexdigest()[:16],
            }

        elif role == RoleType.ARCHIVER:
            return ChallengeType.ARCHIVE_PROOF, {
                'request': 'archive_sample',
                'nonce': hashlib.sha256(str(now + random.random()).encode()).hexdigest()[:16],
            }

        elif role == RoleType.CURATOR:
            return ChallengeType.CURATOR_STATUS, {
                'request': 'curation_activity',
                'window': 86400,  # Last 24 hours
                'nonce': hashlib.sha256(str(now + random.random()).encode()).hexdigest()[:16],
            }

        # Default
        return ChallengeType.ORACLE_DATA, {'nonce': str(now)}

    def respond_to_challenge(
        self,
        challenge_id: str,
        response_data: Dict[str, Any],
    ) -> bool:
        """
        Respond to a challenge.

        Args:
            challenge_id: The challenge to respond to
            response_data: The response data

        Returns:
            True if response accepted and verified
        """
        challenge = self._challenges.get(challenge_id)
        if not challenge:
            logger.warning(f"Challenge {challenge_id} not found")
            return False

        now = int(time.time())

        # Check if expired
        if now > challenge.expires_at:
            challenge.status = ChallengeStatus.EXPIRED
            return False

        # Record response
        challenge.responded_at = now
        challenge.response_data = response_data

        # Verify response
        verified = self._verify_challenge_response(challenge, response_data)
        challenge.verified = verified

        if verified:
            challenge.status = ChallengeStatus.PASSED
            profile = self._get_or_create_profile(challenge.peer_id)
            if profile.activity:
                profile.activity.challenges_passed += 1

            # Add to verified roles
            if challenge.role.value not in profile.verified_roles:
                profile.verified_roles.append(challenge.role.value)

            # Notify callbacks
            for callback in self._on_role_verified_callbacks:
                try:
                    callback(challenge.peer_id, challenge.role)
                except Exception as e:
                    logger.warning(f"Role verified callback error: {e}")

            logger.debug(f"Challenge {challenge_id} PASSED for {challenge.peer_id[:16]}...")

        else:
            challenge.status = ChallengeStatus.FAILED
            profile = self._get_or_create_profile(challenge.peer_id)
            if profile.activity:
                profile.activity.challenges_failed += 1

            # Check for lazy node pattern
            self._check_lazy_node(challenge.peer_id)

            logger.warning(f"Challenge {challenge_id} FAILED for {challenge.peer_id[:16]}...")

        return verified

    def _verify_challenge_response(
        self,
        challenge: RoleChallenge,
        response: Dict[str, Any],
    ) -> bool:
        """
        Verify a challenge response.

        This is a basic implementation - real verification would check
        actual data validity, signatures, etc.
        """
        if not response:
            return False

        # Verify nonce matches
        expected_nonce = challenge.challenge_data.get('nonce')
        response_nonce = response.get('nonce')
        if expected_nonce and response_nonce != expected_nonce:
            logger.debug("Nonce mismatch in challenge response")
            return False

        # Basic verification based on challenge type
        if challenge.challenge_type == ChallengeType.ORACLE_DATA:
            # Check for observation data
            if not response.get('observation'):
                return False
            # Check age
            obs_time = response.get('observation', {}).get('timestamp', 0)
            max_age = challenge.challenge_data.get('max_age', 3600)
            if int(time.time()) - obs_time > max_age:
                return False
            return True

        elif challenge.challenge_type == ChallengeType.RELAY_FORWARD:
            # Check for relay stats
            if 'messages_forwarded' not in response:
                return False
            return True

        elif challenge.challenge_type == ChallengeType.SIGNER_PARTIAL:
            # Check for signing status
            if 'signer_status' not in response:
                return False
            return True

        elif challenge.challenge_type == ChallengeType.ARCHIVE_PROOF:
            # Check for archive data
            if 'archive_sample' not in response:
                return False
            return True

        elif challenge.challenge_type == ChallengeType.CURATOR_STATUS:
            # Check for curation activity
            if 'curations' not in response:
                return False
            return True

        return False

    def _check_lazy_node(self, peer_id: str) -> None:
        """Check if a peer is a lazy node (heartbeats only)."""
        history = self._challenge_history.get(peer_id, [])
        if len(history) < 3:
            return  # Not enough history

        # Check recent challenges
        recent = history[-5:]
        failed = sum(1 for c in recent if c.status == ChallengeStatus.FAILED)

        if failed >= 3:
            profile = self._get_or_create_profile(peer_id)
            profile.lazy_warning_count += 1

            if profile.lazy_warning_count >= 3:
                profile.is_lazy_node = True
                logger.warning(f"Peer {peer_id[:16]}... marked as lazy node")

                # Notify callbacks
                for callback in self._on_lazy_node_callbacks:
                    try:
                        callback(peer_id)
                    except Exception as e:
                        logger.warning(f"Lazy node callback error: {e}")

    # -------------------------------------------------------------------------
    # Bonus Calculations
    # -------------------------------------------------------------------------

    def _update_bonuses(self, profile: IncentiveProfile) -> None:
        """Update bonus calculations for a profile."""
        uptime_bonus = self._calculate_uptime_bonus(profile)
        activity_bonus = self._calculate_activity_bonus(profile)

        profile.uptime_bonus = uptime_bonus
        profile.activity_bonus = activity_bonus
        profile.total_bonus = uptime_bonus + activity_bonus

    def _calculate_uptime_bonus(self, profile: IncentiveProfile) -> float:
        """Calculate uptime-based bonus."""
        if not profile.activity:
            return 0.0

        uptime = profile.activity.get_uptime_percent()

        if uptime >= UPTIME_BONUS_THRESHOLD_HIGH:
            return UPTIME_BONUS_HIGH
        elif uptime >= UPTIME_BONUS_THRESHOLD_MEDIUM:
            return UPTIME_BONUS_MEDIUM
        elif uptime >= UPTIME_BONUS_THRESHOLD_LOW:
            return UPTIME_BONUS_LOW
        return 0.0

    def _calculate_activity_bonus(self, profile: IncentiveProfile) -> float:
        """Calculate activity-based bonus."""
        if not profile.activity:
            return 0.0

        bonus = 0.0
        act = profile.activity

        # Oracle activity bonus
        if act.oracle_observations > 0 and RoleType.ORACLE.value in profile.verified_roles:
            bonus += ACTIVITY_BONUS_ORACLE

        # Relay activity bonus
        if act.messages_relayed > 0 and RoleType.RELAY.value in profile.verified_roles:
            bonus += ACTIVITY_BONUS_RELAY

        # Consensus participation bonus
        if act.consensus_participations > 0:
            bonus += ACTIVITY_BONUS_CONSENSUS

        # Governance participation bonus
        if act.governance_votes > 0:
            bonus += ACTIVITY_BONUS_GOVERNANCE

        return bonus

    def get_reward_multiplier(self, peer_id: str) -> float:
        """
        Get total reward multiplier for a peer.

        Returns:
            1.0 + bonuses, or reduced for lazy nodes
        """
        profile = self._profiles.get(peer_id)
        if not profile:
            return 1.0

        # Lazy nodes get reduced rewards
        if profile.is_lazy_node:
            return 0.5  # 50% reduction

        return 1.0 + profile.total_bonus

    def get_activity_bonus(self, peer_id: str) -> float:
        """Get activity bonus for a peer."""
        profile = self._profiles.get(peer_id)
        if not profile:
            return 0.0
        return profile.activity_bonus

    def get_uptime_bonus(self, peer_id: str) -> float:
        """Get uptime bonus for a peer."""
        profile = self._profiles.get(peer_id)
        if not profile:
            return 0.0
        return profile.uptime_bonus

    # -------------------------------------------------------------------------
    # PubSub Handlers
    # -------------------------------------------------------------------------

    def _on_challenge_message(self, topic: str, data: Any) -> None:
        """Handle incoming challenge from network."""
        try:
            if isinstance(data, bytes):
                message = json.loads(data.decode())
            elif isinstance(data, dict):
                message = data
            else:
                return

            if message.get('type') != 'role_challenge':
                return

            challenge_data = message.get('challenge', {})
            if not challenge_data:
                return

            challenge = RoleChallenge.from_dict(challenge_data)

            # Store if it's for us (placeholder - would check peer_id)
            self._challenges[challenge.challenge_id] = challenge

        except Exception as e:
            logger.warning(f"Error handling challenge message: {e}")

    def _on_response_message(self, topic: str, data: Any) -> None:
        """Handle incoming challenge response from network."""
        try:
            if isinstance(data, bytes):
                message = json.loads(data.decode())
            elif isinstance(data, dict):
                message = data
            else:
                return

            if message.get('type') != 'challenge_response':
                return

            challenge_id = message.get('challenge_id')
            response_data = message.get('response', {})

            if challenge_id:
                self.respond_to_challenge(challenge_id, response_data)

        except Exception as e:
            logger.warning(f"Error handling response message: {e}")

    # -------------------------------------------------------------------------
    # Status and Callbacks
    # -------------------------------------------------------------------------

    def get_profile(self, peer_id: str) -> Optional[IncentiveProfile]:
        """Get incentive profile for a peer."""
        return self._profiles.get(peer_id)

    def get_status(self) -> Dict[str, Any]:
        """Get overall incentive coordinator status."""
        active_challenges = [c for c in self._challenges.values()
                           if c.status == ChallengeStatus.PENDING]
        lazy_nodes = [p for p in self._profiles.values() if p.is_lazy_node]

        return {
            'total_profiles': len(self._profiles),
            'active_challenges': len(active_challenges),
            'lazy_nodes': len(lazy_nodes),
            'total_challenges_issued': sum(
                len(h) for h in self._challenge_history.values()
            ),
        }

    def on_lazy_node_detected(self, callback: callable) -> None:
        """Register callback for lazy node detection: callback(peer_id)."""
        self._on_lazy_node_callbacks.append(callback)

    def on_role_verified(self, callback: callable) -> None:
        """Register callback for role verification: callback(peer_id, role)."""
        self._on_role_verified_callbacks.append(callback)

    def check_expired_challenges(self) -> List[RoleChallenge]:
        """Check and mark expired challenges. Call periodically."""
        now = int(time.time())
        expired = []

        for challenge in self._challenges.values():
            if challenge.status == ChallengeStatus.PENDING and now > challenge.expires_at:
                challenge.status = ChallengeStatus.EXPIRED

                # Count as failed for the peer
                profile = self._get_or_create_profile(challenge.peer_id)
                if profile.activity:
                    profile.activity.challenges_failed += 1

                self._check_lazy_node(challenge.peer_id)
                expired.append(challenge)

        return expired

    def get_peers_needing_challenge(self) -> List[str]:
        """Get list of peers due for a challenge."""
        now = int(time.time())
        needs_challenge = []

        for peer_id, profile in self._profiles.items():
            # Check if has claimed roles
            if not profile.claimed_roles:
                continue

            # Check if due for challenge
            if profile.next_challenge_at == 0 or now >= profile.next_challenge_at:
                needs_challenge.append(peer_id)

        return needs_challenge
