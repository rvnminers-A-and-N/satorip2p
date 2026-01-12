"""
Slashing Mechanism for Satori P2P Network

Penalizes misbehavior with provable offenses. Only clear, verifiable misbehaviors
are subject to slashing - subjective issues are handled through reputation instead.

Provable Misbehaviors (Subject to Slashing):
    - Double-signing: Two valid signatures for conflicting merkle roots in same round
    - Invalid signatures: Submitting bad signatures to multi-sig
    - Archive corruption: Archiver announces data with wrong merkle root
    - Prolonged downtime: Signer consistently misses signing windows

Penalty Structure:
    1. First offense: Warning + reduced rewards for N rounds
    2. Repeat offense: Temporary removal from signer set
    3. Severe (double-sign): Permanent removal + forfeited stake

Harder to Prove (Handled by Reputation System Instead):
    - Oracle submitting intentionally wrong data (intent vs error?)
    - Curator flagging valid oracles maliciously
    - Collusion between signers
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
    from satorip2p.protocol.reputation import ReputationManager

logger = logging.getLogger(__name__)

# Slashing thresholds
DOWNTIME_WARNING_THRESHOLD = 0.80       # 80% uptime = warning
DOWNTIME_REMOVAL_THRESHOLD = 0.50       # 50% uptime = temporary removal
SIGNING_MISS_WARNING_COUNT = 3          # Miss 3 signing rounds = warning
SIGNING_MISS_REMOVAL_COUNT = 10         # Miss 10 signing rounds = removal

# Penalty durations (in rounds or seconds)
WARNING_PENALTY_ROUNDS = 7              # 7 days reduced rewards
TEMP_REMOVAL_DURATION = 30 * 24 * 3600  # 30 days
REWARD_REDUCTION_DURING_WARNING = 0.50  # 50% rewards during warning

# DHT key prefix for slashing records
DHT_SLASHING_PREFIX = "satori:slashing:"

# PubSub topic for slashing events
SLASHING_EVENT_TOPIC = "satori/slashing/events"


class SlashingOffenseType(Enum):
    """Types of slashable offenses."""
    DOUBLE_SIGN = "double_sign"           # Most severe
    INVALID_SIGNATURE = "invalid_signature"
    ARCHIVE_CORRUPTION = "archive_corruption"
    PROLONGED_DOWNTIME = "prolonged_downtime"
    SIGNING_MISS = "signing_miss"


class SlashingPenaltyType(Enum):
    """Types of penalties."""
    WARNING = "warning"                   # Reduced rewards
    TEMPORARY_REMOVAL = "temporary_removal"
    PERMANENT_REMOVAL = "permanent_removal"


class SlashingStatus(Enum):
    """Status of a slashing record."""
    ACTIVE = "active"
    EXPIRED = "expired"
    APPEALED = "appealed"
    REVERSED = "reversed"


@dataclass
class SlashingEvidence:
    """Evidence supporting a slashing claim."""
    evidence_type: str                    # Type of evidence
    data: Dict[str, Any]                  # Evidence data
    witnesses: List[str]                  # Peer IDs who witnessed
    timestamp: int
    signature: str = ""                   # Optional signature proving authenticity

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "SlashingEvidence":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def get_hash(self) -> str:
        """Get hash of evidence for verification."""
        content = json.dumps({
            'type': self.evidence_type,
            'data': self.data,
            'timestamp': self.timestamp,
        }, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()


@dataclass
class SlashingRecord:
    """A single slashing record for an offender."""
    record_id: str                        # Unique ID for this record
    offender_address: str                 # Evrmore address of offender
    offender_peer_id: str                 # libp2p peer ID
    offense_type: SlashingOffenseType
    penalty_type: SlashingPenaltyType
    status: SlashingStatus = SlashingStatus.ACTIVE

    # Details
    round_id: str = ""                    # Round where offense occurred
    details: str = ""
    evidence: List[SlashingEvidence] = field(default_factory=list)

    # Timing
    created_at: int = 0
    expires_at: int = 0                   # 0 = permanent
    resolved_at: int = 0

    # Offense history
    offense_count: int = 1                # Number of this type of offense
    total_offenses: int = 1               # Total offenses by this address

    def to_dict(self) -> dict:
        return {
            'record_id': self.record_id,
            'offender_address': self.offender_address,
            'offender_peer_id': self.offender_peer_id,
            'offense_type': self.offense_type.value,
            'penalty_type': self.penalty_type.value,
            'status': self.status.value,
            'round_id': self.round_id,
            'details': self.details,
            'evidence': [e.to_dict() for e in self.evidence],
            'created_at': self.created_at,
            'expires_at': self.expires_at,
            'resolved_at': self.resolved_at,
            'offense_count': self.offense_count,
            'total_offenses': self.total_offenses,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SlashingRecord":
        record = cls(
            record_id=data['record_id'],
            offender_address=data['offender_address'],
            offender_peer_id=data.get('offender_peer_id', ''),
            offense_type=SlashingOffenseType(data['offense_type']),
            penalty_type=SlashingPenaltyType(data['penalty_type']),
            status=SlashingStatus(data.get('status', 'active')),
            round_id=data.get('round_id', ''),
            details=data.get('details', ''),
            created_at=data.get('created_at', 0),
            expires_at=data.get('expires_at', 0),
            resolved_at=data.get('resolved_at', 0),
            offense_count=data.get('offense_count', 1),
            total_offenses=data.get('total_offenses', 1),
        )
        record.evidence = [
            SlashingEvidence.from_dict(e) for e in data.get('evidence', [])
        ]
        return record

    def is_active(self) -> bool:
        """Check if this slashing is still active."""
        if self.status != SlashingStatus.ACTIVE:
            return False
        if self.expires_at > 0 and time.time() > self.expires_at:
            return False
        return True


@dataclass
class OffenderProfile:
    """Profile tracking all offenses for an address."""
    address: str
    peer_id: str = ""

    # Offense counts by type
    double_sign_count: int = 0
    invalid_signature_count: int = 0
    archive_corruption_count: int = 0
    downtime_count: int = 0
    signing_miss_count: int = 0

    # Current status
    is_warned: bool = False
    is_temporarily_removed: bool = False
    is_permanently_removed: bool = False

    # Timing
    warned_until: int = 0
    removed_until: int = 0
    permanently_removed_at: int = 0

    # History
    total_offenses: int = 0
    first_offense_at: int = 0
    last_offense_at: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "OffenderProfile":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def get_current_status(self) -> str:
        """Get human-readable current status."""
        now = int(time.time())
        if self.is_permanently_removed:
            return "permanently_removed"
        if self.is_temporarily_removed and self.removed_until > now:
            return "temporarily_removed"
        if self.is_warned and self.warned_until > now:
            return "warned"
        return "good_standing"


class SlashingManager:
    """
    Manages slashing for provable misbehaviors.

    Works with ReputationManager for score updates when slashing occurs.
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        reputation_manager: Optional["ReputationManager"] = None,
        storage_path: Optional[Path] = None,
    ):
        """
        Initialize the slashing manager.

        Args:
            peers: Peers instance for network operations
            reputation_manager: ReputationManager for score updates
            storage_path: Path for local slashing storage
        """
        self._peers = peers
        self._reputation_manager = reputation_manager
        self._storage_path = storage_path or Path.home() / ".satori" / "slashing"
        self._storage_path.mkdir(parents=True, exist_ok=True)

        # Slashing records: record_id -> SlashingRecord
        self._records: Dict[str, SlashingRecord] = {}

        # Offender profiles: address -> OffenderProfile
        self._offenders: Dict[str, OffenderProfile] = {}

        # Double-sign detection: (round_id, signer_address) -> merkle_root
        self._round_signatures: Dict[Tuple[str, str], str] = {}

        # Callbacks for slashing events
        self._on_slashing_callbacks: List[callable] = []

        # Load local data
        self._load_local_data()

        self._started = False

    async def start(self) -> None:
        """Start the slashing manager."""
        if self._started:
            return

        if self._peers:
            # Subscribe to slashing event topic
            await self._peers.subscribe_async(
                SLASHING_EVENT_TOPIC,
                self._on_slashing_event
            )

        self._started = True
        logger.info("Slashing manager started")

    async def stop(self) -> None:
        """Stop the slashing manager."""
        if not self._started:
            return

        # Save data
        self._save_local_data()

        if self._peers:
            self._peers.unsubscribe(SLASHING_EVENT_TOPIC)

        self._started = False
        logger.info("Slashing manager stopped")

    def _load_local_data(self) -> None:
        """Load slashing data from local storage."""
        try:
            # Load records
            records_file = self._storage_path / "records.json"
            if records_file.exists():
                with open(records_file, 'r') as f:
                    data = json.load(f)
                for record_id, record_data in data.items():
                    self._records[record_id] = SlashingRecord.from_dict(record_data)

            # Load offender profiles
            offenders_file = self._storage_path / "offenders.json"
            if offenders_file.exists():
                with open(offenders_file, 'r') as f:
                    data = json.load(f)
                for address, profile_data in data.items():
                    self._offenders[address] = OffenderProfile.from_dict(profile_data)

            logger.debug(
                f"Loaded {len(self._records)} slashing records, "
                f"{len(self._offenders)} offender profiles"
            )
        except Exception as e:
            logger.warning(f"Failed to load slashing data: {e}")

    def _save_local_data(self) -> None:
        """Save slashing data to local storage."""
        try:
            # Save records
            records_file = self._storage_path / "records.json"
            records_data = {rid: r.to_dict() for rid, r in self._records.items()}
            with open(records_file, 'w') as f:
                json.dump(records_data, f, indent=2)

            # Save offender profiles
            offenders_file = self._storage_path / "offenders.json"
            offenders_data = {addr: p.to_dict() for addr, p in self._offenders.items()}
            with open(offenders_file, 'w') as f:
                json.dump(offenders_data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save slashing data: {e}")

    def _get_or_create_offender(
        self,
        address: str,
        peer_id: str = ""
    ) -> OffenderProfile:
        """Get or create offender profile."""
        if address not in self._offenders:
            self._offenders[address] = OffenderProfile(address=address, peer_id=peer_id)
        elif peer_id and not self._offenders[address].peer_id:
            self._offenders[address].peer_id = peer_id
        return self._offenders[address]

    def _generate_record_id(self, offense_type: str, address: str) -> str:
        """Generate unique record ID."""
        content = f"{offense_type}:{address}:{time.time()}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _determine_penalty(
        self,
        offense_type: SlashingOffenseType,
        profile: OffenderProfile
    ) -> SlashingPenaltyType:
        """Determine penalty based on offense type and history."""
        # Double-signing is always severe
        if offense_type == SlashingOffenseType.DOUBLE_SIGN:
            return SlashingPenaltyType.PERMANENT_REMOVAL

        # Check offense count for this type
        if offense_type == SlashingOffenseType.INVALID_SIGNATURE:
            count = profile.invalid_signature_count
        elif offense_type == SlashingOffenseType.ARCHIVE_CORRUPTION:
            count = profile.archive_corruption_count
        elif offense_type == SlashingOffenseType.PROLONGED_DOWNTIME:
            count = profile.downtime_count
        elif offense_type == SlashingOffenseType.SIGNING_MISS:
            count = profile.signing_miss_count
        else:
            count = 0

        # Progressive penalties
        if count == 0:
            return SlashingPenaltyType.WARNING
        elif count < 3:
            return SlashingPenaltyType.WARNING
        elif count < 5:
            return SlashingPenaltyType.TEMPORARY_REMOVAL
        else:
            return SlashingPenaltyType.PERMANENT_REMOVAL

    def _calculate_expiry(self, penalty_type: SlashingPenaltyType) -> int:
        """Calculate expiry timestamp for penalty."""
        now = int(time.time())
        if penalty_type == SlashingPenaltyType.WARNING:
            return now + (WARNING_PENALTY_ROUNDS * 24 * 3600)  # Days to seconds
        elif penalty_type == SlashingPenaltyType.TEMPORARY_REMOVAL:
            return now + TEMP_REMOVAL_DURATION
        else:
            return 0  # Permanent

    def report_double_sign(
        self,
        signer_address: str,
        signer_peer_id: str,
        round_id: str,
        merkle_root_1: str,
        merkle_root_2: str,
        witnesses: List[str],
    ) -> Optional[SlashingRecord]:
        """
        Report a double-signing offense.

        Double-signing is when a signer signs two different merkle roots
        for the same round. This is the most severe offense.

        Args:
            signer_address: Address of the offending signer
            signer_peer_id: Peer ID of the offending signer
            round_id: The round where double-signing occurred
            merkle_root_1: First merkle root signed
            merkle_root_2: Second (conflicting) merkle root signed
            witnesses: Peer IDs who can verify this

        Returns:
            SlashingRecord if slashing applied
        """
        if merkle_root_1 == merkle_root_2:
            logger.warning("Cannot report double-sign: merkle roots are identical")
            return None

        # Create evidence
        evidence = SlashingEvidence(
            evidence_type="double_sign",
            data={
                'round_id': round_id,
                'merkle_root_1': merkle_root_1,
                'merkle_root_2': merkle_root_2,
            },
            witnesses=witnesses,
            timestamp=int(time.time()),
        )

        # Apply slashing
        record = self._apply_slashing(
            offender_address=signer_address,
            offender_peer_id=signer_peer_id,
            offense_type=SlashingOffenseType.DOUBLE_SIGN,
            round_id=round_id,
            details=f"Signed conflicting merkle roots: {merkle_root_1[:16]}... vs {merkle_root_2[:16]}...",
            evidence=[evidence],
        )

        logger.error(
            f"DOUBLE-SIGN DETECTED: {signer_address} signed conflicting roots "
            f"in round {round_id}"
        )

        return record

    def report_invalid_signature(
        self,
        signer_address: str,
        signer_peer_id: str,
        round_id: str,
        details: str = "",
    ) -> Optional[SlashingRecord]:
        """
        Report an invalid signature offense.

        Args:
            signer_address: Address of the offending signer
            signer_peer_id: Peer ID of the offending signer
            round_id: The round where invalid signature was submitted
            details: Additional details

        Returns:
            SlashingRecord if slashing applied
        """
        evidence = SlashingEvidence(
            evidence_type="invalid_signature",
            data={'round_id': round_id, 'details': details},
            witnesses=[],
            timestamp=int(time.time()),
        )

        record = self._apply_slashing(
            offender_address=signer_address,
            offender_peer_id=signer_peer_id,
            offense_type=SlashingOffenseType.INVALID_SIGNATURE,
            round_id=round_id,
            details=details or "Submitted invalid signature",
            evidence=[evidence],
        )

        logger.warning(
            f"Invalid signature from {signer_address} in round {round_id}"
        )

        return record

    def report_archive_corruption(
        self,
        archiver_address: str,
        archiver_peer_id: str,
        round_id: str,
        expected_merkle: str,
        actual_merkle: str,
        witnesses: List[str],
    ) -> Optional[SlashingRecord]:
        """
        Report archive corruption offense.

        Args:
            archiver_address: Address of the offending archiver
            archiver_peer_id: Peer ID of the offending archiver
            round_id: The round with corrupted archive
            expected_merkle: Expected merkle root
            actual_merkle: Actual merkle root of corrupted data
            witnesses: Peer IDs who verified corruption

        Returns:
            SlashingRecord if slashing applied
        """
        evidence = SlashingEvidence(
            evidence_type="archive_corruption",
            data={
                'round_id': round_id,
                'expected_merkle': expected_merkle,
                'actual_merkle': actual_merkle,
            },
            witnesses=witnesses,
            timestamp=int(time.time()),
        )

        record = self._apply_slashing(
            offender_address=archiver_address,
            offender_peer_id=archiver_peer_id,
            offense_type=SlashingOffenseType.ARCHIVE_CORRUPTION,
            round_id=round_id,
            details=f"Archive merkle mismatch: expected {expected_merkle[:16]}..., got {actual_merkle[:16]}...",
            evidence=[evidence],
        )

        logger.warning(
            f"Archive corruption by {archiver_address} in round {round_id}"
        )

        return record

    def report_downtime(
        self,
        signer_address: str,
        signer_peer_id: str,
        uptime_percent: float,
        measurement_period: str = "",
    ) -> Optional[SlashingRecord]:
        """
        Report prolonged downtime for a signer.

        Args:
            signer_address: Address of the offline signer
            signer_peer_id: Peer ID of the offline signer
            uptime_percent: Measured uptime percentage (0-100)
            measurement_period: Description of measurement period

        Returns:
            SlashingRecord if slashing applied
        """
        # Check threshold
        if uptime_percent >= DOWNTIME_WARNING_THRESHOLD * 100:
            return None  # Above warning threshold

        evidence = SlashingEvidence(
            evidence_type="prolonged_downtime",
            data={
                'uptime_percent': uptime_percent,
                'measurement_period': measurement_period,
                'threshold': DOWNTIME_WARNING_THRESHOLD * 100,
            },
            witnesses=[],
            timestamp=int(time.time()),
        )

        record = self._apply_slashing(
            offender_address=signer_address,
            offender_peer_id=signer_peer_id,
            offense_type=SlashingOffenseType.PROLONGED_DOWNTIME,
            details=f"Uptime {uptime_percent:.1f}% below {DOWNTIME_WARNING_THRESHOLD*100}% threshold",
            evidence=[evidence],
        )

        logger.warning(
            f"Downtime penalty for {signer_address}: {uptime_percent:.1f}% uptime"
        )

        return record

    def report_signing_miss(
        self,
        signer_address: str,
        signer_peer_id: str,
        round_id: str,
    ) -> Optional[SlashingRecord]:
        """
        Report a missed signing round.

        Args:
            signer_address: Address of the signer who missed
            signer_peer_id: Peer ID of the signer
            round_id: The round that was missed

        Returns:
            SlashingRecord if slashing threshold reached
        """
        profile = self._get_or_create_offender(signer_address, signer_peer_id)

        # Increment miss count
        profile.signing_miss_count += 1

        # Check if threshold reached
        if profile.signing_miss_count < SIGNING_MISS_WARNING_COUNT:
            logger.debug(
                f"Signing miss {profile.signing_miss_count}/{SIGNING_MISS_WARNING_COUNT} "
                f"for {signer_address}"
            )
            return None

        evidence = SlashingEvidence(
            evidence_type="signing_miss",
            data={
                'round_id': round_id,
                'miss_count': profile.signing_miss_count,
            },
            witnesses=[],
            timestamp=int(time.time()),
        )

        record = self._apply_slashing(
            offender_address=signer_address,
            offender_peer_id=signer_peer_id,
            offense_type=SlashingOffenseType.SIGNING_MISS,
            round_id=round_id,
            details=f"Missed {profile.signing_miss_count} signing rounds",
            evidence=[evidence],
        )

        logger.warning(
            f"Signing miss threshold reached for {signer_address}: "
            f"{profile.signing_miss_count} misses"
        )

        return record

    def _apply_slashing(
        self,
        offender_address: str,
        offender_peer_id: str,
        offense_type: SlashingOffenseType,
        round_id: str = "",
        details: str = "",
        evidence: Optional[List[SlashingEvidence]] = None,
    ) -> SlashingRecord:
        """
        Apply slashing penalty to an offender.

        Args:
            offender_address: Address of offender
            offender_peer_id: Peer ID of offender
            offense_type: Type of offense
            round_id: Round where offense occurred
            details: Details of offense
            evidence: Evidence supporting the slashing

        Returns:
            The created SlashingRecord
        """
        now = int(time.time())
        profile = self._get_or_create_offender(offender_address, offender_peer_id)

        # Update offense counts
        if offense_type == SlashingOffenseType.DOUBLE_SIGN:
            profile.double_sign_count += 1
        elif offense_type == SlashingOffenseType.INVALID_SIGNATURE:
            profile.invalid_signature_count += 1
        elif offense_type == SlashingOffenseType.ARCHIVE_CORRUPTION:
            profile.archive_corruption_count += 1
        elif offense_type == SlashingOffenseType.PROLONGED_DOWNTIME:
            profile.downtime_count += 1

        profile.total_offenses += 1
        if profile.first_offense_at == 0:
            profile.first_offense_at = now
        profile.last_offense_at = now

        # Determine penalty
        penalty_type = self._determine_penalty(offense_type, profile)
        expires_at = self._calculate_expiry(penalty_type)

        # Update profile status
        if penalty_type == SlashingPenaltyType.WARNING:
            profile.is_warned = True
            profile.warned_until = expires_at
        elif penalty_type == SlashingPenaltyType.TEMPORARY_REMOVAL:
            profile.is_temporarily_removed = True
            profile.removed_until = expires_at
        elif penalty_type == SlashingPenaltyType.PERMANENT_REMOVAL:
            profile.is_permanently_removed = True
            profile.permanently_removed_at = now

        # Create record
        record_id = self._generate_record_id(offense_type.value, offender_address)
        record = SlashingRecord(
            record_id=record_id,
            offender_address=offender_address,
            offender_peer_id=offender_peer_id,
            offense_type=offense_type,
            penalty_type=penalty_type,
            status=SlashingStatus.ACTIVE,
            round_id=round_id,
            details=details,
            evidence=evidence or [],
            created_at=now,
            expires_at=expires_at,
            offense_count=getattr(profile, f"{offense_type.value}_count", 1),
            total_offenses=profile.total_offenses,
        )

        # Store record
        self._records[record_id] = record

        # Update reputation if manager available
        if self._reputation_manager:
            from satorip2p.protocol.reputation import ReputationEventType
            if offense_type == SlashingOffenseType.DOUBLE_SIGN:
                self._reputation_manager.record_event(
                    offender_peer_id,
                    ReputationEventType.DOUBLE_SIGN,
                    details=details,
                )
            elif offense_type == SlashingOffenseType.INVALID_SIGNATURE:
                self._reputation_manager.record_event(
                    offender_peer_id,
                    ReputationEventType.INVALID_SIGNATURE,
                    details=details,
                )
            elif offense_type == SlashingOffenseType.ARCHIVE_CORRUPTION:
                self._reputation_manager.record_event(
                    offender_peer_id,
                    ReputationEventType.ARCHIVE_CORRUPTED,
                    details=details,
                )

        # Save data
        self._save_local_data()

        # Notify callbacks
        for callback in self._on_slashing_callbacks:
            try:
                callback(record)
            except Exception as e:
                logger.warning(f"Slashing callback error: {e}")

        # Broadcast to network
        self._broadcast_slashing_event(record)

        return record

    def _broadcast_slashing_event(self, record: SlashingRecord) -> None:
        """Broadcast slashing event to network."""
        if not self._peers:
            return

        try:
            message = json.dumps({
                'type': 'slashing_event',
                'record': record.to_dict(),
            }).encode()

            # Use sync broadcast (will be run in background)
            import trio
            trio.from_thread.run_sync(
                lambda: self._peers.broadcast(SLASHING_EVENT_TOPIC, message)
            )
        except Exception as e:
            logger.debug(f"Failed to broadcast slashing event: {e}")

    def _on_slashing_event(self, topic: str, data: Any) -> None:
        """Handle incoming slashing event from network."""
        try:
            if isinstance(data, bytes):
                message = json.loads(data.decode())
            elif isinstance(data, dict):
                message = data
            else:
                return

            if message.get('type') != 'slashing_event':
                return

            record_data = message.get('record', {})
            if not record_data:
                return

            record = SlashingRecord.from_dict(record_data)

            # Store if we don't have it
            if record.record_id not in self._records:
                self._records[record.record_id] = record
                logger.info(
                    f"Received slashing record: {record.offender_address} - "
                    f"{record.offense_type.value}"
                )

        except Exception as e:
            logger.warning(f"Error handling slashing event: {e}")

    def check_double_sign(
        self,
        signer_address: str,
        round_id: str,
        merkle_root: str,
    ) -> bool:
        """
        Check if this signature would be a double-sign.

        Call this before accepting a signature to detect double-signing.

        Args:
            signer_address: Address of signer
            round_id: Round being signed
            merkle_root: Merkle root being signed

        Returns:
            True if this would be a double-sign (different root for same round)
        """
        key = (round_id, signer_address)
        if key in self._round_signatures:
            existing_root = self._round_signatures[key]
            if existing_root != merkle_root:
                return True  # Double-sign detected
        else:
            # Record this signature
            self._round_signatures[key] = merkle_root
        return False

    def is_slashed(self, address: str) -> bool:
        """Check if an address is currently slashed (removed or warned)."""
        profile = self._offenders.get(address)
        if not profile:
            return False

        now = int(time.time())
        if profile.is_permanently_removed:
            return True
        if profile.is_temporarily_removed and profile.removed_until > now:
            return True
        return False

    def is_removed(self, address: str) -> bool:
        """Check if an address is removed (temp or permanent)."""
        profile = self._offenders.get(address)
        if not profile:
            return False

        now = int(time.time())
        if profile.is_permanently_removed:
            return True
        if profile.is_temporarily_removed and profile.removed_until > now:
            return True
        return False

    def get_reward_multiplier(self, address: str) -> float:
        """
        Get reward multiplier based on slashing status.

        Returns:
            1.0 for good standing, reduced for warned, 0 for removed
        """
        profile = self._offenders.get(address)
        if not profile:
            return 1.0

        now = int(time.time())
        if profile.is_permanently_removed:
            return 0.0
        if profile.is_temporarily_removed and profile.removed_until > now:
            return 0.0
        if profile.is_warned and profile.warned_until > now:
            return REWARD_REDUCTION_DURING_WARNING
        return 1.0

    def get_offender_profile(self, address: str) -> Optional[OffenderProfile]:
        """Get offender profile for an address."""
        return self._offenders.get(address)

    def get_records_for_address(self, address: str) -> List[SlashingRecord]:
        """Get all slashing records for an address."""
        return [
            r for r in self._records.values()
            if r.offender_address == address
        ]

    def get_active_slashings(self) -> List[SlashingRecord]:
        """Get all currently active slashing records."""
        return [r for r in self._records.values() if r.is_active()]

    def get_status(self) -> Dict[str, Any]:
        """Get overall slashing status."""
        active_records = self.get_active_slashings()
        return {
            'total_records': len(self._records),
            'active_records': len(active_records),
            'total_offenders': len(self._offenders),
            'permanently_removed': sum(
                1 for p in self._offenders.values() if p.is_permanently_removed
            ),
            'temporarily_removed': sum(
                1 for p in self._offenders.values()
                if p.is_temporarily_removed and not p.is_permanently_removed
            ),
            'warned': sum(
                1 for p in self._offenders.values()
                if p.is_warned and not p.is_temporarily_removed and not p.is_permanently_removed
            ),
        }

    def on_slashing(self, callback: callable) -> None:
        """Register callback for slashing events: callback(SlashingRecord)."""
        self._on_slashing_callbacks.append(callback)
