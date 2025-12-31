"""
satorip2p/protocol/curator.py

Curator protocol for oracle stream quality control.

Signers act as curators who vote on oracle stream quality and trustworthiness.
This ensures data integrity across the network by allowing trusted parties
to flag problematic streams or oracles.

Key Features:
- Stream quality voting (signers vote on stream health)
- Oracle reputation tracking (based on data quality)
- Stream approval/rejection (3-of-5 signer consensus)
- Bad actor flagging and removal
- Quality metrics aggregation

Curator Responsibilities:
1. Monitor oracle data quality and consistency
2. Vote on stream trustworthiness
3. Flag suspicious or malicious oracles
4. Approve new oracle registrations
5. Maintain network data integrity

Usage:
    from satorip2p.protocol.curator import CuratorProtocol

    curator = CuratorProtocol(peers)
    await curator.start()

    # Vote on stream quality (signers only)
    await curator.vote_stream_quality(stream_id, QualityVote.APPROVED)

    # Get stream curation status
    status = curator.get_stream_status(stream_id)

    # Flag a bad oracle
    await curator.flag_oracle(oracle_address, reason="Inconsistent data")
"""

import logging
import time
import json
import hashlib
from typing import Dict, List, Optional, Set, Callable, TYPE_CHECKING, Any
from dataclasses import dataclass, field, asdict
from enum import Enum

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.curator")


# ============================================================================
# CONSTANTS
# ============================================================================

# Voting thresholds (3-of-5 multi-sig style)
CURATOR_VOTE_THRESHOLD = 3           # Votes needed for action
CURATOR_TOTAL_VOTERS = 5             # Total curators (same as signers)

# Reputation scoring
INITIAL_ORACLE_REPUTATION = 0.5      # New oracles start at 50%
MIN_ORACLE_REPUTATION = 0.0          # Minimum reputation
MAX_ORACLE_REPUTATION = 1.0          # Maximum reputation
REPUTATION_DECAY_PER_DAY = 0.01      # Daily decay if no activity
REPUTATION_BOOST_PER_APPROVAL = 0.05 # Boost per curator approval
REPUTATION_PENALTY_PER_FLAG = 0.10   # Penalty per flag

# Stream quality thresholds
MIN_STREAM_QUALITY = 0.3             # Minimum quality to remain active
EXCELLENT_STREAM_QUALITY = 0.9       # Threshold for excellent status

# Time constants
VOTE_EXPIRY_SECONDS = 86400 * 7      # Votes expire after 7 days
FLAG_REVIEW_PERIOD = 86400 * 3       # 3 days to review flags
QUALITY_UPDATE_INTERVAL = 3600       # Update quality metrics hourly

# Topics
CURATOR_VOTE_TOPIC = "satori/curator/votes"
CURATOR_FLAG_TOPIC = "satori/curator/flags"
CURATOR_APPROVAL_TOPIC = "satori/curator/approvals"


# ============================================================================
# ENUMS
# ============================================================================

class QualityVote(Enum):
    """Quality vote options for streams/oracles."""
    APPROVED = "approved"           # Data quality is good
    NEEDS_REVIEW = "needs_review"   # Some concerns, monitor closely
    REJECTED = "rejected"           # Data quality is bad, should be removed
    ABSTAIN = "abstain"             # No opinion


class StreamStatus(Enum):
    """Curation status of a stream."""
    PENDING = "pending"             # Awaiting initial curation
    APPROVED = "approved"           # Curators approved this stream
    PROBATION = "probation"         # Under review due to concerns
    REJECTED = "rejected"           # Curators rejected this stream
    SUSPENDED = "suspended"         # Temporarily suspended


class FlagReason(Enum):
    """Reasons for flagging an oracle or stream."""
    INCONSISTENT_DATA = "inconsistent_data"
    MANIPULATION = "manipulation"
    DELAYED_UPDATES = "delayed_updates"
    MALICIOUS_DATA = "malicious_data"
    IMPERSONATION = "impersonation"
    SPAM = "spam"
    OTHER = "other"


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class CuratorVote:
    """A curator's vote on a stream or oracle."""
    stream_id: str
    curator_address: str            # Signer's Evrmore address
    vote: QualityVote
    timestamp: int
    comment: str = ""
    signature: str = ""             # Signed by curator

    def to_dict(self) -> dict:
        return {
            **asdict(self),
            "vote": self.vote.value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CuratorVote":
        data = dict(data)
        data["vote"] = QualityVote(data["vote"])
        return cls(**data)

    def get_signing_message(self) -> str:
        """Get the message that should be signed."""
        return f"curator_vote:{self.stream_id}:{self.vote.value}:{self.timestamp}"


@dataclass
class OracleFlag:
    """A flag raised against an oracle."""
    oracle_address: str
    flagger_address: str            # Who raised the flag (curator)
    reason: FlagReason
    stream_id: str                  # Which stream this relates to
    timestamp: int
    evidence: str = ""              # Description of evidence
    signature: str = ""
    resolved: bool = False
    resolution: str = ""

    def to_dict(self) -> dict:
        return {
            **asdict(self),
            "reason": self.reason.value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "OracleFlag":
        data = dict(data)
        data["reason"] = FlagReason(data["reason"])
        return cls(**data)


@dataclass
class StreamCuration:
    """Curation state of a stream."""
    stream_id: str
    status: StreamStatus = StreamStatus.PENDING
    quality_score: float = 0.5      # Aggregate quality (0-1)
    votes: Dict[str, CuratorVote] = field(default_factory=dict)  # curator -> vote
    flags: List[OracleFlag] = field(default_factory=list)
    approved_oracles: Set[str] = field(default_factory=set)
    rejected_oracles: Set[str] = field(default_factory=set)
    last_updated: int = 0
    created_at: int = 0

    def __post_init__(self):
        if self.created_at == 0:
            self.created_at = int(time.time())
        if self.last_updated == 0:
            self.last_updated = self.created_at

    def to_dict(self) -> dict:
        return {
            "stream_id": self.stream_id,
            "status": self.status.value,
            "quality_score": self.quality_score,
            "votes": {k: v.to_dict() for k, v in self.votes.items()},
            "flags": [f.to_dict() for f in self.flags],
            "approved_oracles": list(self.approved_oracles),
            "rejected_oracles": list(self.rejected_oracles),
            "last_updated": self.last_updated,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "StreamCuration":
        return cls(
            stream_id=data["stream_id"],
            status=StreamStatus(data["status"]),
            quality_score=data.get("quality_score", 0.5),
            votes={k: CuratorVote.from_dict(v) for k, v in data.get("votes", {}).items()},
            flags=[OracleFlag.from_dict(f) for f in data.get("flags", [])],
            approved_oracles=set(data.get("approved_oracles", [])),
            rejected_oracles=set(data.get("rejected_oracles", [])),
            last_updated=data.get("last_updated", 0),
            created_at=data.get("created_at", 0),
        )


@dataclass
class OracleReputation:
    """Reputation record for an oracle."""
    oracle_address: str
    reputation: float = INITIAL_ORACLE_REPUTATION
    total_observations: int = 0
    approved_streams: int = 0
    rejected_streams: int = 0
    flags_received: int = 0
    flags_resolved_favorably: int = 0
    last_activity: int = 0
    created_at: int = 0

    def __post_init__(self):
        if self.created_at == 0:
            self.created_at = int(time.time())
        if self.last_activity == 0:
            self.last_activity = self.created_at

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "OracleReputation":
        return cls(**data)


# ============================================================================
# CURATOR PROTOCOL
# ============================================================================

class CuratorProtocol:
    """
    Curator protocol for managing oracle stream quality.

    Curators (signers) vote on stream quality and oracle trustworthiness.
    Uses 3-of-5 consensus for major decisions like rejecting streams.
    """

    def __init__(self, peers: "Peers"):
        """
        Initialize CuratorProtocol.

        Args:
            peers: Peers instance for network access
        """
        self._peers = peers
        self._stream_curations: Dict[str, StreamCuration] = {}
        self._oracle_reputations: Dict[str, OracleReputation] = {}
        self._started = False
        self._callbacks: List[Callable] = []

    @property
    def is_curator(self) -> bool:
        """Check if we are a curator (signer)."""
        # Import here to avoid circular dependency
        from .signer import is_authorized_signer
        return is_authorized_signer(self._peers.evrmore_address or "")

    async def start(self) -> None:
        """Start the curator protocol."""
        if self._started:
            return

        # Subscribe to curator topics
        await self._peers.subscribe_async(CURATOR_VOTE_TOPIC, self._on_vote)
        await self._peers.subscribe_async(CURATOR_FLAG_TOPIC, self._on_flag)
        await self._peers.subscribe_async(CURATOR_APPROVAL_TOPIC, self._on_approval)

        self._started = True
        logger.info(f"Curator protocol started (is_curator={self.is_curator})")

    async def stop(self) -> None:
        """Stop the curator protocol."""
        if not self._started:
            return

        self._peers.unsubscribe(CURATOR_VOTE_TOPIC)
        self._peers.unsubscribe(CURATOR_FLAG_TOPIC)
        self._peers.unsubscribe(CURATOR_APPROVAL_TOPIC)

        self._started = False
        logger.info("Curator protocol stopped")

    # ========================================================================
    # CURATOR ACTIONS (Signers Only)
    # ========================================================================

    async def vote_stream_quality(
        self,
        stream_id: str,
        vote: QualityVote,
        comment: str = ""
    ) -> bool:
        """
        Vote on a stream's quality (curators only).

        Args:
            stream_id: Stream to vote on
            vote: Quality vote
            comment: Optional comment

        Returns:
            True if vote was cast successfully
        """
        if not self.is_curator:
            logger.warning("Only curators can vote on stream quality")
            return False

        curator_vote = CuratorVote(
            stream_id=stream_id,
            curator_address=self._peers.evrmore_address or "",
            vote=vote,
            timestamp=int(time.time()),
            comment=comment,
        )

        # Sign the vote
        if hasattr(self._peers, '_identity_bridge') and self._peers._identity_bridge:
            message = curator_vote.get_signing_message()
            curator_vote.signature = self._peers._identity_bridge.sign_message(message)

        # Store locally
        self._record_vote(curator_vote)

        # Broadcast to network
        try:
            await self._peers.broadcast(
                CURATOR_VOTE_TOPIC,
                json.dumps(curator_vote.to_dict()).encode()
            )
            logger.info(f"Cast quality vote for stream {stream_id}: {vote.value}")
            return True
        except Exception as e:
            logger.error(f"Failed to broadcast vote: {e}")
            return False

    async def flag_oracle(
        self,
        oracle_address: str,
        stream_id: str,
        reason: FlagReason,
        evidence: str = ""
    ) -> bool:
        """
        Flag an oracle for review (curators only).

        Args:
            oracle_address: Oracle to flag
            stream_id: Related stream
            reason: Reason for flag
            evidence: Description of evidence

        Returns:
            True if flag was raised successfully
        """
        if not self.is_curator:
            logger.warning("Only curators can flag oracles")
            return False

        flag = OracleFlag(
            oracle_address=oracle_address,
            flagger_address=self._peers.evrmore_address or "",
            reason=reason,
            stream_id=stream_id,
            timestamp=int(time.time()),
            evidence=evidence,
        )

        # Sign the flag
        if hasattr(self._peers, '_identity_bridge') and self._peers._identity_bridge:
            message = f"flag:{oracle_address}:{reason.value}:{flag.timestamp}"
            flag.signature = self._peers._identity_bridge.sign_message(message)

        # Store locally
        self._record_flag(flag)

        # Broadcast to network
        try:
            await self._peers.broadcast(
                CURATOR_FLAG_TOPIC,
                json.dumps(flag.to_dict()).encode()
            )
            logger.info(f"Flagged oracle {oracle_address} for {reason.value}")
            return True
        except Exception as e:
            logger.error(f"Failed to broadcast flag: {e}")
            return False

    async def approve_oracle(self, oracle_address: str, stream_id: str) -> bool:
        """
        Approve an oracle for a stream (curators only).

        Args:
            oracle_address: Oracle to approve
            stream_id: Stream to approve for

        Returns:
            True if approval was recorded
        """
        if not self.is_curator:
            logger.warning("Only curators can approve oracles")
            return False

        approval = {
            "type": "oracle_approval",
            "oracle_address": oracle_address,
            "stream_id": stream_id,
            "curator_address": self._peers.evrmore_address or "",
            "timestamp": int(time.time()),
            "approved": True,
        }

        # Sign
        if hasattr(self._peers, '_identity_bridge') and self._peers._identity_bridge:
            message = f"approve:{oracle_address}:{stream_id}:{approval['timestamp']}"
            approval["signature"] = self._peers._identity_bridge.sign_message(message)

        # Broadcast
        try:
            await self._peers.broadcast(
                CURATOR_APPROVAL_TOPIC,
                json.dumps(approval).encode()
            )
            logger.info(f"Approved oracle {oracle_address} for stream {stream_id}")

            # Update local state
            curation = self._get_or_create_curation(stream_id)
            curation.approved_oracles.add(oracle_address)
            curation.rejected_oracles.discard(oracle_address)
            curation.last_updated = int(time.time())

            return True
        except Exception as e:
            logger.error(f"Failed to broadcast approval: {e}")
            return False

    async def reject_oracle(self, oracle_address: str, stream_id: str) -> bool:
        """
        Reject an oracle for a stream (curators only).

        Args:
            oracle_address: Oracle to reject
            stream_id: Stream to reject from

        Returns:
            True if rejection was recorded
        """
        if not self.is_curator:
            logger.warning("Only curators can reject oracles")
            return False

        rejection = {
            "type": "oracle_rejection",
            "oracle_address": oracle_address,
            "stream_id": stream_id,
            "curator_address": self._peers.evrmore_address or "",
            "timestamp": int(time.time()),
            "approved": False,
        }

        # Sign
        if hasattr(self._peers, '_identity_bridge') and self._peers._identity_bridge:
            message = f"reject:{oracle_address}:{stream_id}:{rejection['timestamp']}"
            rejection["signature"] = self._peers._identity_bridge.sign_message(message)

        # Broadcast
        try:
            await self._peers.broadcast(
                CURATOR_APPROVAL_TOPIC,
                json.dumps(rejection).encode()
            )
            logger.info(f"Rejected oracle {oracle_address} for stream {stream_id}")

            # Update local state
            curation = self._get_or_create_curation(stream_id)
            curation.rejected_oracles.add(oracle_address)
            curation.approved_oracles.discard(oracle_address)
            curation.last_updated = int(time.time())

            # Update reputation
            rep = self._get_or_create_reputation(oracle_address)
            rep.reputation = max(MIN_ORACLE_REPUTATION, rep.reputation - REPUTATION_PENALTY_PER_FLAG)
            rep.rejected_streams += 1

            return True
        except Exception as e:
            logger.error(f"Failed to broadcast rejection: {e}")
            return False

    # ========================================================================
    # QUERY METHODS (Public)
    # ========================================================================

    def get_stream_status(self, stream_id: str) -> Optional[StreamCuration]:
        """Get the curation status of a stream."""
        return self._stream_curations.get(stream_id)

    def get_oracle_reputation(self, oracle_address: str) -> Optional[OracleReputation]:
        """Get reputation record for an oracle."""
        return self._oracle_reputations.get(oracle_address)

    def is_oracle_approved(self, oracle_address: str, stream_id: str) -> bool:
        """Check if an oracle is approved for a stream."""
        curation = self._stream_curations.get(stream_id)
        if not curation:
            return True  # No curation = default approved
        return oracle_address in curation.approved_oracles or oracle_address not in curation.rejected_oracles

    def is_oracle_rejected(self, oracle_address: str, stream_id: str) -> bool:
        """Check if an oracle is rejected from a stream."""
        curation = self._stream_curations.get(stream_id)
        if not curation:
            return False
        return oracle_address in curation.rejected_oracles

    def get_stream_quality(self, stream_id: str) -> float:
        """Get quality score for a stream (0-1)."""
        curation = self._stream_curations.get(stream_id)
        if not curation:
            return 0.5  # Default quality
        return curation.quality_score

    def get_all_curated_streams(self) -> Dict[str, StreamCuration]:
        """Get all curated streams."""
        return dict(self._stream_curations)

    def get_flagged_oracles(self, unresolved_only: bool = True) -> List[OracleFlag]:
        """Get all flagged oracles."""
        flags = []
        for curation in self._stream_curations.values():
            for flag in curation.flags:
                if unresolved_only and flag.resolved:
                    continue
                flags.append(flag)
        return flags

    def get_curator_votes(self, stream_id: str) -> Dict[str, CuratorVote]:
        """Get all curator votes for a stream."""
        curation = self._stream_curations.get(stream_id)
        if not curation:
            return {}
        return dict(curation.votes)

    def get_vote_summary(self, stream_id: str) -> Dict[str, int]:
        """Get vote counts by type for a stream."""
        curation = self._stream_curations.get(stream_id)
        if not curation:
            return {"approved": 0, "needs_review": 0, "rejected": 0, "abstain": 0}

        counts = {v.value: 0 for v in QualityVote}
        for vote in curation.votes.values():
            counts[vote.vote.value] += 1
        return counts

    def get_stats(self) -> dict:
        """Get curator protocol statistics."""
        total_flags = sum(len(c.flags) for c in self._stream_curations.values())
        unresolved_flags = sum(
            1 for c in self._stream_curations.values()
            for f in c.flags if not f.resolved
        )

        status_counts = {}
        for status in StreamStatus:
            status_counts[status.value] = sum(
                1 for c in self._stream_curations.values()
                if c.status == status
            )

        # Calculate average quality score across all curated streams
        avg_quality_score = None
        if self._stream_curations:
            total_quality = sum(c.quality_score for c in self._stream_curations.values())
            avg_quality_score = round(total_quality / len(self._stream_curations), 3)

        return {
            "started": self._started,
            "is_curator": self.is_curator,
            "curated_streams": len(self._stream_curations),
            "tracked_oracles": len(self._oracle_reputations),
            "total_flags": total_flags,
            "unresolved_flags": unresolved_flags,
            "stream_status_counts": status_counts,
            "avg_quality_score": avg_quality_score,
        }

    # ========================================================================
    # INTERNAL METHODS
    # ========================================================================

    def _get_or_create_curation(self, stream_id: str) -> StreamCuration:
        """Get or create a StreamCuration record."""
        if stream_id not in self._stream_curations:
            self._stream_curations[stream_id] = StreamCuration(stream_id=stream_id)
        return self._stream_curations[stream_id]

    def _get_or_create_reputation(self, oracle_address: str) -> OracleReputation:
        """Get or create an OracleReputation record."""
        if oracle_address not in self._oracle_reputations:
            self._oracle_reputations[oracle_address] = OracleReputation(oracle_address=oracle_address)
        return self._oracle_reputations[oracle_address]

    def _record_vote(self, vote: CuratorVote) -> None:
        """Record a curator vote."""
        curation = self._get_or_create_curation(vote.stream_id)
        curation.votes[vote.curator_address] = vote
        curation.last_updated = int(time.time())

        # Recalculate status based on votes
        self._update_stream_status(curation)

    def _record_flag(self, flag: OracleFlag) -> None:
        """Record an oracle flag."""
        curation = self._get_or_create_curation(flag.stream_id)
        curation.flags.append(flag)
        curation.last_updated = int(time.time())

        # Update oracle reputation
        rep = self._get_or_create_reputation(flag.oracle_address)
        rep.flags_received += 1
        rep.reputation = max(MIN_ORACLE_REPUTATION, rep.reputation - REPUTATION_PENALTY_PER_FLAG)

        # If enough flags, set stream to probation
        active_flags = [f for f in curation.flags if not f.resolved]
        if len(active_flags) >= 2 and curation.status == StreamStatus.APPROVED:
            curation.status = StreamStatus.PROBATION
            logger.warning(f"Stream {flag.stream_id} moved to probation due to flags")

    def _update_stream_status(self, curation: StreamCuration) -> None:
        """Update stream status based on votes."""
        vote_counts = self.get_vote_summary(curation.stream_id)

        # 3-of-5 consensus rules
        if vote_counts["approved"] >= CURATOR_VOTE_THRESHOLD:
            curation.status = StreamStatus.APPROVED
            curation.quality_score = min(1.0, curation.quality_score + 0.1)
        elif vote_counts["rejected"] >= CURATOR_VOTE_THRESHOLD:
            curation.status = StreamStatus.REJECTED
            curation.quality_score = max(0.0, curation.quality_score - 0.2)
        elif vote_counts["needs_review"] >= 2:
            curation.status = StreamStatus.PROBATION
        else:
            # Not enough votes for decision
            if curation.status == StreamStatus.PENDING:
                pass  # Stay pending

        # Calculate quality score from votes
        total_votes = sum(vote_counts.values()) - vote_counts["abstain"]
        if total_votes > 0:
            # Approved = 1.0, needs_review = 0.5, rejected = 0.0
            weighted = (
                vote_counts["approved"] * 1.0 +
                vote_counts["needs_review"] * 0.5 +
                vote_counts["rejected"] * 0.0
            )
            curation.quality_score = weighted / total_votes

    def _on_vote(self, topic: str, data: Any) -> None:
        """Handle incoming curator vote."""
        try:
            if isinstance(data, bytes):
                vote_data = json.loads(data.decode())
            elif isinstance(data, dict):
                vote_data = data
            else:
                return

            vote = CuratorVote.from_dict(vote_data)

            # Verify this is from an authorized curator
            from .signer import is_authorized_signer
            if not is_authorized_signer(vote.curator_address):
                logger.warning(f"Vote from non-curator {vote.curator_address} ignored")
                return

            # TODO: Verify signature

            self._record_vote(vote)
            logger.info(f"Received vote from {vote.curator_address} for stream {vote.stream_id}")

        except Exception as e:
            logger.warning(f"Error handling vote: {e}")

    def _on_flag(self, topic: str, data: Any) -> None:
        """Handle incoming oracle flag."""
        try:
            if isinstance(data, bytes):
                flag_data = json.loads(data.decode())
            elif isinstance(data, dict):
                flag_data = data
            else:
                return

            flag = OracleFlag.from_dict(flag_data)

            # Verify this is from an authorized curator
            from .signer import is_authorized_signer
            if not is_authorized_signer(flag.flagger_address):
                logger.warning(f"Flag from non-curator {flag.flagger_address} ignored")
                return

            # TODO: Verify signature

            self._record_flag(flag)
            logger.info(f"Received flag for oracle {flag.oracle_address} from {flag.flagger_address}")

        except Exception as e:
            logger.warning(f"Error handling flag: {e}")

    def _on_approval(self, topic: str, data: Any) -> None:
        """Handle incoming oracle approval/rejection."""
        try:
            if isinstance(data, bytes):
                approval_data = json.loads(data.decode())
            elif isinstance(data, dict):
                approval_data = data
            else:
                return

            curator_address = approval_data.get("curator_address", "")

            # Verify this is from an authorized curator
            from .signer import is_authorized_signer
            if not is_authorized_signer(curator_address):
                logger.warning(f"Approval from non-curator {curator_address} ignored")
                return

            # TODO: Verify signature

            oracle_address = approval_data.get("oracle_address", "")
            stream_id = approval_data.get("stream_id", "")
            approved = approval_data.get("approved", True)

            curation = self._get_or_create_curation(stream_id)

            if approved:
                curation.approved_oracles.add(oracle_address)
                curation.rejected_oracles.discard(oracle_address)
                rep = self._get_or_create_reputation(oracle_address)
                rep.approved_streams += 1
                rep.reputation = min(MAX_ORACLE_REPUTATION, rep.reputation + REPUTATION_BOOST_PER_APPROVAL)
            else:
                curation.rejected_oracles.add(oracle_address)
                curation.approved_oracles.discard(oracle_address)
                rep = self._get_or_create_reputation(oracle_address)
                rep.rejected_streams += 1
                rep.reputation = max(MIN_ORACLE_REPUTATION, rep.reputation - REPUTATION_PENALTY_PER_FLAG)

            curation.last_updated = int(time.time())
            logger.info(f"Oracle {oracle_address} {'approved' if approved else 'rejected'} for stream {stream_id}")

        except Exception as e:
            logger.warning(f"Error handling approval: {e}")
