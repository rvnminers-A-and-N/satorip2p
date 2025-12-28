"""
satorip2p/protocol/governance.py

Decentralized governance and voting protocol for Satori network.

Allows the community to propose and vote on network changes:
- Protocol parameter changes
- Feature additions/removals
- Treasury allocation decisions
- Signer additions/removals

Voting power is based on:
- Stake amount (weighted)
- Node uptime (bonus)
- Signer status (additional weight)

Key Features:
- Proposal creation and submission
- Stake-weighted voting
- Quorum requirements
- Time-bound voting periods
- Execution of passed proposals

Usage:
    from satorip2p.protocol.governance import GovernanceProtocol

    governance = GovernanceProtocol(peers)
    await governance.start()

    # Create a proposal
    proposal = await governance.create_proposal(
        title="Increase relay bonus to 7%",
        description="Proposal to increase relay node bonus...",
        proposal_type=ProposalType.PARAMETER_CHANGE,
        changes={"ROLE_BONUS_RELAY": 0.07}
    )

    # Vote on a proposal
    await governance.vote(proposal_id, VoteChoice.YES)

    # Check proposal status
    status = governance.get_proposal_status(proposal_id)
"""

import logging
import time
import json
import hashlib
from typing import Dict, List, Optional, Set, Callable, TYPE_CHECKING, Any
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime, timezone

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.governance")


# ============================================================================
# CONSTANTS
# ============================================================================

# Voting periods
DEFAULT_VOTING_PERIOD_DAYS = 7        # 1 week voting period
MIN_VOTING_PERIOD_DAYS = 3            # Minimum 3 days
MAX_VOTING_PERIOD_DAYS = 30           # Maximum 30 days

# Quorum requirements (percentage of total voting power)
QUORUM_STANDARD = 0.10                # 10% for standard proposals
QUORUM_MAJOR = 0.20                   # 20% for major changes
QUORUM_CRITICAL = 0.33                # 33% for critical changes (signer changes, etc.)

# Approval thresholds (percentage of votes cast)
APPROVAL_STANDARD = 0.50              # Simple majority
APPROVAL_MAJOR = 0.60                 # 60% for major changes
APPROVAL_CRITICAL = 0.67              # 67% for critical changes

# Proposal limits
MAX_ACTIVE_PROPOSALS = 10             # Maximum concurrent proposals
MIN_STAKE_TO_PROPOSE = 50             # Minimum SATORI stake to create proposal
MIN_STAKE_TO_VOTE = 10                # Minimum SATORI stake to vote

# Voting power weights
STAKE_WEIGHT = 1.0                    # Base weight per SATORI staked
UPTIME_BONUS_90_DAYS = 0.10           # +10% bonus for 90+ day uptime
SIGNER_BONUS = 0.25                   # +25% bonus for signers

# Topics
GOVERNANCE_PROPOSAL_TOPIC = "satori/governance/proposals"
GOVERNANCE_VOTE_TOPIC = "satori/governance/votes"
GOVERNANCE_RESULT_TOPIC = "satori/governance/results"


# ============================================================================
# ENUMS
# ============================================================================

class ProposalType(Enum):
    """Types of governance proposals."""
    PARAMETER_CHANGE = "parameter_change"     # Change protocol constants
    FEATURE_TOGGLE = "feature_toggle"         # Enable/disable features
    TREASURY_SPEND = "treasury_spend"         # Allocate treasury funds
    SIGNER_CHANGE = "signer_change"           # Add/remove signers
    PROTOCOL_UPGRADE = "protocol_upgrade"     # Major protocol changes
    COMMUNITY = "community"                   # General community votes


class ProposalStatus(Enum):
    """Status of a proposal."""
    DRAFT = "draft"                           # Not yet submitted
    ACTIVE = "active"                         # Open for voting
    PASSED = "passed"                         # Approved by voters
    REJECTED = "rejected"                     # Rejected by voters
    EXPIRED = "expired"                       # Voting period ended without quorum
    EXECUTED = "executed"                     # Passed and executed
    CANCELLED = "cancelled"                   # Cancelled by proposer


class VoteChoice(Enum):
    """Vote options."""
    YES = "yes"
    NO = "no"
    ABSTAIN = "abstain"


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class Comment:
    """A comment on a proposal."""
    comment_id: str
    proposal_id: str
    author_address: str               # Evrmore address
    content: str
    timestamp: int
    is_signer: bool = False           # True if author is a signer
    is_status_update: bool = False    # True if this is an official status update
    signature: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Comment":
        return cls(**data)

    @staticmethod
    def generate_id(proposal_id: str, author: str, timestamp: int) -> str:
        """Generate unique comment ID."""
        content = f"{proposal_id}:{author}:{timestamp}"
        return hashlib.sha256(content.encode()).hexdigest()[:12]


@dataclass
class Vote:
    """A vote on a proposal."""
    proposal_id: str
    voter_address: str                # Evrmore address
    choice: VoteChoice
    voting_power: float               # Calculated voting power
    stake_amount: float               # SATORI staked
    timestamp: int
    signature: str = ""

    def to_dict(self) -> dict:
        return {
            **asdict(self),
            "choice": self.choice.value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Vote":
        data = dict(data)
        data["choice"] = VoteChoice(data["choice"])
        return cls(**data)

    def get_signing_message(self) -> str:
        """Get message for signing."""
        return f"vote:{self.proposal_id}:{self.choice.value}:{self.timestamp}"


@dataclass
class Proposal:
    """A governance proposal."""
    proposal_id: str                  # Unique ID (hash of content)
    title: str
    description: str
    proposal_type: ProposalType
    proposer_address: str             # Evrmore address
    created_at: int                   # Unix timestamp
    voting_starts: int                # When voting opens
    voting_ends: int                  # When voting closes
    changes: Dict[str, Any] = field(default_factory=dict)  # Proposed changes
    status: ProposalStatus = ProposalStatus.DRAFT
    votes: Dict[str, Vote] = field(default_factory=dict)  # voter -> vote
    comments: List[Comment] = field(default_factory=list)  # Discussion
    execution_data: Dict[str, Any] = field(default_factory=dict)
    signature: str = ""
    # Signer controls
    pinned: bool = False              # Signers can pin to top
    pinned_by: str = ""               # Who pinned it
    emergency_cancel_votes: Set[str] = field(default_factory=set)  # Signers who voted to cancel
    executed_by: str = ""             # Signer who marked as executed
    executed_at: int = 0              # When it was marked executed

    def to_dict(self) -> dict:
        return {
            "proposal_id": self.proposal_id,
            "title": self.title,
            "description": self.description,
            "proposal_type": self.proposal_type.value,
            "proposer_address": self.proposer_address,
            "created_at": self.created_at,
            "voting_starts": self.voting_starts,
            "voting_ends": self.voting_ends,
            "changes": self.changes,
            "status": self.status.value,
            "votes": {k: v.to_dict() for k, v in self.votes.items()},
            "comments": [c.to_dict() for c in self.comments],
            "execution_data": self.execution_data,
            "signature": self.signature,
            "pinned": self.pinned,
            "pinned_by": self.pinned_by,
            "emergency_cancel_votes": list(self.emergency_cancel_votes),
            "executed_by": self.executed_by,
            "executed_at": self.executed_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Proposal":
        votes = {k: Vote.from_dict(v) for k, v in data.get("votes", {}).items()}
        comments = [Comment.from_dict(c) for c in data.get("comments", [])]
        return cls(
            proposal_id=data["proposal_id"],
            title=data["title"],
            description=data["description"],
            proposal_type=ProposalType(data["proposal_type"]),
            proposer_address=data["proposer_address"],
            created_at=data["created_at"],
            voting_starts=data["voting_starts"],
            voting_ends=data["voting_ends"],
            changes=data.get("changes", {}),
            status=ProposalStatus(data["status"]),
            votes=votes,
            comments=comments,
            execution_data=data.get("execution_data", {}),
            signature=data.get("signature", ""),
            pinned=data.get("pinned", False),
            pinned_by=data.get("pinned_by", ""),
            emergency_cancel_votes=set(data.get("emergency_cancel_votes", [])),
            executed_by=data.get("executed_by", ""),
            executed_at=data.get("executed_at", 0),
        )

    def get_signing_message(self) -> str:
        """Get message for signing."""
        return f"proposal:{self.proposal_id}:{self.title}:{self.created_at}"

    @staticmethod
    def generate_id(title: str, proposer: str, timestamp: int) -> str:
        """Generate unique proposal ID."""
        content = f"{title}:{proposer}:{timestamp}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]


@dataclass
class VoteTally:
    """Vote tally for a proposal."""
    yes_votes: float = 0.0            # Total voting power for YES
    no_votes: float = 0.0             # Total voting power for NO
    abstain_votes: float = 0.0        # Total voting power for ABSTAIN
    total_voters: int = 0
    quorum_reached: bool = False
    approval_reached: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


# ============================================================================
# GOVERNANCE PROTOCOL
# ============================================================================

class GovernanceProtocol:
    """
    Decentralized governance protocol for Satori network.

    Allows stake-weighted voting on network proposals.
    """

    def __init__(self, peers: "Peers"):
        """
        Initialize GovernanceProtocol.

        Args:
            peers: Peers instance for network access
        """
        self._peers = peers
        self._proposals: Dict[str, Proposal] = {}
        self._started = False
        self._total_voting_power_cache: float = 0.0
        self._voting_power_cache_time: int = 0

    async def start(self) -> None:
        """Start the governance protocol."""
        if self._started:
            return

        # Subscribe to governance topics
        await self._peers.subscribe_async(GOVERNANCE_PROPOSAL_TOPIC, self._on_proposal)
        await self._peers.subscribe_async(GOVERNANCE_VOTE_TOPIC, self._on_vote)
        await self._peers.subscribe_async(GOVERNANCE_RESULT_TOPIC, self._on_result)

        self._started = True
        logger.info("Governance protocol started")

        # Start background task to check proposal deadlines
        if hasattr(self._peers, '_nursery') and self._peers._nursery:
            self._peers._nursery.start_soon(self._check_deadlines_loop)

    async def stop(self) -> None:
        """Stop the governance protocol."""
        if not self._started:
            return

        self._peers.unsubscribe(GOVERNANCE_PROPOSAL_TOPIC)
        self._peers.unsubscribe(GOVERNANCE_VOTE_TOPIC)
        self._peers.unsubscribe(GOVERNANCE_RESULT_TOPIC)

        self._started = False
        logger.info("Governance protocol stopped")

    # ========================================================================
    # PROPOSAL CREATION
    # ========================================================================

    async def create_proposal(
        self,
        title: str,
        description: str,
        proposal_type: ProposalType,
        changes: Optional[Dict[str, Any]] = None,
        voting_period_days: int = DEFAULT_VOTING_PERIOD_DAYS,
    ) -> Optional[Proposal]:
        """
        Create a new governance proposal.

        Args:
            title: Proposal title
            description: Detailed description
            proposal_type: Type of proposal
            changes: Proposed changes (for parameter/feature proposals)
            voting_period_days: Voting period length

        Returns:
            Created proposal or None if failed
        """
        if not self._started:
            return None

        # Validate stake requirement
        my_stake = self._get_my_stake()
        if my_stake < MIN_STAKE_TO_PROPOSE:
            logger.warning(f"Insufficient stake to create proposal: {my_stake} < {MIN_STAKE_TO_PROPOSE}")
            return None

        # Check active proposal limit
        active = [p for p in self._proposals.values() if p.status == ProposalStatus.ACTIVE]
        if len(active) >= MAX_ACTIVE_PROPOSALS:
            logger.warning("Maximum active proposals reached")
            return None

        # Validate voting period
        voting_period_days = max(MIN_VOTING_PERIOD_DAYS, min(MAX_VOTING_PERIOD_DAYS, voting_period_days))

        now = int(time.time())
        voting_starts = now + 3600  # Voting starts 1 hour after creation
        voting_ends = voting_starts + (voting_period_days * 86400)

        proposal_id = Proposal.generate_id(
            title,
            self._peers.evrmore_address or "",
            now
        )

        proposal = Proposal(
            proposal_id=proposal_id,
            title=title,
            description=description,
            proposal_type=proposal_type,
            proposer_address=self._peers.evrmore_address or "",
            created_at=now,
            voting_starts=voting_starts,
            voting_ends=voting_ends,
            changes=changes or {},
            status=ProposalStatus.ACTIVE,
        )

        # Sign proposal
        if hasattr(self._peers, '_identity_bridge') and self._peers._identity_bridge:
            proposal.signature = self._peers._identity_bridge.sign_message(
                proposal.get_signing_message()
            )

        # Store locally
        self._proposals[proposal_id] = proposal

        # Broadcast to network
        try:
            await self._peers.broadcast(
                GOVERNANCE_PROPOSAL_TOPIC,
                json.dumps(proposal.to_dict()).encode()
            )
            logger.info(f"Created proposal: {proposal_id} - {title}")
            return proposal
        except Exception as e:
            logger.error(f"Failed to broadcast proposal: {e}")
            return None

    async def cancel_proposal(self, proposal_id: str) -> bool:
        """
        Cancel a proposal (proposer only, before voting ends).

        Args:
            proposal_id: Proposal to cancel

        Returns:
            True if cancelled successfully
        """
        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return False

        if proposal.proposer_address != self._peers.evrmore_address:
            logger.warning("Only proposer can cancel proposal")
            return False

        if proposal.status != ProposalStatus.ACTIVE:
            logger.warning("Can only cancel active proposals")
            return False

        proposal.status = ProposalStatus.CANCELLED
        logger.info(f"Cancelled proposal: {proposal_id}")
        return True

    # ========================================================================
    # VOTING
    # ========================================================================

    async def vote(
        self,
        proposal_id: str,
        choice: VoteChoice,
    ) -> bool:
        """
        Vote on a proposal.

        Args:
            proposal_id: Proposal to vote on
            choice: Vote choice

        Returns:
            True if vote was cast successfully
        """
        if not self._started:
            return False

        proposal = self._proposals.get(proposal_id)
        if not proposal:
            logger.warning(f"Proposal not found: {proposal_id}")
            return False

        # Check proposal is active
        if proposal.status != ProposalStatus.ACTIVE:
            logger.warning(f"Proposal not active: {proposal_id}")
            return False

        # Check voting period
        now = int(time.time())
        if now < proposal.voting_starts:
            logger.warning("Voting has not started yet")
            return False
        if now > proposal.voting_ends:
            logger.warning("Voting has ended")
            return False

        # Check stake requirement
        my_stake = self._get_my_stake()
        if my_stake < MIN_STAKE_TO_VOTE:
            logger.warning(f"Insufficient stake to vote: {my_stake} < {MIN_STAKE_TO_VOTE}")
            return False

        # Calculate voting power
        voting_power = self._calculate_voting_power(my_stake)

        vote = Vote(
            proposal_id=proposal_id,
            voter_address=self._peers.evrmore_address or "",
            choice=choice,
            voting_power=voting_power,
            stake_amount=my_stake,
            timestamp=now,
        )

        # Sign vote
        if hasattr(self._peers, '_identity_bridge') and self._peers._identity_bridge:
            vote.signature = self._peers._identity_bridge.sign_message(
                vote.get_signing_message()
            )

        # Record vote
        proposal.votes[vote.voter_address] = vote

        # Broadcast vote
        try:
            await self._peers.broadcast(
                GOVERNANCE_VOTE_TOPIC,
                json.dumps(vote.to_dict()).encode()
            )
            logger.info(f"Cast vote on {proposal_id}: {choice.value}")
            return True
        except Exception as e:
            logger.error(f"Failed to broadcast vote: {e}")
            return False

    # ========================================================================
    # COMMENTS
    # ========================================================================

    async def add_comment(
        self,
        proposal_id: str,
        content: str,
        is_status_update: bool = False,
    ) -> Optional[Comment]:
        """
        Add a comment to a proposal.

        Args:
            proposal_id: Proposal to comment on
            content: Comment content
            is_status_update: If True, this is an official status update (signers only)

        Returns:
            Created comment or None if failed
        """
        if not self._started:
            return None

        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return None

        # Status updates require signer
        if is_status_update and not self._is_signer():
            logger.warning("Only signers can post status updates")
            return None

        now = int(time.time())
        comment = Comment(
            comment_id=Comment.generate_id(proposal_id, self._peers.evrmore_address or "", now),
            proposal_id=proposal_id,
            author_address=self._peers.evrmore_address or "",
            content=content,
            timestamp=now,
            is_signer=self._is_signer(),
            is_status_update=is_status_update,
        )

        # Sign comment
        if hasattr(self._peers, '_identity_bridge') and self._peers._identity_bridge:
            message = f"comment:{comment.comment_id}:{content[:50]}:{now}"
            comment.signature = self._peers._identity_bridge.sign_message(message)

        # Add to proposal
        proposal.comments.append(comment)

        logger.info(f"Added {'status update' if is_status_update else 'comment'} to {proposal_id}")
        return comment

    def get_comments(self, proposal_id: str) -> List[Comment]:
        """Get all comments for a proposal."""
        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return []
        return list(proposal.comments)

    # ========================================================================
    # SIGNER CONTROLS
    # ========================================================================

    async def mark_executed(self, proposal_id: str) -> bool:
        """
        Mark a passed proposal as executed (signers only).

        Args:
            proposal_id: Proposal to mark

        Returns:
            True if marked successfully
        """
        if not self._is_signer():
            logger.warning("Only signers can mark proposals as executed")
            return False

        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return False

        if proposal.status != ProposalStatus.PASSED:
            logger.warning("Can only mark passed proposals as executed")
            return False

        proposal.status = ProposalStatus.EXECUTED
        proposal.executed_by = self._peers.evrmore_address or ""
        proposal.executed_at = int(time.time())

        logger.info(f"Marked proposal {proposal_id} as executed")
        return True

    async def pin_proposal(self, proposal_id: str, pinned: bool = True) -> bool:
        """
        Pin or unpin a proposal (signers only).

        Args:
            proposal_id: Proposal to pin/unpin
            pinned: True to pin, False to unpin

        Returns:
            True if updated successfully
        """
        if not self._is_signer():
            logger.warning("Only signers can pin proposals")
            return False

        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return False

        proposal.pinned = pinned
        proposal.pinned_by = self._peers.evrmore_address or "" if pinned else ""

        logger.info(f"{'Pinned' if pinned else 'Unpinned'} proposal {proposal_id}")
        return True

    async def emergency_cancel_vote(self, proposal_id: str) -> bool:
        """
        Vote to emergency cancel a malicious proposal (signers only).
        Requires 3-of-5 signers to agree.

        Args:
            proposal_id: Proposal to vote to cancel

        Returns:
            True if vote recorded, proposal cancelled if threshold reached
        """
        if not self._is_signer():
            logger.warning("Only signers can emergency cancel")
            return False

        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return False

        if proposal.status not in [ProposalStatus.ACTIVE, ProposalStatus.PASSED]:
            logger.warning("Can only emergency cancel active/passed proposals")
            return False

        # Add our vote
        proposal.emergency_cancel_votes.add(self._peers.evrmore_address or "")

        # Check if threshold reached (3-of-5)
        if len(proposal.emergency_cancel_votes) >= 3:
            proposal.status = ProposalStatus.CANCELLED
            logger.warning(f"EMERGENCY CANCELLED proposal {proposal_id} by signer consensus")

            # Add status update
            await self.add_comment(
                proposal_id,
                f"Emergency cancelled by signer consensus ({len(proposal.emergency_cancel_votes)}/5 votes)",
                is_status_update=True
            )
        else:
            logger.info(f"Emergency cancel vote recorded for {proposal_id} ({len(proposal.emergency_cancel_votes)}/3 needed)")

        return True

    def get_pinned_proposals(self) -> List[Proposal]:
        """Get all pinned proposals."""
        return [p for p in self._proposals.values() if p.pinned]

    # ========================================================================
    # QUERY METHODS
    # ========================================================================

    def get_proposal(self, proposal_id: str) -> Optional[Proposal]:
        """Get a proposal by ID."""
        return self._proposals.get(proposal_id)

    def get_active_proposals(self) -> List[Proposal]:
        """Get all active proposals."""
        return [p for p in self._proposals.values() if p.status == ProposalStatus.ACTIVE]

    def get_all_proposals(self) -> Dict[str, Proposal]:
        """Get all proposals."""
        return dict(self._proposals)

    def get_proposal_tally(self, proposal_id: str) -> Optional[VoteTally]:
        """Get vote tally for a proposal."""
        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return None

        tally = VoteTally()

        for vote in proposal.votes.values():
            if vote.choice == VoteChoice.YES:
                tally.yes_votes += vote.voting_power
            elif vote.choice == VoteChoice.NO:
                tally.no_votes += vote.voting_power
            elif vote.choice == VoteChoice.ABSTAIN:
                tally.abstain_votes += vote.voting_power
            tally.total_voters += 1

        # Check quorum and approval
        total_power = self._get_total_voting_power()
        quorum = self._get_quorum_requirement(proposal.proposal_type)
        approval = self._get_approval_requirement(proposal.proposal_type)

        total_voted = tally.yes_votes + tally.no_votes + tally.abstain_votes
        if total_power > 0:
            tally.quorum_reached = (total_voted / total_power) >= quorum

        non_abstain = tally.yes_votes + tally.no_votes
        if non_abstain > 0:
            tally.approval_reached = (tally.yes_votes / non_abstain) >= approval

        return tally

    def has_voted(self, proposal_id: str, voter_address: Optional[str] = None) -> bool:
        """Check if an address has voted on a proposal."""
        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return False

        address = voter_address or self._peers.evrmore_address
        return address in proposal.votes

    def get_my_vote(self, proposal_id: str) -> Optional[Vote]:
        """Get our vote on a proposal."""
        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return None
        return proposal.votes.get(self._peers.evrmore_address or "")

    def get_stats(self) -> dict:
        """Get governance statistics."""
        status_counts = {}
        for status in ProposalStatus:
            status_counts[status.value] = sum(
                1 for p in self._proposals.values()
                if p.status == status
            )

        return {
            "started": self._started,
            "total_proposals": len(self._proposals),
            "active_proposals": status_counts.get("active", 0),
            "passed_proposals": status_counts.get("passed", 0),
            "rejected_proposals": status_counts.get("rejected", 0),
            "my_stake": self._get_my_stake(),
            "my_voting_power": self._calculate_voting_power(self._get_my_stake()),
            "can_propose": self._get_my_stake() >= MIN_STAKE_TO_PROPOSE,
            "can_vote": self._get_my_stake() >= MIN_STAKE_TO_VOTE,
        }

    # ========================================================================
    # INTERNAL METHODS
    # ========================================================================

    def _get_my_stake(self) -> float:
        """Get our staked SATORI amount."""
        # TODO: Integrate with staking system
        return 50.0  # Default minimum for now

    def _get_uptime_days(self) -> int:
        """Get our uptime streak in days."""
        # TODO: Integrate with uptime tracker
        return 0

    def _is_signer(self) -> bool:
        """Check if we are a signer."""
        from .signer import is_authorized_signer
        return is_authorized_signer(self._peers.evrmore_address or "")

    def _calculate_voting_power(self, stake: float) -> float:
        """Calculate voting power from stake."""
        power = stake * STAKE_WEIGHT

        # Uptime bonus
        uptime_days = self._get_uptime_days()
        if uptime_days >= 90:
            power *= (1 + UPTIME_BONUS_90_DAYS)

        # Signer bonus
        if self._is_signer():
            power *= (1 + SIGNER_BONUS)

        return power

    def _get_total_voting_power(self) -> float:
        """Get total voting power in network (cached)."""
        now = int(time.time())
        if now - self._voting_power_cache_time < 3600:  # Cache for 1 hour
            return self._total_voting_power_cache

        # TODO: Calculate from actual network stake
        # For now, estimate based on known nodes
        self._total_voting_power_cache = 10000.0  # Placeholder
        self._voting_power_cache_time = now
        return self._total_voting_power_cache

    def _get_quorum_requirement(self, proposal_type: ProposalType) -> float:
        """Get quorum requirement for proposal type."""
        if proposal_type in [ProposalType.SIGNER_CHANGE, ProposalType.PROTOCOL_UPGRADE]:
            return QUORUM_CRITICAL
        elif proposal_type in [ProposalType.TREASURY_SPEND]:
            return QUORUM_MAJOR
        else:
            return QUORUM_STANDARD

    def _get_approval_requirement(self, proposal_type: ProposalType) -> float:
        """Get approval threshold for proposal type."""
        if proposal_type in [ProposalType.SIGNER_CHANGE, ProposalType.PROTOCOL_UPGRADE]:
            return APPROVAL_CRITICAL
        elif proposal_type in [ProposalType.TREASURY_SPEND]:
            return APPROVAL_MAJOR
        else:
            return APPROVAL_STANDARD

    async def _check_deadlines_loop(self) -> None:
        """Background loop to check proposal deadlines."""
        import trio
        logger.info("Governance deadline check loop started")
        try:
            while self._started:
                await trio.sleep(300)  # Check every 5 minutes
                if not self._started:
                    break

                now = int(time.time())
                for proposal in self._proposals.values():
                    if proposal.status != ProposalStatus.ACTIVE:
                        continue

                    if now > proposal.voting_ends:
                        await self._finalize_proposal(proposal)

        except trio.Cancelled:
            pass
        logger.info("Governance deadline check loop stopped")

    async def _finalize_proposal(self, proposal: Proposal) -> None:
        """Finalize a proposal after voting ends."""
        tally = self.get_proposal_tally(proposal.proposal_id)
        if not tally:
            proposal.status = ProposalStatus.EXPIRED
            return

        if not tally.quorum_reached:
            proposal.status = ProposalStatus.EXPIRED
            logger.info(f"Proposal {proposal.proposal_id} expired (quorum not reached)")
        elif tally.approval_reached:
            proposal.status = ProposalStatus.PASSED
            logger.info(f"Proposal {proposal.proposal_id} PASSED")
            # TODO: Execute proposal changes
        else:
            proposal.status = ProposalStatus.REJECTED
            logger.info(f"Proposal {proposal.proposal_id} rejected")

        # Broadcast result
        try:
            result = {
                "proposal_id": proposal.proposal_id,
                "status": proposal.status.value,
                "tally": tally.to_dict(),
                "timestamp": int(time.time()),
            }
            await self._peers.broadcast(
                GOVERNANCE_RESULT_TOPIC,
                json.dumps(result).encode()
            )
        except Exception as e:
            logger.warning(f"Failed to broadcast result: {e}")

    def _on_proposal(self, topic: str, data: Any) -> None:
        """Handle incoming proposal."""
        try:
            if isinstance(data, bytes):
                proposal_data = json.loads(data.decode())
            elif isinstance(data, dict):
                proposal_data = data
            else:
                return

            proposal = Proposal.from_dict(proposal_data)

            # Don't overwrite if we already have it with more votes
            existing = self._proposals.get(proposal.proposal_id)
            if existing and len(existing.votes) > len(proposal.votes):
                return

            self._proposals[proposal.proposal_id] = proposal
            logger.debug(f"Received proposal: {proposal.proposal_id}")

        except Exception as e:
            logger.warning(f"Error handling proposal: {e}")

    def _on_vote(self, topic: str, data: Any) -> None:
        """Handle incoming vote."""
        try:
            if isinstance(data, bytes):
                vote_data = json.loads(data.decode())
            elif isinstance(data, dict):
                vote_data = data
            else:
                return

            vote = Vote.from_dict(vote_data)

            proposal = self._proposals.get(vote.proposal_id)
            if not proposal:
                return

            # TODO: Verify vote signature

            proposal.votes[vote.voter_address] = vote
            logger.debug(f"Received vote on {vote.proposal_id} from {vote.voter_address}")

        except Exception as e:
            logger.warning(f"Error handling vote: {e}")

    def _on_result(self, topic: str, data: Any) -> None:
        """Handle incoming result."""
        try:
            if isinstance(data, bytes):
                result_data = json.loads(data.decode())
            elif isinstance(data, dict):
                result_data = data
            else:
                return

            proposal_id = result_data.get("proposal_id")
            status = result_data.get("status")

            proposal = self._proposals.get(proposal_id)
            if proposal and proposal.status == ProposalStatus.ACTIVE:
                proposal.status = ProposalStatus(status)
                logger.info(f"Proposal {proposal_id} finalized: {status}")

        except Exception as e:
            logger.warning(f"Error handling result: {e}")


# ============================================================================
# GOVERNANCE PARTICIPATION TRACKER
# ============================================================================

@dataclass
class ParticipationStats:
    """Statistics for a user's governance participation."""
    address: str
    total_proposals: int = 0           # Total proposals ever
    eligible_proposals: int = 0        # Proposals user could have voted on
    votes_cast: int = 0                # Actual votes cast
    proposals_created: int = 0         # Proposals created by user
    comments_count: int = 0            # Comments on proposals
    status_updates: int = 0            # Status updates (signers only)
    vote_rate: float = 0.0             # votes_cast / eligible_proposals
    last_vote_time: int = 0            # Timestamp of last vote
    last_proposal_time: int = 0        # Timestamp of last proposal
    last_comment_time: int = 0         # Timestamp of last comment
    governance_tier: Optional[str] = None  # bronze/silver/gold/platinum/diamond
    governance_bonus: float = 0.0      # Current governance multiplier bonus

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ParticipationStats":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class GovernanceParticipationTracker:
    """
    Tracks governance participation for reward multiplier calculation.

    Monitors:
    - Vote rate (% of proposals voted on)
    - Proposals created
    - Comments/discussion participation

    Used by rewards.py to calculate governance bonus multiplier.
    """

    def __init__(
        self,
        governance: Optional[GovernanceProtocol] = None,
        storage_path: Optional[str] = None
    ):
        """
        Initialize GovernanceParticipationTracker.

        Args:
            governance: Optional GovernanceProtocol instance
            storage_path: Optional path for persistent storage
        """
        self.governance = governance
        self.storage_path = storage_path or "~/.satori/governance_participation.json"

        # Participation data by address
        self._stats: Dict[str, ParticipationStats] = {}

        # Load from storage if available
        self._load()

    def _load(self) -> None:
        """Load participation data from storage."""
        import os
        path = os.path.expanduser(self.storage_path)
        try:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    data = json.load(f)
                    for addr, stats_data in data.items():
                        self._stats[addr] = ParticipationStats.from_dict(stats_data)
                logger.info(f"Loaded governance participation data for {len(self._stats)} addresses")
        except Exception as e:
            logger.warning(f"Failed to load participation data: {e}")

    def _save(self) -> None:
        """Save participation data to storage."""
        import os
        path = os.path.expanduser(self.storage_path)
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w') as f:
                data = {addr: stats.to_dict() for addr, stats in self._stats.items()}
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save participation data: {e}")

    def get_stats(self, address: str) -> ParticipationStats:
        """
        Get participation stats for an address.

        Args:
            address: User's blockchain address

        Returns:
            ParticipationStats object
        """
        if address not in self._stats:
            self._stats[address] = ParticipationStats(address=address)
        return self._stats[address]

    def record_vote(self, address: str, proposal_id: str) -> None:
        """
        Record that an address voted on a proposal.

        Args:
            address: Voter's address
            proposal_id: Proposal that was voted on
        """
        stats = self.get_stats(address)
        stats.votes_cast += 1
        stats.last_vote_time = int(time.time())
        self._update_vote_rate(stats)
        self._update_tier(stats)
        self._save()
        logger.debug(f"Recorded vote by {address} on {proposal_id}")

    def record_proposal_created(self, address: str, proposal_id: str) -> None:
        """
        Record that an address created a proposal.

        Args:
            address: Proposer's address
            proposal_id: Proposal that was created
        """
        stats = self.get_stats(address)
        stats.proposals_created += 1
        stats.last_proposal_time = int(time.time())
        self._update_tier(stats)
        self._save()
        logger.debug(f"Recorded proposal creation by {address}: {proposal_id}")

    def record_comment(self, address: str, proposal_id: str, is_status_update: bool = False) -> None:
        """
        Record that an address commented on a proposal.

        Args:
            address: Commenter's address
            proposal_id: Proposal that was commented on
            is_status_update: Whether this was a signer status update
        """
        stats = self.get_stats(address)
        stats.comments_count += 1
        if is_status_update:
            stats.status_updates += 1
        stats.last_comment_time = int(time.time())
        self._update_tier(stats)
        self._save()
        logger.debug(f"Recorded comment by {address} on {proposal_id}")

    def update_eligible_proposals(self, address: str, total_eligible: int) -> None:
        """
        Update the count of proposals a user was eligible to vote on.

        Called periodically to recalculate vote rate.

        Args:
            address: User's address
            total_eligible: Total proposals user could have voted on
        """
        stats = self.get_stats(address)
        stats.eligible_proposals = total_eligible
        self._update_vote_rate(stats)
        self._update_tier(stats)
        self._save()

    def sync_from_governance(self, address: str) -> ParticipationStats:
        """
        Sync participation stats from the governance protocol.

        Scans all proposals to calculate accurate stats.

        Args:
            address: User's address to sync

        Returns:
            Updated ParticipationStats
        """
        if not self.governance:
            return self.get_stats(address)

        stats = self.get_stats(address)
        proposals = self.governance.get_proposals()

        # Reset counts
        stats.votes_cast = 0
        stats.proposals_created = 0
        stats.comments_count = 0
        stats.status_updates = 0
        stats.eligible_proposals = 0

        now = int(time.time())

        for proposal in proposals:
            # Count proposals user was eligible for (had stake at the time)
            # For simplicity, count all non-draft proposals
            if proposal.status != ProposalStatus.DRAFT:
                stats.eligible_proposals += 1

            # Check if user voted
            if address in proposal.votes:
                stats.votes_cast += 1
                vote = proposal.votes[address]
                if vote.timestamp > stats.last_vote_time:
                    stats.last_vote_time = vote.timestamp

            # Check if user created proposal
            if proposal.proposer_address == address:
                stats.proposals_created += 1
                if proposal.created_at > stats.last_proposal_time:
                    stats.last_proposal_time = proposal.created_at

            # Count comments
            for comment in proposal.comments:
                if comment.author_address == address:
                    stats.comments_count += 1
                    if comment.is_status_update:
                        stats.status_updates += 1
                    if comment.timestamp > stats.last_comment_time:
                        stats.last_comment_time = comment.timestamp

        self._update_vote_rate(stats)
        self._update_tier(stats)
        self._save()

        logger.info(
            f"Synced governance participation for {address}: "
            f"votes={stats.votes_cast}/{stats.eligible_proposals}, "
            f"proposals={stats.proposals_created}, "
            f"comments={stats.comments_count}"
        )

        return stats

    def _update_vote_rate(self, stats: ParticipationStats) -> None:
        """Update vote rate calculation."""
        if stats.eligible_proposals > 0:
            stats.vote_rate = stats.votes_cast / stats.eligible_proposals
        else:
            stats.vote_rate = 0.0

    def _update_tier(self, stats: ParticipationStats) -> None:
        """Update governance tier and bonus based on current stats."""
        # Import here to avoid circular dependency
        from .rewards import get_governance_tier, calculate_governance_bonus

        stats.governance_tier = get_governance_tier(
            stats.vote_rate,
            stats.proposals_created,
            stats.comments_count
        )
        stats.governance_bonus = calculate_governance_bonus(
            stats.vote_rate,
            stats.proposals_created,
            stats.comments_count
        )

    def get_governance_multiplier_data(self, address: str) -> Dict[str, Any]:
        """
        Get data needed for rewards.py governance multiplier calculation.

        Args:
            address: User's address

        Returns:
            Dict with vote_rate, proposals_created, comments_count
        """
        stats = self.get_stats(address)
        return {
            'governance_vote_rate': stats.vote_rate,
            'governance_proposals': stats.proposals_created,
            'governance_comments': stats.comments_count,
            'governance_tier': stats.governance_tier,
            'governance_bonus': stats.governance_bonus,
        }

    def get_leaderboard(self, limit: int = 50) -> List[ParticipationStats]:
        """
        Get governance participation leaderboard.

        Sorted by governance bonus (highest first).

        Args:
            limit: Maximum number of entries to return

        Returns:
            List of ParticipationStats sorted by bonus
        """
        sorted_stats = sorted(
            self._stats.values(),
            key=lambda s: s.governance_bonus,
            reverse=True
        )
        return sorted_stats[:limit]
