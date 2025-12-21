"""
Deferred Rewards System for Satori Network.

Handles reward deferral when treasury has insufficient funds:
- Records earned rewards when distribution is blocked
- Tracks deferred rewards per address and round
- Manages catch-up distributions when treasury is funded

Works with TreasuryAlertManager to notify users of deferred status.
"""

import time
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

class DeferralReason(Enum):
    """Reasons for reward deferral."""
    INSUFFICIENT_SATORI = 'insufficient_satori'
    INSUFFICIENT_EVR = 'insufficient_evr'
    TREASURY_CRITICAL = 'treasury_critical'
    DISTRIBUTION_FAILED = 'distribution_failed'


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class DeferredReward:
    """A single deferred reward record."""
    address: str
    round_id: int
    amount: float
    reason: str
    created_at: int = field(default_factory=lambda: int(time.time()))
    paid_at: Optional[int] = None
    paid_tx_hash: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'DeferredReward':
        return cls(**data)

    @property
    def is_paid(self) -> bool:
        return self.paid_at is not None


@dataclass
class DeferredRewardsSummary:
    """Summary of deferred rewards for an address."""
    address: str
    total_pending: float
    deferred_count: int
    deferred_rewards: List[DeferredReward]
    oldest_deferred_at: Optional[int]
    newest_deferred_at: Optional[int]

    def to_dict(self) -> dict:
        return {
            'address': self.address,
            'total_pending': self.total_pending,
            'deferred_count': self.deferred_count,
            'deferred_rewards': [r.to_dict() for r in self.deferred_rewards],
            'oldest_deferred_at': self.oldest_deferred_at,
            'newest_deferred_at': self.newest_deferred_at,
        }


@dataclass
class RoundDeferral:
    """Summary of a deferred round."""
    round_id: int
    total_amount: float
    recipient_count: int
    reason: str
    deferred_at: int
    rewards: List[DeferredReward] = field(default_factory=list)


# =============================================================================
# DEFERRED REWARDS MANAGER
# =============================================================================

class DeferredRewardsManager:
    """
    Manages deferred rewards when treasury has insufficient funds.

    Tracks:
    - Individual deferred rewards per address/round
    - Total deferred amounts
    - Round-level deferrals for batch processing

    Usage:
        manager = DeferredRewardsManager()

        # When distribution is blocked, defer the round
        manager.defer_round(
            round_id=1234,
            rewards={'EAddr1': 5.25, 'EAddr2': 3.10},
            reason=DeferralReason.INSUFFICIENT_SATORI,
        )

        # Get user's deferred rewards
        summary = manager.get_deferred_for_address('EAddr1')

        # When treasury is funded, get rewards to pay
        to_pay = manager.get_unpaid_rewards()

        # Mark as paid after distribution
        manager.mark_paid(
            addresses=['EAddr1', 'EAddr2'],
            round_ids=[1234],
            tx_hash='abc123...',
        )
    """

    def __init__(self):
        # In-memory storage (would be database in production)
        self._deferred_rewards: Dict[str, List[DeferredReward]] = {}  # address -> rewards
        self._deferred_rounds: Dict[int, RoundDeferral] = {}  # round_id -> RoundDeferral

    def defer_round(
        self,
        round_id: int,
        rewards: Dict[str, float],  # address -> amount
        reason: DeferralReason,
    ) -> RoundDeferral:
        """
        Defer an entire round's rewards.

        Args:
            round_id: The round being deferred
            rewards: Dict of address -> reward amount
            reason: Why the round is being deferred

        Returns:
            RoundDeferral summary
        """
        deferred_at = int(time.time())
        total_amount = sum(rewards.values())
        deferred_records = []

        for address, amount in rewards.items():
            record = DeferredReward(
                address=address,
                round_id=round_id,
                amount=amount,
                reason=reason.value,
                created_at=deferred_at,
            )
            deferred_records.append(record)

            # Add to address index
            if address not in self._deferred_rewards:
                self._deferred_rewards[address] = []
            self._deferred_rewards[address].append(record)

        # Create round summary
        round_deferral = RoundDeferral(
            round_id=round_id,
            total_amount=total_amount,
            recipient_count=len(rewards),
            reason=reason.value,
            deferred_at=deferred_at,
            rewards=deferred_records,
        )
        self._deferred_rounds[round_id] = round_deferral

        logger.info(
            f"Deferred round {round_id}: {total_amount:.4f} SATORI "
            f"to {len(rewards)} recipients ({reason.value})"
        )

        return round_deferral

    def defer_single(
        self,
        address: str,
        round_id: int,
        amount: float,
        reason: DeferralReason,
    ) -> DeferredReward:
        """
        Defer a single reward.

        Args:
            address: Recipient address
            round_id: Round ID
            amount: Reward amount
            reason: Deferral reason

        Returns:
            DeferredReward record
        """
        record = DeferredReward(
            address=address,
            round_id=round_id,
            amount=amount,
            reason=reason.value,
        )

        if address not in self._deferred_rewards:
            self._deferred_rewards[address] = []
        self._deferred_rewards[address].append(record)

        logger.info(f"Deferred {amount:.4f} SATORI for {address} (round {round_id})")
        return record

    def get_deferred_for_address(self, address: str) -> DeferredRewardsSummary:
        """
        Get all deferred rewards for an address.

        Args:
            address: The address to query

        Returns:
            DeferredRewardsSummary
        """
        rewards = self._deferred_rewards.get(address, [])
        unpaid = [r for r in rewards if not r.is_paid]

        if not unpaid:
            return DeferredRewardsSummary(
                address=address,
                total_pending=0.0,
                deferred_count=0,
                deferred_rewards=[],
                oldest_deferred_at=None,
                newest_deferred_at=None,
            )

        total = sum(r.amount for r in unpaid)
        timestamps = [r.created_at for r in unpaid]

        return DeferredRewardsSummary(
            address=address,
            total_pending=total,
            deferred_count=len(unpaid),
            deferred_rewards=unpaid,
            oldest_deferred_at=min(timestamps),
            newest_deferred_at=max(timestamps),
        )

    def get_unpaid_rewards(self) -> List[DeferredReward]:
        """
        Get all unpaid deferred rewards.

        Returns:
            List of unpaid DeferredReward records
        """
        unpaid = []
        for rewards in self._deferred_rewards.values():
            unpaid.extend([r for r in rewards if not r.is_paid])
        return unpaid

    def get_unpaid_by_address(self) -> Dict[str, float]:
        """
        Get unpaid totals grouped by address.

        Returns:
            Dict of address -> total unpaid amount
        """
        totals = {}
        for address, rewards in self._deferred_rewards.items():
            unpaid = sum(r.amount for r in rewards if not r.is_paid)
            if unpaid > 0:
                totals[address] = unpaid
        return totals

    def get_total_deferred(self) -> float:
        """Get total amount of deferred rewards."""
        return sum(
            r.amount
            for rewards in self._deferred_rewards.values()
            for r in rewards
            if not r.is_paid
        )

    def get_deferred_round_count(self) -> int:
        """Get number of deferred rounds with unpaid rewards."""
        unpaid_rounds = set()
        for rewards in self._deferred_rewards.values():
            for r in rewards:
                if not r.is_paid:
                    unpaid_rounds.add(r.round_id)
        return len(unpaid_rounds)

    def get_deferred_rounds(self) -> List[RoundDeferral]:
        """Get all deferred rounds."""
        return list(self._deferred_rounds.values())

    def mark_paid(
        self,
        addresses: List[str] = None,
        round_ids: List[int] = None,
        tx_hash: str = None,
    ) -> int:
        """
        Mark rewards as paid.

        Args:
            addresses: Optional filter by addresses
            round_ids: Optional filter by round IDs
            tx_hash: Transaction hash of payment

        Returns:
            Number of rewards marked as paid
        """
        paid_at = int(time.time())
        count = 0

        for address, rewards in self._deferred_rewards.items():
            if addresses and address not in addresses:
                continue

            for reward in rewards:
                if reward.is_paid:
                    continue

                if round_ids and reward.round_id not in round_ids:
                    continue

                reward.paid_at = paid_at
                reward.paid_tx_hash = tx_hash
                count += 1

        logger.info(f"Marked {count} deferred rewards as paid (tx: {tx_hash})")
        return count

    def mark_round_paid(self, round_id: int, tx_hash: str = None) -> int:
        """
        Mark all rewards for a round as paid.

        Args:
            round_id: The round to mark as paid
            tx_hash: Transaction hash

        Returns:
            Number of rewards marked as paid
        """
        return self.mark_paid(round_ids=[round_id], tx_hash=tx_hash)

    def calculate_catchup_distribution(
        self,
        available_satori: float,
        current_round_amount: float,
    ) -> Dict[str, Any]:
        """
        Calculate how much can be paid in a catch-up distribution.

        Prioritizes:
        1. Current round (if possible)
        2. Oldest deferred rounds first

        Args:
            available_satori: SATORI available in treasury
            current_round_amount: Amount needed for current round

        Returns:
            Dict with distribution plan
        """
        deferred_total = self.get_total_deferred()
        total_needed = current_round_amount + deferred_total

        result = {
            'available': available_satori,
            'current_round_needed': current_round_amount,
            'deferred_total': deferred_total,
            'total_needed': total_needed,
            'can_pay_current': False,
            'can_pay_all_deferred': False,
            'deferred_to_pay': [],
            'amount_to_distribute': 0.0,
        }

        if available_satori >= total_needed:
            # Can pay everything!
            result['can_pay_current'] = True
            result['can_pay_all_deferred'] = True
            result['amount_to_distribute'] = total_needed
            result['deferred_to_pay'] = self.get_unpaid_rewards()

        elif available_satori >= current_round_amount:
            # Can pay current, partial deferred
            result['can_pay_current'] = True
            remaining = available_satori - current_round_amount

            # Pay oldest deferred first
            deferred_to_pay = []
            for reward in sorted(self.get_unpaid_rewards(), key=lambda r: r.created_at):
                if remaining >= reward.amount:
                    deferred_to_pay.append(reward)
                    remaining -= reward.amount

            result['deferred_to_pay'] = deferred_to_pay
            result['amount_to_distribute'] = available_satori - remaining

        else:
            # Can't even pay current round
            result['can_pay_current'] = False

        return result

    def get_stats(self) -> dict:
        """Get statistics about deferred rewards."""
        unpaid = self.get_unpaid_rewards()

        if not unpaid:
            return {
                'total_deferred': 0.0,
                'deferred_count': 0,
                'unique_addresses': 0,
                'deferred_rounds': 0,
                'oldest_deferral': None,
                'newest_deferral': None,
            }

        addresses = set(r.address for r in unpaid)
        rounds = set(r.round_id for r in unpaid)
        timestamps = [r.created_at for r in unpaid]

        return {
            'total_deferred': sum(r.amount for r in unpaid),
            'deferred_count': len(unpaid),
            'unique_addresses': len(addresses),
            'deferred_rounds': len(rounds),
            'oldest_deferral': min(timestamps),
            'newest_deferral': max(timestamps),
        }

    def clear_paid_history(self, older_than_days: int = 30) -> int:
        """
        Clear old paid reward records to save memory.

        Args:
            older_than_days: Remove paid records older than this

        Returns:
            Number of records removed
        """
        cutoff = int(time.time()) - (older_than_days * 86400)
        removed = 0

        for address in list(self._deferred_rewards.keys()):
            original_count = len(self._deferred_rewards[address])
            self._deferred_rewards[address] = [
                r for r in self._deferred_rewards[address]
                if not r.is_paid or r.paid_at > cutoff
            ]
            removed += original_count - len(self._deferred_rewards[address])

            # Clean up empty entries
            if not self._deferred_rewards[address]:
                del self._deferred_rewards[address]

        return removed


# =============================================================================
# INTEGRATION HELPERS
# =============================================================================

def should_defer_distribution(
    treasury_satori: float,
    treasury_evr: float,
    required_satori: float,
    required_evr: float,
) -> tuple[bool, Optional[DeferralReason]]:
    """
    Check if a distribution should be deferred.

    Args:
        treasury_satori: Current SATORI balance
        treasury_evr: Current EVR balance
        required_satori: SATORI needed for distribution
        required_evr: EVR needed for transaction fees

    Returns:
        Tuple of (should_defer, reason)
    """
    satori_ok = treasury_satori >= required_satori
    evr_ok = treasury_evr >= required_evr

    if satori_ok and evr_ok:
        return False, None
    elif not satori_ok and not evr_ok:
        return True, DeferralReason.TREASURY_CRITICAL
    elif not satori_ok:
        return True, DeferralReason.INSUFFICIENT_SATORI
    else:
        return True, DeferralReason.INSUFFICIENT_EVR
