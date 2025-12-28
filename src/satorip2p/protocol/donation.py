"""
satorip2p/protocol/donation.py

Treasury donation protocol for Satori P2P network.

Neurons can donate EVR to the treasury and receive SATORI rewards in return.
This module handles donation tracking, tier progression, and achievement badges.

Terminology:
- Round: 1 day (prediction cycle)
- Epoch: 1 week (aggregation period for rewards/stats)
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# Epoch duration in seconds (1 week = 7 days)
# Each epoch represents a weekly reward/governance cycle
# 52 epochs per year, 7 rounds per epoch
EPOCH_DURATION = 7 * 24 * 60 * 60  # 604,800 seconds (7 days)

# Round duration in seconds (1 day)
# Each round is a daily prediction cycle
# 7 rounds per epoch
ROUND_DURATION = 24 * 60 * 60  # 86,400 seconds (1 day)

# Donation tier thresholds (cumulative EVR)
DONATION_TIERS = {
    'bronze': 100,
    'silver': 1_000,
    'gold': 10_000,
    'platinum': 100_000,
    'diamond': 1_000_000,
}

# Tier order for progression
TIER_ORDER = ['none', 'bronze', 'silver', 'gold', 'platinum', 'diamond']

# PubSub topics
DONATION_TOPIC = "satori/treasury/donations"
DONOR_STATS_TOPIC = "satori/treasury/donors"

# DHT key prefixes
DHT_DONATION_PREFIX = "satori:donation:"
DHT_DONOR_PREFIX = "satori:donor:"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class TreasuryDonation:
    """
    A single donation from a neuron to the treasury.

    Donations are sent from the neuron's wallet directly to the treasury
    multi-sig address. The neuron's identity is already known, so no
    external signature verification is needed.
    """
    id: str                           # Unique donation ID (hash of tx_id + donor)
    donor_address: str                # Neuron's wallet address
    amount: float                     # EVR amount donated
    tx_id: str                        # Evrmore transaction ID
    timestamp: int                    # When donation was made
    evr_usd_rate: float = 0.0         # EVR/USD rate at time of donation
    satori_usd_rate: float = 0.0      # SATORI/USD rate at time of donation
    satori_reward: float = 0.0        # Calculated SATORI reward
    status: str = 'pending'           # 'pending', 'approved', 'paid', 'rejected'
    approved_at: Optional[int] = None # When signers approved
    paid_at: Optional[int] = None     # When SATORI was sent
    paid_tx_id: Optional[str] = None  # SATORI payment transaction ID

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TreasuryDonation":
        """Create from dictionary."""
        return cls(**data)

    @staticmethod
    def generate_id(tx_id: str, donor_address: str) -> str:
        """Generate unique donation ID."""
        content = f"{tx_id}:{donor_address}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]


@dataclass
class DonorStats:
    """
    Cumulative donation statistics for a donor.

    Tracks total donations, current tier, and badges earned.
    """
    donor_address: str                # Neuron's wallet address
    total_donated: float = 0.0        # Cumulative EVR donated
    donation_count: int = 0           # Number of donations
    tier: str = 'none'                # Current tier: none/bronze/silver/gold/platinum/diamond
    badges_earned: List[str] = field(default_factory=list)  # Badge names earned
    first_donation: Optional[int] = None  # Timestamp of first donation
    last_donation: Optional[int] = None   # Timestamp of last donation
    total_satori_received: float = 0.0    # Total SATORI received for donations

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DonorStats":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class TierAchievement:
    """
    Record of a tier achievement (first person to reach a tier).

    These are unique achievements - only one person can ever hold each.
    """
    tier: str                         # Tier name (bronze, silver, etc.)
    achiever_address: str             # Address of first person to reach tier
    achieved_at: int                  # Timestamp when tier was reached
    donation_id: str                  # Donation that triggered the achievement
    badge_tx_id: Optional[str] = None # Transaction ID of badge mint

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TierAchievement":
        """Create from dictionary."""
        return cls(**data)


# ============================================================================
# DONATION MANAGER
# ============================================================================

class DonationManager:
    """
    Manages treasury donations via P2P network.

    Handles:
    - Sending donations from neuron wallet to treasury
    - Tracking donation history and cumulative stats
    - Tier progression and badge awards
    - First-to-tier unique achievements

    Usage:
        manager = DonationManager(wallet, treasury_address)
        await manager.start()

        # Donate EVR
        donation = await manager.donate(100.0)

        # Check stats
        stats = await manager.get_my_stats()
        print(f"Total donated: {stats.total_donated} EVR")
        print(f"Current tier: {stats.tier}")
    """

    def __init__(
        self,
        wallet: Any,  # EvrmoreWallet or similar
        treasury_address: str,
        electrumx_client: Any = None,
        exchange_rate_provider: Optional[Callable] = None,
    ):
        """
        Initialize DonationManager.

        Args:
            wallet: Neuron's wallet for sending donations
            treasury_address: Multi-sig treasury address
            electrumx_client: ElectrumX client for blockchain operations
            exchange_rate_provider: Async function returning (evr_usd, satori_usd) rates
        """
        self._wallet = wallet
        self._treasury_address = treasury_address
        self._electrumx = electrumx_client
        self._exchange_rate_provider = exchange_rate_provider

        # Local caches
        self._donations: Dict[str, TreasuryDonation] = {}  # id -> donation
        self._donor_stats: Dict[str, DonorStats] = {}      # address -> stats
        self._tier_achievements: Dict[str, TierAchievement] = {}  # tier -> achievement

        # State
        self._running = False
        self._lock = asyncio.Lock()

        logger.info(f"DonationManager initialized with treasury: {treasury_address}")

    # ========================================================================
    # LIFECYCLE
    # ========================================================================

    async def start(self) -> None:
        """Start the donation manager."""
        if self._running:
            return

        self._running = True
        logger.info("DonationManager started")

    async def stop(self) -> None:
        """Stop the donation manager."""
        self._running = False
        logger.info("DonationManager stopped")

    # ========================================================================
    # DONATION OPERATIONS
    # ========================================================================

    async def donate(self, amount: float) -> TreasuryDonation:
        """
        Send EVR donation to treasury.

        Args:
            amount: EVR amount to donate

        Returns:
            TreasuryDonation record

        Raises:
            ValueError: If amount is invalid
            RuntimeError: If transaction fails
        """
        if amount <= 0:
            raise ValueError("Donation amount must be positive")

        donor_address = self._wallet.address
        timestamp = int(time.time())

        # Get exchange rates for reward calculation
        evr_usd, satori_usd = await self._get_exchange_rates()

        # Calculate SATORI reward (90% of fair value for slippage buffer)
        if satori_usd > 0:
            evr_value_usd = amount * evr_usd
            satori_reward = (evr_value_usd / satori_usd) * 0.90
        else:
            satori_reward = 0.0

        # Send EVR to treasury
        try:
            tx_id = await self._send_evr(amount)
        except Exception as e:
            logger.error(f"Failed to send donation: {e}")
            raise RuntimeError(f"Donation transaction failed: {e}")

        # Create donation record
        donation_id = TreasuryDonation.generate_id(tx_id, donor_address)
        donation = TreasuryDonation(
            id=donation_id,
            donor_address=donor_address,
            amount=amount,
            tx_id=tx_id,
            timestamp=timestamp,
            evr_usd_rate=evr_usd,
            satori_usd_rate=satori_usd,
            satori_reward=satori_reward,
            status='pending',
        )

        # Store locally
        async with self._lock:
            self._donations[donation_id] = donation

            # Update donor stats
            await self._update_donor_stats(donor_address, donation)

        logger.info(
            f"Donation sent: {amount} EVR from {donor_address[:12]}... "
            f"(reward: {satori_reward:.2f} SATORI)"
        )

        return donation

    async def get_donation(self, donation_id: str) -> Optional[TreasuryDonation]:
        """Get a specific donation by ID."""
        return self._donations.get(donation_id)

    async def get_my_donations(self) -> List[TreasuryDonation]:
        """Get all donations from this neuron."""
        donor_address = self._wallet.address
        return [
            d for d in self._donations.values()
            if d.donor_address == donor_address
        ]

    async def get_donations_by_address(self, address: str) -> List[TreasuryDonation]:
        """Get all donations from a specific address."""
        return [
            d for d in self._donations.values()
            if d.donor_address == address
        ]

    async def get_pending_donations(self) -> List[TreasuryDonation]:
        """Get all pending donations awaiting approval."""
        return [
            d for d in self._donations.values()
            if d.status == 'pending'
        ]

    # ========================================================================
    # DONOR STATS
    # ========================================================================

    async def get_my_stats(self) -> DonorStats:
        """Get this neuron's donation stats."""
        return await self.get_donor_stats(self._wallet.address)

    async def get_donor_stats(self, address: str) -> DonorStats:
        """Get donation stats for a specific address."""
        if address not in self._donor_stats:
            self._donor_stats[address] = DonorStats(donor_address=address)
        return self._donor_stats[address]

    async def _update_donor_stats(
        self,
        address: str,
        donation: TreasuryDonation
    ) -> Optional[str]:
        """
        Update donor stats after a donation.

        Returns:
            New tier name if tier upgraded, None otherwise
        """
        stats = await self.get_donor_stats(address)

        # Update cumulative values
        stats.total_donated += donation.amount
        stats.donation_count += 1
        stats.last_donation = donation.timestamp

        if stats.first_donation is None:
            stats.first_donation = donation.timestamp

        # Check for tier upgrade
        old_tier = stats.tier
        new_tier = self._calculate_tier(stats.total_donated)

        if new_tier != old_tier:
            stats.tier = new_tier

            # Add tier badge
            badge_name = f"DONOR_{new_tier.upper()}"
            if badge_name not in stats.badges_earned:
                stats.badges_earned.append(badge_name)

            # Check for first-to-tier achievement
            await self._check_first_to_tier(address, new_tier, donation)

            logger.info(f"Donor {address[:12]}... upgraded to {new_tier} tier!")
            return new_tier

        return None

    def _calculate_tier(self, total_donated: float) -> str:
        """Calculate tier based on cumulative donations."""
        tier = 'none'
        for tier_name in TIER_ORDER[1:]:  # Skip 'none'
            if total_donated >= DONATION_TIERS.get(tier_name, float('inf')):
                tier = tier_name
        return tier

    # ========================================================================
    # ACHIEVEMENTS
    # ========================================================================

    async def _check_first_to_tier(
        self,
        address: str,
        tier: str,
        donation: TreasuryDonation
    ) -> Optional[TierAchievement]:
        """
        Check if this is the first person to reach a tier.

        First-to-tier achievements are unique - only one person ever gets each.

        Returns:
            TierAchievement if this is the first, None otherwise
        """
        if tier in self._tier_achievements:
            # Someone already achieved this tier first
            return None

        # This is the first person to reach this tier!
        achievement = TierAchievement(
            tier=tier,
            achiever_address=address,
            achieved_at=donation.timestamp,
            donation_id=donation.id,
        )

        self._tier_achievements[tier] = achievement

        # Add unique achievement badge to donor stats
        stats = await self.get_donor_stats(address)
        unique_badge = f"FIRST_{tier.upper()}_DONOR"
        if unique_badge not in stats.badges_earned:
            stats.badges_earned.append(unique_badge)

        logger.info(
            f"UNIQUE ACHIEVEMENT: {address[:12]}... is the FIRST {tier} donor!"
        )

        return achievement

    async def get_tier_achievements(self) -> Dict[str, TierAchievement]:
        """Get all first-to-tier achievements."""
        return self._tier_achievements.copy()

    async def get_tier_achievement(self, tier: str) -> Optional[TierAchievement]:
        """Get the first-to-tier achievement for a specific tier."""
        return self._tier_achievements.get(tier)

    async def is_tier_achieved(self, tier: str) -> bool:
        """Check if someone has already achieved first-to-tier."""
        return tier in self._tier_achievements

    # ========================================================================
    # LEADERBOARD
    # ========================================================================

    async def get_top_donors(self, limit: int = 10) -> List[DonorStats]:
        """Get top donors by total donated."""
        sorted_stats = sorted(
            self._donor_stats.values(),
            key=lambda s: s.total_donated,
            reverse=True
        )
        return sorted_stats[:limit]

    async def get_donors_by_tier(self, tier: str) -> List[DonorStats]:
        """Get all donors at a specific tier."""
        return [
            s for s in self._donor_stats.values()
            if s.tier == tier
        ]

    # ========================================================================
    # TREASURY INFO
    # ========================================================================

    def get_treasury_address(self) -> str:
        """Get the treasury multi-sig address."""
        return self._treasury_address

    async def get_treasury_balance(self) -> float:
        """Get current treasury EVR balance."""
        if self._electrumx is None:
            return 0.0

        try:
            balance = await self._electrumx.get_balance(self._treasury_address)
            return balance.get('confirmed', 0) / 100_000_000  # Satoshis to EVR
        except Exception as e:
            logger.error(f"Failed to get treasury balance: {e}")
            return 0.0

    # ========================================================================
    # INTERNAL HELPERS
    # ========================================================================

    async def _get_exchange_rates(self) -> Tuple[float, float]:
        """Get current exchange rates (EVR/USD, SATORI/USD)."""
        if self._exchange_rate_provider:
            try:
                return await self._exchange_rate_provider()
            except Exception as e:
                logger.warning(f"Failed to get exchange rates: {e}")

        # Default rates if provider unavailable
        return (0.002, 0.10)  # $0.002 per EVR, $0.10 per SATORI

    async def _send_evr(self, amount: float) -> str:
        """
        Send EVR from wallet to treasury.

        Returns:
            Transaction ID
        """
        if self._electrumx is None:
            raise RuntimeError("ElectrumX client not configured")

        # Build and broadcast transaction
        # This would use the wallet's send functionality
        # For now, placeholder implementation

        # In real implementation:
        # 1. Get UTXOs from wallet
        # 2. Build transaction to treasury
        # 3. Sign with wallet private key
        # 4. Broadcast via ElectrumX

        raise NotImplementedError(
            "EVR sending requires wallet integration. "
            "Override _send_evr() or provide electrumx_client."
        )

    # ========================================================================
    # SERIALIZATION
    # ========================================================================

    def to_dict(self) -> Dict[str, Any]:
        """Serialize manager state to dictionary."""
        return {
            'treasury_address': self._treasury_address,
            'donations': {k: v.to_dict() for k, v in self._donations.items()},
            'donor_stats': {k: v.to_dict() for k, v in self._donor_stats.items()},
            'tier_achievements': {k: v.to_dict() for k, v in self._tier_achievements.items()},
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        wallet: Any,
        electrumx_client: Any = None,
        exchange_rate_provider: Optional[Callable] = None,
    ) -> "DonationManager":
        """Deserialize manager state from dictionary."""
        manager = cls(
            wallet=wallet,
            treasury_address=data['treasury_address'],
            electrumx_client=electrumx_client,
            exchange_rate_provider=exchange_rate_provider,
        )

        # Restore donations
        for k, v in data.get('donations', {}).items():
            manager._donations[k] = TreasuryDonation.from_dict(v)

        # Restore donor stats
        for k, v in data.get('donor_stats', {}).items():
            manager._donor_stats[k] = DonorStats.from_dict(v)

        # Restore tier achievements
        for k, v in data.get('tier_achievements', {}).items():
            manager._tier_achievements[k] = TierAchievement.from_dict(v)

        return manager


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_epoch_from_timestamp(timestamp: int, genesis: int = 0) -> int:
    """
    Calculate epoch number from timestamp.

    Epochs are 1 week (7 days) periods from genesis.

    Args:
        timestamp: Unix timestamp
        genesis: Network genesis timestamp (epoch 0 start)

    Returns:
        Epoch number
    """
    if timestamp < genesis:
        return 0

    seconds_since_genesis = timestamp - genesis
    return seconds_since_genesis // EPOCH_DURATION


def get_round_from_timestamp(timestamp: int, genesis: int = 0) -> int:
    """
    Calculate round number from timestamp.

    Rounds are 1 day (24 hours) periods from genesis.

    Args:
        timestamp: Unix timestamp
        genesis: Network genesis timestamp (round 0 start)

    Returns:
        Round number
    """
    if timestamp < genesis:
        return 0

    seconds_since_genesis = timestamp - genesis
    return seconds_since_genesis // ROUND_DURATION


def get_epoch_boundaries(epoch: int, genesis: int = 0) -> Tuple[int, int]:
    """
    Get start and end timestamps for an epoch.

    Args:
        epoch: Epoch number
        genesis: Network genesis timestamp

    Returns:
        (epoch_start, epoch_end) timestamps
    """
    epoch_start = genesis + (epoch * EPOCH_DURATION)
    epoch_end = epoch_start + EPOCH_DURATION
    return epoch_start, epoch_end


def get_donation_round_boundaries(round_num: int, genesis: int = 0) -> Tuple[int, int]:
    """
    Get start and end timestamps for a round.

    Args:
        round_num: Round number
        genesis: Network genesis timestamp

    Returns:
        (round_start, round_end) timestamps
    """
    round_start = genesis + (round_num * ROUND_DURATION)
    round_end = round_start + ROUND_DURATION
    return round_start, round_end


def get_tier_for_amount(amount: float) -> str:
    """Get tier for a cumulative donation amount."""
    tier = 'none'
    for tier_name in TIER_ORDER[1:]:
        if amount >= DONATION_TIERS.get(tier_name, float('inf')):
            tier = tier_name
    return tier


def get_tier_threshold(tier: str) -> float:
    """Get EVR threshold for a tier."""
    return DONATION_TIERS.get(tier, 0)


def get_next_tier(current_tier: str) -> Optional[str]:
    """Get the next tier after current."""
    try:
        current_idx = TIER_ORDER.index(current_tier)
        if current_idx < len(TIER_ORDER) - 1:
            return TIER_ORDER[current_idx + 1]
    except ValueError:
        pass
    return None


def get_progress_to_next_tier(total_donated: float) -> Tuple[str, float]:
    """
    Get progress towards next tier.

    Returns:
        (next_tier_name, progress_percentage)
    """
    current_tier = get_tier_for_amount(total_donated)
    next_tier = get_next_tier(current_tier)

    if next_tier is None:
        return ('max', 100.0)

    current_threshold = get_tier_threshold(current_tier) if current_tier != 'none' else 0
    next_threshold = get_tier_threshold(next_tier)

    progress = (total_donated - current_threshold) / (next_threshold - current_threshold)
    return (next_tier, min(progress * 100, 100.0))
