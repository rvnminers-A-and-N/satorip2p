"""
satorip2p/protocol/referral.py

Invite/Referral system for Satori P2P network.

Nodes can refer new users and earn bonus multipliers based on their
referral count. Similar to the donation tier system, referrers earn
achievements and increased reward bonuses.

All referral data is stored in DHT and broadcast via PubSub for
decentralized tracking without central server dependency.
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# Referral tier thresholds and bonuses
REFERRAL_TIERS = {
    'bronze': {'count': 5, 'bonus': 0.02},       # +2%
    'silver': {'count': 25, 'bonus': 0.05},      # +5%
    'gold': {'count': 100, 'bonus': 0.08},       # +8%
    'platinum': {'count': 500, 'bonus': 0.12},   # +12%
    'diamond': {'count': 2000, 'bonus': 0.15},   # +15%
}

# Tier order for progression
REFERRAL_TIER_ORDER = ['none', 'bronze', 'silver', 'gold', 'platinum', 'diamond']

# DHT key prefixes
DHT_REFERRAL_PREFIX = "satori:referral:"     # referral:{new_user} -> referrer
DHT_REFERRER_PREFIX = "satori:referrer:"     # referrer:{address} -> stats
DHT_REFERRAL_LIST_PREFIX = "satori:referrals:"    # referrals:{referrer} -> list

# PubSub topics
REFERRAL_TOPIC = "satori/referrals"
REFERRER_STATS_TOPIC = "satori/referrer/stats"

# Limits
MAX_REFERRALS_PER_QUERY = 100
REFERRAL_CACHE_TTL = 600  # 10 minutes


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Referral:
    """
    A referral record - new user referred by existing user.

    Both parties sign to confirm the referral relationship.
    """
    id: str                           # Unique referral ID
    new_user_address: str             # The new user being referred
    referrer_address: str             # The existing user who referred
    new_user_signature: str           # New user signs to confirm
    referrer_signature: str           # Referrer signs to confirm (optional)
    timestamp: int                    # When referral was created
    confirmed: bool = False           # Whether both parties confirmed
    rewarded: bool = False            # Whether referrer was credited

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Referral":
        """Create from dictionary."""
        return cls(**data)

    @staticmethod
    def generate_id(new_user: str, referrer: str) -> str:
        """Generate unique referral ID."""
        content = f"{new_user}:{referrer}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def get_message_to_sign(self, for_new_user: bool = True) -> str:
        """Get the message that should be signed."""
        role = "NEW_USER" if for_new_user else "REFERRER"
        return f"SATORI_REFERRAL:{role}:{self.new_user_address}:{self.referrer_address}:{self.timestamp}"


@dataclass
class ReferrerStats:
    """
    Statistics for a referrer.

    Tracks referral count, tier, and achievements.
    """
    address: str                      # Referrer's wallet address
    referral_count: int = 0           # Total confirmed referrals
    tier: str = 'none'                # Current tier
    bonus_multiplier: float = 1.0     # Current bonus (1.0 = no bonus)
    total_bonus_earned: float = 0.0   # Total SATORI earned from referral bonus
    achievements: List[str] = field(default_factory=list)
    first_referral_at: Optional[int] = None
    last_referral_at: Optional[int] = None
    updated_at: int = field(default_factory=lambda: int(time.time()))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReferrerStats":
        """Create from dictionary."""
        return cls(**data)

    def get_next_tier(self) -> Optional[str]:
        """Get the next tier to achieve."""
        current_idx = REFERRAL_TIER_ORDER.index(self.tier)
        if current_idx < len(REFERRAL_TIER_ORDER) - 1:
            return REFERRAL_TIER_ORDER[current_idx + 1]
        return None

    def get_progress_to_next_tier(self) -> Dict[str, Any]:
        """Get progress toward next tier."""
        next_tier = self.get_next_tier()
        if not next_tier or next_tier == 'none':
            return {'next_tier': None, 'current': self.referral_count, 'needed': 0, 'progress': 1.0}

        threshold = REFERRAL_TIERS[next_tier]['count']
        current_threshold = REFERRAL_TIERS.get(self.tier, {}).get('count', 0)
        progress_range = threshold - current_threshold
        current_progress = self.referral_count - current_threshold

        return {
            'next_tier': next_tier,
            'current': self.referral_count,
            'needed': threshold,
            'remaining': threshold - self.referral_count,
            'progress': min(1.0, current_progress / progress_range) if progress_range > 0 else 0,
        }


@dataclass
class ReferralTierAchievement:
    """Achievement unlocked when reaching a referral tier."""
    tier: str
    achieved_at: int
    referral_count: int
    bonus: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_tier_for_count(count: int) -> str:
    """Get tier name for a referral count."""
    tier = 'none'
    for tier_name in REFERRAL_TIER_ORDER[1:]:  # Skip 'none'
        if count >= REFERRAL_TIERS[tier_name]['count']:
            tier = tier_name
        else:
            break
    return tier


def get_bonus_for_tier(tier: str) -> float:
    """Get bonus multiplier for a tier."""
    if tier == 'none':
        return 0.0
    return REFERRAL_TIERS.get(tier, {}).get('bonus', 0.0)


def get_bonus_for_count(count: int) -> float:
    """Get bonus multiplier for a referral count."""
    tier = get_tier_for_count(count)
    return get_bonus_for_tier(tier)


def get_tier_threshold(tier: str) -> int:
    """Get the referral count threshold for a tier."""
    if tier == 'none':
        return 0
    return REFERRAL_TIERS.get(tier, {}).get('count', 0)


# ============================================================================
# REFERRAL MANAGER
# ============================================================================

class ReferralManager:
    """
    Manages the invite/referral system for P2P rewards.

    Features:
    - Register referrals (new user → referrer)
    - Track referral counts and tiers
    - Calculate bonus multipliers for reward distribution
    - Broadcast achievements via PubSub
    - Store all data in DHT

    Usage:
        manager = ReferralManager(peers=p2p_peers, wallet=my_wallet)

        # New user registers their referrer
        await manager.register_referral(referrer_address="Ereferrer123...")

        # Get referral bonus for reward calculation
        bonus = await manager.get_referral_bonus("Ereferrer123...")
        # Returns 0.05 for silver tier (5% bonus)
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        wallet: Any = None,
        signer: Optional[Callable] = None,
        verifier: Optional[Callable] = None,
    ):
        """
        Initialize ReferralManager.

        Args:
            peers: Peers instance for P2P operations
            wallet: Wallet for signing
            signer: Optional custom signing function
            verifier: Optional custom verification function
        """
        self._peers = peers
        self._wallet = wallet
        self._signer = signer
        self._verifier = verifier

        # Local caches
        self._referrals: Dict[str, Referral] = {}  # new_user -> Referral
        self._referrer_stats: Dict[str, ReferrerStats] = {}  # referrer -> stats
        self._my_referrals: Set[str] = set()  # Users I referred

        # My referrer (who referred me)
        self._my_referrer: Optional[str] = None

        # Callbacks
        self._tier_callbacks: List[Callable] = []
        self._referral_callbacks: List[Callable] = []

        self._started = False

    async def start(self) -> None:
        """Start the referral manager (no-op, for interface consistency)."""
        self._started = True
        logger.info("ReferralManager started")

    async def stop(self) -> None:
        """Stop the referral manager."""
        self._started = False
        logger.info("ReferralManager stopped")

    # ========================================================================
    # PUBLIC API - NEW USER
    # ========================================================================

    async def register_referral(
        self,
        referrer_address: str,
    ) -> Referral:
        """
        Register that we were referred by someone.

        Called by new users when they join via referral link.

        Args:
            referrer_address: Address of the user who referred us

        Returns:
            The created Referral record

        Raises:
            ValueError: If already registered or invalid referrer
        """
        if not self._wallet:
            raise ValueError("Wallet required")

        new_user_address = self._wallet.address

        # Check if already referred
        if self._my_referrer:
            raise ValueError("Already registered a referrer")

        # Can't refer yourself
        if referrer_address == new_user_address:
            raise ValueError("Cannot refer yourself")

        timestamp = int(time.time())
        referral_id = Referral.generate_id(new_user_address, referrer_address)

        # Create referral record
        referral = Referral(
            id=referral_id,
            new_user_address=new_user_address,
            referrer_address=referrer_address,
            new_user_signature="",
            referrer_signature="",
            timestamp=timestamp,
            confirmed=True,  # Confirmed when new user signs
        )

        # Sign as new user
        message = referral.get_message_to_sign(for_new_user=True)
        referral.new_user_signature = await self._sign_message(message)

        # Store in DHT
        await self._store_referral(referral)

        # Update referrer stats
        await self._increment_referrer_count(referrer_address)

        # Broadcast
        await self._broadcast_referral(referral)

        # Cache locally
        self._referrals[new_user_address] = referral
        self._my_referrer = referrer_address

        logger.info(
            f"Referral registered: {new_user_address[:12]}... referred by {referrer_address[:12]}..."
        )

        return referral

    async def get_my_referrer(self) -> Optional[str]:
        """Get the address of who referred us."""
        if self._my_referrer:
            return self._my_referrer

        if self._wallet:
            referral = await self.get_referral(self._wallet.address)
            if referral:
                self._my_referrer = referral.referrer_address
                return self._my_referrer

        return None

    # ========================================================================
    # PUBLIC API - REFERRER
    # ========================================================================

    async def get_my_stats(self) -> Optional[ReferrerStats]:
        """Get our referral statistics."""
        if not self._wallet:
            return None
        return await self.get_referrer_stats(self._wallet.address)

    async def get_my_referrals(self) -> List[Referral]:
        """Get list of users we referred."""
        if not self._wallet:
            return []
        return await self.get_referrals_by_referrer(self._wallet.address)

    async def get_referrer_stats(self, address: str) -> Optional[ReferrerStats]:
        """
        Get referral statistics for an address.

        Args:
            address: Referrer's wallet address

        Returns:
            ReferrerStats if found
        """
        # Check cache
        if address in self._referrer_stats:
            return self._referrer_stats[address]

        # Look up in DHT
        stats = await self._lookup_referrer_stats(address)

        if stats:
            self._referrer_stats[address] = stats

        return stats

    async def get_referral_bonus(self, address: str) -> float:
        """
        Get the referral bonus multiplier for an address.

        Used during reward calculation to add bonus for active referrers.

        Args:
            address: Referrer's wallet address

        Returns:
            Bonus multiplier (0.0 to 0.15)
        """
        stats = await self.get_referrer_stats(address)
        if stats:
            return get_bonus_for_tier(stats.tier)
        return 0.0

    async def get_referral_multiplier(self, address: str) -> float:
        """
        Get the total referral multiplier (1.0 + bonus).

        Args:
            address: Referrer's wallet address

        Returns:
            Multiplier (1.0 to 1.15)
        """
        bonus = await self.get_referral_bonus(address)
        return 1.0 + bonus

    # ========================================================================
    # PUBLIC API - LOOKUPS
    # ========================================================================

    async def get_referral(self, new_user_address: str) -> Optional[Referral]:
        """Get referral record for a new user."""
        if new_user_address in self._referrals:
            return self._referrals[new_user_address]

        return await self._lookup_referral(new_user_address)

    async def get_referrals_by_referrer(
        self,
        referrer_address: str,
        limit: int = MAX_REFERRALS_PER_QUERY
    ) -> List[Referral]:
        """Get all referrals made by a referrer."""
        # This would require a secondary index in DHT
        # For now, return from local cache
        referrals = []
        for referral in self._referrals.values():
            if referral.referrer_address == referrer_address:
                referrals.append(referral)
                if len(referrals) >= limit:
                    break
        return referrals

    async def get_top_referrers(self, limit: int = 10) -> List[ReferrerStats]:
        """Get top referrers by count."""
        # Sort cached stats by count
        sorted_stats = sorted(
            self._referrer_stats.values(),
            key=lambda s: s.referral_count,
            reverse=True
        )
        return sorted_stats[:limit]

    # ========================================================================
    # SUBSCRIPTION
    # ========================================================================

    async def subscribe_to_referrals(self) -> bool:
        """Subscribe to referral updates via PubSub."""
        if not self._peers or not self._peers._pubsub:
            return False

        try:
            await self._peers.subscribe(REFERRAL_TOPIC, self._handle_referral_message)
            await self._peers.subscribe(REFERRER_STATS_TOPIC, self._handle_stats_message)
            logger.debug("Subscribed to referral topics")
            return True
        except Exception as e:
            logger.warning(f"Failed to subscribe to referral topics: {e}")
            return False

    def on_tier_achieved(self, callback: Callable[[str, ReferralTierAchievement], None]):
        """Register callback for tier achievements."""
        self._tier_callbacks.append(callback)

    def on_new_referral(self, callback: Callable[[Referral], None]):
        """Register callback for new referrals."""
        self._referral_callbacks.append(callback)

    async def _handle_referral_message(self, message: Dict[str, Any]):
        """Handle incoming referral broadcast."""
        try:
            referral = Referral.from_dict(message.get('referral', {}))

            # Verify signature
            if not await self._verify_referral(referral):
                logger.warning(f"Invalid referral signature")
                return

            # Cache it
            self._referrals[referral.new_user_address] = referral

            # Notify callbacks
            for callback in self._referral_callbacks:
                try:
                    callback(referral)
                except Exception as e:
                    logger.warning(f"Referral callback error: {e}")

        except Exception as e:
            logger.warning(f"Failed to handle referral message: {e}")

    async def _handle_stats_message(self, message: Dict[str, Any]):
        """Handle incoming referrer stats update."""
        try:
            stats = ReferrerStats.from_dict(message.get('stats', {}))
            old_stats = self._referrer_stats.get(stats.address)

            # Cache it
            self._referrer_stats[stats.address] = stats

            # Check for tier achievement
            if old_stats and old_stats.tier != stats.tier:
                achievement = ReferralTierAchievement(
                    tier=stats.tier,
                    achieved_at=int(time.time()),
                    referral_count=stats.referral_count,
                    bonus=get_bonus_for_tier(stats.tier),
                )
                for callback in self._tier_callbacks:
                    try:
                        callback(stats.address, achievement)
                    except Exception as e:
                        logger.warning(f"Tier callback error: {e}")

        except Exception as e:
            logger.warning(f"Failed to handle stats message: {e}")

    # ========================================================================
    # DHT OPERATIONS
    # ========================================================================

    async def _store_referral(self, referral: Referral) -> bool:
        """Store referral in DHT."""
        if not self._peers:
            return False

        try:
            dht_key = f"{DHT_REFERRAL_PREFIX}{referral.new_user_address}"
            data = json.dumps(referral.to_dict()).encode()

            if hasattr(self._peers, '_dht') and self._peers._dht:
                await self._peers._dht.put(key=dht_key, value=data, ttl=0)
                return True
        except Exception as e:
            logger.warning(f"Failed to store referral: {e}")

        return False

    async def _lookup_referral(self, new_user_address: str) -> Optional[Referral]:
        """Look up referral from DHT."""
        if not self._peers:
            return None

        try:
            dht_key = f"{DHT_REFERRAL_PREFIX}{new_user_address}"

            if hasattr(self._peers, '_dht') and self._peers._dht:
                data = await self._peers._dht.get(dht_key)
                if data:
                    return Referral.from_dict(json.loads(data.decode()))
        except Exception as e:
            logger.debug(f"Referral lookup failed: {e}")

        return None

    async def _store_referrer_stats(self, stats: ReferrerStats) -> bool:
        """Store referrer stats in DHT."""
        if not self._peers:
            return False

        try:
            dht_key = f"{DHT_REFERRER_STATS_PREFIX}{stats.address}"
            data = json.dumps(stats.to_dict()).encode()

            if hasattr(self._peers, '_dht') and self._peers._dht:
                await self._peers._dht.put(key=dht_key, value=data, ttl=0)
                return True
        except Exception as e:
            logger.warning(f"Failed to store referrer stats: {e}")

        return False

    async def _lookup_referrer_stats(self, address: str) -> Optional[ReferrerStats]:
        """Look up referrer stats from DHT."""
        if not self._peers:
            return None

        try:
            dht_key = f"{DHT_REFERRER_STATS_PREFIX}{address}"

            if hasattr(self._peers, '_dht') and self._peers._dht:
                data = await self._peers._dht.get(dht_key)
                if data:
                    return ReferrerStats.from_dict(json.loads(data.decode()))
        except Exception as e:
            logger.debug(f"Referrer stats lookup failed: {e}")

        return None

    async def _increment_referrer_count(self, referrer_address: str) -> ReferrerStats:
        """Increment referral count and update tier."""
        # Get or create stats
        stats = await self.get_referrer_stats(referrer_address)
        if not stats:
            stats = ReferrerStats(address=referrer_address)

        old_tier = stats.tier

        # Update count
        stats.referral_count += 1
        stats.last_referral_at = int(time.time())
        if not stats.first_referral_at:
            stats.first_referral_at = stats.last_referral_at
        stats.updated_at = int(time.time())

        # Check for tier upgrade
        new_tier = get_tier_for_count(stats.referral_count)
        if new_tier != old_tier:
            stats.tier = new_tier
            stats.bonus_multiplier = 1.0 + get_bonus_for_tier(new_tier)

            # Record achievement
            achievement = f"{new_tier}_tier_achieved"
            if achievement not in stats.achievements:
                stats.achievements.append(achievement)

            logger.info(
                f"Referrer {referrer_address[:12]}... achieved {new_tier} tier "
                f"({stats.referral_count} referrals, +{get_bonus_for_tier(new_tier)*100:.0f}% bonus)"
            )

        # Store updated stats
        await self._store_referrer_stats(stats)

        # Broadcast update
        await self._broadcast_stats(stats)

        # Cache
        self._referrer_stats[referrer_address] = stats

        return stats

    async def _broadcast_referral(self, referral: Referral) -> bool:
        """Broadcast new referral via PubSub."""
        if not self._peers or not self._peers._pubsub:
            return False

        try:
            message = {
                'type': 'new_referral',
                'referral': referral.to_dict(),
                'timestamp': int(time.time()),
            }
            await self._peers.broadcast(REFERRAL_TOPIC, message)
            return True
        except Exception as e:
            logger.warning(f"Failed to broadcast referral: {e}")
            return False

    async def _broadcast_stats(self, stats: ReferrerStats) -> bool:
        """Broadcast referrer stats update via PubSub."""
        if not self._peers or not self._peers._pubsub:
            return False

        try:
            message = {
                'type': 'referrer_stats_update',
                'stats': stats.to_dict(),
                'timestamp': int(time.time()),
            }
            await self._peers.broadcast(REFERRER_STATS_TOPIC, message)
            return True
        except Exception as e:
            logger.warning(f"Failed to broadcast stats: {e}")
            return False

    # ========================================================================
    # SIGNING / VERIFICATION
    # ========================================================================

    async def _sign_message(self, message: str) -> str:
        """Sign a message with the wallet."""
        if self._signer:
            return await self._signer(message)

        if self._wallet and hasattr(self._wallet, 'sign'):
            result = self._wallet.sign(message)
            if asyncio.iscoroutine(result):
                return await result
            return result

        raise ValueError("No signing method available")

    async def _verify_referral(self, referral: Referral) -> bool:
        """Verify a referral's signature."""
        if self._verifier:
            message = referral.get_message_to_sign(for_new_user=True)
            return await self._verifier(
                message,
                referral.new_user_signature,
                referral.new_user_address
            )

        # Default: trust if signature present
        return bool(referral.new_user_signature)

    # ========================================================================
    # SERIALIZATION
    # ========================================================================

    def to_dict(self) -> Dict[str, Any]:
        """Serialize manager state."""
        return {
            'referrals': {k: v.to_dict() for k, v in self._referrals.items()},
            'referrer_stats': {k: v.to_dict() for k, v in self._referrer_stats.items()},
            'my_referrer': self._my_referrer,
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        peers: Optional["Peers"] = None,
        wallet: Any = None,
    ) -> "ReferralManager":
        """Deserialize manager state."""
        manager = cls(peers=peers, wallet=wallet)

        for k, v in data.get('referrals', {}).items():
            manager._referrals[k] = Referral.from_dict(v)

        for k, v in data.get('referrer_stats', {}).items():
            manager._referrer_stats[k] = ReferrerStats.from_dict(v)

        manager._my_referrer = data.get('my_referrer')

        return manager
