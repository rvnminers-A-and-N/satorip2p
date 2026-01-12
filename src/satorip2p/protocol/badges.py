"""
satorip2p/protocol/badges.py

Badge and Achievement System for Satori Network.

Implements on-chain badge issuance using Evrmore unique assets:
- Rank badges (1st-10th place per round/epoch/era/season/cycle)
- Achievement badges (PERFECT, STREAK, etc.)
- Donation tier badges (BRONZE, SILVER, GOLD, etc.)
- Role badges (ORACLE, ARCHIVER, CURATOR, etc.)
- Community badges (MENTOR, RELIABLE, etc.)

Time Hierarchy:
    Round   = 1 day (prediction cycle)
    Epoch   = 7 days / 1 week (aggregation period)
    Era     = 28 days / 4 epochs (lunar-ish cycle, 13 per year)
    Season  = 91 days / 13 epochs (quarterly, 4 per year)
    Cycle   = 364 days / 52 epochs (yearly, 1 per year)

Badge Format:
    SATORI#{timeunit}{id}_{type}_{rank/name}
    Examples:
    - SATORI#R365_1st      (Round 365, 1st place)
    - SATORI#E42_1st       (Epoch 42, 1st place)
    - SATORI#ERA5_CHAMP    (Era 5, Champion)
    - SATORI#S2_CHAMP      (Season 2, Champion)
    - SATORI#C1_LEGEND     (Cycle 1, Legend)
    - SATORI#STREAK_EPOCH  (1 epoch streak)
    - SATORI#STREAK_ERA    (1 era streak)
    - SATORI#GOLD_DONOR    (One-time, not time-specific)

Note: Actual minting requires team coordination and wallet integration.
This module provides the logic and tracking; minting is stubbed.
"""

import time
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any, Set
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# TIME HIERARCHY CONSTANTS
# =============================================================================

# Time unit durations (in seconds)
ROUND_DURATION = 86400              # 1 day
EPOCH_DURATION = ROUND_DURATION * 7  # 7 days
ERA_DURATION = EPOCH_DURATION * 4    # 28 days (4 epochs)
SEASON_DURATION = EPOCH_DURATION * 13  # 91 days (13 epochs)
CYCLE_DURATION = EPOCH_DURATION * 52   # 364 days (52 epochs)

# Time units per parent unit
ROUNDS_PER_EPOCH = 7
EPOCHS_PER_ERA = 4
EPOCHS_PER_SEASON = 13
EPOCHS_PER_CYCLE = 52
ERAS_PER_CYCLE = 13
SEASONS_PER_CYCLE = 4


class TimeUnit(Enum):
    """Time units for badge periodicity."""
    ROUND = "round"      # 1 day
    EPOCH = "epoch"      # 7 days
    ERA = "era"          # 28 days (lunar-ish)
    SEASON = "season"    # 91 days (quarterly)
    CYCLE = "cycle"      # 364 days (yearly)


# Badge prefixes by time unit
TIME_UNIT_PREFIXES = {
    TimeUnit.ROUND: "R",
    TimeUnit.EPOCH: "E",
    TimeUnit.ERA: "ERA",
    TimeUnit.SEASON: "S",
    TimeUnit.CYCLE: "C",
}

# =============================================================================
# BADGE CONSTANTS
# =============================================================================

# Badge type prefixes for asset naming
BADGE_PREFIX = "SATORI#"

# =============================================================================
# BADGE MINTING COSTS (Evrmore blockchain fees)
# =============================================================================
# Evrmore asset creation costs (in EVR):
# - Unique asset: 5 EVR (badges are unique assets)
# - Sub-asset: 100 EVR (not used for badges)
# - Reissuable asset: 500 EVR (not used for badges)
#
# =============================================================================
# ANNUAL BADGE BUDGET BREAKDOWN (Comprehensive)
# =============================================================================
#
# RANK BADGES (worst case: all unique winners)
# -----------------------------------------
# - Daily (Round):    365 days × 10 badges × 5 EVR = 18,250 EVR
# - Weekly (Epoch):   52 weeks × 10 badges × 5 EVR =  2,600 EVR
# - Monthly (Era):    13 eras × 10 badges × 5 EVR  =    650 EVR
# - Quarterly (Season): 4 × 10 badges × 5 EVR      =    200 EVR
# - Yearly (Cycle):   1 × 10 badges × 5 EVR        =     50 EVR
# Subtotal Rank:                                    ~21,750 EVR/year
#
# STREAK BADGES (estimate: 50% of active nodes earn streak badges)
# ----------------------------------------------------------------
# Assuming 500 active nodes, 250 earn various streak badges:
# - Epoch streaks:    250 nodes × 2 badges avg × 5 EVR = 2,500 EVR
# - Era streaks:      100 nodes × 1 badge × 5 EVR      =   500 EVR
# - Season streaks:   50 nodes × 1 badge × 5 EVR       =   250 EVR
# - Cycle streaks:    25 nodes × 1 badge × 5 EVR       =   125 EVR
# Subtotal Streak:                                      ~3,375 EVR/year
#
# ACHIEVEMENT BADGES (estimate based on activity)
# -----------------------------------------------
# - First Blood:      500 new nodes × 5 EVR            = 2,500 EVR
# - Perfect Round:    50 × 5 EVR                       =   250 EVR
# - Perfect Epoch:    10 × 5 EVR                       =    50 EVR
# - Voter badges:     100 × 5 EVR                      =   500 EVR
# - Oracle/Archive:   50 × 5 EVR                       =   250 EVR
# - Comebacks etc:    50 × 5 EVR                       =   250 EVR
# Subtotal Achievement:                                 ~3,800 EVR/year
#
# COMMUNITY BADGES (estimate)
# ---------------------------
# - Mentor badges:    100 × 5 EVR                      =   500 EVR
# - Reliability:      75 × 5 EVR                       =   375 EVR
# - Data Provider:    100 × 5 EVR                      =   500 EVR
# - Connector:        150 × 5 EVR                      =   750 EVR
# Subtotal Community:                                   ~2,125 EVR/year
#
# DONATION BADGES (per tier reached)
# ----------------------------------
# - Bronze donors:    200 × 5 EVR                      = 1,000 EVR
# - Silver donors:    100 × 5 EVR                      =   500 EVR
# - Gold donors:      50 × 5 EVR                       =   250 EVR
# - Platinum donors:  20 × 5 EVR                       =   100 EVR
# - Diamond donors:   5 × 5 EVR                        =    25 EVR
# Subtotal Donation:                                    ~1,875 EVR/year
#
# ROLE BADGES
# -----------
# - Oracle/Archiver/Curator/Validator/Signer roles
# - Est 200 role badges × 5 EVR                        = 1,000 EVR/year
#
# SPECIAL BADGES (one-time, limited)
# ----------------------------------
# - Genesis, Early Adopter, Alpha/Beta Tester, etc.
# - First year only, ~100 badges × 5 EVR               =   500 EVR
#
# =============================================================================
# GRAND TOTAL ESTIMATE
# =============================================================================
# Rank:        21,750 EVR
# Streak:       3,375 EVR
# Achievement:  3,800 EVR
# Community:    2,125 EVR
# Donation:     1,875 EVR
# Role:         1,000 EVR
# Special:        500 EVR
# -----------------------
# TOTAL:      ~34,425 EVR/year
#
# Recommended reserve with 50% buffer: ~52,000 EVR/year
# At $0.002/EVR = ~$104/year for badge minting
# =============================================================================

BADGE_MINT_COST_EVR = 5.0                  # Cost to mint one unique badge asset
BADGE_MINT_FEE_BUFFER_EVR = 0.1            # Transaction fee buffer

# Badge emission limits per time period (prevents runaway costs)
MAX_RANK_BADGES_PER_ROUND = 10             # Top 10 per round
MAX_RANK_BADGES_PER_EPOCH = 10             # Top 10 per epoch
MAX_RANK_BADGES_PER_ERA = 10               # Top 10 per era
MAX_RANK_BADGES_PER_SEASON = 10            # Top 10 per season
MAX_RANK_BADGES_PER_CYCLE = 10             # Top 10 per cycle

MAX_STREAK_BADGES_PER_EPOCH = 100          # Cap streak badges per epoch
MAX_ACHIEVEMENT_BADGES_PER_EPOCH = 50      # Cap achievements per epoch
MAX_COMMUNITY_BADGES_PER_ERA = 50          # Cap community badges per era

# Annual badge budget estimates (in EVR)
ANNUAL_RANK_BADGE_BUDGET_EVR = 22000       # Rank badges (worst case)
ANNUAL_STREAK_BADGE_BUDGET_EVR = 3500      # Streak badges
ANNUAL_ACHIEVEMENT_BADGE_BUDGET_EVR = 4000 # Achievement badges
ANNUAL_COMMUNITY_BADGE_BUDGET_EVR = 2500   # Community badges
ANNUAL_DONATION_BADGE_BUDGET_EVR = 2000    # Donation tier badges
ANNUAL_ROLE_BADGE_BUDGET_EVR = 1000        # Role badges
ANNUAL_SPECIAL_BADGE_BUDGET_EVR = 500      # Special/one-time badges

# Total recommended annual budget
ANNUAL_BADGE_SUBTOTAL_EVR = 35500          # Sum of above categories
ANNUAL_BADGE_BUFFER_EVR = 16500            # ~50% buffer for growth
ANNUAL_TOTAL_BADGE_BUDGET_EVR = 52000      # Recommended treasury reserve

# Rank badge positions (1st through 10th)
RANK_POSITIONS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
RANK_LABELS = {
    1: "1st",
    2: "2nd",
    3: "3rd",
    4: "4th",
    5: "5th",
    6: "6th",
    7: "7th",
    8: "8th",
    9: "9th",
    10: "10th",
}


class BadgeCategory(Enum):
    """Categories of badges."""
    RANK = "rank"           # Placement in round/epoch/era/season/cycle
    ACHIEVEMENT = "achievement"  # Special accomplishments
    STREAK = "streak"       # Consecutive participation
    DONATION = "donation"   # Donation tier milestones
    ROLE = "role"          # Node role badges
    COMMUNITY = "community" # Community contribution badges
    SPECIAL = "special"     # One-time unique achievements


class BadgeRarity(Enum):
    """Badge rarity levels."""
    COMMON = "common"       # Easily obtainable
    UNCOMMON = "uncommon"   # Some effort required
    RARE = "rare"          # Significant achievement
    EPIC = "epic"          # Difficult to obtain
    LEGENDARY = "legendary" # Extremely rare
    MYTHIC = "mythic"      # Exceptionally rare (cycle-level)


# =============================================================================
# STREAK BADGES (based on time hierarchy)
# =============================================================================
STREAK_BADGES = {
    # Epoch-based streaks (weekly)
    "STREAK_EPOCH": {
        "name": "Epoch Dedication",
        "description": "Participated every round for 1 full epoch (7 days)",
        "category": BadgeCategory.STREAK,
        "rarity": BadgeRarity.COMMON,
        "time_unit": TimeUnit.EPOCH,
        "count": 1,
    },
    "STREAK_EPOCH_4": {
        "name": "Era Dedication",
        "description": "Participated every round for 4 epochs (1 era / 28 days)",
        "category": BadgeCategory.STREAK,
        "rarity": BadgeRarity.UNCOMMON,
        "time_unit": TimeUnit.ERA,
        "count": 1,
    },
    "STREAK_EPOCH_13": {
        "name": "Season Dedication",
        "description": "Participated every round for 13 epochs (1 season / 91 days)",
        "category": BadgeCategory.STREAK,
        "rarity": BadgeRarity.RARE,
        "time_unit": TimeUnit.SEASON,
        "count": 1,
    },
    "STREAK_EPOCH_52": {
        "name": "Cycle Dedication",
        "description": "Participated every round for 52 epochs (1 cycle / 364 days)",
        "category": BadgeCategory.STREAK,
        "rarity": BadgeRarity.LEGENDARY,
        "time_unit": TimeUnit.CYCLE,
        "count": 1,
    },
}

# =============================================================================
# RANK BADGES (by time unit)
# =============================================================================
# Rank badges are generated dynamically based on time unit and position
# Format: {TimeUnit}_{position} -> SATORI#E42_1st, SATORI#ERA5_1st, etc.

RANK_BADGE_RARITY = {
    # Daily (round) ranks - common (365 sets per year)
    TimeUnit.ROUND: {1: BadgeRarity.COMMON, 2: BadgeRarity.COMMON, 3: BadgeRarity.COMMON},
    # Weekly (epoch) ranks - uncommon (52 sets per year)
    TimeUnit.EPOCH: {1: BadgeRarity.UNCOMMON, 2: BadgeRarity.UNCOMMON, 3: BadgeRarity.UNCOMMON},
    # Monthly (era) ranks - rare (13 sets per year)
    TimeUnit.ERA: {1: BadgeRarity.RARE, 2: BadgeRarity.RARE, 3: BadgeRarity.RARE},
    # Quarterly (season) ranks - epic (4 sets per year)
    TimeUnit.SEASON: {1: BadgeRarity.EPIC, 2: BadgeRarity.EPIC, 3: BadgeRarity.EPIC},
    # Yearly (cycle) ranks - legendary/mythic (1 set per year)
    TimeUnit.CYCLE: {1: BadgeRarity.MYTHIC, 2: BadgeRarity.LEGENDARY, 3: BadgeRarity.LEGENDARY},
}

# Default rarity for ranks 4-10
DEFAULT_RANK_RARITY = {
    TimeUnit.ROUND: BadgeRarity.COMMON,
    TimeUnit.EPOCH: BadgeRarity.COMMON,
    TimeUnit.ERA: BadgeRarity.UNCOMMON,
    TimeUnit.SEASON: BadgeRarity.RARE,
    TimeUnit.CYCLE: BadgeRarity.EPIC,
}

# =============================================================================
# ACHIEVEMENT BADGES
# =============================================================================
ACHIEVEMENTS = {
    # Prediction achievements
    "PERFECT_ROUND": {
        "name": "Perfect Round",
        "description": "Achieved a perfect prediction score in a round",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.RARE,
    },
    "PERFECT_EPOCH": {
        "name": "Perfect Epoch",
        "description": "Achieved perfect predictions for an entire epoch",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.EPIC,
    },
    "PERFECT_ERA": {
        "name": "Perfect Era",
        "description": "Achieved perfect predictions for an entire era",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.LEGENDARY,
    },
    "FIRST_BLOOD": {
        "name": "First Blood",
        "description": "Made your very first prediction on the network",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.COMMON,
    },
    "COMEBACK_KING": {
        "name": "Comeback King",
        "description": "Rose from bottom 10% to top 10% within a single era",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.RARE,
    },
    "CONSISTENCY_CROWN": {
        "name": "Consistency Crown",
        "description": "Ranked in top 50% every epoch for an entire season",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.EPIC,
    },
    # Governance achievements
    "VOTER100": {
        "name": "Engaged Citizen",
        "description": "Cast 100 governance votes",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.UNCOMMON,
    },
    "VOTER500": {
        "name": "Civic Champion",
        "description": "Cast 500 governance votes",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.RARE,
    },
    "PROPOSER": {
        "name": "Proposal Pioneer",
        "description": "Successfully created a governance proposal",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.RARE,
    },
    "PASSED_PROPOSAL": {
        "name": "Democracy Driver",
        "description": "Had a governance proposal pass",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.EPIC,
    },
    "MULTI_PROPOSER": {
        "name": "Legislative Leader",
        "description": "Had 5 governance proposals pass",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.LEGENDARY,
    },
    # Network contribution achievements
    "FIRST_ORACLE": {
        "name": "Oracle Pioneer",
        "description": "First node to publish an observation for a stream",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.RARE,
    },
    "STREAM_CREATOR": {
        "name": "Stream Creator",
        "description": "Created a new data stream on the network",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.RARE,
    },
    "ARCHIVE_1000": {
        "name": "Archive Guardian",
        "description": "Archived 1000 rounds of historical data",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.RARE,
    },
    "ARCHIVE_10000": {
        "name": "Archive Master",
        "description": "Archived 10,000 rounds of historical data",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.EPIC,
    },
    "BUG_HUNTER": {
        "name": "Bug Hunter",
        "description": "Reported a valid bug that was fixed",
        "category": BadgeCategory.ACHIEVEMENT,
        "rarity": BadgeRarity.RARE,
    },
}

# =============================================================================
# COMMUNITY BADGES
# =============================================================================
COMMUNITY_BADGES = {
    "MENTOR": {
        "name": "Mentor",
        "description": "Successfully helped 5 new nodes bootstrap",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.UNCOMMON,
        "threshold": 5,
    },
    "MENTOR_MASTER": {
        "name": "Master Mentor",
        "description": "Successfully helped 25 new nodes bootstrap",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.RARE,
        "threshold": 25,
    },
    "MENTOR_LEGEND": {
        "name": "Legendary Mentor",
        "description": "Successfully helped 100 new nodes bootstrap",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.EPIC,
        "threshold": 100,
    },
    "RELIABLE": {
        "name": "Reliable Node",
        "description": "Maintained 99.9% uptime for an entire era",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.RARE,
        "uptime_threshold": 0.999,
    },
    "IRON_NODE": {
        "name": "Iron Node",
        "description": "Maintained 99.9% uptime for an entire season",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.EPIC,
        "uptime_threshold": 0.999,
    },
    "TITANIUM_NODE": {
        "name": "Titanium Node",
        "description": "Maintained 99.9% uptime for an entire cycle (1 year)",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.LEGENDARY,
        "uptime_threshold": 0.999,
    },
    "DATA_PROVIDER": {
        "name": "Data Provider",
        "description": "Served 1000 historical data requests to peers",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.UNCOMMON,
        "threshold": 1000,
    },
    "DATA_MASTER": {
        "name": "Data Master",
        "description": "Served 10,000 historical data requests to peers",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.RARE,
        "threshold": 10000,
    },
    "NETWORK_GUARDIAN": {
        "name": "Network Guardian",
        "description": "Contributed to network health monitoring for an era",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.RARE,
    },
    "CONNECTOR": {
        "name": "Connector",
        "description": "Maintained 10+ stable peer connections for an era",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.UNCOMMON,
        "threshold": 10,
    },
    "SUPER_CONNECTOR": {
        "name": "Super Connector",
        "description": "Maintained 50+ stable peer connections for an era",
        "category": BadgeCategory.COMMUNITY,
        "rarity": BadgeRarity.RARE,
        "threshold": 50,
    },
}

# =============================================================================
# SPECIAL BADGES (one-time, unique achievements)
# =============================================================================
SPECIAL_BADGES = {
    "GENESIS": {
        "name": "Genesis",
        "description": "Participated in the genesis round of the network",
        "category": BadgeCategory.SPECIAL,
        "rarity": BadgeRarity.MYTHIC,
    },
    "FOUNDING_MEMBER": {
        "name": "Founding Member",
        "description": "Ranked in the top 100 during the first cycle",
        "category": BadgeCategory.SPECIAL,
        "rarity": BadgeRarity.LEGENDARY,
    },
    "EARLY_ADOPTER": {
        "name": "Early Adopter",
        "description": "Joined the network during the first cycle",
        "category": BadgeCategory.SPECIAL,
        "rarity": BadgeRarity.EPIC,
    },
    "ALPHA_TESTER": {
        "name": "Alpha Tester",
        "description": "Participated in alpha testing phase",
        "category": BadgeCategory.SPECIAL,
        "rarity": BadgeRarity.LEGENDARY,
    },
    "BETA_TESTER": {
        "name": "Beta Tester",
        "description": "Participated in beta testing phase",
        "category": BadgeCategory.SPECIAL,
        "rarity": BadgeRarity.EPIC,
    },
    "FIRST_DONOR": {
        "name": "First Donor",
        "description": "First person to donate at each tier",
        "category": BadgeCategory.SPECIAL,
        "rarity": BadgeRarity.LEGENDARY,
    },
    "CYCLE_CHAMPION": {
        "name": "Cycle Champion",
        "description": "Ranked #1 for an entire cycle (year)",
        "category": BadgeCategory.SPECIAL,
        "rarity": BadgeRarity.MYTHIC,
    },
}

# =============================================================================
# DONATION TIER BADGES
# =============================================================================
# Donation tiers are UNIFIED across the system:
# - Same thresholds for badge awards AND reward multiplier bonuses
# - Reaching a tier earns BOTH the badge AND the ongoing bonus
# - These match exactly with DONATION_TIER_* constants in rewards.py
#
# | Tier     | EVR     | Badge | Bonus | ~USD Value |
# |----------|---------|-------|-------|------------|
# | Bronze   | 500     | ✓     | +4%   | ~$1        |
# | Silver   | 2,500   | ✓     | +8%   | ~$5        |
# | Gold     | 10,000  | ✓     | +12%  | ~$20       |
# | Platinum | 50,000  | ✓     | +16%  | ~$100      |
# | Diamond  | 250,000 | ✓     | +20%  | ~$500      |

DONATION_TIERS = {
    "BRONZE": {"min_evr": 500, "bonus": 0.04, "rarity": BadgeRarity.COMMON},
    "SILVER": {"min_evr": 2500, "bonus": 0.08, "rarity": BadgeRarity.UNCOMMON},
    "GOLD": {"min_evr": 10000, "bonus": 0.12, "rarity": BadgeRarity.RARE},
    "PLATINUM": {"min_evr": 50000, "bonus": 0.16, "rarity": BadgeRarity.EPIC},
    "DIAMOND": {"min_evr": 250000, "bonus": 0.20, "rarity": BadgeRarity.LEGENDARY},
}

# Role badges (earned by fulfilling role requirements)
ROLE_BADGES = {
    "ORACLE": {"description": "Active oracle node publishing observations", "rarity": BadgeRarity.UNCOMMON},
    "ARCHIVER": {"description": "Active archiver storing historical data", "rarity": BadgeRarity.UNCOMMON},
    "CURATOR": {"description": "Active curator managing stream quality", "rarity": BadgeRarity.RARE},
    "VALIDATOR": {"description": "Active validator signing transactions", "rarity": BadgeRarity.RARE},
    "SIGNER": {"description": "Authorized multi-sig signer", "rarity": BadgeRarity.EPIC},
}


# =============================================================================
# TIME HELPER FUNCTIONS
# =============================================================================

def get_current_round(genesis_timestamp: int = 0) -> int:
    """Get current round number since genesis."""
    return int((time.time() - genesis_timestamp) // ROUND_DURATION)


def get_current_epoch(genesis_timestamp: int = 0) -> int:
    """Get current epoch number since genesis."""
    return int((time.time() - genesis_timestamp) // EPOCH_DURATION)


def get_current_era(genesis_timestamp: int = 0) -> int:
    """Get current era number since genesis."""
    return int((time.time() - genesis_timestamp) // ERA_DURATION)


def get_current_season(genesis_timestamp: int = 0) -> int:
    """Get current season number since genesis."""
    return int((time.time() - genesis_timestamp) // SEASON_DURATION)


def get_current_cycle(genesis_timestamp: int = 0) -> int:
    """Get current cycle number since genesis."""
    return int((time.time() - genesis_timestamp) // CYCLE_DURATION)


def get_time_unit_id(time_unit: TimeUnit, genesis_timestamp: int = 0) -> int:
    """Get current ID for a given time unit."""
    if time_unit == TimeUnit.ROUND:
        return get_current_round(genesis_timestamp)
    elif time_unit == TimeUnit.EPOCH:
        return get_current_epoch(genesis_timestamp)
    elif time_unit == TimeUnit.ERA:
        return get_current_era(genesis_timestamp)
    elif time_unit == TimeUnit.SEASON:
        return get_current_season(genesis_timestamp)
    elif time_unit == TimeUnit.CYCLE:
        return get_current_cycle(genesis_timestamp)
    return 0


def get_rank_badge_rarity(time_unit: TimeUnit, rank: int) -> BadgeRarity:
    """Get badge rarity based on time unit and rank position."""
    unit_rarities = RANK_BADGE_RARITY.get(time_unit, {})
    if rank in unit_rarities:
        return unit_rarities[rank]
    return DEFAULT_RANK_RARITY.get(time_unit, BadgeRarity.COMMON)


def format_badge_id(time_unit: TimeUnit, unit_id: int, suffix: str) -> str:
    """Format a badge ID with time unit prefix."""
    prefix = TIME_UNIT_PREFIXES.get(time_unit, "")
    return f"{prefix}{unit_id}_{suffix}"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class Badge:
    """A badge that can be earned."""
    badge_id: str                # Unique identifier (e.g., "E42_1st", "ERA5_CHAMP")
    name: str                    # Human-readable name
    description: str             # Description of how to earn
    category: str                # BadgeCategory value
    rarity: str                  # BadgeRarity value
    asset_name: Optional[str] = None   # Full Evrmore asset name
    time_unit: Optional[str] = None    # TimeUnit value (round/epoch/era/season/cycle)
    time_unit_id: Optional[int] = None # ID within the time unit
    round_id: Optional[int] = None     # Round badge was earned (legacy)
    epoch_id: Optional[int] = None     # Epoch badge was earned (legacy)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class EarnedBadge:
    """A badge earned by a specific address."""
    badge_id: str                # Badge identifier
    address: str                 # Recipient address
    earned_at: int               # Timestamp when earned
    time_unit: Optional[str] = None    # TimeUnit value
    time_unit_id: Optional[int] = None # ID within the time unit
    epoch_id: Optional[int] = None     # Legacy epoch ID
    round_id: Optional[int] = None     # Legacy round ID
    asset_name: Optional[str] = None   # Full Evrmore asset name
    tx_hash: Optional[str] = None      # Transaction that minted the badge
    minted: bool = False               # Whether on-chain asset was minted

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LeaderboardEntry:
    """Entry in leaderboard for rank badges."""
    address: str
    rank: int
    score: float
    time_unit: str               # TimeUnit value
    time_unit_id: int            # ID within the time unit
    epoch_id: Optional[int] = None  # Legacy
    round_id: Optional[int] = None  # Legacy

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# BADGE MANAGER
# =============================================================================

class BadgeManager:
    """
    Manages badge issuance and tracking for the Satori network.

    Provides:
    - Badge definition and discovery
    - Earning condition checking
    - Leaderboard tracking for rank badges
    - Integration hooks for on-chain minting (stubbed)

    Usage:
        manager = BadgeManager()

        # Award a rank badge
        badge = await manager.award_rank_badge(
            address="EAddr...",
            rank=1,
            epoch_id=42,
            round_id=1,
        )

        # Award an achievement badge
        badge = await manager.award_achievement_badge(
            address="EAddr...",
            achievement_id="PERFECT",
            epoch_id=42,
        )

        # Get all badges for an address
        badges = manager.get_badges_for_address("EAddr...")

    Note: Actual on-chain minting requires wallet integration.
    The mint_badge_asset() method is a stub that logs intent.
    """

    def __init__(self, wallet=None):
        """
        Initialize badge manager.

        Args:
            wallet: Optional wallet instance for minting (stubbed)
        """
        self._wallet = wallet

        # In-memory badge storage
        self._earned_badges: Dict[str, List[EarnedBadge]] = {}  # address -> badges
        self._leaderboards: Dict[int, List[LeaderboardEntry]] = {}  # epoch -> entries
        self._minted_assets: Set[str] = set()  # Track minted asset names

    def set_wallet(self, wallet) -> None:
        """Set wallet for badge minting."""
        self._wallet = wallet

    # =========================================================================
    # BADGE DEFINITIONS
    # =========================================================================

    def get_badge_definition(self, badge_id: str) -> Optional[Badge]:
        """Get badge definition by ID."""
        # Check achievements
        if badge_id in ACHIEVEMENTS:
            info = ACHIEVEMENTS[badge_id]
            return Badge(
                badge_id=badge_id,
                name=info["name"],
                description=info["description"],
                category=info["category"].value,
                rarity=info["rarity"].value,
            )

        # Check donation tiers
        for tier, info in DONATION_TIERS.items():
            if badge_id == f"DONOR_{tier}":
                return Badge(
                    badge_id=badge_id,
                    name=f"{tier.title()} Donor",
                    description=f"Donated {info['min_evr']}+ EVR to treasury",
                    category=BadgeCategory.DONATION.value,
                    rarity=info["rarity"].value,
                )

        # Check role badges
        if badge_id in ROLE_BADGES:
            return Badge(
                badge_id=badge_id,
                name=f"{badge_id.title()} Node",
                description=ROLE_BADGES[badge_id]["description"],
                category=BadgeCategory.ROLE.value,
                rarity=BadgeRarity.UNCOMMON.value,
            )

        return None

    def list_all_badges(self) -> List[Badge]:
        """List all available badge definitions."""
        badges = []

        # Add achievements
        for badge_id, info in ACHIEVEMENTS.items():
            badges.append(Badge(
                badge_id=badge_id,
                name=info["name"],
                description=info["description"],
                category=info["category"].value,
                rarity=info["rarity"].value,
            ))

        # Add streak badges
        for badge_id, info in STREAK_BADGES.items():
            badges.append(Badge(
                badge_id=badge_id,
                name=info["name"],
                description=info["description"],
                category=info["category"].value,
                rarity=info["rarity"].value,
                time_unit=info["time_unit"].value,
            ))

        # Add community badges
        for badge_id, info in COMMUNITY_BADGES.items():
            badges.append(Badge(
                badge_id=badge_id,
                name=info["name"],
                description=info["description"],
                category=info["category"].value,
                rarity=info["rarity"].value,
            ))

        # Add donation tiers
        for tier, info in DONATION_TIERS.items():
            badges.append(Badge(
                badge_id=f"DONOR_{tier}",
                name=f"{tier.title()} Donor",
                description=f"Donated {info['min_evr']}+ EVR to treasury",
                category=BadgeCategory.DONATION.value,
                rarity=info["rarity"].value,
            ))

        # Add role badges
        for role, info in ROLE_BADGES.items():
            badges.append(Badge(
                badge_id=role,
                name=f"{role.title()} Node",
                description=info["description"],
                category=BadgeCategory.ROLE.value,
                rarity=BadgeRarity.UNCOMMON.value,
            ))

        return badges

    def get_badge_count_by_category(self) -> Dict[str, int]:
        """Get count of badge types by category."""
        counts = {
            BadgeCategory.RANK.value: 50,  # 10 ranks × 5 time units
            BadgeCategory.STREAK.value: len(STREAK_BADGES),
            BadgeCategory.ACHIEVEMENT.value: len(ACHIEVEMENTS),
            BadgeCategory.COMMUNITY.value: len(COMMUNITY_BADGES),
            BadgeCategory.DONATION.value: len(DONATION_TIERS),
            BadgeCategory.ROLE.value: len(ROLE_BADGES),
        }
        return counts

    # =========================================================================
    # BADGE AWARDING
    # =========================================================================

    async def award_rank_badge(
        self,
        address: str,
        rank: int,
        time_unit: TimeUnit = TimeUnit.EPOCH,
        time_unit_id: Optional[int] = None,
        score: float = 0.0,
        # Legacy parameters for backwards compatibility
        epoch_id: Optional[int] = None,
        round_id: Optional[int] = None,
    ) -> Optional[EarnedBadge]:
        """
        Award a rank badge for placement in any time unit.

        Args:
            address: Recipient address
            rank: Placement (1-10)
            time_unit: Time unit (ROUND, EPOCH, ERA, SEASON, CYCLE)
            time_unit_id: ID within the time unit (auto-calculated if None)
            score: Score that earned the rank
            epoch_id: Legacy - use time_unit=EPOCH instead
            round_id: Legacy - use time_unit=ROUND instead

        Returns:
            EarnedBadge if awarded, None if already earned or invalid rank
        """
        if rank not in RANK_POSITIONS:
            logger.warning(f"Invalid rank {rank}, must be 1-10")
            return None

        # Handle legacy parameters
        if epoch_id is not None and time_unit_id is None:
            time_unit = TimeUnit.EPOCH
            time_unit_id = epoch_id
        if round_id is not None and time_unit_id is None:
            time_unit = TimeUnit.ROUND
            time_unit_id = round_id

        # Auto-calculate time_unit_id if not provided
        if time_unit_id is None:
            time_unit_id = get_time_unit_id(time_unit)

        # Generate badge ID using time unit prefix
        rank_label = RANK_LABELS[rank]
        badge_id = format_badge_id(time_unit, time_unit_id, rank_label)

        # Check if already awarded
        if self._has_badge(address, badge_id):
            return None

        # Get rarity based on time unit and rank
        rarity = get_rank_badge_rarity(time_unit, rank)

        # Generate asset name
        asset_name = f"{BADGE_PREFIX}{badge_id}"

        # Create earned badge
        earned = EarnedBadge(
            badge_id=badge_id,
            address=address,
            earned_at=int(time.time()),
            time_unit=time_unit.value,
            time_unit_id=time_unit_id,
            epoch_id=epoch_id,  # Legacy
            round_id=round_id,  # Legacy
            asset_name=asset_name,
            minted=False,
        )

        # Store
        self._add_earned_badge(address, earned)

        # Update leaderboard
        self._update_leaderboard_v2(address, rank, score, time_unit, time_unit_id)

        logger.info(f"Awarded rank badge {badge_id} ({time_unit.value} {time_unit_id}) to {address}")

        # Attempt mint (stubbed)
        await self._mint_badge_asset(earned)

        return earned

    async def award_achievement_badge(
        self,
        address: str,
        achievement_id: str,
        epoch_id: Optional[int] = None,
    ) -> Optional[EarnedBadge]:
        """
        Award an achievement badge.

        Args:
            address: Recipient address
            achievement_id: Achievement ID (e.g., "PERFECT", "STREAK30")
            epoch_id: Optional epoch when achieved

        Returns:
            EarnedBadge if awarded, None if already earned or invalid
        """
        if achievement_id not in ACHIEVEMENTS:
            logger.warning(f"Unknown achievement: {achievement_id}")
            return None

        # Generate badge ID (epoch-specific if provided)
        if epoch_id:
            badge_id = f"E{epoch_id}_{achievement_id}"
        else:
            badge_id = achievement_id

        # Check if already awarded
        if self._has_badge(address, badge_id):
            return None

        # Generate asset name
        asset_name = f"{BADGE_PREFIX}{badge_id}"

        # Create earned badge
        earned = EarnedBadge(
            badge_id=badge_id,
            address=address,
            earned_at=int(time.time()),
            epoch_id=epoch_id,
            asset_name=asset_name,
            minted=False,
        )

        # Store
        self._add_earned_badge(address, earned)

        info = ACHIEVEMENTS[achievement_id]
        logger.info(
            f"Awarded achievement '{info['name']}' to {address}"
        )

        # Attempt mint (stubbed)
        await self._mint_badge_asset(earned)

        return earned

    async def award_donation_badge(
        self,
        address: str,
        tier: str,
    ) -> Optional[EarnedBadge]:
        """
        Award a donation tier badge.

        Args:
            address: Recipient address
            tier: Tier name (BRONZE, SILVER, GOLD, etc.)

        Returns:
            EarnedBadge if awarded, None if already earned or invalid
        """
        tier = tier.upper()
        if tier not in DONATION_TIERS:
            logger.warning(f"Unknown donation tier: {tier}")
            return None

        badge_id = f"DONOR_{tier}"

        # Check if already awarded
        if self._has_badge(address, badge_id):
            return None

        asset_name = f"{BADGE_PREFIX}{badge_id}"

        earned = EarnedBadge(
            badge_id=badge_id,
            address=address,
            earned_at=int(time.time()),
            asset_name=asset_name,
            minted=False,
        )

        self._add_earned_badge(address, earned)

        logger.info(f"Awarded {tier} donor badge to {address}")

        await self._mint_badge_asset(earned)

        return earned

    async def award_role_badge(
        self,
        address: str,
        role: str,
    ) -> Optional[EarnedBadge]:
        """
        Award a role badge.

        Args:
            address: Recipient address
            role: Role name (ORACLE, ARCHIVER, etc.)

        Returns:
            EarnedBadge if awarded, None if already earned or invalid
        """
        role = role.upper()
        if role not in ROLE_BADGES:
            logger.warning(f"Unknown role: {role}")
            return None

        badge_id = role

        # Check if already awarded
        if self._has_badge(address, badge_id):
            return None

        asset_name = f"{BADGE_PREFIX}{badge_id}"

        earned = EarnedBadge(
            badge_id=badge_id,
            address=address,
            earned_at=int(time.time()),
            asset_name=asset_name,
            minted=False,
        )

        self._add_earned_badge(address, earned)

        logger.info(f"Awarded {role} role badge to {address}")

        await self._mint_badge_asset(earned)

        return earned

    async def award_streak_badge(
        self,
        address: str,
        streak_id: str,
        time_unit_id: Optional[int] = None,
    ) -> Optional[EarnedBadge]:
        """
        Award a streak badge based on time hierarchy.

        Args:
            address: Recipient address
            streak_id: Streak badge ID (STREAK_EPOCH, STREAK_EPOCH_4, etc.)
            time_unit_id: Optional ID when streak was completed

        Returns:
            EarnedBadge if awarded, None if already earned or invalid
        """
        if streak_id not in STREAK_BADGES:
            logger.warning(f"Unknown streak badge: {streak_id}")
            return None

        info = STREAK_BADGES[streak_id]
        time_unit = info["time_unit"]

        # Generate unique badge ID with time reference
        if time_unit_id is not None:
            badge_id = f"{streak_id}_{time_unit_id}"
        else:
            badge_id = streak_id

        # Check if already awarded
        if self._has_badge(address, badge_id):
            return None

        asset_name = f"{BADGE_PREFIX}{badge_id}"

        earned = EarnedBadge(
            badge_id=badge_id,
            address=address,
            earned_at=int(time.time()),
            time_unit=time_unit.value,
            time_unit_id=time_unit_id,
            asset_name=asset_name,
            minted=False,
        )

        self._add_earned_badge(address, earned)

        logger.info(
            f"Awarded streak badge '{info['name']}' to {address}"
        )

        await self._mint_badge_asset(earned)

        return earned

    async def award_community_badge(
        self,
        address: str,
        community_badge_id: str,
        time_unit: Optional[TimeUnit] = None,
        time_unit_id: Optional[int] = None,
    ) -> Optional[EarnedBadge]:
        """
        Award a community badge.

        Args:
            address: Recipient address
            community_badge_id: Badge ID (MENTOR, RELIABLE, DATA_PROVIDER, etc.)
            time_unit: Optional time unit when earned
            time_unit_id: Optional ID within the time unit

        Returns:
            EarnedBadge if awarded, None if already earned or invalid
        """
        if community_badge_id not in COMMUNITY_BADGES:
            logger.warning(f"Unknown community badge: {community_badge_id}")
            return None

        info = COMMUNITY_BADGES[community_badge_id]

        # Generate badge ID
        if time_unit is not None and time_unit_id is not None:
            prefix = TIME_UNIT_PREFIXES.get(time_unit, "")
            badge_id = f"{prefix}{time_unit_id}_{community_badge_id}"
        else:
            badge_id = community_badge_id

        # Check if already awarded
        if self._has_badge(address, badge_id):
            return None

        asset_name = f"{BADGE_PREFIX}{badge_id}"

        earned = EarnedBadge(
            badge_id=badge_id,
            address=address,
            earned_at=int(time.time()),
            time_unit=time_unit.value if time_unit else None,
            time_unit_id=time_unit_id,
            asset_name=asset_name,
            minted=False,
        )

        self._add_earned_badge(address, earned)

        logger.info(
            f"Awarded community badge '{info['name']}' to {address}"
        )

        await self._mint_badge_asset(earned)

        return earned

    # =========================================================================
    # BADGE QUERIES
    # =========================================================================

    def get_badges_for_address(self, address: str) -> List[EarnedBadge]:
        """Get all badges earned by an address."""
        return self._earned_badges.get(address, []).copy()

    def get_badge_count(self, address: str) -> int:
        """Get number of badges earned by an address."""
        return len(self._earned_badges.get(address, []))

    def has_badge(self, address: str, badge_id: str) -> bool:
        """Check if address has a specific badge."""
        return self._has_badge(address, badge_id)

    def get_leaderboard(
        self,
        epoch_id: int,
        limit: int = 10,
    ) -> List[LeaderboardEntry]:
        """Get leaderboard for an epoch (legacy method)."""
        entries = self._leaderboards.get(epoch_id, [])
        return sorted(entries, key=lambda e: e.rank)[:limit]

    def get_leaderboard_v2(
        self,
        time_unit: TimeUnit,
        time_unit_id: int,
        limit: int = 10,
    ) -> List[LeaderboardEntry]:
        """Get leaderboard for any time unit."""
        # Compute leaderboard key (same logic as _update_leaderboard_v2)
        if time_unit == TimeUnit.EPOCH:
            leaderboard_key = time_unit_id
        else:
            offset_map = {
                TimeUnit.ROUND: -1000000,
                TimeUnit.ERA: -2000000,
                TimeUnit.SEASON: -3000000,
                TimeUnit.CYCLE: -4000000,
            }
            leaderboard_key = offset_map.get(time_unit, 0) + time_unit_id

        entries = self._leaderboards.get(leaderboard_key, [])
        return sorted(entries, key=lambda e: e.rank)[:limit]

    def get_badge_holders(self, badge_id: str) -> List[str]:
        """Get all addresses that hold a specific badge."""
        holders = []
        for address, badges in self._earned_badges.items():
            if any(b.badge_id == badge_id for b in badges):
                holders.append(address)
        return holders

    def get_stats(self) -> Dict[str, Any]:
        """Get badge system statistics."""
        total_badges = sum(len(b) for b in self._earned_badges.values())
        total_minted = len(self._minted_assets)

        return {
            "total_addresses_with_badges": len(self._earned_badges),
            "total_badges_earned": total_badges,
            "total_badges_minted": total_minted,
            "epochs_with_leaderboards": len(self._leaderboards),
        }

    # =========================================================================
    # INTERNAL HELPERS
    # =========================================================================

    def _has_badge(self, address: str, badge_id: str) -> bool:
        """Check if address has badge (internal)."""
        badges = self._earned_badges.get(address, [])
        return any(b.badge_id == badge_id for b in badges)

    def _add_earned_badge(self, address: str, badge: EarnedBadge) -> None:
        """Add badge to storage (internal)."""
        if address not in self._earned_badges:
            self._earned_badges[address] = []
        self._earned_badges[address].append(badge)

    def _update_leaderboard(
        self,
        address: str,
        rank: int,
        score: float,
        epoch_id: int,
        round_id: Optional[int],
    ) -> None:
        """Update leaderboard with entry (legacy epoch-based)."""
        if epoch_id not in self._leaderboards:
            self._leaderboards[epoch_id] = []

        entry = LeaderboardEntry(
            address=address,
            rank=rank,
            score=score,
            time_unit=TimeUnit.EPOCH.value,
            time_unit_id=epoch_id,
            epoch_id=epoch_id,
            round_id=round_id,
        )
        self._leaderboards[epoch_id].append(entry)

    def _update_leaderboard_v2(
        self,
        address: str,
        rank: int,
        score: float,
        time_unit: TimeUnit,
        time_unit_id: int,
    ) -> None:
        """Update leaderboard with entry for any time unit."""
        # Use composite key: "{time_unit}_{id}" for storage
        # For backwards compatibility, epoch leaderboards use plain epoch_id
        if time_unit == TimeUnit.EPOCH:
            leaderboard_key = time_unit_id
        else:
            # Use negative keys for non-epoch time units to avoid collision
            # Round: -1000000 + id, Era: -2000000 + id, etc.
            offset_map = {
                TimeUnit.ROUND: -1000000,
                TimeUnit.ERA: -2000000,
                TimeUnit.SEASON: -3000000,
                TimeUnit.CYCLE: -4000000,
            }
            leaderboard_key = offset_map.get(time_unit, 0) + time_unit_id

        if leaderboard_key not in self._leaderboards:
            self._leaderboards[leaderboard_key] = []

        entry = LeaderboardEntry(
            address=address,
            rank=rank,
            score=score,
            time_unit=time_unit.value,
            time_unit_id=time_unit_id,
            epoch_id=time_unit_id if time_unit == TimeUnit.EPOCH else None,
            round_id=time_unit_id if time_unit == TimeUnit.ROUND else None,
        )
        self._leaderboards[leaderboard_key].append(entry)

    async def _mint_badge_asset(self, badge: EarnedBadge) -> bool:
        """
        Mint badge as Evrmore unique asset (STUBBED).

        Actual implementation requires:
        - Wallet integration
        - EVR for asset creation fee (5 EVR for unique asset)
        - Proper signing and broadcast

        Args:
            badge: Badge to mint

        Returns:
            True if minted (always False in stub)
        """
        if not self._wallet:
            logger.debug(
                f"Badge mint stubbed (no wallet): {badge.asset_name}"
            )
            return False

        # Check if already minted
        if badge.asset_name in self._minted_assets:
            return False

        # TODO: Implement actual minting when wallet integration is ready
        # Steps:
        # 1. Check EVR balance for fee (5 EVR)
        # 2. Build asset creation transaction
        # 3. Sign and broadcast
        # 4. Wait for confirmation
        # 5. Update badge.tx_hash and badge.minted

        logger.info(
            f"STUB: Would mint badge asset {badge.asset_name} "
            f"to {badge.address}"
        )

        return False

    def clear_for_testing(self) -> None:
        """Clear all data (for testing only)."""
        self._earned_badges.clear()
        self._leaderboards.clear()
        self._minted_assets.clear()


# =============================================================================
# AUTOMATIC BADGE CHECKER
# =============================================================================

class AutomaticBadgeChecker:
    """
    Automatically checks and awards badges based on node activity.

    Integrates with:
    - UptimeTracker for streak badges
    - RewardDistributionManager for rank badges
    - Governance for voter badges
    - DonationManager for donation badges
    - Community activity for mentoring, reliability, data availability

    Time Hierarchy for streaks:
        - STREAK_EPOCH: 7 consecutive days (1 epoch)
        - STREAK_EPOCH_4: 28 consecutive days (1 era / 4 epochs)
        - STREAK_EPOCH_13: 91 consecutive days (1 season / 13 epochs)
        - STREAK_EPOCH_52: 364 consecutive days (1 cycle / 52 epochs)
    """

    def __init__(self, badge_manager: BadgeManager):
        self._badges = badge_manager

    async def check_streak_badges(
        self,
        address: str,
        streak_days: int,
        current_epoch_id: Optional[int] = None,
    ) -> List[EarnedBadge]:
        """
        Check and award streak badges based on consecutive participation days.

        Uses the time hierarchy:
        - 7 days = 1 epoch = STREAK_EPOCH (common)
        - 28 days = 1 era = STREAK_EPOCH_4 (uncommon)
        - 91 days = 1 season = STREAK_EPOCH_13 (rare)
        - 364 days = 1 cycle = STREAK_EPOCH_52 (legendary)

        Args:
            address: Node address
            streak_days: Number of consecutive participation days
            current_epoch_id: Current epoch ID for badge tracking

        Returns:
            List of badges awarded
        """
        earned = []

        # Epoch streak: 7+ days (1 week)
        if streak_days >= ROUNDS_PER_EPOCH:
            badge = await self._badges.award_streak_badge(
                address, "STREAK_EPOCH", current_epoch_id
            )
            if badge:
                earned.append(badge)

        # Era streak: 28+ days (4 epochs)
        if streak_days >= ROUNDS_PER_EPOCH * EPOCHS_PER_ERA:
            era_id = current_epoch_id // EPOCHS_PER_ERA if current_epoch_id else None
            badge = await self._badges.award_streak_badge(
                address, "STREAK_EPOCH_4", era_id
            )
            if badge:
                earned.append(badge)

        # Season streak: 91+ days (13 epochs)
        if streak_days >= ROUNDS_PER_EPOCH * EPOCHS_PER_SEASON:
            season_id = current_epoch_id // EPOCHS_PER_SEASON if current_epoch_id else None
            badge = await self._badges.award_streak_badge(
                address, "STREAK_EPOCH_13", season_id
            )
            if badge:
                earned.append(badge)

        # Cycle streak: 364+ days (52 epochs)
        if streak_days >= ROUNDS_PER_EPOCH * EPOCHS_PER_CYCLE:
            cycle_id = current_epoch_id // EPOCHS_PER_CYCLE if current_epoch_id else None
            badge = await self._badges.award_streak_badge(
                address, "STREAK_EPOCH_52", cycle_id
            )
            if badge:
                earned.append(badge)

        return earned

    async def check_vote_badges(
        self,
        address: str,
        total_votes: int,
    ) -> List[EarnedBadge]:
        """Check and award voting badges."""
        earned = []

        if total_votes >= 100:
            badge = await self._badges.award_achievement_badge(
                address, "VOTER100"
            )
            if badge:
                earned.append(badge)

        return earned

    async def check_community_badges(
        self,
        address: str,
        nodes_mentored: int = 0,
        uptime_ratio: float = 0.0,
        data_requests_served: int = 0,
        time_unit: Optional[TimeUnit] = None,
        time_unit_id: Optional[int] = None,
    ) -> List[EarnedBadge]:
        """
        Check and award community contribution badges.

        Args:
            address: Node address
            nodes_mentored: Number of new nodes helped to bootstrap
            uptime_ratio: Uptime ratio (0.0 - 1.0) over the period
            data_requests_served: Number of historical data requests fulfilled
            time_unit: Time unit for the badge (era/season for reliability)
            time_unit_id: ID of the time unit

        Returns:
            List of badges awarded
        """
        earned = []

        # Mentor badges
        if nodes_mentored >= 5:
            badge = await self._badges.award_community_badge(
                address, "MENTOR", time_unit, time_unit_id
            )
            if badge:
                earned.append(badge)

        if nodes_mentored >= 25:
            badge = await self._badges.award_community_badge(
                address, "MENTOR_MASTER", time_unit, time_unit_id
            )
            if badge:
                earned.append(badge)

        # Reliability badges (99.9% uptime)
        if uptime_ratio >= 0.999:
            if time_unit == TimeUnit.ERA:
                badge = await self._badges.award_community_badge(
                    address, "RELIABLE", time_unit, time_unit_id
                )
                if badge:
                    earned.append(badge)
            elif time_unit == TimeUnit.SEASON:
                badge = await self._badges.award_community_badge(
                    address, "IRON_NODE", time_unit, time_unit_id
                )
                if badge:
                    earned.append(badge)

        # Data provider badge
        if data_requests_served >= 1000:
            badge = await self._badges.award_community_badge(
                address, "DATA_PROVIDER", time_unit, time_unit_id
            )
            if badge:
                earned.append(badge)

        return earned

    async def award_rank_badges(
        self,
        rankings: List[Dict[str, Any]],
        time_unit: TimeUnit,
        time_unit_id: int,
    ) -> List[EarnedBadge]:
        """
        Award rank badges for any time unit period results.

        Args:
            rankings: List of {address, rank, score} dicts
            time_unit: Time unit (ROUND, EPOCH, ERA, SEASON, CYCLE)
            time_unit_id: ID of the time unit

        Returns:
            List of badges awarded
        """
        earned = []

        for entry in rankings:
            if entry.get("rank", 0) > 10:
                continue

            badge = await self._badges.award_rank_badge(
                address=entry["address"],
                rank=entry["rank"],
                time_unit=time_unit,
                time_unit_id=time_unit_id,
                score=entry.get("score", 0),
            )
            if badge:
                earned.append(badge)

        return earned

    async def award_epoch_rank_badges(
        self,
        rankings: List[Dict[str, Any]],
        epoch_id: int,
    ) -> List[EarnedBadge]:
        """
        Award rank badges for epoch results (legacy method).

        Args:
            rankings: List of {address, rank, score} dicts
            epoch_id: Epoch number

        Returns:
            List of badges awarded
        """
        return await self.award_rank_badges(rankings, TimeUnit.EPOCH, epoch_id)

    async def award_era_rank_badges(
        self,
        rankings: List[Dict[str, Any]],
        era_id: int,
    ) -> List[EarnedBadge]:
        """Award rank badges for era results (monthly-ish, 13 per year)."""
        return await self.award_rank_badges(rankings, TimeUnit.ERA, era_id)

    async def award_season_rank_badges(
        self,
        rankings: List[Dict[str, Any]],
        season_id: int,
    ) -> List[EarnedBadge]:
        """Award rank badges for season results (quarterly, 4 per year)."""
        return await self.award_rank_badges(rankings, TimeUnit.SEASON, season_id)

    async def award_cycle_rank_badges(
        self,
        rankings: List[Dict[str, Any]],
        cycle_id: int,
    ) -> List[EarnedBadge]:
        """Award rank badges for cycle results (yearly, 1 per year)."""
        return await self.award_rank_badges(rankings, TimeUnit.CYCLE, cycle_id)

    async def process_period_end(
        self,
        time_unit: TimeUnit,
        time_unit_id: int,
        rankings: List[Dict[str, Any]],
        node_stats: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, List[EarnedBadge]]:
        """
        Process end of a time period and award all applicable badges.

        This is the main entry point for badge processing at the end of
        each time period (round, epoch, era, season, cycle).

        Args:
            time_unit: The time unit that just ended
            time_unit_id: ID of the time unit
            rankings: Final rankings for the period [{address, rank, score}, ...]
            node_stats: Optional dict of address -> {streak_days, uptime, mentored, ...}

        Returns:
            Dict mapping addresses to badges earned this period
        """
        all_earned: Dict[str, List[EarnedBadge]] = {}

        # Award rank badges
        rank_badges = await self.award_rank_badges(rankings, time_unit, time_unit_id)
        for badge in rank_badges:
            if badge.address not in all_earned:
                all_earned[badge.address] = []
            all_earned[badge.address].append(badge)

        # Process node stats if provided
        if node_stats:
            for address, stats in node_stats.items():
                addr_badges = []

                # Check streak badges
                streak_days = stats.get("streak_days", 0)
                if streak_days > 0:
                    epoch_id = time_unit_id if time_unit == TimeUnit.EPOCH else None
                    streak_badges = await self.check_streak_badges(
                        address, streak_days, epoch_id
                    )
                    addr_badges.extend(streak_badges)

                # Check community badges
                community_badges = await self.check_community_badges(
                    address=address,
                    nodes_mentored=stats.get("nodes_mentored", 0),
                    uptime_ratio=stats.get("uptime_ratio", 0.0),
                    data_requests_served=stats.get("data_requests_served", 0),
                    time_unit=time_unit,
                    time_unit_id=time_unit_id,
                )
                addr_badges.extend(community_badges)

                if addr_badges:
                    if address not in all_earned:
                        all_earned[address] = []
                    all_earned[address].extend(addr_badges)

        logger.info(
            f"Processed {time_unit.value} {time_unit_id} end: "
            f"{sum(len(b) for b in all_earned.values())} badges awarded to "
            f"{len(all_earned)} addresses"
        )

        return all_earned
