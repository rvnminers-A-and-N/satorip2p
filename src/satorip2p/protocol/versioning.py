"""
satorip2p/protocol/versioning.py

Protocol versioning for network upgrades.

Enables smooth protocol upgrades while maintaining backward compatibility:
- Semantic versioning (major.minor.patch)
- Multi-version negotiation
- Feature capability advertising
- Graceful degradation for older peers
- Governance-triggered upgrades
- Upgrade notification system

Usage:
    from satorip2p.protocol.versioning import (
        ProtocolVersion,
        VersionNegotiator,
        CURRENT_VERSION,
    )

    # Check compatibility
    if ProtocolVersion.from_string("1.0.0").is_compatible_with(CURRENT_VERSION):
        # Use v1 features
        pass

    # Negotiate best common version
    negotiator = VersionNegotiator()
    best = negotiator.negotiate(["1.0.0", "1.1.0"], ["1.1.0", "2.0.0"])
    # Returns "1.1.0"
"""

import logging
import time
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any, TYPE_CHECKING
from enum import Enum
from pathlib import Path

if TYPE_CHECKING:
    from satorip2p.peers import Peers

logger = logging.getLogger("satorip2p.protocol.versioning")

# PubSub topics for upgrade notifications
UPGRADE_NOTIFICATION_TOPIC = "satori/protocol/upgrades"
VERSION_ANNOUNCE_TOPIC = "satori/protocol/version"


# ============================================================================
# VERSION CONSTANTS
# ============================================================================

# Current protocol version
PROTOCOL_VERSION = "1.0.0"

# Minimum supported version for backward compatibility
MIN_SUPPORTED_VERSION = "1.0.0"

# Protocol ID format
PROTOCOL_ID_PREFIX = "/satori"

# Feature flags introduced in each version
VERSION_FEATURES: Dict[str, List[str]] = {
    "1.0.0": [
        "basic_messaging",
        "pubsub_gossipsub",
        "dht_kademlia",
        "subscription_announce",
        "peer_announce",
        "message_store",
    ],
    "1.1.0": [
        "delegation_protocol",
        "lending_protocol",
        "enhanced_heartbeat",
        "capability_discovery",
    ],
    "2.0.0": [
        "prediction_scoring_v2",
        "stream_discovery_v2",
        "bandwidth_accounting",
    ],
}


# ============================================================================
# VERSION DATA CLASS
# ============================================================================

@dataclass(frozen=True)
class ProtocolVersion:
    """
    Semantic version for protocol identification.

    Follows semver: MAJOR.MINOR.PATCH
    - MAJOR: Breaking changes
    - MINOR: Backward-compatible features
    - PATCH: Bug fixes
    """
    major: int
    minor: int
    patch: int

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def __repr__(self) -> str:
        return f"ProtocolVersion({self})"

    def __lt__(self, other: "ProtocolVersion") -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __le__(self, other: "ProtocolVersion") -> bool:
        return (self.major, self.minor, self.patch) <= (other.major, other.minor, other.patch)

    def __gt__(self, other: "ProtocolVersion") -> bool:
        return (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)

    def __ge__(self, other: "ProtocolVersion") -> bool:
        return (self.major, self.minor, self.patch) >= (other.major, other.minor, other.patch)

    @classmethod
    def from_string(cls, version_str: str) -> "ProtocolVersion":
        """
        Parse version from string.

        Args:
            version_str: Version string like "1.0.0" or "/satori/1.0.0"

        Returns:
            ProtocolVersion instance

        Raises:
            ValueError: If string is not a valid version
        """
        # Strip protocol prefix if present
        if version_str.startswith(PROTOCOL_ID_PREFIX):
            version_str = version_str.split("/")[-1]

        try:
            parts = version_str.strip().split(".")
            if len(parts) != 3:
                raise ValueError(f"Invalid version format: {version_str}")

            return cls(
                major=int(parts[0]),
                minor=int(parts[1]),
                patch=int(parts[2]),
            )
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid version string '{version_str}': {e}")

    def to_protocol_id(self) -> str:
        """Convert to libp2p protocol ID format."""
        return f"{PROTOCOL_ID_PREFIX}/{self}"

    def is_compatible_with(self, other: "ProtocolVersion") -> bool:
        """
        Check if this version is compatible with another.

        Same major version = backward compatible (minor/patch can differ).
        Different major version = breaking change.

        Args:
            other: Version to check compatibility with

        Returns:
            True if versions can interoperate
        """
        return self.major == other.major

    def is_newer_than(self, other: "ProtocolVersion") -> bool:
        """Check if this version is newer."""
        return self > other

    def get_features(self) -> List[str]:
        """
        Get all features available in this version.

        Accumulates features from 1.0.0 up to this version.

        Returns:
            List of feature identifiers
        """
        features = []
        for version_str, version_features in VERSION_FEATURES.items():
            version = ProtocolVersion.from_string(version_str)
            if version <= self:
                features.extend(version_features)
        return list(set(features))  # Deduplicate

    def supports_feature(self, feature: str) -> bool:
        """Check if this version supports a feature."""
        return feature in self.get_features()


# Current version as object
CURRENT_VERSION = ProtocolVersion.from_string(PROTOCOL_VERSION)
MIN_VERSION = ProtocolVersion.from_string(MIN_SUPPORTED_VERSION)


# ============================================================================
# VERSION NEGOTIATION
# ============================================================================

class VersionNegotiator:
    """
    Negotiates protocol versions between peers.

    Uses preference ordering where newer versions are preferred,
    but only compatible versions are considered.
    """

    def __init__(
        self,
        our_versions: Optional[List[str]] = None,
        min_version: Optional[ProtocolVersion] = None,
    ):
        """
        Initialize negotiator.

        Args:
            our_versions: Versions we support (default: all up to CURRENT)
            min_version: Minimum acceptable version
        """
        self.min_version = min_version or MIN_VERSION

        if our_versions:
            self.our_versions = [ProtocolVersion.from_string(v) for v in our_versions]
        else:
            # Default: support all versions from min to current
            self.our_versions = self._get_default_versions()

        # Sort by preference (newest first)
        self.our_versions.sort(reverse=True)

    def _get_default_versions(self) -> List[ProtocolVersion]:
        """Get default supported versions."""
        versions = []
        for version_str in VERSION_FEATURES.keys():
            version = ProtocolVersion.from_string(version_str)
            if self.min_version <= version <= CURRENT_VERSION:
                versions.append(version)
        return versions

    def negotiate(
        self,
        their_versions: List[str],
    ) -> Optional[ProtocolVersion]:
        """
        Find best common version with a peer.

        Prefers the highest version that both peers support.

        Args:
            their_versions: Versions the other peer supports

        Returns:
            Best common version, or None if no compatibility
        """
        their_parsed = []
        for v in their_versions:
            try:
                their_parsed.append(ProtocolVersion.from_string(v))
            except ValueError:
                logger.debug(f"Ignoring invalid version: {v}")

        if not their_parsed:
            return None

        # Find intersection
        common = []
        for our_v in self.our_versions:
            for their_v in their_parsed:
                if our_v == their_v:
                    common.append(our_v)
                    break

        if not common:
            # Try to find compatible (same major) versions
            for our_v in self.our_versions:
                for their_v in their_parsed:
                    if our_v.is_compatible_with(their_v):
                        # Use the lower of the two compatible versions
                        common.append(min(our_v, their_v))
                        break

        if common:
            # Return highest common version
            return max(common)

        return None

    def get_protocol_ids(self) -> List[str]:
        """Get all supported protocol IDs in preference order."""
        return [v.to_protocol_id() for v in self.our_versions]

    def is_version_acceptable(self, version: str) -> bool:
        """Check if a version meets minimum requirements."""
        try:
            parsed = ProtocolVersion.from_string(version)
            return parsed >= self.min_version
        except ValueError:
            return False


# ============================================================================
# FEATURE REGISTRY
# ============================================================================

class FeatureFlag(Enum):
    """Known protocol features."""
    # Core messaging
    BASIC_MESSAGING = "basic_messaging"
    PUBSUB_GOSSIPSUB = "pubsub_gossipsub"
    DHT_KADEMLIA = "dht_kademlia"

    # Announcements
    SUBSCRIPTION_ANNOUNCE = "subscription_announce"
    PEER_ANNOUNCE = "peer_announce"

    # Storage
    MESSAGE_STORE = "message_store"

    # v1.1 features
    DELEGATION_PROTOCOL = "delegation_protocol"
    LENDING_PROTOCOL = "lending_protocol"
    ENHANCED_HEARTBEAT = "enhanced_heartbeat"
    CAPABILITY_DISCOVERY = "capability_discovery"

    # v2.0 features
    PREDICTION_SCORING_V2 = "prediction_scoring_v2"
    STREAM_DISCOVERY_V2 = "stream_discovery_v2"
    BANDWIDTH_ACCOUNTING = "bandwidth_accounting"


@dataclass
class FeatureSet:
    """
    Set of features supported by a peer.

    Used for capability advertising and feature negotiation.
    """
    features: Set[str] = field(default_factory=set)
    protocol_version: str = PROTOCOL_VERSION

    def __post_init__(self):
        # If no features specified, derive from version
        if not self.features:
            version = ProtocolVersion.from_string(self.protocol_version)
            self.features = set(version.get_features())

    def supports(self, feature: str) -> bool:
        """Check if feature is supported."""
        return feature in self.features

    def supports_all(self, features: List[str]) -> bool:
        """Check if all features are supported."""
        return all(f in self.features for f in features)

    def supports_any(self, features: List[str]) -> bool:
        """Check if any feature is supported."""
        return any(f in self.features for f in features)

    def common_features(self, other: "FeatureSet") -> Set[str]:
        """Get features common to both sets."""
        return self.features & other.features

    def to_list(self) -> List[str]:
        """Convert to sorted list."""
        return sorted(self.features)

    @classmethod
    def from_version(cls, version: str) -> "FeatureSet":
        """Create feature set from protocol version."""
        return cls(protocol_version=version)

    @classmethod
    def from_list(cls, features: List[str], version: str = PROTOCOL_VERSION) -> "FeatureSet":
        """Create from explicit feature list."""
        return cls(features=set(features), protocol_version=version)


# ============================================================================
# COMPATIBILITY MATRIX
# ============================================================================

@dataclass
class VersionInfo:
    """Detailed information about a protocol version."""
    version: str
    name: str
    description: str
    features: List[str]
    breaking_changes: List[str] = field(default_factory=list)
    deprecated_features: List[str] = field(default_factory=list)
    migration_notes: str = ""


COMPATIBILITY_MATRIX: Dict[str, VersionInfo] = {
    "1.0.0": VersionInfo(
        version="1.0.0",
        name="Initial Release",
        description="Core P2P functionality with GossipSub and DHT",
        features=VERSION_FEATURES["1.0.0"],
    ),
    "1.1.0": VersionInfo(
        version="1.1.0",
        name="Pool & Delegation",
        description="Adds pool lending and delegation protocols",
        features=VERSION_FEATURES.get("1.1.0", []),
        migration_notes="New optional protocols; no breaking changes",
    ),
    "2.0.0": VersionInfo(
        version="2.0.0",
        name="Enhanced Scoring",
        description="New prediction scoring algorithm",
        features=VERSION_FEATURES.get("2.0.0", []),
        breaking_changes=["prediction_scoring_v1"],
        migration_notes="Scoring algorithm changed; peers must upgrade for rewards",
    ),
}


def get_version_info(version: str) -> Optional[VersionInfo]:
    """Get detailed info about a version."""
    return COMPATIBILITY_MATRIX.get(version)


def get_breaking_changes(from_version: str, to_version: str) -> List[str]:
    """
    Get breaking changes between two versions.

    Args:
        from_version: Current version
        to_version: Target version

    Returns:
        List of breaking change descriptions
    """
    changes = []
    from_v = ProtocolVersion.from_string(from_version)
    to_v = ProtocolVersion.from_string(to_version)

    for version_str, info in COMPATIBILITY_MATRIX.items():
        version = ProtocolVersion.from_string(version_str)
        if from_v < version <= to_v:
            changes.extend(info.breaking_changes)

    return changes


# ============================================================================
# MESSAGE VERSION HELPERS
# ============================================================================

def add_version_to_message(message: dict) -> dict:
    """
    Add protocol version to a message.

    Args:
        message: Message dict to modify

    Returns:
        Modified message with version
    """
    message["protocol_version"] = PROTOCOL_VERSION
    return message


def get_message_version(message: dict) -> ProtocolVersion:
    """
    Extract protocol version from a message.

    Args:
        message: Message dict

    Returns:
        ProtocolVersion (defaults to 1.0.0 if not present)
    """
    version_str = message.get("protocol_version", "1.0.0")
    try:
        return ProtocolVersion.from_string(version_str)
    except ValueError:
        return MIN_VERSION


def is_message_compatible(message: dict) -> bool:
    """
    Check if a message is from a compatible protocol version.

    Args:
        message: Message dict

    Returns:
        True if message version is compatible
    """
    msg_version = get_message_version(message)
    return msg_version.is_compatible_with(CURRENT_VERSION)


# ============================================================================
# PEER VERSION TRACKING
# ============================================================================

class PeerVersionTracker:
    """
    Tracks protocol versions of connected peers.

    Useful for:
    - Deciding which features to use with a peer
    - Statistics on network upgrade progress
    - Graceful degradation decisions
    """

    def __init__(self):
        self._peer_versions: Dict[str, ProtocolVersion] = {}
        self._peer_features: Dict[str, FeatureSet] = {}

    def register_peer(
        self,
        peer_id: str,
        version: str,
        features: Optional[List[str]] = None,
    ) -> None:
        """
        Register a peer's protocol version.

        Args:
            peer_id: libp2p peer ID
            version: Protocol version string
            features: Optional explicit feature list
        """
        try:
            self._peer_versions[peer_id] = ProtocolVersion.from_string(version)

            if features:
                self._peer_features[peer_id] = FeatureSet.from_list(features, version)
            else:
                self._peer_features[peer_id] = FeatureSet.from_version(version)

        except ValueError as e:
            logger.warning(f"Invalid version from peer {peer_id}: {e}")

    def unregister_peer(self, peer_id: str) -> None:
        """Remove peer from tracking."""
        self._peer_versions.pop(peer_id, None)
        self._peer_features.pop(peer_id, None)

    def get_peer_version(self, peer_id: str) -> Optional[ProtocolVersion]:
        """Get a peer's protocol version."""
        return self._peer_versions.get(peer_id)

    def get_peer_features(self, peer_id: str) -> Optional[FeatureSet]:
        """Get a peer's feature set."""
        return self._peer_features.get(peer_id)

    def peer_supports_feature(self, peer_id: str, feature: str) -> bool:
        """Check if peer supports a feature."""
        features = self._peer_features.get(peer_id)
        return features.supports(feature) if features else False

    def get_peers_with_feature(self, feature: str) -> List[str]:
        """Get all peers supporting a feature."""
        return [
            peer_id
            for peer_id, features in self._peer_features.items()
            if features.supports(feature)
        ]

    def get_compatible_peers(self) -> List[str]:
        """Get peers with compatible protocol version."""
        return [
            peer_id
            for peer_id, version in self._peer_versions.items()
            if version.is_compatible_with(CURRENT_VERSION)
        ]

    def get_network_stats(self) -> Dict[str, int]:
        """Get version distribution across known peers."""
        version_counts: Dict[str, int] = {}
        for version in self._peer_versions.values():
            key = str(version)
            version_counts[key] = version_counts.get(key, 0) + 1
        return version_counts

    def get_upgrade_progress(self, target_version: str) -> Tuple[int, int]:
        """
        Get network upgrade progress.

        Args:
            target_version: Target version string

        Returns:
            (upgraded_count, total_count)
        """
        target = ProtocolVersion.from_string(target_version)
        total = len(self._peer_versions)
        upgraded = sum(1 for v in self._peer_versions.values() if v >= target)
        return upgraded, total


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def get_supported_versions() -> List[str]:
    """Get list of all supported protocol versions."""
    return list(VERSION_FEATURES.keys())


def get_current_version() -> str:
    """Get current protocol version string."""
    return PROTOCOL_VERSION


def get_current_features() -> List[str]:
    """Get features supported in current version."""
    return CURRENT_VERSION.get_features()


def check_compatibility(version: str) -> Tuple[bool, str]:
    """
    Check if a version is compatible and get reason.

    Args:
        version: Version string to check

    Returns:
        (is_compatible, reason_message)
    """
    try:
        parsed = ProtocolVersion.from_string(version)

        if parsed < MIN_VERSION:
            return False, f"Version {version} is below minimum {MIN_SUPPORTED_VERSION}"

        if not parsed.is_compatible_with(CURRENT_VERSION):
            return False, f"Version {version} has incompatible major version (current: {CURRENT_VERSION})"

        return True, "Compatible"

    except ValueError as e:
        return False, f"Invalid version format: {e}"


# ============================================================================
# UPGRADE STATUS
# ============================================================================

class UpgradeStatus(Enum):
    """Status of a protocol upgrade."""
    PROPOSED = "proposed"         # Upgrade proposed via governance
    APPROVED = "approved"         # Governance approved, pending activation
    ACTIVATING = "activating"     # Activation in progress
    ACTIVE = "active"             # Upgrade complete, new version active
    REJECTED = "rejected"         # Governance rejected
    FAILED = "failed"             # Activation failed


class UpgradeType(Enum):
    """Types of protocol upgrades."""
    MINOR = "minor"               # Backward-compatible features
    MAJOR = "major"               # Breaking changes
    PATCH = "patch"               # Bug fixes only
    FEATURE = "feature"           # Single feature addition


@dataclass
class UpgradeProposal:
    """A protocol upgrade proposal from governance."""
    proposal_id: str              # Governance proposal ID
    target_version: str           # Version to upgrade to
    upgrade_type: UpgradeType
    title: str
    description: str
    features: List[str]           # New features in this version
    breaking_changes: List[str]   # Breaking changes if any
    migration_notes: str = ""

    # Activation timing
    activation_height: int = 0    # Block height for activation (if time-locked)
    activation_timestamp: int = 0  # Timestamp for activation
    grace_period_days: int = 7    # Days between approval and activation

    # Status
    status: UpgradeStatus = UpgradeStatus.PROPOSED
    proposed_at: int = 0
    approved_at: int = 0
    activated_at: int = 0

    def to_dict(self) -> dict:
        return {
            'proposal_id': self.proposal_id,
            'target_version': self.target_version,
            'upgrade_type': self.upgrade_type.value,
            'title': self.title,
            'description': self.description,
            'features': self.features,
            'breaking_changes': self.breaking_changes,
            'migration_notes': self.migration_notes,
            'activation_height': self.activation_height,
            'activation_timestamp': self.activation_timestamp,
            'grace_period_days': self.grace_period_days,
            'status': self.status.value,
            'proposed_at': self.proposed_at,
            'approved_at': self.approved_at,
            'activated_at': self.activated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "UpgradeProposal":
        return cls(
            proposal_id=data['proposal_id'],
            target_version=data['target_version'],
            upgrade_type=UpgradeType(data.get('upgrade_type', 'minor')),
            title=data.get('title', ''),
            description=data.get('description', ''),
            features=data.get('features', []),
            breaking_changes=data.get('breaking_changes', []),
            migration_notes=data.get('migration_notes', ''),
            activation_height=data.get('activation_height', 0),
            activation_timestamp=data.get('activation_timestamp', 0),
            grace_period_days=data.get('grace_period_days', 7),
            status=UpgradeStatus(data.get('status', 'proposed')),
            proposed_at=data.get('proposed_at', 0),
            approved_at=data.get('approved_at', 0),
            activated_at=data.get('activated_at', 0),
        )


@dataclass
class UpgradeNotification:
    """Notification about a pending or completed upgrade."""
    notification_id: str
    upgrade_proposal_id: str
    target_version: str
    notification_type: str        # "proposed", "approved", "activating", "activated"
    message: str
    timestamp: int
    sender_peer_id: str = ""
    urgency: str = "normal"       # "low", "normal", "high", "critical"

    def to_dict(self) -> dict:
        return {
            'notification_id': self.notification_id,
            'upgrade_proposal_id': self.upgrade_proposal_id,
            'target_version': self.target_version,
            'notification_type': self.notification_type,
            'message': self.message,
            'timestamp': self.timestamp,
            'sender_peer_id': self.sender_peer_id,
            'urgency': self.urgency,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "UpgradeNotification":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# ============================================================================
# UPGRADE MANAGER
# ============================================================================

class UpgradeManager:
    """
    Manages protocol upgrades triggered by governance.

    Handles:
    - Upgrade proposal tracking
    - Activation scheduling
    - Peer notification
    - Graceful degradation during transitions
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        storage_path: Optional[Path] = None,
    ):
        """
        Initialize the upgrade manager.

        Args:
            peers: Peers instance for network operations
            storage_path: Path for local storage
        """
        self._peers = peers
        self._storage_path = storage_path or Path.home() / ".satori" / "upgrades"
        self._storage_path.mkdir(parents=True, exist_ok=True)

        # Active proposals: proposal_id -> UpgradeProposal
        self._proposals: Dict[str, UpgradeProposal] = {}

        # Notification history
        self._notifications: List[UpgradeNotification] = []

        # Callbacks
        self._on_upgrade_proposed_callbacks: List[callable] = []
        self._on_upgrade_approved_callbacks: List[callable] = []
        self._on_upgrade_activated_callbacks: List[callable] = []

        # Load local data
        self._load_local_data()

        self._started = False

    async def start(self) -> None:
        """Start the upgrade manager."""
        if self._started:
            return

        if self._peers:
            await self._peers.subscribe_async(
                UPGRADE_NOTIFICATION_TOPIC,
                self._on_upgrade_notification
            )

        self._started = True
        logger.info("Upgrade manager started")

    async def stop(self) -> None:
        """Stop the upgrade manager."""
        if not self._started:
            return

        self._save_local_data()

        if self._peers:
            self._peers.unsubscribe(UPGRADE_NOTIFICATION_TOPIC)

        self._started = False
        logger.info("Upgrade manager stopped")

    def _load_local_data(self) -> None:
        """Load data from local storage."""
        try:
            proposals_file = self._storage_path / "proposals.json"
            if proposals_file.exists():
                with open(proposals_file, 'r') as f:
                    data = json.load(f)
                for proposal_id, proposal_data in data.items():
                    self._proposals[proposal_id] = UpgradeProposal.from_dict(proposal_data)

            logger.debug(f"Loaded {len(self._proposals)} upgrade proposals")
        except Exception as e:
            logger.warning(f"Failed to load upgrade data: {e}")

    def _save_local_data(self) -> None:
        """Save data to local storage."""
        try:
            proposals_file = self._storage_path / "proposals.json"
            proposals_data = {pid: p.to_dict() for pid, p in self._proposals.items()}
            with open(proposals_file, 'w') as f:
                json.dump(proposals_data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save upgrade data: {e}")

    # -------------------------------------------------------------------------
    # Governance Integration
    # -------------------------------------------------------------------------

    def register_upgrade_proposal(
        self,
        proposal_id: str,
        target_version: str,
        title: str,
        description: str,
        features: Optional[List[str]] = None,
        breaking_changes: Optional[List[str]] = None,
        migration_notes: str = "",
        grace_period_days: int = 7,
    ) -> UpgradeProposal:
        """
        Register a new upgrade proposal from governance.

        Args:
            proposal_id: Governance proposal ID
            target_version: Version to upgrade to
            title: Proposal title
            description: Proposal description
            features: New features in this version
            breaking_changes: Breaking changes if any
            migration_notes: Migration instructions
            grace_period_days: Days between approval and activation

        Returns:
            The created UpgradeProposal
        """
        now = int(time.time())

        # Determine upgrade type from version comparison
        target = ProtocolVersion.from_string(target_version)
        if target.major > CURRENT_VERSION.major:
            upgrade_type = UpgradeType.MAJOR
        elif target.minor > CURRENT_VERSION.minor:
            upgrade_type = UpgradeType.MINOR
        else:
            upgrade_type = UpgradeType.PATCH

        proposal = UpgradeProposal(
            proposal_id=proposal_id,
            target_version=target_version,
            upgrade_type=upgrade_type,
            title=title,
            description=description,
            features=features or [],
            breaking_changes=breaking_changes or [],
            migration_notes=migration_notes,
            grace_period_days=grace_period_days,
            status=UpgradeStatus.PROPOSED,
            proposed_at=now,
        )

        self._proposals[proposal_id] = proposal
        self._save_local_data()

        # Notify callbacks
        for callback in self._on_upgrade_proposed_callbacks:
            try:
                callback(proposal)
            except Exception as e:
                logger.warning(f"Upgrade proposed callback error: {e}")

        # Broadcast notification
        self._broadcast_notification(
            proposal,
            "proposed",
            f"Protocol upgrade to v{target_version} proposed: {title}",
            urgency="normal",
        )

        logger.info(f"Upgrade proposal registered: {proposal_id} -> v{target_version}")

        return proposal

    def approve_upgrade(self, proposal_id: str) -> bool:
        """
        Mark an upgrade as approved by governance.

        Args:
            proposal_id: Governance proposal ID

        Returns:
            True if approved
        """
        proposal = self._proposals.get(proposal_id)
        if not proposal:
            logger.warning(f"Upgrade proposal not found: {proposal_id}")
            return False

        now = int(time.time())
        proposal.status = UpgradeStatus.APPROVED
        proposal.approved_at = now

        # Set activation time
        proposal.activation_timestamp = now + (proposal.grace_period_days * 24 * 3600)

        self._save_local_data()

        # Notify callbacks
        for callback in self._on_upgrade_approved_callbacks:
            try:
                callback(proposal)
            except Exception as e:
                logger.warning(f"Upgrade approved callback error: {e}")

        # Broadcast notification
        urgency = "high" if proposal.upgrade_type == UpgradeType.MAJOR else "normal"
        self._broadcast_notification(
            proposal,
            "approved",
            f"Protocol upgrade to v{proposal.target_version} APPROVED! "
            f"Activates in {proposal.grace_period_days} days.",
            urgency=urgency,
        )

        logger.info(f"Upgrade approved: {proposal_id} -> v{proposal.target_version}")

        return True

    def reject_upgrade(self, proposal_id: str) -> bool:
        """Mark an upgrade as rejected by governance."""
        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return False

        proposal.status = UpgradeStatus.REJECTED
        self._save_local_data()

        self._broadcast_notification(
            proposal,
            "rejected",
            f"Protocol upgrade to v{proposal.target_version} was rejected.",
            urgency="low",
        )

        logger.info(f"Upgrade rejected: {proposal_id}")
        return True

    def activate_upgrade(self, proposal_id: str) -> bool:
        """
        Activate an approved upgrade.

        This should be called when the activation time is reached.

        Args:
            proposal_id: Governance proposal ID

        Returns:
            True if activated
        """
        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return False

        if proposal.status != UpgradeStatus.APPROVED:
            logger.warning(f"Cannot activate non-approved upgrade: {proposal_id}")
            return False

        now = int(time.time())
        proposal.status = UpgradeStatus.ACTIVE
        proposal.activated_at = now

        self._save_local_data()

        # Notify callbacks
        for callback in self._on_upgrade_activated_callbacks:
            try:
                callback(proposal)
            except Exception as e:
                logger.warning(f"Upgrade activated callback error: {e}")

        # Broadcast notification
        self._broadcast_notification(
            proposal,
            "activated",
            f"Protocol upgrade to v{proposal.target_version} is NOW ACTIVE!",
            urgency="critical",
        )

        logger.info(f"Upgrade ACTIVATED: {proposal_id} -> v{proposal.target_version}")

        return True

    # -------------------------------------------------------------------------
    # Notification System
    # -------------------------------------------------------------------------

    def _broadcast_notification(
        self,
        proposal: UpgradeProposal,
        notification_type: str,
        message: str,
        urgency: str = "normal",
    ) -> None:
        """Broadcast upgrade notification to network."""
        import hashlib

        notification = UpgradeNotification(
            notification_id=hashlib.sha256(
                f"{proposal.proposal_id}:{notification_type}:{time.time()}".encode()
            ).hexdigest()[:16],
            upgrade_proposal_id=proposal.proposal_id,
            target_version=proposal.target_version,
            notification_type=notification_type,
            message=message,
            timestamp=int(time.time()),
            urgency=urgency,
        )

        self._notifications.append(notification)

        # Broadcast to network
        if self._peers:
            try:
                message_bytes = json.dumps({
                    'type': 'upgrade_notification',
                    'notification': notification.to_dict(),
                }).encode()

                import trio
                trio.from_thread.run_sync(
                    lambda: self._peers.broadcast(UPGRADE_NOTIFICATION_TOPIC, message_bytes)
                )
            except Exception as e:
                logger.debug(f"Failed to broadcast upgrade notification: {e}")

    def _on_upgrade_notification(self, topic: str, data: Any) -> None:
        """Handle incoming upgrade notification from network."""
        try:
            if isinstance(data, bytes):
                message = json.loads(data.decode())
            elif isinstance(data, dict):
                message = data
            else:
                return

            if message.get('type') != 'upgrade_notification':
                return

            notification_data = message.get('notification', {})
            if not notification_data:
                return

            notification = UpgradeNotification.from_dict(notification_data)

            # Store if not duplicate
            existing_ids = {n.notification_id for n in self._notifications}
            if notification.notification_id not in existing_ids:
                self._notifications.append(notification)
                logger.info(
                    f"Upgrade notification: {notification.message} "
                    f"(urgency: {notification.urgency})"
                )

        except Exception as e:
            logger.warning(f"Error handling upgrade notification: {e}")

    # -------------------------------------------------------------------------
    # Query Methods
    # -------------------------------------------------------------------------

    def get_pending_upgrades(self) -> List[UpgradeProposal]:
        """Get all approved but not yet activated upgrades."""
        return [
            p for p in self._proposals.values()
            if p.status == UpgradeStatus.APPROVED
        ]

    def get_active_upgrades(self) -> List[UpgradeProposal]:
        """Get all activated upgrades."""
        return [
            p for p in self._proposals.values()
            if p.status == UpgradeStatus.ACTIVE
        ]

    def check_pending_activations(self) -> List[UpgradeProposal]:
        """
        Check for upgrades ready to activate.

        Call this periodically to trigger activations.

        Returns:
            List of proposals ready to activate
        """
        now = int(time.time())
        ready = []

        for proposal in self._proposals.values():
            if proposal.status == UpgradeStatus.APPROVED:
                if proposal.activation_timestamp > 0 and now >= proposal.activation_timestamp:
                    ready.append(proposal)

        return ready

    def get_upgrade_status(self) -> Dict[str, Any]:
        """Get overall upgrade status."""
        pending = self.get_pending_upgrades()
        ready = self.check_pending_activations()

        return {
            'current_version': PROTOCOL_VERSION,
            'min_supported_version': MIN_SUPPORTED_VERSION,
            'total_proposals': len(self._proposals),
            'pending_upgrades': len(pending),
            'ready_to_activate': len(ready),
            'recent_notifications': len(self._notifications[-10:]),
        }

    def get_recent_notifications(self, limit: int = 20) -> List[UpgradeNotification]:
        """Get recent upgrade notifications."""
        return self._notifications[-limit:]

    def should_warn_incompatibility(self, peer_version: str) -> Tuple[bool, str]:
        """
        Check if a peer's version will become incompatible with pending upgrade.

        Args:
            peer_version: Peer's current version

        Returns:
            (should_warn, warning_message)
        """
        pending = self.get_pending_upgrades()
        if not pending:
            return False, ""

        try:
            peer_v = ProtocolVersion.from_string(peer_version)
        except ValueError:
            return True, f"Invalid peer version: {peer_version}"

        for proposal in pending:
            target_v = ProtocolVersion.from_string(proposal.target_version)
            if not peer_v.is_compatible_with(target_v):
                days_left = (proposal.activation_timestamp - int(time.time())) // 86400
                return True, (
                    f"Peer running v{peer_version} will be incompatible after "
                    f"v{proposal.target_version} activates in {days_left} days"
                )

        return False, ""

    # -------------------------------------------------------------------------
    # Callbacks
    # -------------------------------------------------------------------------

    def on_upgrade_proposed(self, callback: callable) -> None:
        """Register callback for new upgrade proposals: callback(UpgradeProposal)."""
        self._on_upgrade_proposed_callbacks.append(callback)

    def on_upgrade_approved(self, callback: callable) -> None:
        """Register callback for approved upgrades: callback(UpgradeProposal)."""
        self._on_upgrade_approved_callbacks.append(callback)

    def on_upgrade_activated(self, callback: callable) -> None:
        """Register callback for activated upgrades: callback(UpgradeProposal)."""
        self._on_upgrade_activated_callbacks.append(callback)
