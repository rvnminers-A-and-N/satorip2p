"""
satorip2p/protocol/versioning.py

Protocol versioning for network upgrades.

Enables smooth protocol upgrades while maintaining backward compatibility:
- Semantic versioning (major.minor.patch)
- Multi-version negotiation
- Feature capability advertising
- Graceful degradation for older peers

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
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum

logger = logging.getLogger("satorip2p.protocol.versioning")


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
