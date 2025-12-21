"""
satorip2p/tests/test_versioning.py

Tests for protocol versioning.
"""

import pytest
from satorip2p.protocol.versioning import (
    ProtocolVersion,
    VersionNegotiator,
    FeatureFlag,
    FeatureSet,
    VersionInfo,
    PeerVersionTracker,
    PROTOCOL_VERSION,
    MIN_SUPPORTED_VERSION,
    CURRENT_VERSION,
    MIN_VERSION,
    VERSION_FEATURES,
    COMPATIBILITY_MATRIX,
    get_version_info,
    get_breaking_changes,
    get_supported_versions,
    get_current_version,
    get_current_features,
    check_compatibility,
    add_version_to_message,
    get_message_version,
    is_message_compatible,
    PROTOCOL_ID_PREFIX,
)


class TestProtocolVersion:
    """Test ProtocolVersion dataclass."""

    def test_version_creation(self):
        """Test creating a version."""
        v = ProtocolVersion(1, 2, 3)
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3

    def test_version_string(self):
        """Test version to string conversion."""
        v = ProtocolVersion(1, 2, 3)
        assert str(v) == "1.2.3"

    def test_version_from_string(self):
        """Test parsing version from string."""
        v = ProtocolVersion.from_string("1.2.3")
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3

    def test_version_from_string_with_prefix(self):
        """Test parsing version with protocol prefix."""
        v = ProtocolVersion.from_string("/satori/1.2.3")
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3

    def test_version_from_string_invalid(self):
        """Test invalid version string."""
        with pytest.raises(ValueError):
            ProtocolVersion.from_string("invalid")

        with pytest.raises(ValueError):
            ProtocolVersion.from_string("1.2")

        with pytest.raises(ValueError):
            ProtocolVersion.from_string("1.2.3.4")

    def test_version_comparison_less_than(self):
        """Test version less than comparison."""
        v1 = ProtocolVersion(1, 0, 0)
        v2 = ProtocolVersion(2, 0, 0)
        assert v1 < v2

        v3 = ProtocolVersion(1, 1, 0)
        assert v1 < v3

        v4 = ProtocolVersion(1, 0, 1)
        assert v1 < v4

    def test_version_comparison_greater_than(self):
        """Test version greater than comparison."""
        v1 = ProtocolVersion(2, 0, 0)
        v2 = ProtocolVersion(1, 0, 0)
        assert v1 > v2

    def test_version_comparison_equal(self):
        """Test version equality."""
        v1 = ProtocolVersion(1, 0, 0)
        v2 = ProtocolVersion(1, 0, 0)
        assert v1 == v2
        assert v1 <= v2
        assert v1 >= v2

    def test_version_to_protocol_id(self):
        """Test conversion to protocol ID."""
        v = ProtocolVersion(1, 0, 0)
        assert v.to_protocol_id() == "/satori/1.0.0"

    def test_version_compatibility_same_major(self):
        """Test compatibility with same major version."""
        v1 = ProtocolVersion(1, 0, 0)
        v2 = ProtocolVersion(1, 5, 2)
        assert v1.is_compatible_with(v2)
        assert v2.is_compatible_with(v1)

    def test_version_compatibility_different_major(self):
        """Test incompatibility with different major version."""
        v1 = ProtocolVersion(1, 0, 0)
        v2 = ProtocolVersion(2, 0, 0)
        assert not v1.is_compatible_with(v2)
        assert not v2.is_compatible_with(v1)

    def test_version_is_newer_than(self):
        """Test is_newer_than method."""
        v1 = ProtocolVersion(2, 0, 0)
        v2 = ProtocolVersion(1, 9, 9)
        assert v1.is_newer_than(v2)
        assert not v2.is_newer_than(v1)

    def test_version_get_features(self):
        """Test getting features for a version."""
        v = ProtocolVersion(1, 0, 0)
        features = v.get_features()
        assert "basic_messaging" in features
        assert "pubsub_gossipsub" in features

    def test_version_supports_feature(self):
        """Test checking feature support."""
        v = ProtocolVersion(1, 0, 0)
        assert v.supports_feature("basic_messaging")
        assert not v.supports_feature("nonexistent_feature")


class TestVersionNegotiator:
    """Test VersionNegotiator class."""

    def test_negotiator_creation(self):
        """Test creating a negotiator."""
        negotiator = VersionNegotiator()
        assert negotiator.min_version == MIN_VERSION
        assert len(negotiator.our_versions) > 0

    def test_negotiator_with_custom_versions(self):
        """Test negotiator with custom version list."""
        negotiator = VersionNegotiator(our_versions=["1.0.0"])
        assert len(negotiator.our_versions) == 1
        assert negotiator.our_versions[0] == ProtocolVersion(1, 0, 0)

    def test_negotiate_exact_match(self):
        """Test negotiating exact version match."""
        negotiator = VersionNegotiator(our_versions=["1.0.0"])
        result = negotiator.negotiate(["1.0.0"])
        assert result == ProtocolVersion(1, 0, 0)

    def test_negotiate_multiple_versions(self):
        """Test negotiating with multiple versions."""
        negotiator = VersionNegotiator(our_versions=["1.0.0", "1.1.0"])
        result = negotiator.negotiate(["1.0.0", "1.1.0"])
        # Should return highest common version
        assert result == ProtocolVersion(1, 1, 0)

    def test_negotiate_no_common(self):
        """Test negotiation with no common version."""
        negotiator = VersionNegotiator(our_versions=["1.0.0"])
        result = negotiator.negotiate(["2.0.0"])
        # No exact match, should try compatible version
        # Since 1.x and 2.x are incompatible, returns None
        assert result is None

    def test_negotiate_compatible_fallback(self):
        """Test negotiation falls back to compatible version."""
        negotiator = VersionNegotiator(our_versions=["1.0.0", "1.1.0"])
        result = negotiator.negotiate(["1.2.0"])
        # 1.2.0 is compatible with 1.x series
        # Should find compatible match
        assert result is not None
        assert result.major == 1

    def test_get_protocol_ids(self):
        """Test getting protocol IDs."""
        negotiator = VersionNegotiator(our_versions=["1.0.0"])
        ids = negotiator.get_protocol_ids()
        assert "/satori/1.0.0" in ids

    def test_is_version_acceptable(self):
        """Test version acceptability check."""
        negotiator = VersionNegotiator(min_version=ProtocolVersion(1, 0, 0))
        assert negotiator.is_version_acceptable("1.0.0")
        assert negotiator.is_version_acceptable("1.5.0")
        assert not negotiator.is_version_acceptable("0.9.0")
        assert not negotiator.is_version_acceptable("invalid")


class TestFeatureSet:
    """Test FeatureSet class."""

    def test_feature_set_from_version(self):
        """Test creating feature set from version."""
        fs = FeatureSet.from_version("1.0.0")
        assert fs.supports("basic_messaging")

    def test_feature_set_from_list(self):
        """Test creating feature set from explicit list."""
        fs = FeatureSet.from_list(["feature1", "feature2"])
        assert fs.supports("feature1")
        assert fs.supports("feature2")
        assert not fs.supports("feature3")

    def test_feature_set_supports_all(self):
        """Test checking multiple features."""
        fs = FeatureSet.from_list(["a", "b", "c"])
        assert fs.supports_all(["a", "b"])
        assert not fs.supports_all(["a", "d"])

    def test_feature_set_supports_any(self):
        """Test checking any feature."""
        fs = FeatureSet.from_list(["a", "b"])
        assert fs.supports_any(["a", "x"])
        assert fs.supports_any(["b", "y"])
        assert not fs.supports_any(["x", "y"])

    def test_feature_set_common(self):
        """Test finding common features."""
        fs1 = FeatureSet.from_list(["a", "b", "c"])
        fs2 = FeatureSet.from_list(["b", "c", "d"])
        common = fs1.common_features(fs2)
        assert common == {"b", "c"}

    def test_feature_set_to_list(self):
        """Test converting to list."""
        fs = FeatureSet.from_list(["b", "a", "c"])
        lst = fs.to_list()
        assert lst == ["a", "b", "c"]  # Sorted


class TestPeerVersionTracker:
    """Test PeerVersionTracker class."""

    def test_tracker_creation(self):
        """Test creating a tracker."""
        tracker = PeerVersionTracker()
        assert len(tracker._peer_versions) == 0

    def test_register_peer(self):
        """Test registering a peer."""
        tracker = PeerVersionTracker()
        tracker.register_peer("peer1", "1.0.0")
        assert tracker.get_peer_version("peer1") == ProtocolVersion(1, 0, 0)

    def test_register_peer_with_features(self):
        """Test registering peer with explicit features."""
        tracker = PeerVersionTracker()
        tracker.register_peer("peer1", "1.0.0", features=["custom_feature"])
        fs = tracker.get_peer_features("peer1")
        assert fs.supports("custom_feature")

    def test_unregister_peer(self):
        """Test unregistering a peer."""
        tracker = PeerVersionTracker()
        tracker.register_peer("peer1", "1.0.0")
        tracker.unregister_peer("peer1")
        assert tracker.get_peer_version("peer1") is None

    def test_peer_supports_feature(self):
        """Test checking peer feature support."""
        tracker = PeerVersionTracker()
        tracker.register_peer("peer1", "1.0.0")
        assert tracker.peer_supports_feature("peer1", "basic_messaging")
        assert not tracker.peer_supports_feature("peer1", "nonexistent")
        assert not tracker.peer_supports_feature("unknown", "basic_messaging")

    def test_get_peers_with_feature(self):
        """Test getting peers with feature."""
        tracker = PeerVersionTracker()
        tracker.register_peer("peer1", "1.0.0")
        tracker.register_peer("peer2", "1.0.0", features=["special"])

        peers = tracker.get_peers_with_feature("basic_messaging")
        assert "peer1" in peers

        peers = tracker.get_peers_with_feature("special")
        assert "peer2" in peers
        assert "peer1" not in peers

    def test_get_compatible_peers(self):
        """Test getting compatible peers."""
        tracker = PeerVersionTracker()
        tracker.register_peer("peer1", "1.0.0")
        # Note: tracker doesn't actually register incompatible peers normally
        # but we can test the method exists
        peers = tracker.get_compatible_peers()
        assert "peer1" in peers

    def test_get_network_stats(self):
        """Test getting network statistics."""
        tracker = PeerVersionTracker()
        tracker.register_peer("peer1", "1.0.0")
        tracker.register_peer("peer2", "1.0.0")
        tracker.register_peer("peer3", "1.1.0")

        stats = tracker.get_network_stats()
        assert stats.get("1.0.0") == 2
        assert stats.get("1.1.0") == 1

    def test_get_upgrade_progress(self):
        """Test getting upgrade progress."""
        tracker = PeerVersionTracker()
        tracker.register_peer("peer1", "1.0.0")
        tracker.register_peer("peer2", "1.1.0")
        tracker.register_peer("peer3", "1.1.0")

        upgraded, total = tracker.get_upgrade_progress("1.1.0")
        assert total == 3
        assert upgraded == 2  # peer2 and peer3


class TestMessageVersioning:
    """Test message version helpers."""

    def test_add_version_to_message(self):
        """Test adding version to message."""
        msg = {"type": "test"}
        result = add_version_to_message(msg)
        assert result["protocol_version"] == PROTOCOL_VERSION

    def test_get_message_version(self):
        """Test extracting version from message."""
        msg = {"protocol_version": "1.2.3"}
        version = get_message_version(msg)
        assert version == ProtocolVersion(1, 2, 3)

    def test_get_message_version_default(self):
        """Test default version for messages without version."""
        msg = {}
        version = get_message_version(msg)
        assert version == MIN_VERSION

    def test_is_message_compatible(self):
        """Test message compatibility check."""
        compatible_msg = {"protocol_version": "1.0.0"}
        assert is_message_compatible(compatible_msg)

        # Version 1.x should be compatible with current 1.x
        compatible_msg2 = {"protocol_version": "1.5.0"}
        assert is_message_compatible(compatible_msg2)


class TestCompatibilityMatrix:
    """Test compatibility matrix functionality."""

    def test_get_version_info(self):
        """Test getting version info."""
        info = get_version_info("1.0.0")
        assert info is not None
        assert info.version == "1.0.0"
        assert info.name == "Initial Release"

    def test_get_version_info_unknown(self):
        """Test getting info for unknown version."""
        info = get_version_info("99.99.99")
        assert info is None

    def test_get_breaking_changes(self):
        """Test getting breaking changes between versions."""
        # 1.0.0 to 1.1.0 should have no breaking changes
        changes = get_breaking_changes("1.0.0", "1.1.0")
        assert len(changes) == 0

        # 1.0.0 to 2.0.0 should have breaking changes
        changes = get_breaking_changes("1.0.0", "2.0.0")
        assert "prediction_scoring_v1" in changes

    def test_get_supported_versions(self):
        """Test getting all supported versions."""
        versions = get_supported_versions()
        assert "1.0.0" in versions

    def test_get_current_version(self):
        """Test getting current version string."""
        version = get_current_version()
        assert version == PROTOCOL_VERSION

    def test_get_current_features(self):
        """Test getting current features."""
        features = get_current_features()
        assert "basic_messaging" in features


class TestCheckCompatibility:
    """Test check_compatibility function."""

    def test_compatible_version(self):
        """Test compatible version check."""
        is_compat, msg = check_compatibility("1.0.0")
        assert is_compat
        assert msg == "Compatible"

    def test_incompatible_major_version(self):
        """Test incompatible major version."""
        is_compat, msg = check_compatibility("2.0.0")
        assert not is_compat
        assert "incompatible major version" in msg.lower()

    def test_below_minimum_version(self):
        """Test version below minimum."""
        is_compat, msg = check_compatibility("0.1.0")
        assert not is_compat
        assert "below minimum" in msg.lower()

    def test_invalid_version_format(self):
        """Test invalid version format."""
        is_compat, msg = check_compatibility("invalid")
        assert not is_compat
        assert "invalid" in msg.lower()


class TestFeatureFlags:
    """Test FeatureFlag enum."""

    def test_feature_flag_values(self):
        """Test feature flag values."""
        assert FeatureFlag.BASIC_MESSAGING.value == "basic_messaging"
        assert FeatureFlag.PUBSUB_GOSSIPSUB.value == "pubsub_gossipsub"
        assert FeatureFlag.DHT_KADEMLIA.value == "dht_kademlia"

    def test_feature_flag_v11_features(self):
        """Test v1.1 feature flags."""
        assert FeatureFlag.DELEGATION_PROTOCOL.value == "delegation_protocol"
        assert FeatureFlag.LENDING_PROTOCOL.value == "lending_protocol"

    def test_feature_flag_v20_features(self):
        """Test v2.0 feature flags."""
        assert FeatureFlag.PREDICTION_SCORING_V2.value == "prediction_scoring_v2"
        assert FeatureFlag.BANDWIDTH_ACCOUNTING.value == "bandwidth_accounting"


class TestConstants:
    """Test module constants."""

    def test_protocol_version_format(self):
        """Test protocol version follows semver."""
        parts = PROTOCOL_VERSION.split(".")
        assert len(parts) == 3
        for part in parts:
            assert part.isdigit()

    def test_current_version_matches_constant(self):
        """Test CURRENT_VERSION matches PROTOCOL_VERSION."""
        assert str(CURRENT_VERSION) == PROTOCOL_VERSION

    def test_min_version_lte_current(self):
        """Test MIN_VERSION is less than or equal to CURRENT_VERSION."""
        assert MIN_VERSION <= CURRENT_VERSION

    def test_protocol_id_prefix(self):
        """Test protocol ID prefix."""
        assert PROTOCOL_ID_PREFIX == "/satori"

    def test_version_features_structure(self):
        """Test VERSION_FEATURES has expected structure."""
        assert "1.0.0" in VERSION_FEATURES
        assert isinstance(VERSION_FEATURES["1.0.0"], list)
        assert len(VERSION_FEATURES["1.0.0"]) > 0

    def test_compatibility_matrix_structure(self):
        """Test COMPATIBILITY_MATRIX has expected structure."""
        assert "1.0.0" in COMPATIBILITY_MATRIX
        info = COMPATIBILITY_MATRIX["1.0.0"]
        assert isinstance(info, VersionInfo)
        assert info.version == "1.0.0"


class TestVersionInfoDataclass:
    """Test VersionInfo dataclass."""

    def test_version_info_creation(self):
        """Test creating VersionInfo."""
        info = VersionInfo(
            version="1.0.0",
            name="Test",
            description="Test version",
            features=["feature1"],
        )
        assert info.version == "1.0.0"
        assert info.name == "Test"
        assert "feature1" in info.features

    def test_version_info_defaults(self):
        """Test VersionInfo default values."""
        info = VersionInfo(
            version="1.0.0",
            name="Test",
            description="Test",
            features=[],
        )
        assert info.breaking_changes == []
        assert info.deprecated_features == []
        assert info.migration_notes == ""

    def test_version_info_with_breaking_changes(self):
        """Test VersionInfo with breaking changes."""
        info = VersionInfo(
            version="2.0.0",
            name="Major Update",
            description="Breaking changes",
            features=["new_feature"],
            breaking_changes=["old_feature"],
        )
        assert "old_feature" in info.breaking_changes


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_version_with_leading_zeros(self):
        """Test version parsing with leading zeros."""
        v = ProtocolVersion.from_string("01.02.03")
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3

    def test_version_with_whitespace(self):
        """Test version parsing with whitespace."""
        v = ProtocolVersion.from_string("  1.0.0  ")
        assert v.major == 1

    def test_negotiate_empty_list(self):
        """Test negotiation with empty version list."""
        negotiator = VersionNegotiator(our_versions=["1.0.0"])
        result = negotiator.negotiate([])
        assert result is None

    def test_negotiate_invalid_versions(self):
        """Test negotiation ignores invalid versions."""
        negotiator = VersionNegotiator(our_versions=["1.0.0"])
        result = negotiator.negotiate(["invalid", "also_invalid"])
        assert result is None

    def test_negotiate_mixed_valid_invalid(self):
        """Test negotiation with mix of valid and invalid."""
        negotiator = VersionNegotiator(our_versions=["1.0.0"])
        result = negotiator.negotiate(["invalid", "1.0.0", "bad"])
        assert result == ProtocolVersion(1, 0, 0)

    def test_tracker_register_invalid_version(self):
        """Test tracker handles invalid version registration."""
        tracker = PeerVersionTracker()
        tracker.register_peer("peer1", "invalid")
        # Should not crash, peer won't be registered
        assert tracker.get_peer_version("peer1") is None

    def test_message_version_invalid_format(self):
        """Test message with invalid version format."""
        msg = {"protocol_version": "not_a_version"}
        version = get_message_version(msg)
        # Should return MIN_VERSION as fallback
        assert version == MIN_VERSION


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
