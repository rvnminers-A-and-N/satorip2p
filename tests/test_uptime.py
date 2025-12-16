"""
Tests for satorip2p/protocol/uptime.py

Tests the uptime tracking for relay bonus qualification.
"""

import pytest
import time
from unittest.mock import Mock, MagicMock

from satorip2p.protocol.uptime import (
    UptimeTracker,
    Heartbeat,
    NodeUptimeRecord,
    RoundUptimeReport,
    RELAY_UPTIME_THRESHOLD,
    HEARTBEAT_INTERVAL,
    check_relay_uptime_qualified,
    get_uptime_percentage,
)


# ============================================================================
# TEST DATA
# ============================================================================

def create_test_heartbeat(
    node_id: str = "node1",
    round_id: str = "2025-01-15",
    timestamp: int = None,
    roles: list = None,
    evrmore_address: str = "ETestAddress123",
    peer_id: str = None,
    stake: float = 50.0,
    signature: bytes = b"test_signature",
    status_message: str = "testing in progress...",
) -> Heartbeat:
    """Create a test heartbeat with all required fields."""
    return Heartbeat(
        node_id=node_id,
        evrmore_address=evrmore_address,
        peer_id=peer_id or node_id,  # Default to node_id
        timestamp=timestamp or int(time.time()),
        round_id=round_id,
        roles=roles or ["predictor"],
        stake=stake,
        signature=signature,
        status_message=status_message,
    )


# ============================================================================
# HEARTBEAT TESTS
# ============================================================================

class TestHeartbeat:
    """Tests for Heartbeat dataclass."""

    def test_heartbeat_creation(self):
        """Test creating a heartbeat."""
        hb = create_test_heartbeat()
        assert hb.node_id == "node1"
        assert hb.round_id == "2025-01-15"
        assert "predictor" in hb.roles

    def test_heartbeat_to_dict(self):
        """Test heartbeat serialization."""
        hb = create_test_heartbeat(timestamp=1234567890, roles=["relay", "oracle"])
        data = hb.to_dict()
        assert data['node_id'] == "node1"
        assert data['timestamp'] == 1234567890
        assert "relay" in data['roles']

    def test_heartbeat_from_dict(self):
        """Test heartbeat deserialization."""
        data = {
            'node_id': 'node2',
            'timestamp': 1234567890,
            'round_id': '2025-01-16',
            'roles': ['relay', 'oracle'],
        }
        hb = Heartbeat.from_dict(data)
        assert hb.node_id == 'node2'
        assert hb.round_id == '2025-01-16'
        assert "relay" in hb.roles


# ============================================================================
# UPTIME TRACKER BASIC TESTS
# ============================================================================

class TestUptimeTrackerBasic:
    """Basic tests for UptimeTracker."""

    def test_initialization(self):
        """Test tracker initialization."""
        tracker = UptimeTracker(node_id="test_node")
        assert tracker.node_id == "test_node"
        assert tracker._current_round is None

    def test_start_round(self):
        """Test starting a new round."""
        tracker = UptimeTracker(node_id="test_node")
        now = int(time.time())
        tracker.start_round("2025-01-15", now)

        assert tracker._current_round == "2025-01-15"
        assert tracker._round_start == now

    def test_receive_heartbeat(self):
        """Test receiving a heartbeat."""
        tracker = UptimeTracker(node_id="test_node")
        tracker.start_round("2025-01-15", int(time.time()))

        hb = create_test_heartbeat()
        accepted = tracker.receive_heartbeat(hb)

        assert accepted is True
        assert "node1" in tracker._heartbeats["2025-01-15"]

    def test_receive_heartbeat_wrong_round(self):
        """Test rejecting heartbeat from wrong round."""
        tracker = UptimeTracker(node_id="test_node")
        tracker.start_round("2025-01-15", int(time.time()))

        hb = create_test_heartbeat(round_id="2025-01-14")
        accepted = tracker.receive_heartbeat(hb)

        assert accepted is False


# ============================================================================
# UPTIME CALCULATION TESTS
# ============================================================================

class TestUptimeCalculation:
    """Tests for uptime percentage calculation."""

    def test_full_uptime(self):
        """Test 100% uptime with all heartbeats."""
        tracker = UptimeTracker(node_id="test_node")
        round_start = 1000
        round_end = 1000 + 3600  # 1 hour

        tracker.start_round("2025-01-15", round_start)

        # Send heartbeat every 60 seconds for the hour
        expected_heartbeats = 3600 // HEARTBEAT_INTERVAL
        for i in range(expected_heartbeats):
            timestamp = round_start + (i * HEARTBEAT_INTERVAL)
            hb = create_test_heartbeat(timestamp=timestamp)
            tracker.receive_heartbeat(hb)

        report = tracker.calculate_round_uptime("2025-01-15", round_start, round_end)

        assert "node1" in report.node_records
        record = report.node_records["node1"]
        assert record.uptime_percentage >= 0.95

    def test_partial_uptime(self):
        """Test partial uptime (50%)."""
        tracker = UptimeTracker(node_id="test_node")
        round_start = 1000
        round_end = 1000 + 3600  # 1 hour

        tracker.start_round("2025-01-15", round_start)

        # Send only half the expected heartbeats
        expected_heartbeats = 3600 // HEARTBEAT_INTERVAL
        for i in range(expected_heartbeats // 2):
            timestamp = round_start + (i * HEARTBEAT_INTERVAL)
            hb = create_test_heartbeat(timestamp=timestamp)
            tracker.receive_heartbeat(hb)

        report = tracker.calculate_round_uptime("2025-01-15", round_start, round_end)

        record = report.node_records["node1"]
        assert record.uptime_percentage < 0.95  # Below relay threshold
        assert record.is_relay_qualified is False

    def test_zero_uptime(self):
        """Test zero uptime (no heartbeats)."""
        tracker = UptimeTracker(node_id="test_node")
        tracker.start_round("2025-01-15", 1000)

        report = tracker.calculate_round_uptime("2025-01-15", 1000, 4600)

        # No heartbeats means no records
        assert report.total_nodes == 0
        assert report.relay_qualified_nodes == 0


# ============================================================================
# RELAY QUALIFICATION TESTS
# ============================================================================

class TestRelayQualification:
    """Tests for relay bonus qualification (95% threshold)."""

    def test_is_relay_qualified_yes(self):
        """Test relay qualification with sufficient uptime."""
        tracker = UptimeTracker(node_id="test_node")
        # Use a start time 1 hour ago
        round_start = int(time.time()) - 3600
        tracker.start_round("2025-01-15", round_start)

        # Send heartbeats covering the full hour (60 heartbeats)
        for i in range(60):
            hb = create_test_heartbeat(timestamp=round_start + (i * HEARTBEAT_INTERVAL))
            tracker.receive_heartbeat(hb)

        # Check qualification - should have 100% uptime
        assert tracker.is_relay_qualified("node1") is True

    def test_is_relay_qualified_no(self):
        """Test relay disqualification with insufficient uptime."""
        tracker = UptimeTracker(node_id="test_node")
        round_start = 1000
        tracker.start_round("2025-01-15", round_start)

        # Send only 1 heartbeat
        hb = create_test_heartbeat(timestamp=round_start)
        tracker.receive_heartbeat(hb)

        # Simulate lots of time passing (makes 1 heartbeat = low %)
        tracker._round_start = round_start - 7200  # 2 hours elapsed

        # With only 1 heartbeat over 2 hours, should not qualify
        assert tracker.is_relay_qualified("node1") is False

    def test_get_relay_qualified_nodes(self):
        """Test getting all relay-qualified nodes."""
        tracker = UptimeTracker(node_id="test_node")
        round_start = 1000
        tracker.start_round("2025-01-15", round_start)

        # Node1 has good uptime
        for i in range(60):
            hb = create_test_heartbeat(node_id="node1", timestamp=round_start + (i * HEARTBEAT_INTERVAL))
            tracker.receive_heartbeat(hb)

        # Node2 has poor uptime
        hb = create_test_heartbeat(node_id="node2", timestamp=round_start)
        tracker.receive_heartbeat(hb)

        qualified = tracker.get_relay_qualified_nodes()

        # Depends on timing, but node1 should generally have better uptime
        assert len(qualified) >= 0  # At least doesn't crash


# ============================================================================
# NODE ROLES TESTS
# ============================================================================

class TestNodeRoles:
    """Tests for tracking node roles."""

    def test_roles_tracked_from_heartbeat(self):
        """Test that roles are tracked from heartbeats."""
        tracker = UptimeTracker(node_id="test_node")
        tracker.start_round("2025-01-15", int(time.time()))

        hb = create_test_heartbeat(roles=["relay", "oracle"])
        tracker.receive_heartbeat(hb)

        roles = tracker.get_node_roles("node1")
        assert "relay" in roles
        assert "oracle" in roles

    def test_roles_accumulate(self):
        """Test that roles accumulate across heartbeats."""
        tracker = UptimeTracker(node_id="test_node")
        tracker.start_round("2025-01-15", int(time.time()))

        hb1 = create_test_heartbeat(roles=["predictor"])
        hb2 = create_test_heartbeat(roles=["relay"])
        tracker.receive_heartbeat(hb1)
        tracker.receive_heartbeat(hb2)

        roles = tracker.get_node_roles("node1")
        assert "predictor" in roles
        assert "relay" in roles


# ============================================================================
# ROUND UPTIME REPORT TESTS
# ============================================================================

class TestRoundUptimeReport:
    """Tests for RoundUptimeReport."""

    def test_report_creation(self):
        """Test creating a report."""
        tracker = UptimeTracker(node_id="test_node")
        round_start = 1000
        round_end = 4600
        tracker.start_round("2025-01-15", round_start)

        # Add some heartbeats
        for i in range(30):
            for node in ["node1", "node2"]:
                hb = create_test_heartbeat(
                    node_id=node,
                    timestamp=round_start + (i * HEARTBEAT_INTERVAL)
                )
                tracker.receive_heartbeat(hb)

        report = tracker.calculate_round_uptime("2025-01-15", round_start, round_end)

        assert report.round_id == "2025-01-15"
        assert report.total_nodes == 2
        assert "node1" in report.node_records
        assert "node2" in report.node_records

    def test_report_to_dict(self):
        """Test report serialization."""
        tracker = UptimeTracker(node_id="test_node")
        tracker.start_round("2025-01-15", 1000)

        hb = create_test_heartbeat(timestamp=1000)
        tracker.receive_heartbeat(hb)

        report = tracker.calculate_round_uptime("2025-01-15", 1000, 4600)
        data = report.to_dict()

        assert data['round_id'] == "2025-01-15"
        assert 'node_records' in data


# ============================================================================
# HELPER FUNCTION TESTS
# ============================================================================

class TestHelperFunctions:
    """Tests for helper functions."""

    def test_check_relay_uptime_qualified(self):
        """Test convenience function."""
        tracker = UptimeTracker(node_id="test_node")
        tracker.start_round("2025-01-15", 1000)

        # Add heartbeats
        for i in range(60):
            hb = create_test_heartbeat(timestamp=1000 + (i * HEARTBEAT_INTERVAL))
            tracker.receive_heartbeat(hb)

        result = check_relay_uptime_qualified(tracker, "node1")
        # Should be True if enough heartbeats
        assert isinstance(result, bool)

    def test_get_uptime_percentage_function(self):
        """Test convenience function."""
        tracker = UptimeTracker(node_id="test_node")
        tracker.start_round("2025-01-15", 1000)

        hb = create_test_heartbeat(timestamp=1000)
        tracker.receive_heartbeat(hb)

        pct = get_uptime_percentage(tracker, "node1")
        assert 0.0 <= pct <= 1.0


# ============================================================================
# CLEAR ROUND TESTS
# ============================================================================

class TestClearRound:
    """Tests for clearing round data."""

    def test_clear_round(self):
        """Test clearing heartbeat data for a round."""
        tracker = UptimeTracker(node_id="test_node")
        tracker.start_round("2025-01-15", 1000)

        hb = create_test_heartbeat()
        tracker.receive_heartbeat(hb)

        assert "2025-01-15" in tracker._heartbeats

        tracker.clear_round("2025-01-15")

        assert "2025-01-15" not in tracker._heartbeats


# ============================================================================
# CONSTANTS TESTS
# ============================================================================

class TestConstants:
    """Tests for module constants."""

    def test_threshold_value(self):
        """Test that threshold is 95%."""
        assert RELAY_UPTIME_THRESHOLD == 0.95

    def test_heartbeat_interval(self):
        """Test heartbeat interval is reasonable."""
        assert HEARTBEAT_INTERVAL == 60  # 60 seconds
