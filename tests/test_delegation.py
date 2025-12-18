"""
Tests for satorip2p/protocol/delegation.py

Tests the P2P delegation/proxy protocol for stake delegation.
"""

import pytest
import time
import json
from unittest.mock import Mock, MagicMock, AsyncMock

from satorip2p.protocol.delegation import (
    DelegationManager,
    DelegationRecord,
    CharityUpdate,
    DELEGATION_ANNOUNCEMENT_TOPIC,
    DELEGATION_REMOVAL_TOPIC,
    DELEGATION_CHARITY_TOPIC,
)


# ============================================================================
# TEST DATA
# ============================================================================

def create_test_delegation_record(
    parent_address: str = "EParentTest123",
    child_address: str = "EChildTest456",
    vault_address: str = "EVaultTest789",
    amount: float = 1000.0,
    pointed: bool = True,
    charity: bool = False,
    alias: str = "Test Delegation",
    timestamp: int = None,
    signature: str = "",
) -> DelegationRecord:
    """Create a test delegation record."""
    return DelegationRecord(
        parent_address=parent_address,
        child_address=child_address,
        vault_address=vault_address,
        amount=amount,
        pointed=pointed,
        charity=charity,
        alias=alias,
        timestamp=timestamp or int(time.time()),
        signature=signature,
    )


def create_test_charity_update(
    child_address: str = "EChildTest456",
    parent_address: str = "EParentTest123",
    charity: bool = True,
    timestamp: int = None,
    signature: str = "",
) -> CharityUpdate:
    """Create a test charity update."""
    return CharityUpdate(
        child_address=child_address,
        parent_address=parent_address,
        charity=charity,
        timestamp=timestamp or int(time.time()),
        signature=signature,
    )


def create_mock_peers():
    """Create mock Peers object."""
    mock_peers = Mock()
    mock_peers.publish = AsyncMock()
    mock_peers.subscribe = AsyncMock()
    mock_peers.dht_get = AsyncMock(return_value=None)
    mock_peers.dht_put = AsyncMock()
    mock_peers.rendezvous_register = AsyncMock()
    mock_peers.rendezvous_discover = AsyncMock(return_value=[])
    return mock_peers


def create_mock_wallet():
    """Create mock wallet/identity."""
    mock_wallet = Mock()
    mock_wallet.address = "ETestWallet789"
    mock_wallet.pubkey = "testpubkey789"
    mock_wallet.sign = Mock(return_value=b"testsig123")
    mock_wallet.verify = Mock(return_value=True)
    return mock_wallet


# ============================================================================
# DELEGATION RECORD TESTS
# ============================================================================

class TestDelegationRecord:
    """Tests for DelegationRecord dataclass."""

    def test_delegation_record_creation(self):
        """Test creating a delegation record."""
        record = create_test_delegation_record()
        assert record.parent_address == "EParentTest123"
        assert record.child_address == "EChildTest456"
        assert record.amount == 1000.0
        assert record.pointed is True
        assert record.charity is False

    def test_delegation_record_to_dict(self):
        """Test delegation record serialization."""
        record = create_test_delegation_record(timestamp=1234567890)
        data = record.to_dict()
        assert data['parent_address'] == "EParentTest123"
        assert data['child_address'] == "EChildTest456"
        assert data['amount'] == 1000.0
        assert data['pointed'] is True
        assert data['charity'] is False
        assert data['timestamp'] == 1234567890

    def test_delegation_record_from_dict(self):
        """Test delegation record deserialization."""
        data = {
            'parent_address': 'EParent789',
            'child_address': 'EChild789',
            'vault_address': 'EVault789',
            'amount': 2000.0,
            'pointed': False,
            'charity': True,
            'alias': 'Test Alias',
            'timestamp': 1234567890,
            'signature': 'sig789',
        }
        record = DelegationRecord.from_dict(data)
        assert record.parent_address == 'EParent789'
        assert record.child_address == 'EChild789'
        assert record.amount == 2000.0
        assert record.charity is True

    def test_delegation_record_hash_deterministic(self):
        """Test that delegation record hash is deterministic."""
        record1 = create_test_delegation_record(timestamp=1234567890)
        record2 = create_test_delegation_record(timestamp=1234567890)
        assert record1.get_record_hash() == record2.get_record_hash()

    def test_different_records_different_hashes(self):
        """Test that different records have different hashes."""
        record1 = create_test_delegation_record(child_address="EChild1")
        record2 = create_test_delegation_record(child_address="EChild2")
        assert record1.get_record_hash() != record2.get_record_hash()


# ============================================================================
# CHARITY UPDATE TESTS
# ============================================================================

class TestCharityUpdate:
    """Tests for CharityUpdate dataclass."""

    def test_charity_update_creation(self):
        """Test creating a charity update."""
        update = create_test_charity_update()
        assert update.child_address == "EChildTest456"
        assert update.parent_address == "EParentTest123"
        assert update.charity is True

    def test_charity_update_to_dict(self):
        """Test charity update serialization."""
        update = create_test_charity_update(timestamp=1234567890)
        data = update.to_dict()
        assert data['child_address'] == "EChildTest456"
        assert data['parent_address'] == "EParentTest123"
        assert data['charity'] is True
        assert data['timestamp'] == 1234567890

    def test_charity_update_from_dict(self):
        """Test charity update deserialization."""
        data = {
            'child_address': 'EChild999',
            'parent_address': 'EParent999',
            'charity': False,
            'timestamp': 1234567890,
            'signature': 'sig999',
        }
        update = CharityUpdate.from_dict(data)
        assert update.child_address == 'EChild999'
        assert update.charity is False


# ============================================================================
# DELEGATION MANAGER BASIC TESTS
# ============================================================================

class TestDelegationManagerBasic:
    """Basic tests for DelegationManager."""

    def test_initialization(self):
        """Test manager initialization."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = DelegationManager(peers=peers, wallet=wallet)

        assert manager._peers == peers
        assert manager._wallet == wallet
        assert manager._my_address == "ETestWallet789"

    @pytest.mark.asyncio
    async def test_delegate_to(self):
        """Test delegating to a parent."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EChildAddress"
        manager = DelegationManager(peers=peers, wallet=wallet)

        result = await manager.delegate_to(
            parent_address="EParentAddress",
            amount=1000.0,
            alias="My Delegation",
        )

        assert result is not None
        assert result.parent_address == "EParentAddress"
        assert result.child_address == "EChildAddress"
        assert result.amount == 1000.0
        peers.publish.assert_called()

    @pytest.mark.asyncio
    async def test_remove_delegation(self):
        """Test removing a delegation."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EChildAddress"
        manager = DelegationManager(peers=peers, wallet=wallet)

        # First delegate
        await manager.delegate_to(
            parent_address="EParentAddress",
            amount=1000.0,
        )

        # Then remove
        result = await manager.remove_delegation()

        assert result is True
        assert manager._my_delegation is None

    @pytest.mark.asyncio
    async def test_get_my_delegate(self):
        """Test getting my current delegation."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EChildAddress"
        manager = DelegationManager(peers=peers, wallet=wallet)

        # No delegation yet
        result = await manager.get_my_delegate()
        assert result is None

        # Add delegation
        await manager.delegate_to(
            parent_address="EParentAddress",
            amount=1000.0,
        )

        # Now should return the delegation
        result = await manager.get_my_delegate()
        assert result is not None
        assert result.parent_address == "EParentAddress"


# ============================================================================
# DELEGATION MANAGER PROXY TESTS
# ============================================================================

class TestDelegationManagerProxy:
    """Tests for proxy (parent) operations."""

    @pytest.mark.asyncio
    async def test_get_proxy_children(self):
        """Test getting children delegating to me."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EParentAddress"
        manager = DelegationManager(peers=peers, wallet=wallet)

        # Add some delegations pointing to this parent
        manager._delegations["EChild1"] = create_test_delegation_record(
            parent_address="EParentAddress",
            child_address="EChild1",
        )
        manager._delegations["EChild2"] = create_test_delegation_record(
            parent_address="EParentAddress",
            child_address="EChild2",
        )
        manager._delegations["EChild3"] = create_test_delegation_record(
            parent_address="EDifferentParent",  # Different parent
            child_address="EChild3",
        )

        children = await manager.get_proxy_children()

        assert len(children) == 2
        assert all(c.parent_address == "EParentAddress" for c in children)

    @pytest.mark.asyncio
    async def test_set_charity_status_to_true(self):
        """Test marking a child as charity."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EParentAddress"
        manager = DelegationManager(peers=peers, wallet=wallet)

        # Add a child delegation
        manager._delegations["EChildAddress"] = create_test_delegation_record(
            parent_address="EParentAddress",
            child_address="EChildAddress",
            charity=False,
        )

        result = await manager.set_charity_status(
            child_address="EChildAddress",
            charity=True,
        )

        assert result is True
        assert manager._delegations["EChildAddress"].charity is True
        peers.publish.assert_called()

    @pytest.mark.asyncio
    async def test_set_charity_status_to_false(self):
        """Test unmarking a child as charity."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EParentAddress"
        manager = DelegationManager(peers=peers, wallet=wallet)

        # Add a charity child
        manager._delegations["EChildAddress"] = create_test_delegation_record(
            parent_address="EParentAddress",
            child_address="EChildAddress",
            charity=True,
        )

        result = await manager.set_charity_status(
            child_address="EChildAddress",
            charity=False,
        )

        assert result is True
        assert manager._delegations["EChildAddress"].charity is False

    @pytest.mark.asyncio
    async def test_remove_proxy_child(self):
        """Test removing a child from proxy list."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EParentAddress"
        manager = DelegationManager(peers=peers, wallet=wallet)

        # Add a child
        manager._delegations["EChildAddress"] = create_test_delegation_record(
            parent_address="EParentAddress",
            child_address="EChildAddress",
        )

        result = await manager.remove_proxy_child("EChildAddress")

        assert result is True
        assert "EChildAddress" not in manager._delegations


# ============================================================================
# DELEGATION MANAGER PUBSUB TESTS
# ============================================================================

class TestDelegationManagerPubSub:
    """Tests for PubSub message handling."""

    @pytest.mark.asyncio
    async def test_handle_delegation_announcement(self):
        """Test handling incoming delegation announcements."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = DelegationManager(peers=peers, wallet=wallet)

        # Simulate receiving a delegation announcement
        record = create_test_delegation_record(
            parent_address="ERemoteParent",
            child_address="ERemoteChild",
        )
        message_data = json.dumps(record.to_dict()).encode()

        await manager._handle_delegation_announcement(message_data)

        assert "ERemoteChild" in manager._delegations
        assert manager._delegations["ERemoteChild"].parent_address == "ERemoteParent"

    @pytest.mark.asyncio
    async def test_handle_delegation_removal(self):
        """Test handling incoming delegation removal."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = DelegationManager(peers=peers, wallet=wallet)

        # Add existing delegation
        manager._delegations["ERemoteChild"] = create_test_delegation_record(
            parent_address="ERemoteParent",
            child_address="ERemoteChild",
        )

        # Simulate removal message
        removal_data = {
            'child_address': 'ERemoteChild',
            'timestamp': int(time.time()),
            'signature': 'sig',
        }
        message_data = json.dumps(removal_data).encode()

        await manager._handle_delegation_removal(message_data)

        # Should be removed
        assert "ERemoteChild" not in manager._delegations

    @pytest.mark.asyncio
    async def test_handle_charity_update(self):
        """Test handling incoming charity updates."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = DelegationManager(peers=peers, wallet=wallet)

        # Add existing delegation
        manager._delegations["ERemoteChild"] = create_test_delegation_record(
            parent_address="ERemoteParent",
            child_address="ERemoteChild",
            charity=False,
        )

        # Simulate charity update
        update = create_test_charity_update(
            child_address="ERemoteChild",
            parent_address="ERemoteParent",
            charity=True,
        )
        message_data = json.dumps(update.to_dict()).encode()

        await manager._handle_charity_update(message_data)

        assert manager._delegations["ERemoteChild"].charity is True


# ============================================================================
# DELEGATION MANAGER DHT TESTS
# ============================================================================

class TestDelegationManagerDHT:
    """Tests for DHT operations."""

    @pytest.mark.asyncio
    async def test_store_to_dht(self):
        """Test storing delegation to DHT."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = DelegationManager(peers=peers, wallet=wallet)

        record = create_test_delegation_record()
        await manager._store_delegation_to_dht(record)

        peers.dht_put.assert_called()

    @pytest.mark.asyncio
    async def test_retrieve_from_dht(self):
        """Test retrieving delegation from DHT."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = DelegationManager(peers=peers, wallet=wallet)

        # Mock DHT returning data
        record = create_test_delegation_record(child_address="ETestChild")
        peers.dht_get = AsyncMock(return_value=json.dumps(record.to_dict()).encode())

        result = await manager._get_delegation_from_dht("ETestChild")

        assert result is not None
        assert result.child_address == "ETestChild"


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestDelegationManagerIntegration:
    """Integration tests for DelegationManager."""

    @pytest.mark.asyncio
    async def test_full_delegation_flow(self):
        """Test the complete delegation flow."""
        peers = create_mock_peers()

        # Create parent (the one receiving delegations)
        parent_wallet = create_mock_wallet()
        parent_wallet.address = "EParentAddress"
        parent_manager = DelegationManager(peers=peers, wallet=parent_wallet)

        # Create child (the one delegating)
        child_wallet = create_mock_wallet()
        child_wallet.address = "EChildAddress"
        child_manager = DelegationManager(peers=peers, wallet=child_wallet)

        # Child delegates to parent
        delegation = await child_manager.delegate_to(
            parent_address="EParentAddress",
            amount=1000.0,
            alias="My Delegation",
        )
        assert delegation is not None

        # Simulate parent receiving the delegation (via PubSub)
        message_data = json.dumps(delegation.to_dict()).encode()
        await parent_manager._handle_delegation_announcement(message_data)

        # Parent should see the child
        children = await parent_manager.get_proxy_children()
        assert len(children) == 1
        assert children[0].child_address == "EChildAddress"

        # Parent marks child as charity
        result = await parent_manager.set_charity_status(
            child_address="EChildAddress",
            charity=True,
        )
        assert result is True

        # Verify charity status
        children = await parent_manager.get_proxy_children()
        assert children[0].charity is True

        # Child removes delegation
        result = await child_manager.remove_delegation()
        assert result is True

        # Verify child's delegation is gone
        my_delegate = await child_manager.get_my_delegate()
        assert my_delegate is None

    @pytest.mark.asyncio
    async def test_multiple_children_delegation(self):
        """Test multiple children delegating to one parent."""
        peers = create_mock_peers()

        # Create parent
        parent_wallet = create_mock_wallet()
        parent_wallet.address = "EParentAddress"
        parent_manager = DelegationManager(peers=peers, wallet=parent_wallet)

        # Create multiple children
        children = []
        for i in range(3):
            child_wallet = create_mock_wallet()
            child_wallet.address = f"EChild{i}"
            child_manager = DelegationManager(peers=peers, wallet=child_wallet)
            children.append(child_manager)

            # Each child delegates
            delegation = await child_manager.delegate_to(
                parent_address="EParentAddress",
                amount=1000.0 * (i + 1),
            )

            # Parent receives delegation
            message_data = json.dumps(delegation.to_dict()).encode()
            await parent_manager._handle_delegation_announcement(message_data)

        # Parent should see all children
        proxy_children = await parent_manager.get_proxy_children()
        assert len(proxy_children) == 3

        # Total delegated should be 1000 + 2000 + 3000 = 6000
        total = sum(c.amount for c in proxy_children)
        assert total == 6000.0

    @pytest.mark.asyncio
    async def test_re_delegation(self):
        """Test changing delegation from one parent to another."""
        peers = create_mock_peers()

        # Create child
        child_wallet = create_mock_wallet()
        child_wallet.address = "EChildAddress"
        child_manager = DelegationManager(peers=peers, wallet=child_wallet)

        # First delegation
        delegation1 = await child_manager.delegate_to(
            parent_address="EParent1",
            amount=1000.0,
        )
        assert delegation1.parent_address == "EParent1"

        # Change delegation to different parent
        delegation2 = await child_manager.delegate_to(
            parent_address="EParent2",
            amount=1000.0,
        )

        # Should now point to new parent
        assert delegation2.parent_address == "EParent2"

        # Get my delegate should return new parent
        my_delegate = await child_manager.get_my_delegate()
        assert my_delegate.parent_address == "EParent2"
