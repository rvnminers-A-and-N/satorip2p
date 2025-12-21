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
    child_proxy: float = 1000.0,
    pointed: bool = True,
    charity: bool = False,
    alias: str = "Test Delegation",
    timestamp: int = None,
    signature: str = "",
    delegation_id: str = "",
) -> DelegationRecord:
    """Create a test delegation record."""
    return DelegationRecord(
        delegation_id=delegation_id,
        parent_address=parent_address,
        child_address=child_address,
        vault_address=vault_address,
        child_proxy=child_proxy,
        pointed=pointed,
        charity=charity,
        alias=alias,
        timestamp=timestamp or int(time.time()),
        signature=signature,
    )


def create_test_charity_update(
    child_address: str = "EChildTest456",
    delegation_id: str = "test_delegation_id",
    charity: bool = True,
    timestamp: int = None,
    signature: str = "",
) -> CharityUpdate:
    """Create a test charity update."""
    return CharityUpdate(
        delegation_id=delegation_id,
        child_address=child_address,
        charity=charity,
        timestamp=timestamp or int(time.time()),
        signature=signature,
    )


def create_mock_peers():
    """Create mock Peers object."""
    mock_peers = Mock()
    mock_peers.publish = AsyncMock()
    mock_peers.subscribe = AsyncMock()
    mock_peers.get_dht = AsyncMock(return_value=None)
    mock_peers.put_dht = AsyncMock()
    mock_peers.register_rendezvous = AsyncMock()
    mock_peers.discover_rendezvous = AsyncMock(return_value=[])
    return mock_peers


def add_delegation_to_manager(
    manager: DelegationManager,
    record: DelegationRecord,
) -> None:
    """Add a delegation record to the manager with proper indexing."""
    delegation_id = record.delegation_id
    manager._delegations[delegation_id] = record
    manager._parent_children[record.parent_address].append(delegation_id)
    manager._child_delegations[record.child_address].append(delegation_id)
    manager._my_delegate[record.child_address] = record.parent_address


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
        assert record.child_proxy == 1000.0
        assert record.pointed is True
        assert record.charity is False

    def test_delegation_record_to_dict(self):
        """Test delegation record serialization."""
        record = create_test_delegation_record(timestamp=1234567890)
        data = record.to_dict()
        assert data['parent_address'] == "EParentTest123"
        assert data['child_address'] == "EChildTest456"
        assert data['child_proxy'] == 1000.0
        assert data['pointed'] is True
        assert data['charity'] is False
        assert data['timestamp'] == 1234567890

    def test_delegation_record_from_dict(self):
        """Test delegation record deserialization."""
        data = {
            'delegation_id': 'test_id_123',
            'parent_address': 'EParent789',
            'child_address': 'EChild789',
            'vault_address': 'EVault789',
            'child_proxy': 2000.0,
            'pointed': False,
            'charity': True,
            'alias': 'Test Alias',
            'timestamp': 1234567890,
            'signature': 'sig789',
        }
        record = DelegationRecord.from_dict(data)
        assert record.parent_address == 'EParent789'
        assert record.child_address == 'EChild789'
        assert record.child_proxy == 2000.0
        assert record.charity is True

    def test_delegation_record_hash_deterministic(self):
        """Test that delegation record hash is deterministic."""
        record1 = create_test_delegation_record(timestamp=1234567890, delegation_id="test123")
        record2 = create_test_delegation_record(timestamp=1234567890, delegation_id="test123")
        assert record1.get_record_hash() == record2.get_record_hash()

    def test_different_records_different_hashes(self):
        """Test that different records have different hashes."""
        record1 = create_test_delegation_record(child_address="EChild1", delegation_id="test1")
        record2 = create_test_delegation_record(child_address="EChild2", delegation_id="test2")
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
        assert update.delegation_id == "test_delegation_id"
        assert update.charity is True

    def test_charity_update_to_dict(self):
        """Test charity update serialization."""
        update = create_test_charity_update(timestamp=1234567890)
        data = update.to_dict()
        assert data['child_address'] == "EChildTest456"
        assert data['delegation_id'] == "test_delegation_id"
        assert data['charity'] is True
        assert data['timestamp'] == 1234567890

    def test_charity_update_from_dict(self):
        """Test charity update deserialization."""
        data = {
            'delegation_id': 'del_999',
            'child_address': 'EChild999',
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
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        assert manager.peers == peers
        assert manager.wallet == wallet
        assert manager.wallet_address == "ETestWallet789"

    @pytest.mark.asyncio
    async def test_delegate_to(self):
        """Test delegating to a parent."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EChildAddress"
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        success, message = await manager.delegate_to(
            child_address=wallet.address,
            parent_address="EParentAddress",
            amount=1000.0,
            alias="My Delegation",
        )

        assert success is True
        # Verify delegation was recorded
        assert manager.get_my_delegate(wallet.address) == "EParentAddress"

    @pytest.mark.asyncio
    async def test_remove_delegation(self):
        """Test removing a delegation."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EChildAddress"
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # First delegate
        await manager.delegate_to(
            child_address=wallet.address,
            parent_address="EParentAddress",
            amount=1000.0,
        )

        # Then remove
        success, message = await manager.remove_delegation(child_address=wallet.address)

        assert success is True

    def test_get_my_delegate(self):
        """Test getting my current delegation."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EChildAddress"
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # No delegation yet - returns empty string
        result = manager.get_my_delegate(child_address=wallet.address)
        assert result == ""

        # Simulate having a delegation via _my_delegate dict
        manager._my_delegate[wallet.address] = "EParentAddress"

        # Now should return the parent address
        result = manager.get_my_delegate(child_address=wallet.address)
        assert result == "EParentAddress"


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
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Add some delegations pointing to this parent
        add_delegation_to_manager(manager, create_test_delegation_record(
            delegation_id="delegation1",
            parent_address="EParentAddress",
            child_address="EChild1",
        ))
        add_delegation_to_manager(manager, create_test_delegation_record(
            delegation_id="delegation2",
            parent_address="EParentAddress",
            child_address="EChild2",
        ))
        add_delegation_to_manager(manager, create_test_delegation_record(
            delegation_id="delegation3",
            parent_address="EDifferentParent",  # Different parent
            child_address="EChild3",
        ))

        children = await manager.get_proxy_children(parent_address="EParentAddress")

        assert len(children) == 2
        assert all(c.parent_address == "EParentAddress" for c in children)

    @pytest.mark.asyncio
    async def test_set_charity_status_to_true(self):
        """Test marking a child as charity."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EParentAddress"
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Add a child delegation using proper manager method
        add_delegation_to_manager(manager, create_test_delegation_record(
            delegation_id="test_delegation_1",
            parent_address="EParentAddress",
            child_address="EChildAddress",
            charity=False,
        ))

        success, message = await manager.set_charity_status(
            delegation_id="test_delegation_1",
            child_address="EChildAddress",
            charity=True,
        )

        assert success is True
        assert manager._delegations["test_delegation_1"].charity is True

    @pytest.mark.asyncio
    async def test_set_charity_status_to_false(self):
        """Test unmarking a child as charity."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EParentAddress"
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Add a charity child using proper manager method
        add_delegation_to_manager(manager, create_test_delegation_record(
            delegation_id="test_delegation_2",
            parent_address="EParentAddress",
            child_address="EChildAddress",
            charity=True,
        ))

        success, message = await manager.set_charity_status(
            delegation_id="test_delegation_2",
            child_address="EChildAddress",
            charity=False,
        )

        assert success is True
        assert manager._delegations["test_delegation_2"].charity is False

    @pytest.mark.asyncio
    async def test_remove_proxy_child(self):
        """Test removing a child from proxy list."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EParentAddress"
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Add a child using proper manager method
        add_delegation_to_manager(manager, create_test_delegation_record(
            delegation_id="test_delegation_3",
            parent_address="EParentAddress",
            child_address="EChildAddress",
        ))

        success, message = await manager.remove_proxy_child(
            delegation_id="test_delegation_3",
            child_address="EChildAddress",
        )

        assert success is True
        # Delegation is soft-deleted (marked with deleted timestamp), not removed
        assert manager._delegations["test_delegation_3"].deleted > 0
        assert manager._delegations["test_delegation_3"].is_active() is False


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
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Create a delegation record
        record = create_test_delegation_record(
            delegation_id="remote_delegation_1",
            parent_address="ERemoteParent",
            child_address="ERemoteChild",
        )

        # Simulate receiving a delegation message (format expected by _on_delegation_message)
        message_data = {
            "type": "delegation",
            "data": record.to_dict()
        }

        await manager._on_delegation_message(message_data)

        assert "remote_delegation_1" in manager._delegations
        assert manager._delegations["remote_delegation_1"].parent_address == "ERemoteParent"

    @pytest.mark.asyncio
    async def test_handle_delegation_removal(self):
        """Test handling incoming delegation removal."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Add existing delegation
        add_delegation_to_manager(manager, create_test_delegation_record(
            delegation_id="remote_delegation_2",
            parent_address="ERemoteParent",
            child_address="ERemoteChild",
        ))

        # Create removal record with deleted timestamp
        removal_record = create_test_delegation_record(
            delegation_id="remote_delegation_2",
            parent_address="ERemoteParent",
            child_address="ERemoteChild",
        )
        removal_record.deleted = int(time.time())

        # Simulate removal message
        message_data = {
            "type": "delegation_removal",
            "data": removal_record.to_dict()
        }

        await manager._on_delegation_removal_message(message_data)

        # Should be marked as deleted
        assert manager._delegations["remote_delegation_2"].deleted > 0

    @pytest.mark.asyncio
    async def test_handle_charity_update(self):
        """Test handling incoming charity updates."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Add existing delegation
        add_delegation_to_manager(manager, create_test_delegation_record(
            delegation_id="remote_delegation_3",
            parent_address="ERemoteParent",
            child_address="ERemoteChild",
            charity=False,
        ))

        # Simulate charity update with newer timestamp
        update = create_test_charity_update(
            delegation_id="remote_delegation_3",
            child_address="ERemoteChild",
            charity=True,
        )
        # Set update timestamp to be newer than the record
        update.timestamp = int(time.time()) + 100

        message_data = {
            "type": "charity_update",
            "data": update.to_dict()
        }

        await manager._on_charity_update_message(message_data)

        assert manager._delegations["remote_delegation_3"].charity is True


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
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        record = create_test_delegation_record(delegation_id="dht_test_1")
        await manager._store_delegation_in_dht(record)

        peers.put_dht.assert_called()

    @pytest.mark.asyncio
    async def test_retrieve_from_dht(self):
        """Test retrieving delegation from DHT."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = DelegationManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Mock DHT returning data
        record = create_test_delegation_record(delegation_id="dht_test_2", child_address="ETestChild")
        peers.get_dht = AsyncMock(return_value=json.dumps(record.to_dict()).encode())

        result = await manager._get_delegation_from_dht("dht_test_2")

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
        parent_manager = DelegationManager(peers=peers, wallet=parent_wallet, wallet_address=parent_wallet.address)

        # Create child (the one delegating)
        child_wallet = create_mock_wallet()
        child_wallet.address = "EChildAddress"
        child_manager = DelegationManager(peers=peers, wallet=child_wallet, wallet_address=child_wallet.address)

        # Child delegates to parent
        success, message = await child_manager.delegate_to(
            child_address=child_wallet.address,
            parent_address="EParentAddress",
            amount=1000.0,
            alias="My Delegation",
        )
        assert success is True

        # Simulate parent receiving the delegation (via PubSub message)
        # Get the delegation from child manager and add to parent
        child_delegation_id = list(child_manager._delegations.keys())[0]
        delegation = child_manager._delegations[child_delegation_id]
        message_data = {
            "type": "delegation",
            "data": delegation.to_dict()
        }
        await parent_manager._on_delegation_message(message_data)

        # Parent should see the child
        children = await parent_manager.get_proxy_children(parent_address="EParentAddress")
        assert len(children) == 1
        assert children[0].child_address == "EChildAddress"

        # Parent marks child as charity
        success, msg = await parent_manager.set_charity_status(
            delegation_id=children[0].delegation_id,
            child_address="EChildAddress",
            charity=True,
        )
        assert success is True

        # Verify charity status
        children = await parent_manager.get_proxy_children(parent_address="EParentAddress")
        assert children[0].charity is True

        # Child removes delegation
        success, msg = await child_manager.remove_delegation(child_address=child_wallet.address)
        assert success is True

        # Verify child's delegation is gone
        my_delegate = child_manager.get_my_delegate(child_wallet.address)
        assert my_delegate == ""

    @pytest.mark.asyncio
    async def test_multiple_children_delegation(self):
        """Test multiple children delegating to one parent."""
        peers = create_mock_peers()

        # Create parent
        parent_wallet = create_mock_wallet()
        parent_wallet.address = "EParentAddress"
        parent_manager = DelegationManager(peers=peers, wallet=parent_wallet, wallet_address=parent_wallet.address)

        # Create multiple children and delegate
        for i in range(3):
            child_wallet = create_mock_wallet()
            child_wallet.address = f"EChild{i}"
            child_manager = DelegationManager(peers=peers, wallet=child_wallet, wallet_address=child_wallet.address)

            # Each child delegates
            success, msg = await child_manager.delegate_to(
                child_address=child_wallet.address,
                parent_address="EParentAddress",
                amount=1000.0 * (i + 1),
            )
            assert success is True

            # Get the delegation and send to parent
            child_delegation_id = list(child_manager._delegations.keys())[0]
            delegation = child_manager._delegations[child_delegation_id]
            message_data = {
                "type": "delegation",
                "data": delegation.to_dict()
            }
            await parent_manager._on_delegation_message(message_data)

        # Parent should see all children
        proxy_children = await parent_manager.get_proxy_children(parent_address="EParentAddress")
        assert len(proxy_children) == 3

        # Total delegated should be 1000 + 2000 + 3000 = 6000
        total = sum(c.child_proxy for c in proxy_children)
        assert total == 6000.0

    @pytest.mark.asyncio
    async def test_re_delegation(self):
        """Test changing delegation from one parent to another."""
        peers = create_mock_peers()

        # Create child
        child_wallet = create_mock_wallet()
        child_wallet.address = "EChildAddress"
        child_manager = DelegationManager(peers=peers, wallet=child_wallet, wallet_address=child_wallet.address)

        # First delegation
        success1, msg1 = await child_manager.delegate_to(
            child_address=child_wallet.address,
            parent_address="EParent1",
            amount=1000.0,
        )
        assert success1 is True
        assert child_manager.get_my_delegate(child_wallet.address) == "EParent1"

        # Change delegation to different parent
        success2, msg2 = await child_manager.delegate_to(
            child_address=child_wallet.address,
            parent_address="EParent2",
            amount=1000.0,
        )
        assert success2 is True

        # Should now point to new parent
        my_delegate = child_manager.get_my_delegate(child_wallet.address)
        assert my_delegate == "EParent2"
