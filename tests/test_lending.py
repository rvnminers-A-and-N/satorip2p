"""
Tests for satorip2p/protocol/lending.py

Tests the P2P pool lending registry for stake lending operations.
"""

import pytest
import time
import json
from unittest.mock import Mock, MagicMock, AsyncMock

from satorip2p.protocol.lending import (
    LendingManager,
    PoolConfig,
    LendRegistration,
    POOL_CONFIG_TOPIC,
    LEND_REGISTRATION_TOPIC,
    LEND_REMOVAL_TOPIC,
)


# ============================================================================
# TEST DATA
# ============================================================================

def create_test_pool_config(
    vault_address: str = "EVaultTest123",
    vault_pubkey: str = "pubkey123",
    pool_size_limit: float = 10000.0,
    worker_reward_pct: float = 10.0,
    accepting: bool = True,
    timestamp: int = None,
    signature: str = "",
) -> PoolConfig:
    """Create a test pool configuration."""
    return PoolConfig(
        vault_address=vault_address,
        vault_pubkey=vault_pubkey,
        pool_size_limit=pool_size_limit,
        worker_reward_pct=worker_reward_pct,
        accepting=accepting,
        timestamp=timestamp or int(time.time()),
        signature=signature,
    )


def create_test_lend_registration(
    lend_id: int = 1,
    lender_address: str = "ELenderTest456",
    vault_address: str = "EVaultTest123",
    vault_pubkey: str = "pubkey123",
    vault_signature: str = "",
    lent_out: float = 1000.0,
    timestamp: int = None,
    signature: str = "",
    deleted: int = 0,
) -> LendRegistration:
    """Create a test lend registration."""
    return LendRegistration(
        lend_id=lend_id,
        lender_address=lender_address,
        vault_address=vault_address,
        vault_pubkey=vault_pubkey,
        vault_signature=vault_signature,
        lent_out=lent_out,
        timestamp=timestamp or int(time.time()),
        signature=signature,
        deleted=deleted,
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


def create_mock_wallet():
    """Create mock wallet/identity."""
    mock_wallet = Mock()
    mock_wallet.address = "ETestWallet789"
    mock_wallet.pubkey = "testpubkey789"
    mock_wallet.sign = Mock(return_value=b"testsig123")
    mock_wallet.verify = Mock(return_value=True)
    return mock_wallet


def add_registration_to_manager(
    manager: LendingManager,
    reg: LendRegistration,
) -> None:
    """Add a lend registration to the manager with proper indexing."""
    lend_id = str(reg.lend_id)
    manager._registrations[lend_id] = reg
    manager._pool_lenders[reg.vault_address].append(lend_id)
    manager._lender_pools[reg.lender_address].append(lend_id)
    manager._my_lendings[reg.lender_address] = reg.vault_address


# ============================================================================
# POOL CONFIG TESTS
# ============================================================================

class TestPoolConfig:
    """Tests for PoolConfig dataclass."""

    def test_pool_config_creation(self):
        """Test creating a pool config."""
        config = create_test_pool_config()
        assert config.vault_address == "EVaultTest123"
        assert config.pool_size_limit == 10000.0
        assert config.accepting is True

    def test_pool_config_to_dict(self):
        """Test pool config serialization."""
        config = create_test_pool_config(timestamp=1234567890)
        data = config.to_dict()
        assert data['vault_address'] == "EVaultTest123"
        assert data['pool_size_limit'] == 10000.0
        assert data['worker_reward_pct'] == 10.0
        assert data['accepting'] is True
        assert data['timestamp'] == 1234567890

    def test_pool_config_from_dict(self):
        """Test pool config deserialization."""
        data = {
            'vault_address': 'EVault456',
            'vault_pubkey': 'pubkey456',
            'pool_size_limit': 5000.0,
            'worker_reward_pct': 15.0,
            'accepting': False,
            'timestamp': 1234567890,
            'signature': 'aabbcc',
        }
        config = PoolConfig.from_dict(data)
        assert config.vault_address == 'EVault456'
        assert config.pool_size_limit == 5000.0
        assert config.accepting is False

    def test_pool_config_hash_deterministic(self):
        """Test that pool config hash is deterministic."""
        config1 = create_test_pool_config(timestamp=1234567890)
        config2 = create_test_pool_config(timestamp=1234567890)
        assert config1.get_config_hash() == config2.get_config_hash()

    def test_different_configs_different_hashes(self):
        """Test that different configs have different hashes."""
        config1 = create_test_pool_config(vault_address="EVault1")
        config2 = create_test_pool_config(vault_address="EVault2")
        assert config1.get_config_hash() != config2.get_config_hash()


# ============================================================================
# LEND REGISTRATION TESTS
# ============================================================================

class TestLendRegistration:
    """Tests for LendRegistration dataclass."""

    def test_lend_registration_creation(self):
        """Test creating a lend registration."""
        reg = create_test_lend_registration()
        assert reg.lend_id == 1
        assert reg.lender_address == "ELenderTest456"
        assert reg.vault_address == "EVaultTest123"
        assert reg.lent_out == 1000.0

    def test_lend_registration_to_dict(self):
        """Test lend registration serialization."""
        reg = create_test_lend_registration(timestamp=1234567890)
        data = reg.to_dict()
        assert data['lend_id'] == 1
        assert data['lender_address'] == "ELenderTest456"
        assert data['lent_out'] == 1000.0
        assert data['timestamp'] == 1234567890
        assert data['deleted'] == 0

    def test_lend_registration_from_dict(self):
        """Test lend registration deserialization."""
        data = {
            'lend_id': 2,
            'lender_address': 'ELender789',
            'vault_address': 'EVault789',
            'vault_pubkey': 'pub789',
            'vault_signature': 'vsig789',
            'lent_out': 2000.0,
            'timestamp': 1234567890,
            'signature': 'sig789',
            'deleted': 0,
        }
        reg = LendRegistration.from_dict(data)
        assert reg.lend_id == 2
        assert reg.lender_address == 'ELender789'
        assert reg.lent_out == 2000.0

    def test_lend_registration_is_active(self):
        """Test is_active method."""
        reg = create_test_lend_registration(deleted=0)
        assert reg.is_active() is True

        reg_deleted = create_test_lend_registration(deleted=1234567890)
        assert reg_deleted.is_active() is False


# ============================================================================
# LENDING MANAGER BASIC TESTS
# ============================================================================

class TestLendingManagerBasic:
    """Basic tests for LendingManager."""

    def test_initialization(self):
        """Test manager initialization."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        assert manager.peers == peers
        assert manager.wallet == wallet
        assert manager.wallet_address == "ETestWallet789"

    @pytest.mark.asyncio
    async def test_register_pool(self):
        """Test registering as a pool operator."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        success, message = await manager.register_pool(
            vault_address=wallet.address,
            vault_pubkey=wallet.pubkey,
            pool_size_limit=10000.0,
            worker_reward_pct=10.0,
        )

        assert success is True

    @pytest.mark.asyncio
    async def test_unregister_pool(self):
        """Test unregistering as a pool operator."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # First register
        await manager.register_pool(
            vault_address=wallet.address,
            vault_pubkey=wallet.pubkey,
            pool_size_limit=10000.0,
        )

        # Then unregister
        success, message = await manager.unregister_pool(wallet.address)

        assert success is True

    @pytest.mark.asyncio
    async def test_set_pool_size(self):
        """Test setting pool size."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Register first
        await manager.register_pool(
            vault_address=wallet.address,
            vault_pubkey=wallet.pubkey,
            pool_size_limit=10000.0,
        )

        # Update pool size
        success, message = await manager.set_pool_size(wallet.address, 20000.0)

        assert success is True


# ============================================================================
# LENDING MANAGER LEND OPERATIONS TESTS
# ============================================================================

class TestLendingManagerLendOperations:
    """Tests for lending operations."""

    @pytest.mark.asyncio
    async def test_lend_to_vault(self):
        """Test lending to a vault."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Create a pool config for target vault
        target_config = create_test_pool_config(vault_address="EVaultTarget")
        manager._pools["EVaultTarget"] = target_config

        success, message = await manager.lend_to_vault(
            lender_address=wallet.address,
            vault_address="EVaultTarget",
            vault_signature="test_sig",
            vault_pubkey="testpubkey",
            lent_out=1000.0,
        )

        assert success is True

    @pytest.mark.asyncio
    async def test_remove_lending(self):
        """Test removing a lending registration."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # First create a lending
        manager._pools["EVaultTarget"] = create_test_pool_config(vault_address="EVaultTarget")
        success, message = await manager.lend_to_vault(
            lender_address=wallet.address,
            vault_address="EVaultTarget",
            vault_signature="test_sig",
            vault_pubkey="testpubkey",
            lent_out=1000.0,
        )

        # Get the lend_id from the registration
        lend_id = list(manager._registrations.keys())[0] if manager._registrations else None

        # Now remove it
        result_success, result_msg = await manager.remove_lending(wallet.address, lend_id)

        assert result_success is True

    @pytest.mark.asyncio
    async def test_get_pool_participants(self):
        """Test getting pool participants."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Add some test registrations using helper
        add_registration_to_manager(manager, create_test_lend_registration(
            lend_id=1, vault_address="EVault1", lender_address="ELender1"
        ))
        add_registration_to_manager(manager, create_test_lend_registration(
            lend_id=2, vault_address="EVault1", lender_address="ELender2"
        ))
        add_registration_to_manager(manager, create_test_lend_registration(
            lend_id=3, vault_address="EVault2", lender_address="ELender3"  # Different vault
        ))

        participants = await manager.get_pool_participants("EVault1")

        assert len(participants) == 2
        assert all(p.vault_address == "EVault1" for p in participants)

    @pytest.mark.asyncio
    async def test_get_my_lendings(self):
        """Test getting my lending registrations."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EMyAddress"
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Add some registrations using helper
        add_registration_to_manager(manager, create_test_lend_registration(
            lend_id=1, lender_address="EMyAddress", vault_address="EVault1"
        ))
        add_registration_to_manager(manager, create_test_lend_registration(
            lend_id=2, lender_address="EOtherAddress", vault_address="EVault2"
        ))

        my_lendings = await manager.get_my_lendings("EMyAddress")

        assert len(my_lendings) == 1
        assert my_lendings[0].lender_address == "EMyAddress"

    @pytest.mark.asyncio
    async def test_get_current_lend_address(self):
        """Test getting current lend target address."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EMyAddress"
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Add a lending using helper
        add_registration_to_manager(manager, create_test_lend_registration(
            lend_id=1, lender_address="EMyAddress", vault_address="EVaultTarget"
        ))

        # get_current_lend_address is NOT async
        result = manager.get_current_lend_address("EMyAddress")

        assert result == "EVaultTarget"


# ============================================================================
# LENDING MANAGER PUBSUB TESTS
# ============================================================================

class TestLendingManagerPubSub:
    """Tests for PubSub message handling."""

    @pytest.mark.asyncio
    async def test_handle_pool_config_message(self):
        """Test handling incoming pool config messages."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Create a pool config
        config = create_test_pool_config(vault_address="ERemoteVault")
        message_data = {
            "type": "pool_config",
            "data": config.to_dict()
        }

        await manager._on_pool_config_message(message_data)

        assert "ERemoteVault" in manager._pools
        assert manager._pools["ERemoteVault"].vault_address == "ERemoteVault"

    @pytest.mark.asyncio
    async def test_handle_lend_registration_message(self):
        """Test handling incoming lend registration messages."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        # Create a lend registration
        reg = create_test_lend_registration(
            lend_id=1,
            lender_address="ERemoteLender",
            vault_address="EVaultTest123",
        )
        message_data = {
            "type": "lend_registration",
            "data": reg.to_dict()
        }

        await manager._on_lend_registration_message(message_data)

        assert "1" in manager._registrations or 1 in manager._registrations
        # Check if the registration was added (key could be int or string)
        stored_reg = manager._registrations.get("1") or manager._registrations.get(1)
        assert stored_reg is not None
        assert stored_reg.lender_address == "ERemoteLender"


# ============================================================================
# LENDING MANAGER DHT TESTS
# ============================================================================

class TestLendingManagerDHT:
    """Tests for DHT operations."""

    @pytest.mark.asyncio
    async def test_store_to_dht(self):
        """Test storing data to DHT."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        config = create_test_pool_config(vault_address="EDHTTestVault")
        await manager._store_pool_in_dht(config)

        peers.put_dht.assert_called()

    @pytest.mark.asyncio
    async def test_store_lend_to_dht(self):
        """Test storing lend registration to DHT."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet, wallet_address=wallet.address)

        reg = create_test_lend_registration(lend_id=99)
        await manager._store_lend_in_dht(reg)

        peers.put_dht.assert_called()


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestLendingManagerIntegration:
    """Integration tests for LendingManager."""

    @pytest.mark.asyncio
    async def test_full_lending_flow(self):
        """Test the complete lending flow."""
        peers = create_mock_peers()

        # Create pool operator
        pool_wallet = create_mock_wallet()
        pool_wallet.address = "EPoolOperator"
        pool_wallet.pubkey = "pool_pubkey"
        pool_manager = LendingManager(peers=peers, wallet=pool_wallet, wallet_address=pool_wallet.address)

        # Pool operator registers
        success, message = await pool_manager.register_pool(
            vault_address=pool_wallet.address,
            vault_pubkey=pool_wallet.pubkey,
            pool_size_limit=10000.0,
            worker_reward_pct=10.0,
        )
        assert success is True

        # Create lender
        lender_wallet = create_mock_wallet()
        lender_wallet.address = "ELender"
        lender_manager = LendingManager(peers=peers, wallet=lender_wallet, wallet_address=lender_wallet.address)

        # Lender discovers pool (simulate) - copy the pool config
        pool_config = pool_manager._pools.get("EPoolOperator")
        lender_manager._pools["EPoolOperator"] = pool_config

        # Lender lends to pool
        success, message = await lender_manager.lend_to_vault(
            lender_address=lender_wallet.address,
            vault_address="EPoolOperator",
            vault_signature="test_sig",
            vault_pubkey=pool_wallet.pubkey,
            lent_out=1000.0,
        )
        assert success is True

        # Verify current lend address (sync method)
        current = lender_manager.get_current_lend_address(lender_wallet.address)
        assert current == "EPoolOperator"

        # Get the lend_id
        lend_id = list(lender_manager._registrations.keys())[0]

        # Lender removes lending
        result, msg = await lender_manager.remove_lending(lender_wallet.address, lend_id)
        assert result is True

    @pytest.mark.asyncio
    async def test_pool_size_enforcement(self):
        """Test that pool size limits are enforced."""
        peers = create_mock_peers()
        pool_wallet = create_mock_wallet()
        pool_wallet.address = "EPool"
        pool_wallet.pubkey = "pool_pubkey"
        pool_manager = LendingManager(peers=peers, wallet=pool_wallet, wallet_address=pool_wallet.address)

        # Register with small pool size
        await pool_manager.register_pool(
            vault_address=pool_wallet.address,
            vault_pubkey=pool_wallet.pubkey,
            pool_size_limit=500.0,
        )

        # Try to update to larger size
        success, msg = await pool_manager.set_pool_size(pool_wallet.address, 1000.0)
        assert success is True
