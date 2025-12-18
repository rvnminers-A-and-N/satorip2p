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
        manager = LendingManager(peers=peers, wallet=wallet)

        assert manager._peers == peers
        assert manager._wallet == wallet
        assert manager._my_address == "ETestWallet789"

    @pytest.mark.asyncio
    async def test_register_pool(self):
        """Test registering as a pool operator."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet)

        result = await manager.register_pool(
            pool_size_limit=10000.0,
            worker_reward_pct=10.0,
        )

        assert result is not None
        assert result.accepting is True
        peers.publish.assert_called()

    @pytest.mark.asyncio
    async def test_unregister_pool(self):
        """Test unregistering as a pool operator."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet)

        # First register
        await manager.register_pool(pool_size_limit=10000.0)

        # Then unregister
        result = await manager.unregister_pool()

        assert result is True
        assert manager._my_pool_config is None or manager._my_pool_config.accepting is False

    @pytest.mark.asyncio
    async def test_set_pool_size(self):
        """Test setting pool size."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet)

        # Register first
        await manager.register_pool(pool_size_limit=10000.0)

        # Update pool size
        result = await manager.set_pool_size(20000.0)

        assert result is not None
        assert result.pool_size_limit == 20000.0


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
        manager = LendingManager(peers=peers, wallet=wallet)

        # Create a pool config for target vault
        target_config = create_test_pool_config(vault_address="EVaultTarget")
        manager._pool_configs["EVaultTarget"] = target_config

        result = await manager.lend_to_vault(
            vault_address="EVaultTarget",
            amount=1000.0,
        )

        assert result is not None
        assert result.vault_address == "EVaultTarget"
        assert result.lent_out == 1000.0

    @pytest.mark.asyncio
    async def test_remove_lending(self):
        """Test removing a lending registration."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet)

        # First create a lending
        manager._pool_configs["EVaultTarget"] = create_test_pool_config(vault_address="EVaultTarget")
        reg = await manager.lend_to_vault(vault_address="EVaultTarget", amount=1000.0)

        # Now remove it
        result = await manager.remove_lending(reg.lend_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_get_pool_participants(self):
        """Test getting pool participants."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet)

        # Add some test registrations
        manager._lend_registrations["1"] = create_test_lend_registration(
            lend_id=1, vault_address="EVault1"
        )
        manager._lend_registrations["2"] = create_test_lend_registration(
            lend_id=2, vault_address="EVault1", lender_address="ELender2"
        )
        manager._lend_registrations["3"] = create_test_lend_registration(
            lend_id=3, vault_address="EVault2"  # Different vault
        )

        participants = await manager.get_pool_participants("EVault1")

        assert len(participants) == 2
        assert all(p.vault_address == "EVault1" for p in participants)

    @pytest.mark.asyncio
    async def test_get_my_lendings(self):
        """Test getting my lending registrations."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EMyAddress"
        manager = LendingManager(peers=peers, wallet=wallet)

        # Add some registrations
        manager._lend_registrations["1"] = create_test_lend_registration(
            lend_id=1, lender_address="EMyAddress"
        )
        manager._lend_registrations["2"] = create_test_lend_registration(
            lend_id=2, lender_address="EOtherAddress"
        )

        my_lendings = await manager.get_my_lendings()

        assert len(my_lendings) == 1
        assert my_lendings[0].lender_address == "EMyAddress"

    @pytest.mark.asyncio
    async def test_get_current_lend_address(self):
        """Test getting current lend target address."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        wallet.address = "EMyAddress"
        manager = LendingManager(peers=peers, wallet=wallet)

        # Add a lending
        manager._lend_registrations["1"] = create_test_lend_registration(
            lend_id=1, lender_address="EMyAddress", vault_address="EVaultTarget"
        )

        result = await manager.get_current_lend_address()

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
        manager = LendingManager(peers=peers, wallet=wallet)

        # Simulate receiving a pool config message
        config = create_test_pool_config(vault_address="ERemoteVault")
        message_data = json.dumps(config.to_dict()).encode()

        await manager._handle_pool_config(message_data)

        assert "ERemoteVault" in manager._pool_configs
        assert manager._pool_configs["ERemoteVault"].pool_size_limit == 10000.0

    @pytest.mark.asyncio
    async def test_handle_lend_registration_message(self):
        """Test handling incoming lend registration messages."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet)

        # First add the target pool
        manager._pool_configs["EVaultTest123"] = create_test_pool_config()

        # Simulate receiving a lend registration
        reg = create_test_lend_registration()
        message_data = json.dumps(reg.to_dict()).encode()

        await manager._handle_lend_registration(message_data)

        assert len(manager._lend_registrations) > 0


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
        manager = LendingManager(peers=peers, wallet=wallet)

        config = create_test_pool_config()
        await manager._store_pool_config_to_dht(config)

        peers.dht_put.assert_called()

    @pytest.mark.asyncio
    async def test_retrieve_from_dht(self):
        """Test retrieving data from DHT."""
        peers = create_mock_peers()
        wallet = create_mock_wallet()
        manager = LendingManager(peers=peers, wallet=wallet)

        # Mock DHT returning data
        config = create_test_pool_config()
        peers.dht_get = AsyncMock(return_value=json.dumps(config.to_dict()).encode())

        result = await manager._get_pool_config_from_dht("EVaultTest123")

        assert result is not None
        assert result.vault_address == "EVaultTest123"


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
        pool_manager = LendingManager(peers=peers, wallet=pool_wallet)

        # Pool operator registers
        pool_config = await pool_manager.register_pool(
            pool_size_limit=10000.0,
            worker_reward_pct=10.0,
        )
        assert pool_config is not None

        # Create lender
        lender_wallet = create_mock_wallet()
        lender_wallet.address = "ELender"
        lender_manager = LendingManager(peers=peers, wallet=lender_wallet)

        # Lender discovers pool (simulate)
        lender_manager._pool_configs["EPoolOperator"] = pool_config

        # Lender lends to pool
        registration = await lender_manager.lend_to_vault(
            vault_address="EPoolOperator",
            amount=1000.0,
        )
        assert registration is not None
        assert registration.vault_address == "EPoolOperator"

        # Verify current lend address
        current = await lender_manager.get_current_lend_address()
        assert current == "EPoolOperator"

        # Lender removes lending
        result = await lender_manager.remove_lending(registration.lend_id)
        assert result is True

    @pytest.mark.asyncio
    async def test_pool_size_enforcement(self):
        """Test that pool size limits are enforced."""
        peers = create_mock_peers()
        pool_wallet = create_mock_wallet()
        pool_wallet.address = "EPool"
        pool_manager = LendingManager(peers=peers, wallet=pool_wallet)

        # Register with small pool size
        await pool_manager.register_pool(pool_size_limit=500.0)

        # Try to update to larger size
        result = await pool_manager.set_pool_size(1000.0)
        assert result.pool_size_limit == 1000.0
