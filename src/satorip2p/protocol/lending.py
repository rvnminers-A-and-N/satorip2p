"""
satorip2p/protocol/lending.py

P2P Pool Lending Registry Protocol.

Enables decentralized pool lending without central server dependency:
- Pool operators register/unregister their pools
- Lenders join/leave pools
- Pool configurations (size limits, worker rewards) stored in DHT
- Real-time updates via PubSub

Matches API from satorilib/server/server.py:
- lendToAddress(), lendRemove(), lendAddress()
- poolAccepting(), setPoolSize(), poolParticipants()
- poolAddresses(), poolAddressRemove()
"""

import time
import json
import hashlib
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from collections import defaultdict

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.lending")


# ============================================================================
# CONSTANTS
# ============================================================================

# PubSub Topics
POOL_CONFIG_TOPIC = "satori/lending/pools"
LEND_REGISTRATION_TOPIC = "satori/lending/registrations"
LEND_REMOVAL_TOPIC = "satori/lending/removals"

# Rendezvous namespace for pool discovery
POOL_RENDEZVOUS_PREFIX = "satori/pools/"

# DHT key prefixes
DHT_POOL_PREFIX = "satori:pool:"
DHT_LEND_PREFIX = "satori:lend:"

# Defaults
DEFAULT_POOL_SIZE_LIMIT = 0.0  # 0 = unlimited
DEFAULT_WORKER_REWARD_PCT = 0.0
REGISTRATION_TTL = 3600  # 1 hour TTL for rendezvous


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class PoolConfig:
    """
    Pool operator's configuration.

    Represents a vault that is accepting lenders.
    Broadcast when pool status changes.
    """
    vault_address: str          # Pool operator's vault address
    vault_pubkey: str           # Vault public key for verification
    pool_size_limit: float = DEFAULT_POOL_SIZE_LIMIT  # Max stake (0 = unlimited)
    worker_reward_pct: float = DEFAULT_WORKER_REWARD_PCT  # Worker reward %
    accepting: bool = True      # Whether pool is accepting new lenders
    timestamp: int = 0          # Unix timestamp
    signature: str = ""         # Vault signature for verification

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = int(time.time())

    def get_signing_message(self) -> str:
        """Get deterministic message for signing."""
        return f"{self.vault_address}:{self.vault_pubkey}:{self.pool_size_limit}:{self.worker_reward_pct}:{self.accepting}:{self.timestamp}"

    def get_config_hash(self) -> str:
        """Get deterministic hash of pool config for verification."""
        return hashlib.sha256(self.get_signing_message().encode()).hexdigest()

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PoolConfig":
        """Create from dictionary."""
        return cls(
            vault_address=data.get("vault_address", ""),
            vault_pubkey=data.get("vault_pubkey", ""),
            pool_size_limit=float(data.get("pool_size_limit", DEFAULT_POOL_SIZE_LIMIT)),
            worker_reward_pct=float(data.get("worker_reward_pct", DEFAULT_WORKER_REWARD_PCT)),
            accepting=bool(data.get("accepting", True)),
            timestamp=int(data.get("timestamp", 0)),
            signature=data.get("signature", ""),
        )


@dataclass
class LendRegistration:
    """
    A lender registering with a vault/pool.

    Schema matches vault-pool.html JavaScript expectations.
    """
    lend_id: str                # Unique lending ID (hash-based)
    lender_address: str         # Wallet address of lender
    vault_address: str          # Target vault/pool address
    vault_pubkey: str           # Vault's public key
    vault_signature: str        # Vault's approval signature
    lent_out: float = 0.0       # Amount lent/staked
    timestamp: int = 0          # Registration timestamp
    signature: str = ""         # Lender's signature
    deleted: int = 0            # Soft delete timestamp (0 = active)
    alias: str = ""             # Optional display name

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = int(time.time())
        if not self.lend_id:
            self.lend_id = self._generate_id()

    def _generate_id(self) -> str:
        """Generate unique lend ID from addresses and timestamp."""
        data = f"{self.lender_address}:{self.vault_address}:{self.timestamp}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def get_signing_message(self) -> str:
        """Get deterministic message for signing."""
        return f"{self.lend_id}:{self.lender_address}:{self.vault_address}:{self.lent_out}:{self.timestamp}"

    def is_active(self) -> bool:
        """Check if registration is active (not deleted)."""
        return self.deleted == 0

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "LendRegistration":
        """Create from dictionary."""
        return cls(
            lend_id=data.get("lend_id", ""),
            lender_address=data.get("lender_address", ""),
            vault_address=data.get("vault_address", ""),
            vault_pubkey=data.get("vault_pubkey", ""),
            vault_signature=data.get("vault_signature", ""),
            lent_out=float(data.get("lent_out", 0.0)),
            timestamp=int(data.get("timestamp", 0)),
            signature=data.get("signature", ""),
            deleted=int(data.get("deleted", 0)),
            alias=data.get("alias", ""),
        )


# ============================================================================
# LENDING MANAGER
# ============================================================================

class LendingManager:
    """
    Manages P2P pool lending operations.

    Provides decentralized alternatives to central server endpoints:
    - poolAccepting() -> register_pool() / unregister_pool()
    - setPoolSize() -> set_pool_size()
    - lendToAddress() -> lend_to_vault()
    - lendRemove() -> remove_lending()
    - poolParticipants() -> get_pool_participants()
    - poolAddresses() -> get_my_lendings()
    - lendAddress() -> get_current_lend_address()

    Usage:
        manager = LendingManager(peers, wallet_address, wallet)
        await manager.start()

        # As pool operator
        await manager.register_pool(vault_address, vault_pubkey, signature)
        await manager.set_pool_size(vault_address, 10000.0)

        # As lender
        await manager.lend_to_vault(lender_addr, vault_addr, vault_sig, vault_pubkey)
        lendings = await manager.get_my_lendings(lender_addr)
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        wallet_address: str = "",
        wallet: Any = None,
    ):
        """
        Initialize LendingManager.

        Args:
            peers: P2P peers instance for networking
            wallet_address: This node's wallet address
            wallet: Wallet instance for signing (optional)
        """
        self.peers = peers
        self.wallet_address = wallet_address
        self.wallet = wallet

        # Local caches
        self._pools: Dict[str, PoolConfig] = {}  # vault_address -> PoolConfig
        self._registrations: Dict[str, LendRegistration] = {}  # lend_id -> LendRegistration
        self._my_lendings: Dict[str, str] = {}  # lender_address -> vault_address (current)

        # Indexes for fast lookup
        self._pool_lenders: Dict[str, List[str]] = defaultdict(list)  # vault -> [lend_ids]
        self._lender_pools: Dict[str, List[str]] = defaultdict(list)  # lender -> [lend_ids]

        # Callbacks
        self._on_pool_registered: List[Callable] = []
        self._on_lend_registered: List[Callable] = []
        self._on_lend_removed: List[Callable] = []

        # State
        self._started = False
        self._subscribed_topics: List[str] = []

    # ========================================================================
    # LIFECYCLE
    # ========================================================================

    async def start(self) -> bool:
        """Start the lending manager and subscribe to topics."""
        if self._started:
            return True

        if not self.peers:
            logger.warning("No peers instance, running in local-only mode")
            self._started = True
            return True

        try:
            # Subscribe to PubSub topics
            await self._subscribe_to_topics()

            # Load existing data from DHT
            await self._load_from_dht()

            self._started = True
            logger.info("LendingManager started")
            return True

        except Exception as e:
            logger.error(f"Failed to start LendingManager: {e}")
            return False

    async def stop(self) -> None:
        """Stop the lending manager."""
        if not self._started:
            return

        try:
            # Unsubscribe from topics
            for topic in self._subscribed_topics:
                if self.peers:
                    await self.peers.unsubscribe(topic)
            self._subscribed_topics.clear()

        except Exception as e:
            logger.error(f"Error stopping LendingManager: {e}")

        self._started = False
        logger.info("LendingManager stopped")

    async def _subscribe_to_topics(self) -> None:
        """Subscribe to lending-related PubSub topics."""
        if not self.peers:
            return

        topics = [
            (POOL_CONFIG_TOPIC, self._on_pool_config_message),
            (LEND_REGISTRATION_TOPIC, self._on_lend_registration_message),
            (LEND_REMOVAL_TOPIC, self._on_lend_removal_message),
        ]

        for topic, handler in topics:
            try:
                await self.peers.subscribe_async(topic, handler)
                self._subscribed_topics.append(topic)
                logger.debug(f"Subscribed to {topic}")
            except Exception as e:
                logger.warning(f"Failed to subscribe to {topic}: {e}")

    async def _load_from_dht(self) -> None:
        """Load existing pool and lending data from DHT."""
        # DHT loading would happen here in production
        # For now, rely on PubSub for discovery
        pass

    # ========================================================================
    # POOL OPERATOR METHODS
    # ========================================================================

    async def register_pool(
        self,
        vault_address: str,
        vault_pubkey: str,
        signature: str = "",
        pool_size_limit: float = DEFAULT_POOL_SIZE_LIMIT,
        worker_reward_pct: float = DEFAULT_WORKER_REWARD_PCT,
    ) -> tuple[bool, str]:
        """
        Register vault as accepting lenders (poolAccepting(True)).

        Args:
            vault_address: Vault address to register as pool
            vault_pubkey: Vault's public key
            signature: Vault's signature (optional, can sign locally)
            pool_size_limit: Maximum stake to accept
            worker_reward_pct: Worker reward percentage

        Returns:
            (success, message)
        """
        try:
            config = PoolConfig(
                vault_address=vault_address,
                vault_pubkey=vault_pubkey,
                pool_size_limit=pool_size_limit,
                worker_reward_pct=worker_reward_pct,
                accepting=True,
                signature=signature,
            )

            # Sign if wallet available and no signature provided
            if not signature and self.wallet:
                try:
                    msg = config.get_signing_message()
                    config.signature = self.wallet.sign(msg.encode()).hex()
                except Exception as e:
                    logger.warning(f"Failed to sign pool config: {e}")

            # Store locally
            self._pools[vault_address] = config

            # Broadcast to network
            await self._broadcast_pool_config(config)

            # Store in DHT
            await self._store_pool_in_dht(config)

            # Register in Rendezvous for discovery
            await self._register_pool_rendezvous(config)

            # Trigger callbacks
            for callback in self._on_pool_registered:
                try:
                    callback(config)
                except Exception as e:
                    logger.error(f"Pool registered callback error: {e}")

            logger.info(f"Pool registered: {vault_address}")
            return (True, "Pool registered successfully")

        except Exception as e:
            logger.error(f"Failed to register pool: {e}")
            return (False, str(e))

    async def unregister_pool(
        self,
        vault_address: str,
        signature: str = "",
    ) -> tuple[bool, str]:
        """
        Unregister vault from accepting lenders (poolAccepting(False)).

        Args:
            vault_address: Vault address to unregister
            signature: Vault's signature

        Returns:
            (success, message)
        """
        try:
            # Get existing config or create new one
            config = self._pools.get(vault_address)
            if config:
                config.accepting = False
                config.timestamp = int(time.time())
                config.signature = signature
            else:
                config = PoolConfig(
                    vault_address=vault_address,
                    vault_pubkey="",
                    accepting=False,
                    signature=signature,
                )

            # Update local cache
            self._pools[vault_address] = config

            # Broadcast update
            await self._broadcast_pool_config(config)

            # Update DHT
            await self._store_pool_in_dht(config)

            logger.info(f"Pool unregistered: {vault_address}")
            return (True, "Pool unregistered successfully")

        except Exception as e:
            logger.error(f"Failed to unregister pool: {e}")
            return (False, str(e))

    async def set_pool_size(
        self,
        vault_address: str,
        pool_size_limit: float,
        signature: str = "",
    ) -> tuple[bool, str]:
        """
        Set pool stake limit (setPoolSize).

        Args:
            vault_address: Vault address
            pool_size_limit: Maximum stake limit
            signature: Vault's signature

        Returns:
            (success, message)
        """
        try:
            config = self._pools.get(vault_address)
            if not config:
                return (False, "Pool not found")

            config.pool_size_limit = pool_size_limit
            config.timestamp = int(time.time())
            if signature:
                config.signature = signature

            # Broadcast update
            await self._broadcast_pool_config(config)

            # Update DHT
            await self._store_pool_in_dht(config)

            logger.info(f"Pool size set: {vault_address} -> {pool_size_limit}")
            return (True, "Pool size updated")

        except Exception as e:
            logger.error(f"Failed to set pool size: {e}")
            return (False, str(e))

    async def set_worker_reward(
        self,
        vault_address: str,
        reward_pct: float,
        signature: str = "",
    ) -> tuple[bool, str]:
        """
        Set pool worker reward percentage (setPoolWorkerReward).

        Args:
            vault_address: Vault address
            reward_pct: Worker reward percentage (0-100)
            signature: Vault's signature

        Returns:
            (success, message)
        """
        try:
            config = self._pools.get(vault_address)
            if not config:
                return (False, "Pool not found")

            config.worker_reward_pct = reward_pct
            config.timestamp = int(time.time())
            if signature:
                config.signature = signature

            # Broadcast update
            await self._broadcast_pool_config(config)

            # Update DHT
            await self._store_pool_in_dht(config)

            logger.info(f"Worker reward set: {vault_address} -> {reward_pct}%")
            return (True, "Worker reward updated")

        except Exception as e:
            logger.error(f"Failed to set worker reward: {e}")
            return (False, str(e))

    async def get_pool_participants(
        self,
        vault_address: str,
    ) -> List[LendRegistration]:
        """
        Get lenders in a pool (poolParticipants).

        Args:
            vault_address: Vault address to query

        Returns:
            List of active LendRegistration objects
        """
        lend_ids = self._pool_lenders.get(vault_address, [])
        participants = []

        for lend_id in lend_ids:
            reg = self._registrations.get(lend_id)
            if reg and reg.is_active():
                participants.append(reg)

        return participants

    def get_pool_config(self, vault_address: str) -> Optional[PoolConfig]:
        """Get pool configuration for a vault."""
        return self._pools.get(vault_address)

    def get_accepting_pools(self) -> List[PoolConfig]:
        """Get all pools currently accepting lenders."""
        return [p for p in self._pools.values() if p.accepting]

    # ========================================================================
    # LENDER METHODS
    # ========================================================================

    async def lend_to_vault(
        self,
        lender_address: str,
        vault_address: str,
        vault_signature: str,
        vault_pubkey: str,
        lent_out: float = 0.0,
        signature: str = "",
    ) -> tuple[bool, str]:
        """
        Register lending to a vault (lendToAddress).

        Args:
            lender_address: Wallet address of lender
            vault_address: Target vault address
            vault_signature: Vault's approval signature
            vault_pubkey: Vault's public key
            lent_out: Amount being lent/staked
            signature: Lender's signature

        Returns:
            (success, message)
        """
        try:
            # Check if pool is accepting
            pool = self._pools.get(vault_address)
            if pool and not pool.accepting:
                return (False, "Pool is not accepting new lenders")

            # Check pool size limit
            if pool and pool.pool_size_limit > 0:
                participants = await self.get_pool_participants(vault_address)
                current_total = sum(r.lent_out for r in participants)
                if current_total + lent_out > pool.pool_size_limit:
                    return (False, "Pool stake limit would be exceeded")

            reg = LendRegistration(
                lend_id="",  # Will be generated
                lender_address=lender_address,
                vault_address=vault_address,
                vault_pubkey=vault_pubkey,
                vault_signature=vault_signature,
                lent_out=lent_out,
                signature=signature,
            )

            # Sign if wallet available
            if not signature and self.wallet:
                try:
                    msg = reg.get_signing_message()
                    reg.signature = self.wallet.sign(msg.encode()).hex()
                except Exception as e:
                    logger.warning(f"Failed to sign lend registration: {e}")

            # Store locally
            self._registrations[reg.lend_id] = reg
            self._pool_lenders[vault_address].append(reg.lend_id)
            self._lender_pools[lender_address].append(reg.lend_id)
            self._my_lendings[lender_address] = vault_address

            # Broadcast to network
            await self._broadcast_lend_registration(reg)

            # Store in DHT
            await self._store_lend_in_dht(reg)

            # Trigger callbacks
            for callback in self._on_lend_registered:
                try:
                    callback(reg)
                except Exception as e:
                    logger.error(f"Lend registered callback error: {e}")

            logger.info(f"Lend registered: {lender_address} -> {vault_address}")
            return (True, "Lending registered successfully")

        except Exception as e:
            logger.error(f"Failed to register lending: {e}")
            return (False, str(e))

    async def remove_lending(
        self,
        lender_address: str,
        signature: str = "",
    ) -> tuple[bool, str]:
        """
        Remove all lending registrations (lendRemove).

        Args:
            lender_address: Lender's wallet address
            signature: Lender's signature

        Returns:
            (success, message)
        """
        try:
            lend_ids = self._lender_pools.get(lender_address, []).copy()

            for lend_id in lend_ids:
                reg = self._registrations.get(lend_id)
                if reg and reg.is_active():
                    reg.deleted = int(time.time())

                    # Broadcast removal
                    await self._broadcast_lend_removal(reg)

                    # Update DHT
                    await self._store_lend_in_dht(reg)

            # Clear current lending
            if lender_address in self._my_lendings:
                del self._my_lendings[lender_address]

            logger.info(f"Lending removed for: {lender_address}")
            return (True, "Lending removed successfully")

        except Exception as e:
            logger.error(f"Failed to remove lending: {e}")
            return (False, str(e))

    async def remove_lending_by_id(
        self,
        lend_id: str,
        signature: str = "",
    ) -> tuple[bool, str]:
        """
        Remove specific lending registration (poolAddressRemove).

        Args:
            lend_id: Lending ID to remove
            signature: Signature for authorization

        Returns:
            (success, message)
        """
        try:
            reg = self._registrations.get(lend_id)
            if not reg:
                return (False, "Lending registration not found")

            if not reg.is_active():
                return (False, "Lending already removed")

            reg.deleted = int(time.time())

            # Broadcast removal
            await self._broadcast_lend_removal(reg)

            # Update DHT
            await self._store_lend_in_dht(reg)

            # Trigger callbacks
            for callback in self._on_lend_removed:
                try:
                    callback(reg)
                except Exception as e:
                    logger.error(f"Lend removed callback error: {e}")

            logger.info(f"Lending removed: {lend_id}")
            return (True, "Lending removed successfully")

        except Exception as e:
            logger.error(f"Failed to remove lending by ID: {e}")
            return (False, str(e))

    def get_current_lend_address(self, lender_address: str) -> str:
        """
        Get current lending address (lendAddress).

        Args:
            lender_address: Lender's wallet address

        Returns:
            Vault address currently lending to, or empty string
        """
        return self._my_lendings.get(lender_address, "")

    async def get_my_lendings(
        self,
        lender_address: str,
    ) -> List[LendRegistration]:
        """
        Get all pools lender is participating in (poolAddresses).

        Args:
            lender_address: Lender's wallet address

        Returns:
            List of active LendRegistration objects
        """
        lend_ids = self._lender_pools.get(lender_address, [])
        lendings = []

        for lend_id in lend_ids:
            reg = self._registrations.get(lend_id)
            if reg and reg.is_active():
                lendings.append(reg)

        return lendings

    # ========================================================================
    # CALLBACKS
    # ========================================================================

    def on_pool_registered(self, callback: Callable[[PoolConfig], None]) -> None:
        """Register callback for pool registration events."""
        self._on_pool_registered.append(callback)

    def on_lend_registered(self, callback: Callable[[LendRegistration], None]) -> None:
        """Register callback for lending registration events."""
        self._on_lend_registered.append(callback)

    def on_lend_removed(self, callback: Callable[[LendRegistration], None]) -> None:
        """Register callback for lending removal events."""
        self._on_lend_removed.append(callback)

    # ========================================================================
    # INTERNAL - PUBSUB
    # ========================================================================

    async def _broadcast_pool_config(self, config: PoolConfig) -> None:
        """Broadcast pool configuration to network."""
        if not self.peers:
            return

        try:
            message = {
                "type": "pool_config",
                "data": config.to_dict(),
                "timestamp": int(time.time()),
            }
            await self.peers.broadcast(POOL_CONFIG_TOPIC, message)
        except Exception as e:
            logger.error(f"Failed to broadcast pool config: {e}")

    async def _broadcast_lend_registration(self, reg: LendRegistration) -> None:
        """Broadcast lending registration to network."""
        if not self.peers:
            return

        try:
            message = {
                "type": "lend_registration",
                "data": reg.to_dict(),
                "timestamp": int(time.time()),
            }
            await self.peers.broadcast(LEND_REGISTRATION_TOPIC, message)
        except Exception as e:
            logger.error(f"Failed to broadcast lend registration: {e}")

    async def _broadcast_lend_removal(self, reg: LendRegistration) -> None:
        """Broadcast lending removal to network."""
        if not self.peers:
            return

        try:
            message = {
                "type": "lend_removal",
                "data": reg.to_dict(),
                "timestamp": int(time.time()),
            }
            await self.peers.broadcast(LEND_REMOVAL_TOPIC, message)
        except Exception as e:
            logger.error(f"Failed to broadcast lend removal: {e}")

    async def _on_pool_config_message(self, data: dict) -> None:
        """Handle incoming pool config message."""
        try:
            if data.get("type") != "pool_config":
                return

            config = PoolConfig.from_dict(data.get("data", {}))

            # Update if newer
            existing = self._pools.get(config.vault_address)
            if not existing or config.timestamp > existing.timestamp:
                self._pools[config.vault_address] = config
                logger.debug(f"Pool config updated: {config.vault_address}")

                # Trigger callbacks
                for callback in self._on_pool_registered:
                    try:
                        callback(config)
                    except Exception as e:
                        logger.error(f"Pool config callback error: {e}")

        except Exception as e:
            logger.error(f"Error handling pool config message: {e}")

    async def _on_lend_registration_message(self, data: dict) -> None:
        """Handle incoming lend registration message."""
        try:
            if data.get("type") != "lend_registration":
                return

            reg = LendRegistration.from_dict(data.get("data", {}))

            # Update if newer
            existing = self._registrations.get(reg.lend_id)
            if not existing or reg.timestamp > existing.timestamp:
                self._registrations[reg.lend_id] = reg

                # Update indexes
                if reg.lend_id not in self._pool_lenders[reg.vault_address]:
                    self._pool_lenders[reg.vault_address].append(reg.lend_id)
                if reg.lend_id not in self._lender_pools[reg.lender_address]:
                    self._lender_pools[reg.lender_address].append(reg.lend_id)

                logger.debug(f"Lend registration updated: {reg.lend_id}")

                # Trigger callbacks
                for callback in self._on_lend_registered:
                    try:
                        callback(reg)
                    except Exception as e:
                        logger.error(f"Lend registration callback error: {e}")

        except Exception as e:
            logger.error(f"Error handling lend registration message: {e}")

    async def _on_lend_removal_message(self, data: dict) -> None:
        """Handle incoming lend removal message."""
        try:
            if data.get("type") != "lend_removal":
                return

            reg = LendRegistration.from_dict(data.get("data", {}))

            # Update if newer
            existing = self._registrations.get(reg.lend_id)
            if existing and reg.deleted > existing.deleted:
                existing.deleted = reg.deleted
                logger.debug(f"Lend registration marked deleted: {reg.lend_id}")

                # Trigger callbacks
                for callback in self._on_lend_removed:
                    try:
                        callback(reg)
                    except Exception as e:
                        logger.error(f"Lend removal callback error: {e}")

        except Exception as e:
            logger.error(f"Error handling lend removal message: {e}")

    # ========================================================================
    # INTERNAL - DHT
    # ========================================================================

    async def _store_pool_in_dht(self, config: PoolConfig) -> None:
        """Store pool config in DHT."""
        if not self.peers:
            return

        try:
            key = f"{DHT_POOL_PREFIX}{config.vault_address}"
            value = json.dumps(config.to_dict())
            await self.peers.put_dht(key, value.encode())
        except Exception as e:
            logger.warning(f"Failed to store pool in DHT: {e}")

    async def _store_lend_in_dht(self, reg: LendRegistration) -> None:
        """Store lend registration in DHT."""
        if not self.peers:
            return

        try:
            key = f"{DHT_LEND_PREFIX}{reg.lend_id}"
            value = json.dumps(reg.to_dict())
            await self.peers.put_dht(key, value.encode())
        except Exception as e:
            logger.warning(f"Failed to store lend in DHT: {e}")

    async def _register_pool_rendezvous(self, config: PoolConfig) -> None:
        """Register pool in Rendezvous for discovery."""
        if not self.peers:
            return

        try:
            namespace = f"{POOL_RENDEZVOUS_PREFIX}{config.vault_address}"
            await self.peers.register_rendezvous(namespace, ttl=REGISTRATION_TTL)
        except Exception as e:
            logger.warning(f"Failed to register pool in Rendezvous: {e}")
