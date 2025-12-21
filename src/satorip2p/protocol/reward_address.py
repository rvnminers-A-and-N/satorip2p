"""
satorip2p/protocol/reward_address.py

Custom reward address management for Satori P2P network.

Allows nodes to specify a different payout address for rewards,
enabling cold wallet, paper wallet, or hardware wallet payouts.

The reward address is stored in DHT and signed by the wallet to prove ownership.
This integrates with the delegation system - if delegated, rewards can route
through the delegation chain before reaching the final payout address.
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# DHT key prefixes
DHT_REWARD_ADDR_PREFIX = "satori:reward_addr:"

# PubSub topics
REWARD_ADDR_TOPIC = "satori/reward_address/updates"

# Cache duration
CACHE_TTL = 300  # 5 minutes


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class RewardAddressRecord:
    """
    A custom reward address registration.

    The wallet owner signs this record to prove they control the wallet
    and authorize rewards to be sent to the payout_address.
    """
    wallet_address: str        # The node's wallet address (signer)
    payout_address: str        # Where rewards should be sent
    signature: str             # Wallet signature proving ownership
    pubkey: str                # Public key for verification
    timestamp: int             # When this was set
    expires_at: Optional[int] = None  # Optional expiration (0 = never)
    memo: str = ""             # Optional memo (e.g., "Cold wallet")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RewardAddressRecord":
        """Create from dictionary."""
        return cls(**data)

    def is_expired(self) -> bool:
        """Check if this record has expired."""
        if not self.expires_at or self.expires_at == 0:
            return False
        return int(time.time()) > self.expires_at

    def get_message_to_sign(self) -> str:
        """
        Get the message that should be signed.

        Format: "SATORI_REWARD_ADDR:{wallet}:{payout}:{timestamp}"
        """
        return f"SATORI_REWARD_ADDR:{self.wallet_address}:{self.payout_address}:{self.timestamp}"

    @staticmethod
    def generate_id(wallet_address: str) -> str:
        """Generate DHT key for this wallet."""
        return f"{DHT_REWARD_ADDR_PREFIX}{wallet_address}"


@dataclass
class RewardAddressCache:
    """Cached reward address with TTL."""
    record: RewardAddressRecord
    cached_at: int
    ttl: int = CACHE_TTL

    def is_stale(self) -> bool:
        """Check if cache entry is stale."""
        return int(time.time()) - self.cached_at > self.ttl


# ============================================================================
# REWARD ADDRESS MANAGER
# ============================================================================

class RewardAddressManager:
    """
    Manages custom reward addresses for P2P reward distribution.

    Features:
    - Set custom payout address (signed by wallet)
    - Store in DHT for network-wide access
    - Cache lookups for performance
    - Broadcast updates via PubSub
    - Integrate with delegation system

    Usage:
        manager = RewardAddressManager(peers=p2p_peers, wallet=my_wallet)

        # Set custom reward address
        await manager.set_reward_address(
            payout_address="EcoldWallet123...",
            memo="Hardware wallet"
        )

        # Look up reward address for distribution
        payout = await manager.get_payout_address("Ewallet123...")
        # Returns custom address if set, otherwise returns wallet address
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        wallet: Any = None,
        signer: Optional[Callable] = None,
        verifier: Optional[Callable] = None,
    ):
        """
        Initialize RewardAddressManager.

        Args:
            peers: Peers instance for P2P operations
            wallet: Wallet for signing (must have .address and .sign())
            signer: Optional custom signing function
            verifier: Optional custom verification function
        """
        self._peers = peers
        self._wallet = wallet
        self._signer = signer
        self._verifier = verifier

        # Local cache: wallet_address -> RewardAddressCache
        self._cache: Dict[str, RewardAddressCache] = {}

        # Our own record (if set)
        self._my_record: Optional[RewardAddressRecord] = None

        # Callbacks for updates
        self._update_callbacks: List[Callable] = []

    # ========================================================================
    # PUBLIC API
    # ========================================================================

    async def set_reward_address(
        self,
        payout_address: str,
        memo: str = "",
        expires_in: Optional[int] = None,
    ) -> RewardAddressRecord:
        """
        Set a custom reward/payout address.

        Args:
            payout_address: Address where rewards should be sent
            memo: Optional description (e.g., "Cold wallet")
            expires_in: Optional seconds until expiration (None = never)

        Returns:
            The created RewardAddressRecord

        Raises:
            ValueError: If wallet not available or signing fails
        """
        if not self._wallet:
            raise ValueError("Wallet required to set reward address")

        wallet_address = self._wallet.address
        timestamp = int(time.time())
        expires_at = timestamp + expires_in if expires_in else None

        # Create record
        record = RewardAddressRecord(
            wallet_address=wallet_address,
            payout_address=payout_address,
            signature="",  # Will be set after signing
            pubkey=self._get_pubkey(),
            timestamp=timestamp,
            expires_at=expires_at,
            memo=memo,
        )

        # Sign the record
        message = record.get_message_to_sign()
        signature = await self._sign_message(message)
        record.signature = signature

        # Store in DHT
        await self._store_in_dht(record)

        # Broadcast update
        await self._broadcast_update(record)

        # Cache locally
        self._my_record = record
        self._cache[wallet_address] = RewardAddressCache(
            record=record,
            cached_at=timestamp,
        )

        logger.info(
            f"Reward address set: {wallet_address[:12]}... → {payout_address[:12]}..."
        )

        return record

    async def remove_reward_address(self) -> bool:
        """
        Remove custom reward address (revert to wallet address).

        Returns:
            True if removed successfully
        """
        if not self._wallet:
            raise ValueError("Wallet required")

        wallet_address = self._wallet.address

        # Create a "removal" record (payout = wallet, indicating default)
        record = RewardAddressRecord(
            wallet_address=wallet_address,
            payout_address=wallet_address,  # Same as wallet = no custom address
            signature="",
            pubkey=self._get_pubkey(),
            timestamp=int(time.time()),
            memo="removed",
        )

        message = record.get_message_to_sign()
        record.signature = await self._sign_message(message)

        # Store removal in DHT
        await self._store_in_dht(record)

        # Broadcast
        await self._broadcast_update(record)

        # Clear local cache
        self._my_record = None
        if wallet_address in self._cache:
            del self._cache[wallet_address]

        logger.info(f"Reward address removed for {wallet_address[:12]}...")
        return True

    async def get_payout_address(self, wallet_address: str) -> str:
        """
        Get the payout address for a wallet.

        If a custom reward address is set and valid, returns that.
        Otherwise, returns the wallet address itself.

        Args:
            wallet_address: The wallet to look up

        Returns:
            The address where rewards should be sent
        """
        record = await self.get_reward_address_record(wallet_address)

        if record and not record.is_expired():
            # Custom address is set
            if record.payout_address != record.wallet_address:
                return record.payout_address

        # Default: rewards go to wallet address
        return wallet_address

    async def get_reward_address_record(
        self,
        wallet_address: str
    ) -> Optional[RewardAddressRecord]:
        """
        Get the full reward address record for a wallet.

        Args:
            wallet_address: The wallet to look up

        Returns:
            RewardAddressRecord if found, None otherwise
        """
        # Check cache first
        if wallet_address in self._cache:
            cached = self._cache[wallet_address]
            if not cached.is_stale():
                return cached.record

        # Look up in DHT
        record = await self._lookup_in_dht(wallet_address)

        if record:
            # Verify signature
            if await self._verify_record(record):
                # Cache it
                self._cache[wallet_address] = RewardAddressCache(
                    record=record,
                    cached_at=int(time.time()),
                )
                return record
            else:
                logger.warning(f"Invalid signature for reward address: {wallet_address}")

        return None

    async def get_batch_payout_addresses(
        self,
        wallet_addresses: List[str]
    ) -> Dict[str, str]:
        """
        Get payout addresses for multiple wallets at once.

        Args:
            wallet_addresses: List of wallets to look up

        Returns:
            Dict mapping wallet_address -> payout_address
        """
        results = {}

        # Batch lookup (could be parallelized)
        for wallet in wallet_addresses:
            results[wallet] = await self.get_payout_address(wallet)

        return results

    def get_my_reward_address(self) -> Optional[str]:
        """Get our own custom reward address (if set)."""
        if self._my_record and not self._my_record.is_expired():
            if self._my_record.payout_address != self._my_record.wallet_address:
                return self._my_record.payout_address
        return None

    def has_custom_address(self, wallet_address: str) -> bool:
        """Check if a wallet has a custom reward address (from cache)."""
        if wallet_address in self._cache:
            record = self._cache[wallet_address].record
            return (
                not record.is_expired() and
                record.payout_address != record.wallet_address
            )
        return False

    # ========================================================================
    # SUBSCRIPTION
    # ========================================================================

    async def subscribe_to_updates(self) -> bool:
        """Subscribe to reward address updates via PubSub."""
        if not self._peers or not self._peers._pubsub:
            return False

        try:
            await self._peers.subscribe(
                REWARD_ADDR_TOPIC,
                self._handle_update_message
            )
            logger.debug("Subscribed to reward address updates")
            return True
        except Exception as e:
            logger.warning(f"Failed to subscribe to reward address updates: {e}")
            return False

    def on_update(self, callback: Callable[[RewardAddressRecord], None]):
        """Register callback for reward address updates."""
        self._update_callbacks.append(callback)

    async def _handle_update_message(self, message: Dict[str, Any]):
        """Handle incoming reward address update."""
        try:
            record = RewardAddressRecord.from_dict(message.get('record', {}))

            # Verify signature
            if not await self._verify_record(record):
                logger.warning(f"Invalid reward address update from {record.wallet_address}")
                return

            # Update cache
            self._cache[record.wallet_address] = RewardAddressCache(
                record=record,
                cached_at=int(time.time()),
            )

            # Notify callbacks
            for callback in self._update_callbacks:
                try:
                    callback(record)
                except Exception as e:
                    logger.warning(f"Callback error: {e}")

        except Exception as e:
            logger.warning(f"Failed to handle reward address update: {e}")

    # ========================================================================
    # DHT OPERATIONS
    # ========================================================================

    async def _store_in_dht(self, record: RewardAddressRecord) -> bool:
        """Store reward address record in DHT."""
        if not self._peers:
            return False

        try:
            dht_key = record.generate_id(record.wallet_address)
            data = json.dumps(record.to_dict()).encode()

            if hasattr(self._peers, '_dht') and self._peers._dht:
                await self._peers._dht.put(
                    key=dht_key,
                    value=data,
                    ttl=0  # Permanent until updated
                )
                logger.debug(f"Stored reward address in DHT: {dht_key}")
                return True
        except Exception as e:
            logger.warning(f"Failed to store reward address in DHT: {e}")

        return False

    async def _lookup_in_dht(
        self,
        wallet_address: str
    ) -> Optional[RewardAddressRecord]:
        """Look up reward address record from DHT."""
        if not self._peers:
            return None

        try:
            dht_key = RewardAddressRecord.generate_id(wallet_address)

            if hasattr(self._peers, '_dht') and self._peers._dht:
                data = await self._peers._dht.get(dht_key)
                if data:
                    record_dict = json.loads(data.decode())
                    return RewardAddressRecord.from_dict(record_dict)
        except Exception as e:
            logger.debug(f"DHT lookup failed for reward address: {e}")

        return None

    async def _broadcast_update(self, record: RewardAddressRecord) -> bool:
        """Broadcast reward address update via PubSub."""
        if not self._peers or not self._peers._pubsub:
            return False

        try:
            message = {
                'type': 'reward_address_update',
                'record': record.to_dict(),
                'timestamp': int(time.time()),
            }

            await self._peers.broadcast(REWARD_ADDR_TOPIC, message)
            return True
        except Exception as e:
            logger.warning(f"Failed to broadcast reward address update: {e}")
            return False

    # ========================================================================
    # SIGNING / VERIFICATION
    # ========================================================================

    def _get_pubkey(self) -> str:
        """Get public key from wallet."""
        if self._wallet and hasattr(self._wallet, 'pubkey'):
            return self._wallet.pubkey
        return ""

    async def _sign_message(self, message: str) -> str:
        """Sign a message with the wallet."""
        if self._signer:
            return await self._signer(message)

        if self._wallet and hasattr(self._wallet, 'sign'):
            # Handle both sync and async sign methods
            result = self._wallet.sign(message)
            if asyncio.iscoroutine(result):
                return await result
            return result

        raise ValueError("No signing method available")

    async def _verify_record(self, record: RewardAddressRecord) -> bool:
        """Verify a reward address record's signature."""
        if self._verifier:
            message = record.get_message_to_sign()
            return await self._verifier(message, record.signature, record.pubkey)

        # Default: trust the signature if it's present
        # In production, implement proper Evrmore signature verification
        return bool(record.signature)

    # ========================================================================
    # SERIALIZATION
    # ========================================================================

    def to_dict(self) -> Dict[str, Any]:
        """Serialize manager state."""
        return {
            'my_record': self._my_record.to_dict() if self._my_record else None,
            'cache': {
                addr: {
                    'record': cache.record.to_dict(),
                    'cached_at': cache.cached_at,
                }
                for addr, cache in self._cache.items()
            },
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        peers: Optional["Peers"] = None,
        wallet: Any = None,
    ) -> "RewardAddressManager":
        """Deserialize manager state."""
        manager = cls(peers=peers, wallet=wallet)

        if data.get('my_record'):
            manager._my_record = RewardAddressRecord.from_dict(data['my_record'])

        for addr, cache_data in data.get('cache', {}).items():
            manager._cache[addr] = RewardAddressCache(
                record=RewardAddressRecord.from_dict(cache_data['record']),
                cached_at=cache_data['cached_at'],
            )

        return manager
