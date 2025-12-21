"""
satorip2p/protocol/delegation.py

P2P Delegation/Proxy Protocol.

Enables decentralized stake delegation without central server dependency:
- Nodes can delegate their stake to parent nodes
- Parents can view/manage their delegated children
- Charity status for delegations
- Rewards routing via proxy

Matches API from satorilib/server/server.py:
- delegateGet(), delegateRemove()
- stakeProxyChildren(), stakeProxyCharity(), stakeProxyCharityNot()
- stakeProxyRemove()
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

logger = logging.getLogger("satorip2p.protocol.delegation")


# ============================================================================
# CONSTANTS
# ============================================================================

# PubSub Topics
DELEGATION_ANNOUNCEMENT_TOPIC = "satori/delegation/announcements"
DELEGATION_REMOVAL_TOPIC = "satori/delegation/removals"
DELEGATION_CHARITY_TOPIC = "satori/delegation/charity"

# Rendezvous namespace for proxy discovery
PROXY_RENDEZVOUS_PREFIX = "satori/proxies/"

# DHT key prefixes
DHT_DELEGATION_PREFIX = "satori:delegate:"
DHT_PROXY_CHILDREN_PREFIX = "satori:proxy:"

# Defaults
REGISTRATION_TTL = 3600  # 1 hour TTL for rendezvous


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class DelegationRecord:
    """
    Stake delegation record - a child delegating to a parent.

    Schema matches vault-stake.html JavaScript expectations.
    Used by stakeProxyChildren() to return delegations.
    """
    delegation_id: str          # Unique delegation ID
    parent_address: str         # Parent node's wallet address
    child_address: str          # Child's wallet address
    vault_address: str          # Child's vault address
    child_proxy: float = 0.0    # Amount delegated (stake)
    pointed: bool = True        # Whether rewards route to parent
    charity: bool = False       # Marked as charity donation
    alias: str = ""             # Display name
    timestamp: int = 0          # Registration timestamp
    signature: str = ""         # Child's signature
    deleted: int = 0            # Soft delete timestamp (0 = active)

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = int(time.time())
        if not self.delegation_id:
            self.delegation_id = self._generate_id()

    def _generate_id(self) -> str:
        """Generate unique delegation ID from addresses and timestamp."""
        data = f"{self.child_address}:{self.parent_address}:{self.timestamp}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def get_signing_message(self) -> str:
        """Get deterministic message for signing."""
        return f"{self.delegation_id}:{self.child_address}:{self.parent_address}:{self.child_proxy}:{self.timestamp}"

    def get_record_hash(self) -> str:
        """Get deterministic hash of this record for verification."""
        data = f"{self.delegation_id}:{self.parent_address}:{self.child_address}:{self.vault_address}:{self.child_proxy}:{self.pointed}:{self.charity}:{self.timestamp}"
        return hashlib.sha256(data.encode()).hexdigest()

    def is_active(self) -> bool:
        """Check if delegation is active (not deleted)."""
        return self.deleted == 0

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "DelegationRecord":
        """Create from dictionary."""
        return cls(
            delegation_id=data.get("delegation_id", ""),
            parent_address=data.get("parent_address", ""),
            child_address=data.get("child_address", ""),
            vault_address=data.get("vault_address", ""),
            child_proxy=float(data.get("child_proxy", 0.0)),
            pointed=bool(data.get("pointed", True)),
            charity=bool(data.get("charity", False)),
            alias=data.get("alias", ""),
            timestamp=int(data.get("timestamp", 0)),
            signature=data.get("signature", ""),
            deleted=int(data.get("deleted", 0)),
        )

    def to_proxy_child_format(self) -> dict:
        """
        Convert to format expected by vault-stake.html.

        Returns dict matching ProxyChild JavaScript schema:
        {child, child_proxy, alias, address, vaultaddress, pointed}
        """
        return {
            "child": self.delegation_id,  # Used as childId
            "child_proxy": self.child_proxy,
            "alias": self.alias,
            "address": self.child_address,
            "vaultaddress": self.vault_address,
            "pointed": self.pointed,
            "charity": self.charity,
        }


@dataclass
class CharityUpdate:
    """Update to charity status of a delegation."""
    delegation_id: str
    child_address: str
    charity: bool
    timestamp: int = 0
    signature: str = ""

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = int(time.time())

    def get_signing_message(self) -> str:
        """Get deterministic message for signing."""
        return f"{self.delegation_id}:{self.child_address}:{self.charity}:{self.timestamp}"

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "CharityUpdate":
        """Create from dictionary."""
        return cls(
            delegation_id=data.get("delegation_id", ""),
            child_address=data.get("child_address", ""),
            charity=bool(data.get("charity", False)),
            timestamp=int(data.get("timestamp", 0)),
            signature=data.get("signature", ""),
        )


# ============================================================================
# DELEGATION MANAGER
# ============================================================================

class DelegationManager:
    """
    Manages P2P delegation/proxy operations.

    Provides decentralized alternatives to central server endpoints:
    - delegateGet() -> get_my_delegate()
    - delegateRemove() -> remove_delegation()
    - stakeProxyChildren() -> get_proxy_children()
    - stakeProxyCharity() -> set_charity_status(True)
    - stakeProxyCharityNot() -> set_charity_status(False)
    - stakeProxyRemove() -> remove_proxy_child()

    Usage:
        manager = DelegationManager(peers, wallet_address, wallet)
        await manager.start()

        # As child delegating to parent
        await manager.delegate_to(my_addr, parent_addr, amount)
        delegate = manager.get_my_delegate(my_addr)

        # As parent managing delegated children
        children = await manager.get_proxy_children(my_addr)
        await manager.set_charity_status(delegation_id, child_addr, True)
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        wallet_address: str = "",
        wallet: Any = None,
    ):
        """
        Initialize DelegationManager.

        Args:
            peers: P2P peers instance for networking
            wallet_address: This node's wallet address
            wallet: Wallet instance for signing (optional)
        """
        self.peers = peers
        self.wallet_address = wallet_address
        self.wallet = wallet

        # Local caches
        self._delegations: Dict[str, DelegationRecord] = {}  # delegation_id -> record
        self._my_delegate: Dict[str, str] = {}  # child_address -> parent_address

        # Indexes for fast lookup
        self._parent_children: Dict[str, List[str]] = defaultdict(list)  # parent -> [delegation_ids]
        self._child_delegations: Dict[str, List[str]] = defaultdict(list)  # child -> [delegation_ids]

        # Callbacks
        self._on_delegation_created: List[Callable] = []
        self._on_delegation_removed: List[Callable] = []
        self._on_charity_updated: List[Callable] = []

        # State
        self._started = False
        self._subscribed_topics: List[str] = []

    # ========================================================================
    # LIFECYCLE
    # ========================================================================

    async def start(self) -> bool:
        """Start the delegation manager and subscribe to topics."""
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
            logger.info("DelegationManager started")
            return True

        except Exception as e:
            logger.error(f"Failed to start DelegationManager: {e}")
            return False

    async def stop(self) -> None:
        """Stop the delegation manager."""
        if not self._started:
            return

        try:
            # Unsubscribe from topics
            for topic in self._subscribed_topics:
                if self.peers:
                    await self.peers.unsubscribe(topic)
            self._subscribed_topics.clear()

        except Exception as e:
            logger.error(f"Error stopping DelegationManager: {e}")

        self._started = False
        logger.info("DelegationManager stopped")

    async def _subscribe_to_topics(self) -> None:
        """Subscribe to delegation-related PubSub topics."""
        if not self.peers:
            return

        topics = [
            (DELEGATION_ANNOUNCEMENT_TOPIC, self._on_delegation_message),
            (DELEGATION_REMOVAL_TOPIC, self._on_delegation_removal_message),
            (DELEGATION_CHARITY_TOPIC, self._on_charity_update_message),
        ]

        for topic, handler in topics:
            try:
                await self.peers.subscribe(topic, handler)
                self._subscribed_topics.append(topic)
                logger.debug(f"Subscribed to {topic}")
            except Exception as e:
                logger.warning(f"Failed to subscribe to {topic}: {e}")

    async def _load_from_dht(self) -> None:
        """Load existing delegation data from DHT."""
        # DHT loading would happen here in production
        # For now, rely on PubSub for discovery
        pass

    # ========================================================================
    # CHILD (DELEGATOR) METHODS
    # ========================================================================

    async def delegate_to(
        self,
        child_address: str,
        parent_address: str,
        amount: float = 0.0,
        vault_address: str = "",
        alias: str = "",
        signature: str = "",
    ) -> tuple[bool, str]:
        """
        Delegate stake to a parent node.

        Args:
            child_address: Child's wallet address (delegator)
            parent_address: Parent's wallet address (delegate target)
            amount: Amount to delegate
            vault_address: Child's vault address
            alias: Display name
            signature: Child's signature

        Returns:
            (success, message)
        """
        try:
            # Remove existing delegation if any
            existing_parent = self._my_delegate.get(child_address)
            if existing_parent:
                await self.remove_delegation(child_address, signature)

            record = DelegationRecord(
                delegation_id="",  # Will be generated
                parent_address=parent_address,
                child_address=child_address,
                vault_address=vault_address or child_address,
                child_proxy=amount,
                alias=alias,
                signature=signature,
            )

            # Sign if wallet available
            if not signature and self.wallet:
                try:
                    msg = record.get_signing_message()
                    record.signature = self.wallet.sign(msg.encode()).hex()
                except Exception as e:
                    logger.warning(f"Failed to sign delegation: {e}")

            # Store locally
            self._delegations[record.delegation_id] = record
            self._parent_children[parent_address].append(record.delegation_id)
            self._child_delegations[child_address].append(record.delegation_id)
            self._my_delegate[child_address] = parent_address

            # Broadcast to network
            await self._broadcast_delegation(record)

            # Store in DHT
            await self._store_delegation_in_dht(record)

            # Register in Rendezvous for discovery
            await self._register_proxy_rendezvous(record)

            # Trigger callbacks
            for callback in self._on_delegation_created:
                try:
                    callback(record)
                except Exception as e:
                    logger.error(f"Delegation created callback error: {e}")

            logger.info(f"Delegation created: {child_address} -> {parent_address}")
            return (True, "Delegation created successfully")

        except Exception as e:
            logger.error(f"Failed to create delegation: {e}")
            return (False, str(e))

    async def remove_delegation(
        self,
        child_address: str,
        signature: str = "",
    ) -> tuple[bool, str]:
        """
        Remove delegation (delegateRemove).

        Args:
            child_address: Child's wallet address
            signature: Child's signature

        Returns:
            (success, message)
        """
        try:
            delegation_ids = self._child_delegations.get(child_address, []).copy()

            for delegation_id in delegation_ids:
                record = self._delegations.get(delegation_id)
                if record and record.is_active():
                    record.deleted = int(time.time())

                    # Broadcast removal
                    await self._broadcast_delegation_removal(record)

                    # Update DHT
                    await self._store_delegation_in_dht(record)

                    # Trigger callbacks
                    for callback in self._on_delegation_removed:
                        try:
                            callback(record)
                        except Exception as e:
                            logger.error(f"Delegation removed callback error: {e}")

            # Clear current delegate
            if child_address in self._my_delegate:
                del self._my_delegate[child_address]

            logger.info(f"Delegation removed for: {child_address}")
            return (True, "Delegation removed successfully")

        except Exception as e:
            logger.error(f"Failed to remove delegation: {e}")
            return (False, str(e))

    def get_my_delegate(self, child_address: str) -> str:
        """
        Get current delegate address (delegateGet).

        Args:
            child_address: Child's wallet address

        Returns:
            Parent address delegating to, or empty string
        """
        return self._my_delegate.get(child_address, "")

    # ========================================================================
    # PARENT (DELEGATE TARGET) METHODS
    # ========================================================================

    async def get_proxy_children(
        self,
        parent_address: str,
    ) -> List[DelegationRecord]:
        """
        Get children delegating to this parent (stakeProxyChildren).

        Args:
            parent_address: Parent's wallet address

        Returns:
            List of active DelegationRecord objects
        """
        delegation_ids = self._parent_children.get(parent_address, [])
        children = []

        for delegation_id in delegation_ids:
            record = self._delegations.get(delegation_id)
            if record and record.is_active():
                children.append(record)

        return children

    async def get_proxy_children_formatted(
        self,
        parent_address: str,
    ) -> List[dict]:
        """
        Get children in format expected by vault-stake.html.

        Args:
            parent_address: Parent's wallet address

        Returns:
            List of dicts matching ProxyChild JavaScript schema
        """
        children = await self.get_proxy_children(parent_address)
        return [c.to_proxy_child_format() for c in children]

    async def set_charity_status(
        self,
        delegation_id: str,
        child_address: str,
        charity: bool,
        signature: str = "",
    ) -> tuple[bool, str]:
        """
        Set charity status (stakeProxyCharity / stakeProxyCharityNot).

        Args:
            delegation_id: Delegation ID (childId in API)
            child_address: Child's wallet address
            charity: True for charity, False for non-charity
            signature: Parent's signature

        Returns:
            (success, message)
        """
        try:
            # Find the delegation record
            record = self._delegations.get(delegation_id)
            if not record:
                # Try to find by child address
                for did in self._child_delegations.get(child_address, []):
                    r = self._delegations.get(did)
                    if r and r.is_active():
                        record = r
                        break

            if not record:
                return (False, "Delegation not found")

            if not record.is_active():
                return (False, "Delegation is not active")

            # Update charity status
            record.charity = charity
            record.timestamp = int(time.time())

            # Create charity update for broadcast
            update = CharityUpdate(
                delegation_id=delegation_id,
                child_address=child_address,
                charity=charity,
                signature=signature,
            )

            # Sign if wallet available
            if not signature and self.wallet:
                try:
                    msg = update.get_signing_message()
                    update.signature = self.wallet.sign(msg.encode()).hex()
                except Exception as e:
                    logger.warning(f"Failed to sign charity update: {e}")

            # Broadcast update
            await self._broadcast_charity_update(update)

            # Update DHT
            await self._store_delegation_in_dht(record)

            # Trigger callbacks
            for callback in self._on_charity_updated:
                try:
                    callback(record, charity)
                except Exception as e:
                    logger.error(f"Charity update callback error: {e}")

            status = "charity" if charity else "non-charity"
            logger.info(f"Delegation {delegation_id} marked as {status}")
            return (True, f"Delegation marked as {status}")

        except Exception as e:
            logger.error(f"Failed to set charity status: {e}")
            return (False, str(e))

    async def remove_proxy_child(
        self,
        delegation_id: str,
        child_address: str,
        signature: str = "",
    ) -> tuple[bool, str]:
        """
        Remove a proxy child (stakeProxyRemove).

        Args:
            delegation_id: Delegation ID (childId in API)
            child_address: Child's wallet address
            signature: Parent's signature

        Returns:
            (success, message)
        """
        try:
            record = self._delegations.get(delegation_id)
            if not record:
                return (False, "Delegation not found")

            if not record.is_active():
                return (False, "Delegation already removed")

            # Verify child address matches
            if record.child_address != child_address:
                return (False, "Child address mismatch")

            record.deleted = int(time.time())

            # Broadcast removal
            await self._broadcast_delegation_removal(record)

            # Update DHT
            await self._store_delegation_in_dht(record)

            # Update local state
            if child_address in self._my_delegate:
                del self._my_delegate[child_address]

            # Trigger callbacks
            for callback in self._on_delegation_removed:
                try:
                    callback(record)
                except Exception as e:
                    logger.error(f"Delegation removed callback error: {e}")

            logger.info(f"Proxy child removed: {delegation_id}")
            return (True, "Proxy child removed successfully")

        except Exception as e:
            logger.error(f"Failed to remove proxy child: {e}")
            return (False, str(e))

    # ========================================================================
    # CALLBACKS
    # ========================================================================

    def on_delegation_created(self, callback: Callable[[DelegationRecord], None]) -> None:
        """Register callback for delegation creation events."""
        self._on_delegation_created.append(callback)

    def on_delegation_removed(self, callback: Callable[[DelegationRecord], None]) -> None:
        """Register callback for delegation removal events."""
        self._on_delegation_removed.append(callback)

    def on_charity_updated(self, callback: Callable[[DelegationRecord, bool], None]) -> None:
        """Register callback for charity status update events."""
        self._on_charity_updated.append(callback)

    # ========================================================================
    # INTERNAL - PUBSUB
    # ========================================================================

    async def _broadcast_delegation(self, record: DelegationRecord) -> None:
        """Broadcast delegation to network."""
        if not self.peers:
            return

        try:
            message = {
                "type": "delegation",
                "data": record.to_dict(),
                "timestamp": int(time.time()),
            }
            await self.peers.broadcast(DELEGATION_ANNOUNCEMENT_TOPIC, message)
        except Exception as e:
            logger.error(f"Failed to broadcast delegation: {e}")

    async def _broadcast_delegation_removal(self, record: DelegationRecord) -> None:
        """Broadcast delegation removal to network."""
        if not self.peers:
            return

        try:
            message = {
                "type": "delegation_removal",
                "data": record.to_dict(),
                "timestamp": int(time.time()),
            }
            await self.peers.broadcast(DELEGATION_REMOVAL_TOPIC, message)
        except Exception as e:
            logger.error(f"Failed to broadcast delegation removal: {e}")

    async def _broadcast_charity_update(self, update: CharityUpdate) -> None:
        """Broadcast charity status update to network."""
        if not self.peers:
            return

        try:
            message = {
                "type": "charity_update",
                "data": update.to_dict(),
                "timestamp": int(time.time()),
            }
            await self.peers.broadcast(DELEGATION_CHARITY_TOPIC, message)
        except Exception as e:
            logger.error(f"Failed to broadcast charity update: {e}")

    async def _on_delegation_message(self, data: dict) -> None:
        """Handle incoming delegation message."""
        try:
            if data.get("type") != "delegation":
                return

            record = DelegationRecord.from_dict(data.get("data", {}))

            # Update if newer
            existing = self._delegations.get(record.delegation_id)
            if not existing or record.timestamp > existing.timestamp:
                self._delegations[record.delegation_id] = record

                # Update indexes
                if record.delegation_id not in self._parent_children[record.parent_address]:
                    self._parent_children[record.parent_address].append(record.delegation_id)
                if record.delegation_id not in self._child_delegations[record.child_address]:
                    self._child_delegations[record.child_address].append(record.delegation_id)

                # Update delegate tracking
                if record.is_active():
                    self._my_delegate[record.child_address] = record.parent_address

                logger.debug(f"Delegation updated: {record.delegation_id}")

                # Trigger callbacks
                for callback in self._on_delegation_created:
                    try:
                        callback(record)
                    except Exception as e:
                        logger.error(f"Delegation message callback error: {e}")

        except Exception as e:
            logger.error(f"Error handling delegation message: {e}")

    async def _on_delegation_removal_message(self, data: dict) -> None:
        """Handle incoming delegation removal message."""
        try:
            if data.get("type") != "delegation_removal":
                return

            record = DelegationRecord.from_dict(data.get("data", {}))

            # Update if newer
            existing = self._delegations.get(record.delegation_id)
            if existing and record.deleted > existing.deleted:
                existing.deleted = record.deleted

                # Update delegate tracking
                if record.child_address in self._my_delegate:
                    del self._my_delegate[record.child_address]

                logger.debug(f"Delegation marked deleted: {record.delegation_id}")

                # Trigger callbacks
                for callback in self._on_delegation_removed:
                    try:
                        callback(existing)
                    except Exception as e:
                        logger.error(f"Delegation removal callback error: {e}")

        except Exception as e:
            logger.error(f"Error handling delegation removal message: {e}")

    async def _on_charity_update_message(self, data: dict) -> None:
        """Handle incoming charity update message."""
        try:
            if data.get("type") != "charity_update":
                return

            update = CharityUpdate.from_dict(data.get("data", {}))

            # Find and update the record
            record = self._delegations.get(update.delegation_id)
            if record and update.timestamp > record.timestamp:
                record.charity = update.charity
                record.timestamp = update.timestamp
                logger.debug(f"Delegation charity updated: {update.delegation_id} -> {update.charity}")

                # Trigger callbacks
                for callback in self._on_charity_updated:
                    try:
                        callback(record, update.charity)
                    except Exception as e:
                        logger.error(f"Charity update callback error: {e}")

        except Exception as e:
            logger.error(f"Error handling charity update message: {e}")

    # ========================================================================
    # INTERNAL - DHT
    # ========================================================================

    async def _store_delegation_in_dht(self, record: DelegationRecord) -> None:
        """Store delegation record in DHT."""
        if not self.peers:
            return

        try:
            key = f"{DHT_DELEGATION_PREFIX}{record.delegation_id}"
            value = json.dumps(record.to_dict())
            await self.peers.put_dht(key, value.encode())
        except Exception as e:
            logger.warning(f"Failed to store delegation in DHT: {e}")

    async def _get_delegation_from_dht(self, delegation_id: str) -> Optional[DelegationRecord]:
        """Retrieve delegation record from DHT."""
        if not self.peers:
            return None

        try:
            key = f"{DHT_DELEGATION_PREFIX}{delegation_id}"
            data = await self.peers.get_dht(key)
            if data:
                record_dict = json.loads(data.decode() if isinstance(data, bytes) else data)
                return DelegationRecord.from_dict(record_dict)
        except Exception as e:
            logger.warning(f"Failed to get delegation from DHT: {e}")

        return None

    async def _register_proxy_rendezvous(self, record: DelegationRecord) -> None:
        """Register delegation in Rendezvous for discovery."""
        if not self.peers:
            return

        try:
            namespace = f"{PROXY_RENDEZVOUS_PREFIX}{record.parent_address}"
            await self.peers.register_rendezvous(namespace, ttl=REGISTRATION_TTL)
        except Exception as e:
            logger.warning(f"Failed to register proxy in Rendezvous: {e}")
