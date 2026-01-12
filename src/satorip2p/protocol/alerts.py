"""
Treasury Alert System for Satori Network.

Handles network-wide alerts for treasury edge cases:
- Insufficient SATORI for reward distribution
- Insufficient EVR for transaction fees
- Critical treasury states

Broadcasts alerts via PubSub and provides API for querying status.

Storage: Uses RedundantStorage for three-tier persistence:
1. Memory cache - Fast access
2. Local disk - Crash recovery
3. DHT - Network redundancy and recovery
"""

import time
import logging
import hashlib
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    from .storage import AlertStorage

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

# PubSub topics for alerts
TOPIC_TREASURY_ALERTS = 'satori/alerts/treasury'
TOPIC_DISTRIBUTION_ALERTS = 'satori/alerts/distribution'
TOPIC_SYSTEM_ALERTS = 'satori/alerts/system'

# Alert severities
class AlertSeverity(Enum):
    INFO = 'info'
    WARNING = 'warning'
    CRITICAL = 'critical'


# Alert types
class AlertType(Enum):
    # Treasury alerts
    INSUFFICIENT_SATORI = 'insufficient_satori'
    INSUFFICIENT_EVR = 'insufficient_evr'
    TREASURY_CRITICAL = 'treasury_critical'
    TREASURY_HEALTHY = 'treasury_healthy'

    # Distribution alerts
    DISTRIBUTION_SUCCESS = 'distribution_success'
    DISTRIBUTION_DEFERRED = 'distribution_deferred'
    DISTRIBUTION_FAILED = 'distribution_failed'
    REWARDS_RESUMED = 'rewards_resumed'
    PARTIAL_CATCHUP = 'partial_catchup'

    # System alerts
    SYSTEM_MAINTENANCE = 'system_maintenance'
    SYSTEM_UPGRADE = 'system_upgrade'


# EVR fee estimates (Evrmore network)
EVR_FEES = {
    'simple_transfer': 0.01,         # Single output transfer
    'batch_distribution_small': 0.05, # ~10 recipients
    'batch_distribution': 0.1,        # ~50 recipients
    'large_distribution': 0.5,        # ~500 recipients
    'asset_issuance_unique': 5.0,     # Create unique asset
    'asset_issuance_sub': 100.0,      # Create sub-asset
    'buffer_multiplier': 1.5,         # Safety buffer
}

# Minimum balances to consider "healthy"
MIN_HEALTHY_SATORI = 10000.0  # 10k SATORI
MIN_HEALTHY_EVR = 1.0         # 1 EVR


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class AlertAction:
    """An action button for an alert."""
    label: str           # Button text (e.g., "Donate EVR")
    action_type: str     # 'donate_evr', 'view_status', 'none'
    url: Optional[str] = None
    description: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TreasuryAlert:
    """A treasury alert broadcast to the network."""
    alert_id: str
    alert_type: str
    severity: str
    message: str
    details: Dict[str, Any]
    action: str  # Legacy single action text
    actions: List[AlertAction] = field(default_factory=list)  # Action buttons
    treasury_address: Optional[str] = None
    donate_url: Optional[str] = None
    timestamp: int = field(default_factory=lambda: int(time.time()))

    def to_dict(self) -> dict:
        data = asdict(self)
        data['actions'] = [a.to_dict() if hasattr(a, 'to_dict') else a for a in self.actions]
        return data

    @classmethod
    def from_dict(cls, data: dict) -> 'TreasuryAlert':
        actions_data = data.pop('actions', [])
        alert = cls(**data)
        alert.actions = [
            AlertAction(**a) if isinstance(a, dict) else a
            for a in actions_data
        ]
        return alert


@dataclass
class TreasuryStatus:
    """Current treasury status."""
    status: str  # 'healthy', 'warning', 'critical'
    satori_balance: float
    evr_balance: float
    satori_required: float
    evr_required: float
    active_alerts: List[TreasuryAlert]
    deferred_rounds: int
    total_deferred_rewards: float
    last_successful_distribution: Optional[int]
    timestamp: int = field(default_factory=lambda: int(time.time()))

    def to_dict(self) -> dict:
        return {
            'status': self.status,
            'satori_balance': self.satori_balance,
            'evr_balance': self.evr_balance,
            'satori_required': self.satori_required,
            'evr_required': self.evr_required,
            'active_alerts': [a.to_dict() for a in self.active_alerts],
            'deferred_rounds': self.deferred_rounds,
            'total_deferred_rewards': self.total_deferred_rewards,
            'last_successful_distribution': self.last_successful_distribution,
            'timestamp': self.timestamp,
        }


@dataclass
class AlertHistoryEntry:
    """Historical alert record."""
    alert: TreasuryAlert
    resolved_at: Optional[int] = None
    resolution: Optional[str] = None


# =============================================================================
# TREASURY ALERT MANAGER
# =============================================================================

class TreasuryAlertManager:
    """
    Manages treasury alerts and broadcasts them to the network.

    Monitors treasury balances and broadcasts alerts when:
    - SATORI balance is too low for reward distribution
    - EVR balance is too low for transaction fees
    - Both are critically low

    Storage:
    - Uses RedundantStorage for three-tier persistence (memory + disk + DHT)
    - Alert history survives node restarts
    - Active alerts recoverable from network

    Usage:
        manager = TreasuryAlertManager(peers, treasury_address)
        manager.set_balance_getter(get_balances_func)

        # Check and broadcast alerts
        status = await manager.check_treasury_status(required_satori=1369.0)

        # Subscribe to alerts
        manager.on_alert(callback)

        # Sync to DHT for network redundancy
        await manager.sync_to_dht()

        # Recover from DHT if local data lost
        await manager.recover_from_dht()
    """

    def __init__(
        self,
        peers=None,
        treasury_address: str = None,
        donate_url: str = 'https://satorinet.io/donate',
        storage_dir: Path = None,
    ):
        self._peers = peers
        self._treasury_address = treasury_address
        self._donate_url = donate_url
        self._storage_dir = storage_dir

        # Balance getter function (injected)
        self._get_balances: Optional[Callable[[], Dict[str, float]]] = None

        # Alert callbacks
        self._alert_callbacks: List[Callable[[TreasuryAlert], None]] = []

        # Current state
        self._active_alerts: List[TreasuryAlert] = []
        self._alert_history: List[AlertHistoryEntry] = []
        self._deferred_rounds: int = 0
        self._total_deferred_rewards: float = 0.0
        self._last_successful_distribution: Optional[int] = None

        # Redundant storage backend
        self._storage: Optional["AlertStorage"] = None
        self._dht_sync_pending: bool = False

        # Subscribe to incoming alerts if peers available
        if self._peers:
            self._peers.subscribe(TOPIC_TREASURY_ALERTS, self._on_alert_received)

    async def start(self) -> None:
        """Start the alert manager and initialize storage."""
        await self.initialize_storage()

    async def stop(self) -> None:
        """Stop the alert manager and sync to DHT."""
        # Final sync before shutdown
        if self._storage and self._dht_sync_pending:
            await self.sync_to_dht()

        if self._peers:
            self._peers.unsubscribe(TOPIC_TREASURY_ALERTS)

    def set_peers(self, peers) -> None:
        """Set the P2P peers instance."""
        self._peers = peers
        if peers:
            peers.subscribe(TOPIC_TREASURY_ALERTS, self._on_alert_received)
        if self._storage:
            self._storage.set_peers(peers)

    def set_storage(self, storage: "AlertStorage") -> None:
        """Set the redundant storage backend."""
        self._storage = storage

    async def initialize_storage(self) -> None:
        """Initialize redundant storage and load persisted data."""
        if self._storage is None:
            from .storage import AlertStorage
            self._storage = AlertStorage(
                peers=self._peers,
                storage_dir=self._storage_dir,
            )

        # Load any persisted data from disk/DHT
        await self._load_from_storage()

    async def _load_from_storage(self) -> None:
        """Load alerts from persistent storage."""
        if not self._storage:
            return

        try:
            # Load active alerts
            stored_active = await self._storage.get_active_alerts()
            self._active_alerts = [
                TreasuryAlert(
                    alert_id=a.alert_id,
                    alert_type=a.alert_type,
                    severity=a.severity,
                    message=a.message,
                    details=a.details,
                    action="",  # Legacy field
                    timestamp=a.timestamp,
                )
                for a in stored_active
            ]

            # Load history
            stored_history = await self._storage.get_history(limit=100)
            self._alert_history = [
                AlertHistoryEntry(
                    alert=TreasuryAlert(
                        alert_id=a.alert_id,
                        alert_type=a.alert_type,
                        severity=a.severity,
                        message=a.message,
                        details=a.details,
                        action="",
                        timestamp=a.timestamp,
                    ),
                    resolved_at=a.resolved_at,
                    resolution=a.resolution,
                )
                for a in stored_history
            ]

            logger.info(
                f"Loaded {len(self._active_alerts)} active alerts, "
                f"{len(self._alert_history)} history from storage"
            )
        except Exception as e:
            logger.error(f"Failed to load from storage: {e}")

    def set_treasury_address(self, address: str) -> None:
        """Set the treasury address."""
        self._treasury_address = address

    def set_balance_getter(self, getter: Callable[[], Dict[str, float]]) -> None:
        """
        Set the function to get treasury balances.

        The getter should return: {'satori': float, 'evr': float}
        """
        self._get_balances = getter

    def on_alert(self, callback: Callable[[TreasuryAlert], None]) -> None:
        """Register a callback for alerts."""
        self._alert_callbacks.append(callback)

    def _notify_callbacks(self, alert: TreasuryAlert) -> None:
        """Notify all registered callbacks."""
        for callback in self._alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")

    def _on_alert_received(self, topic: str, data: Any) -> None:
        """Handle incoming alert from P2P network."""
        try:
            if isinstance(data, dict):
                alert = TreasuryAlert.from_dict(data)
                self._process_incoming_alert(alert)
        except Exception as e:
            logger.error(f"Error processing incoming alert: {e}")

    def _process_incoming_alert(self, alert: TreasuryAlert) -> None:
        """Process an incoming alert."""
        # Update active alerts
        if alert.alert_type in [AlertType.TREASURY_HEALTHY.value, AlertType.REWARDS_RESUMED.value]:
            # Clear related alerts
            self._active_alerts = [
                a for a in self._active_alerts
                if a.alert_type not in [
                    AlertType.INSUFFICIENT_SATORI.value,
                    AlertType.INSUFFICIENT_EVR.value,
                    AlertType.TREASURY_CRITICAL.value,
                ]
            ]
        else:
            # Add/update alert
            self._active_alerts = [
                a for a in self._active_alerts
                if a.alert_type != alert.alert_type
            ]
            self._active_alerts.append(alert)

        # Notify callbacks
        self._notify_callbacks(alert)

    def _generate_alert_id(self, alert_type: str) -> str:
        """Generate unique alert ID."""
        data = f"{alert_type}:{int(time.time())}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def get_treasury_balances(self) -> Dict[str, float]:
        """Get current treasury balances."""
        if self._get_balances:
            try:
                return self._get_balances()
            except Exception as e:
                logger.error(f"Error getting balances: {e}")
        return {'satori': 0.0, 'evr': 0.0}

    def estimate_evr_required(
        self,
        recipient_count: int,
        include_asset_issuance: bool = False,
        asset_type: str = None,
    ) -> float:
        """
        Estimate EVR required for an operation.

        Args:
            recipient_count: Number of reward recipients
            include_asset_issuance: Whether to include asset creation
            asset_type: 'unique' or 'sub' for asset creation

        Returns:
            Estimated EVR required with buffer
        """
        # Distribution fee estimate
        if recipient_count <= 10:
            base_fee = EVR_FEES['batch_distribution_small']
        elif recipient_count <= 50:
            base_fee = EVR_FEES['batch_distribution']
        else:
            base_fee = EVR_FEES['large_distribution']

        # Add per-recipient cost
        per_recipient = 0.002  # ~0.002 EVR per output
        distribution_fee = base_fee + (recipient_count * per_recipient)

        # Asset issuance if needed
        asset_fee = 0.0
        if include_asset_issuance:
            if asset_type == 'unique':
                asset_fee = EVR_FEES['asset_issuance_unique']
            elif asset_type == 'sub':
                asset_fee = EVR_FEES['asset_issuance_sub']

        total = (distribution_fee + asset_fee) * EVR_FEES['buffer_multiplier']
        return round(total, 4)

    async def check_treasury_status(
        self,
        required_satori: float,
        recipient_count: int = 100,
        include_asset_issuance: bool = False,
        asset_type: str = None,
    ) -> TreasuryStatus:
        """
        Check treasury status and broadcast alerts if needed.

        Args:
            required_satori: SATORI needed for this distribution
            recipient_count: Number of recipients
            include_asset_issuance: Whether assets need to be issued
            asset_type: Type of asset to issue

        Returns:
            Current treasury status
        """
        balances = self.get_treasury_balances()
        satori_balance = balances.get('satori', 0.0)
        evr_balance = balances.get('evr', 0.0)

        evr_required = self.estimate_evr_required(
            recipient_count,
            include_asset_issuance,
            asset_type,
        )

        # Determine status
        satori_ok = satori_balance >= required_satori
        evr_ok = evr_balance >= evr_required

        if satori_ok and evr_ok:
            status = 'healthy'
        elif not satori_ok and not evr_ok:
            status = 'critical'
        else:
            status = 'warning'

        # Generate alerts if needed
        alerts_to_broadcast = []

        if not satori_ok and not evr_ok:
            # Both insufficient - critical
            alert = self._create_critical_alert(
                satori_balance, required_satori,
                evr_balance, evr_required,
            )
            alerts_to_broadcast.append(alert)
        elif not satori_ok:
            # SATORI insufficient
            alert = self._create_insufficient_satori_alert(
                satori_balance, required_satori,
            )
            alerts_to_broadcast.append(alert)
        elif not evr_ok:
            # EVR insufficient
            blocked_ops = ['reward_distribution']
            if include_asset_issuance:
                blocked_ops.append('asset_issuance')
            alert = self._create_insufficient_evr_alert(
                evr_balance, evr_required, blocked_ops,
            )
            alerts_to_broadcast.append(alert)
        else:
            # Everything healthy - clear alerts if any were active
            if self._active_alerts:
                alert = self._create_healthy_alert(satori_balance, evr_balance)
                alerts_to_broadcast.append(alert)

        # Broadcast alerts
        for alert in alerts_to_broadcast:
            await self._broadcast_alert(alert)

        return TreasuryStatus(
            status=status,
            satori_balance=satori_balance,
            evr_balance=evr_balance,
            satori_required=required_satori,
            evr_required=evr_required,
            active_alerts=self._active_alerts.copy(),
            deferred_rounds=self._deferred_rounds,
            total_deferred_rewards=self._total_deferred_rewards,
            last_successful_distribution=self._last_successful_distribution,
        )

    def _create_insufficient_satori_alert(
        self,
        available: float,
        required: float,
    ) -> TreasuryAlert:
        """Create insufficient SATORI alert."""
        return TreasuryAlert(
            alert_id=self._generate_alert_id(AlertType.INSUFFICIENT_SATORI.value),
            alert_type=AlertType.INSUFFICIENT_SATORI.value,
            severity=AlertSeverity.WARNING.value,
            message=(
                "Insufficient SATORI for this payment round. "
                "Your prediction scores are being recorded and will be paid "
                "when the Satori team refills the treasury."
            ),
            details={
                'required_satori': required,
                'available_satori': available,
                'shortfall': round(required - available, 4),
                'deferred_rounds': self._deferred_rounds,
            },
            action=(
                "No action required. SATORI refills are managed by the Satori team. "
                "Your rewards will be distributed automatically when treasury is funded."
            ),
            actions=[
                AlertAction(
                    label="View Status",
                    action_type="view_status",
                    description="Check current treasury status",
                ),
            ],
            treasury_address=self._treasury_address,
            donate_url=self._donate_url,
        )

    def _create_insufficient_evr_alert(
        self,
        available: float,
        required: float,
        blocked_operations: List[str],
    ) -> TreasuryAlert:
        """Create insufficient EVR alert."""
        return TreasuryAlert(
            alert_id=self._generate_alert_id(AlertType.INSUFFICIENT_EVR.value),
            alert_type=AlertType.INSUFFICIENT_EVR.value,
            severity=AlertSeverity.CRITICAL.value,
            message=(
                "Treasury needs EVR to complete reward distribution. "
                "Please donate EVR to resume network operations."
            ),
            details={
                'required_evr': required,
                'available_evr': available,
                'shortfall': round(required - available, 4),
                'blocked_operations': blocked_operations,
            },
            action="Donate EVR to treasury address to resume operations.",
            actions=[
                AlertAction(
                    label="Donate EVR",
                    action_type="donate_evr",
                    url=self._donate_url,
                    description="Send EVR to treasury for transaction fees",
                ),
            ],
            treasury_address=self._treasury_address,
            donate_url=self._donate_url,
        )

    def _create_critical_alert(
        self,
        satori_available: float,
        satori_required: float,
        evr_available: float,
        evr_required: float,
    ) -> TreasuryAlert:
        """Create critical treasury alert (both low)."""
        return TreasuryAlert(
            alert_id=self._generate_alert_id(AlertType.TREASURY_CRITICAL.value),
            alert_type=AlertType.TREASURY_CRITICAL.value,
            severity=AlertSeverity.CRITICAL.value,
            message=(
                "Treasury critically low. Network reward operations paused. "
                "Your prediction scores are being recorded. "
                "Please donate EVR to help with transaction fees. "
                "SATORI refills are managed by the Satori team."
            ),
            details={
                'satori_status': 'insufficient',
                'evr_status': 'insufficient',
                'satori_shortfall': round(satori_required - satori_available, 4),
                'evr_shortfall': round(evr_required - evr_available, 4),
                'deferred_rounds': self._deferred_rounds,
            },
            action=(
                "Donate EVR to treasury for transaction fees. "
                "SATORI refills are managed by the Satori team."
            ),
            actions=[
                AlertAction(
                    label="Donate EVR",
                    action_type="donate_evr",
                    url=self._donate_url,
                    description="Send EVR to treasury for transaction fees",
                ),
            ],
            treasury_address=self._treasury_address,
            donate_url=self._donate_url,
        )

    def _create_healthy_alert(
        self,
        satori_balance: float,
        evr_balance: float,
    ) -> TreasuryAlert:
        """Create treasury healthy alert (clears warnings)."""
        return TreasuryAlert(
            alert_id=self._generate_alert_id(AlertType.TREASURY_HEALTHY.value),
            alert_type=AlertType.TREASURY_HEALTHY.value,
            severity=AlertSeverity.INFO.value,
            message="Treasury balance restored. Normal operations resumed.",
            details={
                'satori_balance': satori_balance,
                'evr_balance': evr_balance,
            },
            action="No action required.",
            treasury_address=self._treasury_address,
        )

    def create_rewards_resumed_alert(
        self,
        deferred_rounds_paid: int,
        total_paid: float,
    ) -> TreasuryAlert:
        """Create rewards resumed alert (after catch-up)."""
        return TreasuryAlert(
            alert_id=self._generate_alert_id(AlertType.REWARDS_RESUMED.value),
            alert_type=AlertType.REWARDS_RESUMED.value,
            severity=AlertSeverity.INFO.value,
            message=(
                f"Treasury funded! Distributing current rewards plus "
                f"{deferred_rounds_paid} deferred rounds."
            ),
            details={
                'deferred_rounds_paid': deferred_rounds_paid,
                'total_paid': total_paid,
            },
            action="Check your wallet for incoming rewards.",
            treasury_address=self._treasury_address,
        )

    def create_distribution_success_alert(
        self,
        round_id: int,
        total_distributed: float,
        recipient_count: int,
        tx_hash: str = None,
    ) -> TreasuryAlert:
        """Create distribution success alert."""
        return TreasuryAlert(
            alert_id=self._generate_alert_id(AlertType.DISTRIBUTION_SUCCESS.value),
            alert_type=AlertType.DISTRIBUTION_SUCCESS.value,
            severity=AlertSeverity.INFO.value,
            message=f"Round {round_id} rewards distributed successfully.",
            details={
                'round_id': round_id,
                'total_distributed': total_distributed,
                'recipient_count': recipient_count,
                'tx_hash': tx_hash,
            },
            action="Check your wallet for incoming rewards.",
        )

    async def _broadcast_alert(self, alert: TreasuryAlert) -> None:
        """Broadcast alert to the network."""
        # Update local state
        self._process_incoming_alert(alert)

        # Add to history
        self._alert_history.append(AlertHistoryEntry(alert=alert))

        # Persist to storage
        await self._persist_alert_to_storage(alert)

        # Broadcast via P2P
        if self._peers:
            try:
                await self._peers.publish(
                    TOPIC_TREASURY_ALERTS,
                    alert.to_dict(),
                )
                logger.info(f"Broadcast alert: {alert.alert_type}")
            except Exception as e:
                logger.error(f"Failed to broadcast alert: {e}")

    async def _persist_alert_to_storage(self, alert: TreasuryAlert) -> None:
        """Persist alert to redundant storage."""
        if not self._storage:
            return

        try:
            from .storage import StoredAlert

            stored = StoredAlert(
                alert_id=alert.alert_id,
                alert_type=alert.alert_type,
                severity=alert.severity,
                message=alert.message,
                details=alert.details,
                timestamp=alert.timestamp,
            )
            await self._storage.add_active_alert(stored)
            self._dht_sync_pending = True
        except Exception as e:
            logger.error(f"Failed to persist alert: {e}")

    async def broadcast_alert(self, alert: TreasuryAlert) -> None:
        """Public method to broadcast a custom alert."""
        await self._broadcast_alert(alert)

    def record_deferred_round(self, amount: float) -> None:
        """Record a deferred distribution round."""
        self._deferred_rounds += 1
        self._total_deferred_rewards += amount

    def clear_deferred_rounds(self, paid_rounds: int, paid_amount: float) -> None:
        """Clear deferred rounds after catch-up payment."""
        self._deferred_rounds = max(0, self._deferred_rounds - paid_rounds)
        self._total_deferred_rewards = max(0, self._total_deferred_rewards - paid_amount)

    def record_successful_distribution(self) -> None:
        """Record a successful distribution."""
        self._last_successful_distribution = int(time.time())

    def get_active_alerts(self) -> List[TreasuryAlert]:
        """Get currently active alerts."""
        return self._active_alerts.copy()

    def get_alert_history(self, limit: int = 20) -> List[AlertHistoryEntry]:
        """Get alert history."""
        return self._alert_history[-limit:]

    def get_status_summary(self) -> dict:
        """Get current status summary."""
        balances = self.get_treasury_balances()

        if not self._active_alerts:
            status = 'healthy'
        elif any(a.severity == AlertSeverity.CRITICAL.value for a in self._active_alerts):
            status = 'critical'
        else:
            status = 'warning'

        return {
            'status': status,
            'satori_balance': balances.get('satori', 0.0),
            'evr_balance': balances.get('evr', 0.0),
            'active_alerts': [a.to_dict() for a in self._active_alerts],
            'deferred_rounds': self._deferred_rounds,
            'total_deferred_rewards': self._total_deferred_rewards,
            'last_successful_distribution': self._last_successful_distribution,
        }

    # =========================================================================
    # DHT STORAGE METHODS
    # =========================================================================

    async def sync_to_dht(self) -> int:
        """
        Sync alerts to DHT for network redundancy.

        Should be called periodically (e.g., every 5 minutes).

        Returns:
            Number of keys synced
        """
        if not self._storage:
            return 0

        synced = await self._storage.sync_to_dht()
        self._dht_sync_pending = False
        logger.info(f"Synced {synced} alert keys to DHT")
        return synced

    async def recover_from_dht(self) -> int:
        """
        Recover alerts from DHT if local data is missing/corrupted.

        Called on startup if local storage is empty.

        Returns:
            Number of records recovered
        """
        if not self._storage:
            return 0

        # Attempt recovery
        recovered = await self._storage.recover_from_dht(["active", "history"])

        if recovered > 0:
            # Reload from storage
            await self._load_from_storage()
            logger.info(f"Recovered {recovered} alert records from DHT")

        return recovered

    async def resolve_alert_with_storage(
        self,
        alert_type: str,
        resolution: str = "resolved",
    ) -> bool:
        """
        Resolve an alert and persist to storage.

        Args:
            alert_type: Type of alert to resolve
            resolution: Resolution message

        Returns:
            True if alert was resolved
        """
        # Update local state
        resolved = False
        for i, alert in enumerate(self._active_alerts):
            if alert.alert_type == alert_type:
                self._active_alerts.pop(i)
                self._alert_history.append(
                    AlertHistoryEntry(
                        alert=alert,
                        resolved_at=int(time.time()),
                        resolution=resolution,
                    )
                )
                resolved = True
                break

        # Persist to storage
        if resolved and self._storage:
            await self._storage.resolve_alert(alert_type, resolution)
            self._dht_sync_pending = True

        return resolved

    def get_storage_stats(self) -> dict:
        """Get storage statistics."""
        stats = {
            'in_memory_active': len(self._active_alerts),
            'in_memory_history': len(self._alert_history),
            'dht_sync_pending': self._dht_sync_pending,
            'storage_enabled': self._storage is not None,
        }

        if self._storage:
            stats['storage_stats'] = self._storage.get_stats()

        return stats


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def check_evr_for_operation(
    treasury_evr: float,
    operation: str,
    recipient_count: int = 1,
) -> dict:
    """
    Check if treasury has enough EVR for an operation.

    Args:
        treasury_evr: Current EVR balance
        operation: Operation type ('reward_distribution', 'asset_issuance_unique', etc.)
        recipient_count: Number of recipients (for distributions)

    Returns:
        Dict with 'sufficient', 'required', 'available', 'shortfall'
    """
    if operation == 'reward_distribution':
        if recipient_count <= 10:
            base = EVR_FEES['batch_distribution_small']
        elif recipient_count <= 50:
            base = EVR_FEES['batch_distribution']
        else:
            base = EVR_FEES['large_distribution']
        required = (base + recipient_count * 0.002) * EVR_FEES['buffer_multiplier']
    elif operation == 'asset_issuance_unique':
        required = EVR_FEES['asset_issuance_unique']
    elif operation == 'asset_issuance_sub':
        required = EVR_FEES['asset_issuance_sub']
    else:
        required = EVR_FEES['simple_transfer']

    return {
        'sufficient': treasury_evr >= required,
        'required': round(required, 4),
        'available': treasury_evr,
        'shortfall': round(max(0, required - treasury_evr), 4),
    }
