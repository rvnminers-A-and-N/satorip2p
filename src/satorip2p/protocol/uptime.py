"""
satorip2p/protocol/uptime.py

Uptime tracking for relay bonus qualification.

Tracks node presence via heartbeats and calculates uptime percentage
for each round. Relay nodes must maintain ≥95% uptime to qualify
for the relay bonus (+5% multiplier).

Key Features:
- Heartbeat-based presence detection
- Per-round uptime calculation
- DHT-based peer discovery for verification
- 95% threshold for relay bonus
"""

import logging
import time
import random
import hashlib
from typing import Any, Dict, List, Optional, Set, Callable, TYPE_CHECKING, Union
from dataclasses import dataclass, field, asdict
from collections import defaultdict

if TYPE_CHECKING:
    from ..peers import Peers
    from ..signing import EvrmoreWallet

# Import signing module - uses python-evrmorelib directly
try:
    from ..signing import EvrmoreWallet, sign_message, verify_message
    SIGNING_AVAILABLE = True
except ImportError:
    SIGNING_AVAILABLE = False

# Import activity stats storage for persistence
try:
    from .storage import ActivityStatsStorage, ActivityStats
    ACTIVITY_STORAGE_AVAILABLE = True
except ImportError:
    ACTIVITY_STORAGE_AVAILABLE = False
    ActivityStatsStorage = None
    ActivityStats = None

logger = logging.getLogger("satorip2p.protocol.uptime")


# ============================================================================
# CONSTANTS
# ============================================================================

# Heartbeat settings
HEARTBEAT_INTERVAL = 60          # Send heartbeat every 60 seconds
HEARTBEAT_TIMEOUT = 180          # Node considered offline after 3 missed heartbeats
MAX_MISSED_HEARTBEATS = 3        # Max consecutive misses before marking offline

# Uptime thresholds
RELAY_UPTIME_THRESHOLD = 0.95    # 95% uptime required for relay bonus

# PubSub topics
HEARTBEAT_TOPIC = "satori/heartbeat"
ROUND_SYNC_TOPIC = "satori/round-sync"

# Protocol version for compatibility
HEARTBEAT_PROTOCOL_VERSION = "1.0.0"

# Round settings - aligned with Satori Network prediction rounds
# Each round is 1 day, 7 rounds per epoch (weekly)
ROUND_DURATION = 86400  # 24 hours in seconds (1 day)
EPOCH_DURATION = 7 * 86400  # 604,800 seconds (1 week)
ROUND_SYNC_INTERVAL = 60  # Broadcast round info every 60 seconds (matches heartbeat)

# NOTE: Bootstrap peers are configured in satorip2p/config.py (BOOTSTRAP_PEERS)
# All PubSub topics (including heartbeat) use the same P2P mesh network.

# Fun status messages - 60 total so each shows ~24 times/day (once per hour)
HEARTBEAT_STATUS_MESSAGES = [
    "Vibing in the mesh...",
    "Beep boop, still here!",
    "Crunching predictions...",
    "Riding the data waves...",
    "Quantum entangled and ready!",
    "Caffeinated and operational...",
    "Syncing with the cosmos...",
    "Neurons firing on all cylinders!",
    "Decentralizing the future...",
    "Proof of consciousness achieved!",
    "Streaming consciousness...",
    "In the zone, making predictions...",
    "Satori-fying the network...",
    "Zen and the art of uptime...",
    "Enlightenment in progress...",
    "One with the blockchain...",
    "Predicting the unpredictable...",
    "Distributed and feeling good!",
    "Mesh-merizing the competition...",
    "Staking my claim to existence!",
    "Heartbeat goes brrr...",
    "Living my best node life...",
    "Relay-xing and prospering...",
    "Oracle-ing around the clock...",
    "Consensus is my middle name...",
    "Born to predict, forced to wait...",
    "Making satoshi proud...",
    "Trustlessly trusting the process...",
    "Peer-to-peer and loving it!",
    "Immutably here for you...",
    "Forecasting the future...",
    "Reading the data tea leaves...",
    "Crystal ball calibrated...",
    "Probability waves collapsing...",
    "I predict... I'll still be here!",
    "Prophesying with precision...",
    "Future looks distributed...",
    "Accuracy loading...",
    "Gossiping with the mesh...",
    "Relaying good vibes...",
    "DHT diving for peers...",
    "Propagating through the network...",
    "Packets flowing smoothly...",
    "NAT punched, feeling good...",
    "Bootstrap complete, thriving...",
    "Connected and protected...",
    "Stacking sats of wisdom...",
    "Hash rate: maximum chill...",
    "Block by block, we rise...",
    "Consensus achieved: still awesome...",
    "Signed, sealed, delivered...",
    "Immutable and unstoppable...",
    "Decentralized and loving it...",
    "Trustless but trusting...",
    "Have you tried turning it off and on again?",
    "404: Downtime not found...",
    "Running on coffee and algorithms...",
    "To predict, or not to predict...",
    "All systems nominal...",
    "Uptime is my superpower...",
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_current_round() -> tuple:
    """
    Get the current round ID and start time based on UTC midnight.

    Rounds align with Satori Network prediction rounds:
    - Start at 00:00 UTC daily
    - Last 24 hours
    - Round ID format: "round_YYYY-MM-DD"

    Returns:
        Tuple of (round_id, round_start_timestamp)
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    # Get today's midnight UTC
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    round_start = int(midnight.timestamp())
    round_id = f"round_{midnight.strftime('%Y-%m-%d')}"

    return round_id, round_start


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class Heartbeat:
    """
    A heartbeat from a node proving it's alive and active.

    Contains all information needed to:
    - Verify the node is online (timestamp)
    - Link to on-chain identity (evrmore_address)
    - Verify authenticity (signature)
    - Track roles for bonus multipliers (roles)
    - Ensure compatibility (version)
    - Add some personality (status_message)

    ALL fields are required for production use. This ensures:
    - Heartbeats can be verified cryptographically
    - On-chain identity can be confirmed
    - Role multipliers can be applied correctly
    """
    # Core identity - ALL REQUIRED
    node_id: str                    # libp2p peer ID (primary identifier)
    evrmore_address: str            # Evrmore address for rewards (verifiable on-chain)
    peer_id: str                    # libp2p peer ID (for P2P routing)

    # Timing - ALL REQUIRED
    timestamp: int                  # Unix timestamp (for freshness)
    round_id: str                   # Current round (e.g., "2025-01-15")

    # Role information - ALL REQUIRED
    roles: List[str]                # ["predictor", "relay", "oracle", "signer"]
    stake: float                    # Current stake amount (verifiable on-chain)

    # Authenticity - MUST be set before broadcast (use python-evrmorelib)
    # Default empty allows creation before signing, but validate before broadcast
    signature: bytes = field(default=b"", repr=False)

    # Personality - REQUIRED (use get_random_status() for fun!)
    status_message: str = ""        # Default empty, set via get_random_status()

    # Protocol info - has sensible default
    version: str = HEARTBEAT_PROTOCOL_VERSION  # For compatibility checks

    def to_dict(self) -> dict:
        return {
            'node_id': self.node_id,
            'timestamp': self.timestamp,
            'round_id': self.round_id,
            'evrmore_address': self.evrmore_address,
            'peer_id': self.peer_id,
            'roles': self.roles,
            'stake': self.stake,
            'version': self.version,
            'signature': self.signature.hex() if self.signature else "",
            'status_message': self.status_message,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Heartbeat":
        """
        Create Heartbeat from dict.

        All fields should be present for valid heartbeats.
        Uses .get() with defaults for backward compatibility with older versions.
        Use is_valid_for_broadcast() to verify required fields are populated.
        """
        return cls(
            node_id=data.get('node_id', ''),
            evrmore_address=data.get('evrmore_address', ''),
            peer_id=data.get('peer_id', ''),
            timestamp=data.get('timestamp', 0),
            round_id=data.get('round_id', ''),
            roles=data.get('roles', []),
            stake=data.get('stake', 0.0),
            signature=bytes.fromhex(data['signature']) if data.get('signature') else b"",
            status_message=data.get('status_message', ''),
            version=data.get('version', HEARTBEAT_PROTOCOL_VERSION),
        )

    def get_signing_content(self) -> str:
        """Get deterministic content for signing (excludes signature itself)."""
        return f"{self.node_id}:{self.timestamp}:{self.round_id}:{self.evrmore_address}:{','.join(self.roles)}"

    def get_content_hash(self) -> str:
        """Get hash of heartbeat content for verification."""
        return hashlib.sha256(self.get_signing_content().encode()).hexdigest()

    @staticmethod
    def get_random_status() -> str:
        """Get a random fun status message."""
        return random.choice(HEARTBEAT_STATUS_MESSAGES)

    def is_valid_for_broadcast(self) -> tuple[bool, str]:
        """
        Validate heartbeat has all required fields populated for broadcast.

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self.node_id:
            return False, "node_id is required"
        if not self.evrmore_address:
            return False, "evrmore_address is required"
        if not self.peer_id:
            return False, "peer_id is required"
        if not self.timestamp:
            return False, "timestamp is required"
        if not self.round_id:
            return False, "round_id is required"
        if not self.roles:
            return False, "roles is required (at least one role)"
        if self.stake <= 0:
            return False, "stake must be positive"
        if not self.signature:
            return False, "signature is required for broadcast"
        if not self.status_message:
            return False, "status_message is required"
        return True, ""


@dataclass
class NodeUptimeRecord:
    """Uptime record for a single node in a round."""
    node_id: str
    round_id: str
    first_seen: int          # First heartbeat timestamp
    last_seen: int           # Last heartbeat timestamp
    heartbeat_count: int     # Total heartbeats received
    expected_heartbeats: int # Expected based on round duration
    uptime_percentage: float # Calculated uptime
    is_relay_qualified: bool # Meets 95% threshold
    uptime_streak_days: int = 0  # Consecutive days with ≥95% uptime

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class NodeStreakRecord:
    """Tracks uptime streak across rounds for a node."""
    node_id: str
    streak_days: int = 0              # Current consecutive days with ≥95% uptime
    last_qualified_round: str = ""    # Last round where node had ≥95% uptime
    streak_start_date: str = ""       # Date streak started (YYYY-MM-DD)
    longest_streak: int = 0           # Longest streak ever achieved

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RoundUptimeReport:
    """Uptime report for all nodes in a round."""
    round_id: str
    round_start: int
    round_end: int
    total_nodes: int
    relay_qualified_nodes: int
    node_records: Dict[str, NodeUptimeRecord]  # node_id -> record

    def to_dict(self) -> dict:
        return {
            'round_id': self.round_id,
            'round_start': self.round_start,
            'round_end': self.round_end,
            'total_nodes': self.total_nodes,
            'relay_qualified_nodes': self.relay_qualified_nodes,
            'node_records': {k: v.to_dict() for k, v in self.node_records.items()},
        }


# ============================================================================
# UPTIME TRACKER
# ============================================================================

class UptimeTracker:
    """
    Tracks node uptime via heartbeats.

    Flow:
    1. Nodes send periodic heartbeats (every 60s)
    2. Tracker records heartbeat timestamps
    3. At round end, calculate uptime percentage
    4. Nodes with ≥95% uptime qualify for relay bonus

    Usage:
        tracker = UptimeTracker(peers, node_id="my_node")

        # Start sending heartbeats
        tracker.start_heartbeating()

        # At round end, get uptime report
        report = tracker.calculate_round_uptime(round_id, round_start, round_end)
    """

    def __init__(
        self,
        peers: Optional["Peers"] = None,
        node_id: Optional[str] = None,
        evrmore_address: Optional[str] = None,
        peer_id: Optional[str] = None,
        stake: float = 0.0,
        private_key: Optional[bytes] = None,
        wallet: Optional[Union["EvrmoreWallet", any]] = None,
    ):
        """
        Initialize UptimeTracker.

        Args:
            peers: Peers instance for P2P communication
            node_id: This node's ID
            evrmore_address: Evrmore address for reward distribution
            peer_id: libp2p peer ID
            stake: Current stake amount
            private_key: Private key for signing heartbeats (deprecated, use wallet)
            wallet: EvrmoreWallet or satorilib EvrmoreIdentity for signing
        """
        self.peers = peers
        self.peer_id = peer_id or ""
        self.stake = stake
        self.private_key = private_key  # Deprecated, kept for backward compatibility
        self.wallet = wallet

        # Get address from wallet if not provided
        if wallet and not evrmore_address:
            self.evrmore_address = getattr(wallet, 'address', '') or ""
        else:
            self.evrmore_address = evrmore_address or ""

        # node_id should be the evrmore address (unique node identifier)
        # Fall back to peer_id if no evrmore address available
        if node_id:
            self.node_id = node_id
        elif self.evrmore_address:
            self.node_id = self.evrmore_address
        elif self.peer_id:
            self.node_id = self.peer_id
        else:
            self.node_id = "unknown"

        # Heartbeat storage: {round_id: {node_id: [timestamps]}}
        self._heartbeats: Dict[str, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))

        # Current round
        self._current_round: Optional[str] = None
        self._round_start: int = 0

        # Node roles (declared by each node)
        # Keyed by node_id (usually evrmore address)
        self._node_roles: Dict[str, Set[str]] = defaultdict(set)
        # Also keyed by peer_id (libp2p peer ID) for P2P lookups
        self._peer_roles: Dict[str, Set[str]] = defaultdict(set)

        # Heartbeat sending state
        self._is_heartbeating: bool = False
        self._last_heartbeat: int = 0
        self._last_status_message: str = ""  # Track most recent heartbeat message

        # Heartbeat counters for UI
        self._heartbeats_sent: int = 0
        self._heartbeats_received: int = 0

        # Role callback for dynamic role determination
        # This allows the application to provide roles dynamically based on current state
        self._role_callback: Optional[Callable[[], List[str]]] = None

        # Activity stats storage for persistence (survives restart)
        self._activity_storage: Optional[ActivityStatsStorage] = None
        if ACTIVITY_STORAGE_AVAILABLE:
            from pathlib import Path
            storage_dir = Path.home() / ".satori" / "storage"
            self._activity_storage = ActivityStatsStorage(
                peers=peers,
                storage_dir=storage_dir,
                node_id=self.node_id,
            )
            logger.info("Activity stats storage initialized")

        # Round sync state
        self._known_rounds: Dict[str, int] = {}  # round_id -> start_time
        self._last_round_sync: int = 0

        # Uptime streak tracking: {node_id: NodeStreakRecord}
        self._uptime_streaks: Dict[str, NodeStreakRecord] = {}

        # Node stakes tracking: {node_id: stake_amount}
        # Updated from heartbeat data for governance voting power calculation
        self._node_stakes: Dict[str, float] = {}
        self._node_stakes_updated: int = 0  # Last update timestamp

        # Callbacks for external listeners (e.g., WebSocket bridge)
        self._on_heartbeat_received: Optional[Callable[[Heartbeat], None]] = None
        self._on_heartbeat_sent: Optional[Callable[[Heartbeat], None]] = None

        # Recent heartbeats storage for UI display (circular buffer)
        self._recent_heartbeats: List[Heartbeat] = []
        self._recent_heartbeats_max: int = 100  # Keep last 100 heartbeats

        # Subscribe to heartbeat topic if peers available
        if self.peers:
            self._setup_pubsub()

    async def _setup_pubsub_async(self) -> None:
        """Subscribe to heartbeat and round sync PubSub topics (async version).

        Subscriptions are done sequentially with a small delay between them
        to avoid race conditions in py-libp2p's GossipSub implementation.
        """
        if not self.peers:
            return

        import trio

        # Subscribe to heartbeat topic first
        try:
            await self.peers.subscribe_async(
                HEARTBEAT_TOPIC,
                self._handle_heartbeat_message
            )
            logger.info(f"Subscribed to heartbeat topic: {HEARTBEAT_TOPIC}")
        except Exception as e:
            logger.error(f"Failed to subscribe to heartbeat topic: {e}")

        # Small delay between subscriptions to let GossipSub stabilize
        await trio.sleep(0.2)

        # Subscribe to round sync topic
        try:
            await self.peers.subscribe_async(
                ROUND_SYNC_TOPIC,
                self._handle_round_sync_message
            )
            logger.info(f"Subscribed to round sync topic: {ROUND_SYNC_TOPIC}")
        except Exception as e:
            logger.error(f"Failed to subscribe to round sync topic: {e}")

    def _setup_pubsub(self) -> None:
        """Subscribe to heartbeat PubSub topic (sync fallback - limited)."""
        if not self.peers:
            return

        try:
            # Local callback only - for backwards compatibility
            # Full GossipSub subscription happens in _setup_pubsub_async
            self.peers.subscribe(
                HEARTBEAT_TOPIC,
                self._handle_heartbeat_message
            )
            logger.info(f"Registered local heartbeat callback for {HEARTBEAT_TOPIC}")
        except Exception as e:
            logger.error(f"Failed to setup heartbeat callback: {e}")

    def _handle_heartbeat_message(self, stream_id: str, data: Any) -> None:
        """Handle incoming heartbeat from PubSub."""
        try:
            import json
            # Data may be bytes (raw) or already deserialized dict
            if isinstance(data, bytes):
                message = json.loads(data.decode())
            elif isinstance(data, dict):
                message = data
            else:
                logger.warning(f"Unexpected heartbeat data type: {type(data)}")
                return

            heartbeat = Heartbeat.from_dict(message)
            logger.debug(f"Received heartbeat from {heartbeat.node_id}: {heartbeat.status_message}")
            self.receive_heartbeat(heartbeat)
        except Exception as e:
            logger.error(f"Failed to handle heartbeat message: {e}")

    def _handle_round_sync_message(self, stream_id: str, data: Any) -> None:
        """Handle incoming round sync message from PubSub."""
        try:
            import json
            # Data may be bytes (raw) or already deserialized dict
            if isinstance(data, bytes):
                message = json.loads(data.decode())
            elif isinstance(data, dict):
                message = data
            else:
                logger.warning(f"Unexpected round sync data type: {type(data)}")
                return

            round_id = message.get('round_id')
            round_start = message.get('round_start')
            sender_node = message.get('node_id', 'unknown')

            if not round_id or not round_start:
                return

            # Track known rounds (for debugging/monitoring)
            self._known_rounds[round_id] = round_start

            # All nodes should calculate the same round (UTC midnight aligned)
            # If we receive a different round, it might be from a node with clock skew
            # or from a different day. Just log it for now.
            if round_id != self._current_round:
                logger.debug(f"Received round sync for {round_id} from {sender_node} (we're on {self._current_round})")

                # If we don't have a round yet, adopt theirs
                if not self._current_round:
                    logger.info(f"Adopting round {round_id} from {sender_node}")
                    self.start_round(round_id, round_start)

        except Exception as e:
            logger.error(f"Failed to handle round sync message: {e}")

    async def _broadcast_round_sync(self) -> None:
        """Broadcast our current round to the network."""
        if not self.peers or not self._current_round:
            return

        # Check if pubsub is ready (avoid broadcasting before connection is established)
        if hasattr(self.peers, '_pubsub') and not self.peers._pubsub:
            logger.debug("Skipping round sync broadcast: pubsub not ready")
            return

        try:
            message = {
                'round_id': self._current_round,
                'round_start': self._round_start,
                'node_id': self.node_id,
                'timestamp': int(time.time()),
            }
            await self.peers.broadcast(ROUND_SYNC_TOPIC, message)
            self._last_round_sync = int(time.time())
            logger.debug(f"Broadcast round sync: {self._current_round}")
        except Exception as e:
            logger.warning(f"Failed to broadcast round sync: {e}")

    # ========================================================================
    # CALLBACK PROPERTIES
    # ========================================================================

    @property
    def on_heartbeat_received(self) -> Optional[Callable[[Heartbeat], None]]:
        """Callback invoked when a heartbeat is received from the network."""
        return self._on_heartbeat_received

    @on_heartbeat_received.setter
    def on_heartbeat_received(self, callback: Optional[Callable[[Heartbeat], None]]) -> None:
        """Set callback for received heartbeats."""
        self._on_heartbeat_received = callback

    @property
    def on_heartbeat_sent(self) -> Optional[Callable[[Heartbeat], None]]:
        """Callback invoked when we send a heartbeat to the network."""
        return self._on_heartbeat_sent

    @on_heartbeat_sent.setter
    def on_heartbeat_sent(self, callback: Optional[Callable[[Heartbeat], None]]) -> None:
        """Set callback for sent heartbeats."""
        self._on_heartbeat_sent = callback

    @property
    def role_callback(self) -> Optional[Callable[[], List[str]]]:
        """Callback to get current node roles dynamically."""
        return self._role_callback

    @role_callback.setter
    def role_callback(self, callback: Optional[Callable[[], List[str]]]) -> None:
        """Set callback for dynamic role determination.

        The callback should return a list of current roles (e.g., ['oracle', 'relay']).
        If not set, defaults to ['predictor'].
        """
        self._role_callback = callback

    # ========================================================================
    # PUBLIC API
    # ========================================================================

    async def start(self, nursery=None) -> bool:
        """
        Start the uptime tracker with heartbeat sending.

        This starts a background task that sends heartbeats every HEARTBEAT_INTERVAL.

        Args:
            nursery: Optional trio nursery to spawn heartbeat loop in.
                     If not provided, the loop must be started manually with run_heartbeat_loop().

        Returns:
            True if started successfully
        """
        if self._is_heartbeating:
            return True

        self._is_heartbeating = True

        # Setup proper GossipSub subscription for receiving heartbeats and round sync
        await self._setup_pubsub_async()

        # Start the current round based on UTC midnight (aligns with Satori prediction rounds)
        round_id, round_start = get_current_round()
        if self._current_round != round_id:
            self.start_round(round_id, round_start)
            logger.info(f"Started round {round_id} (UTC midnight aligned)")

        # Try to recover stats from DHT if local storage is empty (after restart/data loss)
        if self._activity_storage:
            try:
                recovered = await self.recover_stats_from_dht()
                if recovered > 0:
                    logger.info(f"Recovered {recovered} activity stats entries from DHT")
            except Exception as e:
                logger.debug(f"DHT stats recovery skipped: {e}")

        # Broadcast our round to help other nodes sync (delay to ensure pubsub is ready)
        import trio
        await trio.sleep(1.0)  # Wait for pubsub connections to stabilize
        await self._broadcast_round_sync()

        # Spawn heartbeat loop if nursery provided
        if nursery is not None:
            nursery.start_soon(self.run_heartbeat_loop)
            logger.info("Uptime tracker started with heartbeat loop")
        else:
            logger.info("Uptime tracker started, heartbeat sending enabled (call run_heartbeat_loop manually)")

        return True

    async def run_heartbeat_loop(self) -> None:
        """
        Run the heartbeat sending loop.

        This should be called as a background task after start().
        Sends heartbeats every HEARTBEAT_INTERVAL seconds.
        Also handles periodic DHT sync and integrity verification.
        """
        import trio

        # Track last sync/verification times
        last_dht_sync = 0
        last_integrity_check = 0
        DHT_SYNC_INTERVAL = 300  # 5 minutes
        INTEGRITY_CHECK_INTERVAL = 900  # 15 minutes

        logger.info("Heartbeat loop started")
        while self._is_heartbeating:
            try:
                now = int(time.time())

                # Check for round change (new day at midnight UTC)
                current_round_id, current_round_start = get_current_round()
                if self._current_round != current_round_id:
                    logger.info(f"Round changed: {self._current_round} -> {current_round_id}")
                    self.start_round(current_round_id, current_round_start)
                    # Reset heartbeat counters for new round
                    self._heartbeats_sent = 0
                    self._heartbeats_received = 0
                    # Broadcast round sync to help other nodes
                    await self._broadcast_round_sync()
                    # Sync stats to DHT before round change (preserve old round data)
                    if self._activity_storage:
                        try:
                            synced = await self.sync_stats_to_dht()
                            if synced > 0:
                                logger.info(f"Synced {synced} stats entries to DHT on round change")
                        except Exception as e:
                            logger.debug(f"DHT sync on round change failed: {e}")

                # Send heartbeat (async version)
                heartbeat = await self.send_heartbeat_async()
                if heartbeat:
                    logger.info(f"Sent heartbeat: node_id={heartbeat.node_id} status={heartbeat.status_message}")
                else:
                    logger.debug("Heartbeat not sent (no active round)")

                # Periodically broadcast round sync (every ROUND_SYNC_INTERVAL)
                if now - self._last_round_sync >= ROUND_SYNC_INTERVAL:
                    await self._broadcast_round_sync()

                # Periodically sync stats to DHT (every 5 minutes)
                if self._activity_storage and now - last_dht_sync >= DHT_SYNC_INTERVAL:
                    try:
                        synced = await self.sync_stats_to_dht()
                        if synced > 0:
                            logger.debug(f"Periodic DHT sync: {synced} entries synced")
                        last_dht_sync = now
                    except Exception as e:
                        logger.debug(f"Periodic DHT sync failed: {e}")

                # Periodically verify stats integrity (every 15 minutes)
                if self._activity_storage and now - last_integrity_check >= INTEGRITY_CHECK_INTERVAL:
                    try:
                        result = await self.verify_stats_integrity()
                        if not result.get("match", True):
                            discrepancies = result.get("discrepancies", [])
                            logger.warning(f"Stats integrity check: {len(discrepancies)} discrepancies found")
                            for d in discrepancies:
                                logger.warning(f"  - {d['field']}: local={d['local']}, dht={d['dht']}, diff={d['diff']}")
                        else:
                            logger.debug("Stats integrity check: OK")
                        last_integrity_check = now
                    except Exception as e:
                        logger.debug(f"Stats integrity check failed: {e}")

            except Exception as e:
                logger.warning(f"Failed to send heartbeat: {e}")

            # Wait for next interval
            await trio.sleep(HEARTBEAT_INTERVAL)

        logger.info("Heartbeat loop stopped")

    async def stop(self) -> None:
        """Stop heartbeat sending and sync final stats to DHT."""
        self._is_heartbeating = False

        # Final sync to DHT before shutdown (preserve stats)
        if self._activity_storage:
            try:
                synced = await self.sync_stats_to_dht()
                if synced > 0:
                    logger.info(f"Final DHT sync on shutdown: {synced} entries synced")
            except Exception as e:
                logger.debug(f"Final DHT sync failed: {e}")

        logger.info("Uptime tracker stopped")

    def start_round(self, round_id: str, round_start: int) -> None:
        """
        Start tracking a new round.

        Args:
            round_id: Unique identifier for the round
            round_start: Unix timestamp of round start
        """
        self._current_round = round_id
        self._round_start = round_start

        # Update activity storage with current round
        if self._activity_storage:
            self._activity_storage.set_current_round(round_id)

        logger.info(f"Uptime tracking started for round: {round_id}")

    async def send_heartbeat_async(self, roles: Optional[List[str]] = None) -> Optional[Heartbeat]:
        """
        Send a heartbeat to the network (async version).

        Args:
            roles: List of roles this node is performing

        Returns:
            The heartbeat sent, or None if not in a round
        """
        if not self._current_round:
            logger.debug("No active round, not sending heartbeat")
            return None

        now = int(time.time())

        # Rate limit heartbeats
        if now - self._last_heartbeat < HEARTBEAT_INTERVAL - 5:
            return None

        # Get peer_id from peers if available (dynamic lookup)
        current_peer_id = self.peer_id
        if not current_peer_id and self.peers:
            current_peer_id = getattr(self.peers, 'peer_id', '') or ''

        # Determine roles: use provided roles, then callback, then default to node
        heartbeat_roles = roles
        if heartbeat_roles is None and self._role_callback is not None:
            try:
                heartbeat_roles = self._role_callback()
            except Exception as e:
                logger.debug(f"Role callback failed: {e}")
                heartbeat_roles = None
        if not heartbeat_roles:
            heartbeat_roles = ["node"]

        # Create heartbeat with all fields
        heartbeat = Heartbeat(
            node_id=self.node_id,
            timestamp=now,
            round_id=self._current_round,
            evrmore_address=self.evrmore_address,
            peer_id=current_peer_id,
            roles=heartbeat_roles,
            stake=self.stake,
            version=HEARTBEAT_PROTOCOL_VERSION,
            status_message=Heartbeat.get_random_status(),
        )

        # Sign the heartbeat if we have a wallet or private key
        if self.wallet is not None or self.private_key:
            heartbeat.signature = self._sign_heartbeat(heartbeat)

        # Validate heartbeat before broadcast (warn but don't block)
        is_valid, error = heartbeat.is_valid_for_broadcast()
        if not is_valid:
            logger.debug(f"Heartbeat validation note: {error}")

        # Record our own heartbeat
        self.receive_heartbeat(heartbeat)

        # Broadcast via PubSub (async)
        if self.peers:
            try:
                await self.peers.broadcast(
                    HEARTBEAT_TOPIC,
                    heartbeat.to_dict()
                )
                logger.debug(f"Broadcast heartbeat: {heartbeat.status_message}")
            except Exception as e:
                logger.error(f"Failed to broadcast heartbeat: {e}")

        self._last_heartbeat = now
        self._last_status_message = heartbeat.status_message  # Store for UI display
        self._heartbeats_sent += 1

        # Persist to storage (survives restart)
        if self._activity_storage:
            try:
                await self._activity_storage.increment_heartbeat_sent()
            except Exception as e:
                logger.debug(f"Failed to persist heartbeat sent stat: {e}")

        # Notify external listeners that we sent a heartbeat
        if self._on_heartbeat_sent:
            try:
                self._on_heartbeat_sent(heartbeat)
            except Exception as e:
                logger.debug(f"Heartbeat sent callback error: {e}")

        return heartbeat

    def send_heartbeat(self, roles: Optional[List[str]] = None) -> Optional[Heartbeat]:
        """
        Send a heartbeat to the network (sync version - for backwards compatibility).
        Note: This version cannot broadcast over pubsub. Use send_heartbeat_async instead.

        Args:
            roles: List of roles this node is performing

        Returns:
            The heartbeat sent, or None if not in a round
        """
        if not self._current_round:
            logger.debug("No active round, not sending heartbeat")
            return None

        now = int(time.time())

        # Rate limit heartbeats
        if now - self._last_heartbeat < HEARTBEAT_INTERVAL - 5:
            return None

        # Get peer_id from peers if available (dynamic lookup)
        current_peer_id = self.peer_id
        if not current_peer_id and self.peers:
            current_peer_id = getattr(self.peers, 'peer_id', '') or ''

        # Create heartbeat with all fields
        heartbeat = Heartbeat(
            node_id=self.node_id,
            timestamp=now,
            round_id=self._current_round,
            evrmore_address=self.evrmore_address,
            peer_id=current_peer_id,
            roles=roles or ["predictor"],
            stake=self.stake,
            version=HEARTBEAT_PROTOCOL_VERSION,
            status_message=Heartbeat.get_random_status(),
        )

        # Sign the heartbeat if we have a wallet or private key
        if self.wallet is not None or self.private_key:
            heartbeat.signature = self._sign_heartbeat(heartbeat)

        # Validate heartbeat before broadcast (warn but don't block)
        is_valid, error = heartbeat.is_valid_for_broadcast()
        if not is_valid:
            logger.debug(f"Heartbeat validation note: {error}")

        # Record our own heartbeat
        self.receive_heartbeat(heartbeat)

        # Note: Cannot broadcast in sync version - use send_heartbeat_async
        logger.warning("send_heartbeat (sync) called - use send_heartbeat_async for network broadcast")

        self._last_heartbeat = now
        self._last_status_message = heartbeat.status_message  # Store for UI display
        self._heartbeats_sent += 1
        return heartbeat

    def _sign_heartbeat(self, heartbeat: Heartbeat) -> bytes:
        """
        Sign a heartbeat with our wallet.

        Uses python-evrmorelib for real ECDSA signing when a wallet is provided.

        Args:
            heartbeat: The heartbeat to sign

        Returns:
            Signature bytes (base64 encoded)
        """
        # Get content to sign
        content = heartbeat.get_signing_content()

        # Try wallet-based signing first (real ECDSA via python-evrmorelib)
        if self.wallet is not None:
            try:
                # Both EvrmoreWallet and satorilib EvrmoreIdentity have sign()
                return self.wallet.sign(content)
            except Exception as e:
                logger.warning(f"Wallet signing failed: {e}, falling back to placeholder")

        # Fallback to placeholder signing (backward compatibility)
        if self.private_key:
            # Placeholder: hash of content + key (NOT PRODUCTION SAFE)
            placeholder_sig = hashlib.sha256(
                content.encode() + self.private_key
            ).digest()
            return placeholder_sig

        return b""

    def _verify_heartbeat_signature(self, heartbeat: Heartbeat) -> bool:
        """
        Verify a heartbeat's signature.

        Uses python-evrmorelib for real signature verification.

        Args:
            heartbeat: The heartbeat to verify

        Returns:
            True if signature is valid (or no signature required)
        """
        if not heartbeat.signature:
            # No signature - could be from older version
            logger.debug(f"No signature on heartbeat from {heartbeat.node_id}")
            return True

        # Need address to verify against
        if not heartbeat.evrmore_address:
            logger.warning(f"Cannot verify signature: no evrmore_address in heartbeat")
            return len(heartbeat.signature) > 0  # Fallback to existence check

        # Try real verification using python-evrmorelib
        if SIGNING_AVAILABLE:
            try:
                content = heartbeat.get_signing_content()
                return verify_message(
                    message=content,
                    signature=heartbeat.signature,
                    address=heartbeat.evrmore_address,
                )
            except Exception as e:
                logger.warning(f"Signature verification failed: {e}")
                return False

        # Fallback: just check signature exists
        return len(heartbeat.signature) > 0

    def receive_heartbeat(self, heartbeat: Heartbeat) -> bool:
        """
        Receive a heartbeat from a node.

        Args:
            heartbeat: The heartbeat to process

        Returns:
            True if heartbeat was accepted
        """
        # Validate round
        if heartbeat.round_id != self._current_round:
            logger.debug(f"Heartbeat for wrong round: {heartbeat.round_id}")
            return False

        # Deduplicate: check if we already recorded this exact timestamp for this node
        # This prevents double-counting when GossipSub delivers our own message back
        existing_timestamps = self._heartbeats[heartbeat.round_id][heartbeat.node_id]
        if heartbeat.timestamp in existing_timestamps:
            logger.debug(f"Duplicate heartbeat from {heartbeat.node_id} at {heartbeat.timestamp}, ignoring")
            return False

        # Store heartbeat timestamp
        self._heartbeats[heartbeat.round_id][heartbeat.node_id].append(heartbeat.timestamp)

        # Store in recent heartbeats buffer for UI display
        self._recent_heartbeats.append(heartbeat)
        # Trim to max size (circular buffer behavior)
        if len(self._recent_heartbeats) > self._recent_heartbeats_max:
            self._recent_heartbeats = self._recent_heartbeats[-self._recent_heartbeats_max:]

        # Store node roles (by both node_id and peer_id for different lookups)
        for role in heartbeat.roles:
            self._node_roles[heartbeat.node_id].add(role)
            # Also store by peer_id if available (for P2P peer lookups)
            if heartbeat.peer_id:
                self._peer_roles[heartbeat.peer_id].add(role)

        # Store node stake (for governance voting power calculation)
        if heartbeat.stake > 0:
            self._node_stakes[heartbeat.node_id] = heartbeat.stake
            self._node_stakes_updated = int(time.time())

        # Increment received counter (for heartbeats from others)
        if heartbeat.node_id != self.node_id:
            self._heartbeats_received += 1

            # Persist to storage (spawn async task)
            if self._activity_storage and self.peers:
                try:
                    self.peers.spawn_background_task(
                        self._activity_storage.increment_heartbeat_received
                    )
                except Exception as e:
                    logger.debug(f"Failed to spawn heartbeat received persist task: {e}")

        # Notify external listeners
        if self._on_heartbeat_received:
            try:
                self._on_heartbeat_received(heartbeat)
            except Exception as e:
                logger.debug(f"Heartbeat received callback error: {e}")

        logger.debug(f"Received heartbeat from {heartbeat.node_id}")
        return True

    def calculate_round_uptime(
        self,
        round_id: str,
        round_start: int,
        round_end: int,
    ) -> RoundUptimeReport:
        """
        Calculate uptime for all nodes in a round.

        Args:
            round_id: Round identifier
            round_start: Round start timestamp
            round_end: Round end timestamp

        Returns:
            RoundUptimeReport with uptime data for all nodes
        """
        round_duration = round_end - round_start
        expected_heartbeats = max(1, round_duration // HEARTBEAT_INTERVAL)

        node_records: Dict[str, NodeUptimeRecord] = {}
        relay_qualified_count = 0

        heartbeats_in_round = self._heartbeats.get(round_id, {})

        for node_id, timestamps in heartbeats_in_round.items():
            if not timestamps:
                continue

            # Calculate metrics
            heartbeat_count = len(timestamps)
            first_seen = min(timestamps)
            last_seen = max(timestamps)

            # Calculate uptime percentage
            # Method: Count heartbeats vs expected, cap at 100%
            uptime_percentage = min(1.0, heartbeat_count / expected_heartbeats)

            # Alternative method: Check coverage of round
            # (how much of the round the node was present)
            if heartbeat_count > 1:
                # Time between first and last heartbeat
                presence_duration = last_seen - first_seen
                # Add buffer for last heartbeat
                presence_duration += HEARTBEAT_INTERVAL
                coverage = min(1.0, presence_duration / round_duration)
                # Use the more conservative estimate
                uptime_percentage = min(uptime_percentage, coverage)

            is_relay_qualified = uptime_percentage >= RELAY_UPTIME_THRESHOLD
            if is_relay_qualified:
                relay_qualified_count += 1

            node_records[node_id] = NodeUptimeRecord(
                node_id=node_id,
                round_id=round_id,
                first_seen=first_seen,
                last_seen=last_seen,
                heartbeat_count=heartbeat_count,
                expected_heartbeats=expected_heartbeats,
                uptime_percentage=uptime_percentage,
                is_relay_qualified=is_relay_qualified,
            )

        return RoundUptimeReport(
            round_id=round_id,
            round_start=round_start,
            round_end=round_end,
            total_nodes=len(node_records),
            relay_qualified_nodes=relay_qualified_count,
            node_records=node_records,
        )

    def get_node_uptime(self, node_id: str, round_id: Optional[str] = None) -> float:
        """
        Get uptime percentage for a specific node.

        Args:
            node_id: Node to check
            round_id: Round to check (default: current round)

        Returns:
            Uptime percentage (0.0 - 1.0)
        """
        round_id = round_id or self._current_round
        if not round_id:
            return 0.0

        timestamps = self._heartbeats.get(round_id, {}).get(node_id, [])
        if not timestamps:
            return 0.0

        # Simple calculation: heartbeats / expected
        now = int(time.time())
        elapsed = now - self._round_start
        expected = max(1, elapsed // HEARTBEAT_INTERVAL)

        return min(1.0, len(timestamps) / expected)

    def get_uptime_percentage(self) -> float:
        """
        Get our own uptime percentage for the current round.

        Returns:
            Uptime percentage (0.0 - 1.0)
        """
        return self.get_node_uptime(self.node_id)

    def is_relay_qualified(self, node_id: str, round_id: Optional[str] = None) -> bool:
        """
        Check if a node qualifies for relay bonus.

        Args:
            node_id: Node to check
            round_id: Round to check (default: current round)

        Returns:
            True if node has ≥95% uptime
        """
        uptime = self.get_node_uptime(node_id, round_id)
        return uptime >= RELAY_UPTIME_THRESHOLD

    def get_relay_qualified_nodes(self, round_id: Optional[str] = None) -> Set[str]:
        """
        Get all nodes that qualify for relay bonus.

        Args:
            round_id: Round to check (default: current round)

        Returns:
            Set of node IDs with ≥95% uptime
        """
        round_id = round_id or self._current_round
        if not round_id:
            return set()

        qualified = set()
        for node_id in self._heartbeats.get(round_id, {}).keys():
            if self.is_relay_qualified(node_id, round_id):
                qualified.add(node_id)

        return qualified

    def get_node_roles(self, node_id: str) -> Set[str]:
        """Get declared roles for a node by evrmore address."""
        return self._node_roles.get(node_id, set())

    def get_peer_roles(self, peer_id: str) -> Set[str]:
        """Get declared roles for a peer by libp2p peer_id."""
        return self._peer_roles.get(peer_id, set())

    def get_recent_heartbeats(self, limit: int = 20) -> List[Heartbeat]:
        """
        Get recent heartbeats for UI display.

        Args:
            limit: Maximum number of heartbeats to return

        Returns:
            List of most recent Heartbeat objects, newest first
        """
        # Return most recent heartbeats, sorted by timestamp descending
        sorted_heartbeats = sorted(
            self._recent_heartbeats,
            key=lambda h: h.timestamp,
            reverse=True
        )
        return sorted_heartbeats[:limit]

    def get_uptime_streak(self, node_id: str) -> int:
        """
        Get current uptime streak days for a node.

        Args:
            node_id: Node to check

        Returns:
            Number of consecutive days with ≥95% uptime
        """
        if node_id in self._uptime_streaks:
            return self._uptime_streaks[node_id].streak_days
        return 0

    def get_uptime_streak_days(self, node_id: str) -> int:
        """
        Alias for get_uptime_streak() - used by GovernanceProtocol.

        Args:
            node_id: Node to check

        Returns:
            Number of consecutive days with ≥95% uptime
        """
        return self.get_uptime_streak(node_id)

    def get_uptime_streak_record(self, node_id: str) -> Optional[NodeStreakRecord]:
        """
        Get full uptime streak record for a node.

        Args:
            node_id: Node to check

        Returns:
            NodeStreakRecord or None if no record exists
        """
        return self._uptime_streaks.get(node_id)

    def get_node_stake(self, node_id: str) -> float:
        """
        Get stake amount for a node (from heartbeat data).

        Args:
            node_id: Node to check

        Returns:
            Stake amount, or 0.0 if unknown
        """
        return self._node_stakes.get(node_id, 0.0)

    def get_all_node_stakes(self) -> Dict[str, float]:
        """
        Get all known node stakes.

        Returns:
            Dict mapping node_id to stake amount
        """
        return dict(self._node_stakes)

    def get_total_network_stake(self) -> float:
        """
        Calculate total stake across all known active nodes.

        Used by GovernanceProtocol for voting power calculations.

        Returns:
            Sum of all known node stakes
        """
        return sum(self._node_stakes.values())

    def get_active_node_count(self) -> int:
        """
        Get count of nodes that have sent heartbeats recently.

        Returns:
            Number of active nodes with known stakes
        """
        return len(self._node_stakes)

    def update_uptime_streak(self, node_id: str, round_id: str, is_qualified: bool) -> int:
        """
        Update uptime streak for a node based on round qualification.

        Call this at the end of each round to update streak tracking.

        Args:
            node_id: Node to update
            round_id: Round that just ended (format: "round_YYYY-MM-DD")
            is_qualified: Whether the node met 95% uptime threshold

        Returns:
            New streak days count
        """
        # Extract date from round_id
        round_date = round_id.replace("round_", "") if round_id.startswith("round_") else round_id

        # Get or create streak record
        if node_id not in self._uptime_streaks:
            self._uptime_streaks[node_id] = NodeStreakRecord(node_id=node_id)

        record = self._uptime_streaks[node_id]

        if is_qualified:
            # Check if this continues the streak (consecutive days)
            if record.last_qualified_round:
                # Extract date from last round
                last_date = record.last_qualified_round.replace("round_", "")
                # Check if dates are consecutive (simplified check)
                from datetime import datetime, timedelta
                try:
                    last_dt = datetime.strptime(last_date, "%Y-%m-%d")
                    current_dt = datetime.strptime(round_date, "%Y-%m-%d")
                    expected_dt = last_dt + timedelta(days=1)

                    if current_dt == expected_dt:
                        # Consecutive day - extend streak
                        record.streak_days += 1
                    elif current_dt == last_dt:
                        # Same day (re-processing) - no change
                        pass
                    else:
                        # Gap in streak - reset
                        record.streak_days = 1
                        record.streak_start_date = round_date
                except ValueError:
                    # Date parsing failed - treat as new streak
                    record.streak_days = 1
                    record.streak_start_date = round_date
            else:
                # First qualified round
                record.streak_days = 1
                record.streak_start_date = round_date

            record.last_qualified_round = round_id

            # Update longest streak
            if record.streak_days > record.longest_streak:
                record.longest_streak = record.streak_days

            logger.debug(f"Node {node_id} uptime streak: {record.streak_days} days")
        else:
            # Did not qualify - reset streak
            if record.streak_days > 0:
                logger.info(f"Node {node_id} uptime streak reset (was {record.streak_days} days)")
            record.streak_days = 0
            record.streak_start_date = ""

        return record.streak_days

    def get_top_uptime_streaks(self, limit: int = 10) -> List[NodeStreakRecord]:
        """
        Get nodes with highest uptime streaks.

        Args:
            limit: Maximum number of results

        Returns:
            List of NodeStreakRecord sorted by streak_days descending
        """
        sorted_records = sorted(
            self._uptime_streaks.values(),
            key=lambda r: r.streak_days,
            reverse=True
        )
        return sorted_records[:limit]

    def clear_round(self, round_id: str) -> None:
        """
        Clear heartbeat data for a round (after processing).

        Args:
            round_id: Round to clear
        """
        if round_id in self._heartbeats:
            del self._heartbeats[round_id]
            logger.debug(f"Cleared heartbeat data for round: {round_id}")

    # ========================================================================
    # PERSISTENT STATS METHODS
    # ========================================================================

    async def load_persisted_stats(self) -> Dict[str, int]:
        """
        Load persisted activity stats from storage.

        Call this on startup to restore counts after restart.

        Returns:
            Dict with heartbeats_sent, heartbeats_received, etc.
        """
        if not self._activity_storage:
            return {
                "heartbeats_sent": self._heartbeats_sent,
                "heartbeats_received": self._heartbeats_received,
                "predictions": 0,
                "observations": 0,
                "consensus_votes": 0,
                "governance_votes": 0,
            }

        try:
            totals = await self._activity_storage.get_total_stats()

            # Optionally sync in-memory counters to match storage
            # (useful if we want counters to reflect historical totals)
            # self._heartbeats_sent = totals.get("heartbeats_sent", 0)
            # self._heartbeats_received = totals.get("heartbeats_received", 0)

            logger.info(f"Loaded persisted stats: {totals}")
            return totals
        except Exception as e:
            logger.warning(f"Failed to load persisted stats: {e}")
            return {
                "heartbeats_sent": self._heartbeats_sent,
                "heartbeats_received": self._heartbeats_received,
                "predictions": 0,
                "observations": 0,
                "consensus_votes": 0,
                "governance_votes": 0,
            }

    async def get_persisted_stats(
        self,
        round_id: str = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Get persisted activity stats for a specific round or current round.

        Args:
            round_id: Optional round to get stats for (defaults to current)

        Returns:
            Dict with all activity stats, or None if not found
        """
        if not self._activity_storage:
            return None

        try:
            stats = await self._activity_storage.get_stats(round_id=round_id)
            if stats:
                return stats.to_dict()
            return None
        except Exception as e:
            logger.warning(f"Failed to get persisted stats: {e}")
            return None

    async def sync_stats_to_dht(self) -> int:
        """
        Sync local activity stats to DHT for network redundancy.

        Returns:
            Number of keys synced
        """
        if not self._activity_storage:
            return 0

        try:
            return await self._activity_storage.sync_to_dht()
        except Exception as e:
            logger.warning(f"Failed to sync stats to DHT: {e}")
            return 0

    async def recover_stats_from_dht(self, keys: List[str] = None) -> int:
        """
        Recover activity stats from DHT (after restart or data loss).

        Args:
            keys: Optional specific keys to recover

        Returns:
            Number of keys recovered
        """
        if not self._activity_storage:
            return 0

        try:
            if keys:
                return await self._activity_storage.recover_from_dht(keys)
            else:
                # Recover all our stats
                all_keys = [f"{self.node_id}:{self._current_round or 'current'}"]
                return await self._activity_storage.recover_from_dht(all_keys)
        except Exception as e:
            logger.warning(f"Failed to recover stats from DHT: {e}")
            return 0

    async def verify_stats_integrity(self) -> Dict[str, Any]:
        """
        Verify local stats against DHT consensus.

        Returns:
            Dict with verification results including any discrepancies
        """
        if not self._activity_storage:
            return {"match": True, "discrepancies": [], "error": "Storage not available"}

        try:
            return await self._activity_storage.verify_against_dht()
        except Exception as e:
            logger.warning(f"Failed to verify stats integrity: {e}")
            return {"match": False, "discrepancies": [], "error": str(e)}

    def get_activity_storage(self) -> Optional[ActivityStatsStorage]:
        """Get the activity stats storage instance for external access."""
        return self._activity_storage


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def check_relay_uptime_qualified(
    uptime_tracker: UptimeTracker,
    node_id: str,
    round_id: Optional[str] = None,
) -> bool:
    """
    Convenience function to check relay qualification.

    Args:
        uptime_tracker: UptimeTracker instance
        node_id: Node to check
        round_id: Round to check (optional)

    Returns:
        True if node qualifies for relay bonus
    """
    return uptime_tracker.is_relay_qualified(node_id, round_id)


def get_uptime_percentage(
    uptime_tracker: UptimeTracker,
    node_id: str,
    round_id: Optional[str] = None,
) -> float:
    """
    Convenience function to get uptime percentage.

    Args:
        uptime_tracker: UptimeTracker instance
        node_id: Node to check
        round_id: Round to check (optional)

    Returns:
        Uptime percentage (0.0 - 1.0)
    """
    return uptime_tracker.get_node_uptime(node_id, round_id)
