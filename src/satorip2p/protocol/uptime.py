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
from typing import Dict, List, Optional, Set, Callable, TYPE_CHECKING, Union
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

# Protocol version for compatibility
HEARTBEAT_PROTOCOL_VERSION = "1.0.0"

# NOTE: Bootstrap peers are configured in satorip2p/config.py (BOOTSTRAP_PEERS)
# All PubSub topics (including heartbeat) use the same P2P mesh network.

# Fun status messages - because why not have personality?
HEARTBEAT_STATUS_MESSAGES = [
    "vibing in the mesh...",
    "beep boop, still here!",
    "crunching predictions...",
    "riding the data waves...",
    "quantum entangled and ready!",
    "caffeinated and operational...",
    "syncing with the cosmos...",
    "neurons firing on all cylinders!",
    "decentralizing the future...",
    "proof of consciousness achieved!",
    "streaming consciousness...",
    "in the zone, making predictions...",
    "satori-fying the network...",
    "zen and the art of uptime...",
    "enlightenment in progress...",
    "one with the blockchain...",
    "predicting the unpredictable...",
    "distributed and feeling good!",
    "mesh-merizing the competition...",
    "staking my claim to existence!",
    "heartbeat goes brrr...",
    "living my best node life...",
    "relay-xing and prospering...",
    "oracle-ing around the clock...",
    "consensus is my middle name...",
    "born to predict, forced to wait...",
    "making satoshi proud...",
    "trustlessly trusting the process...",
    "peer-to-peer and loving it!",
    "immutably here for you...",
]


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
        self.node_id = node_id or "unknown"
        self.peer_id = peer_id or ""
        self.stake = stake
        self.private_key = private_key  # Deprecated, kept for backward compatibility
        self.wallet = wallet

        # Get address from wallet if not provided
        if wallet and not evrmore_address:
            self.evrmore_address = getattr(wallet, 'address', '') or ""
        else:
            self.evrmore_address = evrmore_address or ""

        # Heartbeat storage: {round_id: {node_id: [timestamps]}}
        self._heartbeats: Dict[str, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))

        # Current round
        self._current_round: Optional[str] = None
        self._round_start: int = 0

        # Node roles (declared by each node)
        self._node_roles: Dict[str, Set[str]] = defaultdict(set)

        # Heartbeat sending state
        self._is_heartbeating: bool = False
        self._last_heartbeat: int = 0

        # Subscribe to heartbeat topic if peers available
        if self.peers:
            self._setup_pubsub()

    def _setup_pubsub(self) -> None:
        """Subscribe to heartbeat PubSub topic."""
        if not self.peers:
            return

        try:
            self.peers.subscribe(
                HEARTBEAT_TOPIC,
                self._handle_heartbeat_message
            )
            logger.info(f"Subscribed to {HEARTBEAT_TOPIC}")
        except Exception as e:
            logger.error(f"Failed to setup heartbeat PubSub: {e}")

    def _handle_heartbeat_message(self, message: dict) -> None:
        """Handle incoming heartbeat from PubSub."""
        try:
            heartbeat = Heartbeat.from_dict(message)
            self.receive_heartbeat(heartbeat)
        except Exception as e:
            logger.error(f"Failed to handle heartbeat message: {e}")

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

        # Auto-start a round if not already in one
        if not self._current_round:
            import time
            round_id = f"round_{int(time.time())}"
            self.start_round(round_id, int(time.time()))

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
        """
        import trio

        logger.info("Heartbeat loop started")
        while self._is_heartbeating:
            try:
                # Send heartbeat
                heartbeat = self.send_heartbeat()
                if heartbeat:
                    logger.debug(f"Sent heartbeat: {heartbeat.node_id[:16]}...")
                else:
                    logger.debug("Heartbeat not sent (no active round)")
            except Exception as e:
                logger.warning(f"Failed to send heartbeat: {e}")

            # Wait for next interval
            await trio.sleep(HEARTBEAT_INTERVAL)

        logger.info("Heartbeat loop stopped")

    async def stop(self) -> None:
        """Stop heartbeat sending."""
        self._is_heartbeating = False
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
        logger.info(f"Uptime tracking started for round: {round_id}")

    def send_heartbeat(self, roles: Optional[List[str]] = None) -> Optional[Heartbeat]:
        """
        Send a heartbeat to the network.

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

        # Broadcast via PubSub
        if self.peers:
            try:
                self.peers.broadcast(
                    HEARTBEAT_TOPIC,
                    heartbeat.to_dict()
                )
                logger.debug(f"Sent heartbeat: {heartbeat.status_message}")
            except Exception as e:
                logger.error(f"Failed to broadcast heartbeat: {e}")

        self._last_heartbeat = now
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

        # Store heartbeat timestamp
        self._heartbeats[heartbeat.round_id][heartbeat.node_id].append(heartbeat.timestamp)

        # Store node roles
        for role in heartbeat.roles:
            self._node_roles[heartbeat.node_id].add(role)

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
        """Get declared roles for a node."""
        return self._node_roles.get(node_id, set())

    def clear_round(self, round_id: str) -> None:
        """
        Clear heartbeat data for a round (after processing).

        Args:
            round_id: Round to clear
        """
        if round_id in self._heartbeats:
            del self._heartbeats[round_id]
            logger.debug(f"Cleared heartbeat data for round: {round_id}")


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
