"""
satorip2p/protocol/identify.py

Custom Identify protocol for peer information exchange.

This is a Satori-native implementation that doesn't depend on libp2p's
optional identify module. Uses GossipSub for announcements.

Features:
- Peer identity announcement
- Protocol capabilities exchange
- Address sharing
- Evrmore wallet address linking
"""

import logging
import time
import json
from typing import Dict, List, Optional, Set, TYPE_CHECKING
from dataclasses import dataclass, asdict, field

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.identify")

# Protocol constants
IDENTIFY_TOPIC = "satori/identify"
IDENTIFY_REQUEST_TOPIC = "satori/identify/request"
IDENTIFY_PROTOCOL_VERSION = "1.0.0"
IDENTITY_CACHE_TTL = 300  # 5 minutes


@dataclass
class PeerIdentity:
    """Peer identity information."""
    peer_id: str                      # libp2p peer ID
    evrmore_address: str              # Evrmore wallet address
    public_key: str                   # Public key (hex)
    listen_addresses: List[str]       # Multiaddresses
    protocols: List[str]              # Supported protocols
    agent_version: str                # Software version
    roles: List[str]                  # Node roles (predictor, relay, oracle, signer)
    timestamp: float                  # When this identity was generated
    version: str = IDENTIFY_PROTOCOL_VERSION

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PeerIdentity":
        return cls(**data)


@dataclass
class IdentifyRequest:
    """Request for peer identity."""
    requester_id: str                 # Requester's peer ID
    target_id: Optional[str] = None   # Specific target, or None for broadcast
    timestamp: float = field(default_factory=time.time)
    version: str = IDENTIFY_PROTOCOL_VERSION

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "IdentifyRequest":
        return cls(**data)


class IdentifyProtocol:
    """
    Custom identify protocol for satorip2p.

    Allows peers to exchange information about their capabilities,
    addresses, and identity. Uses GossipSub for broadcast.

    Usage:
        identify = IdentifyProtocol(peers)
        await identify.start()

        # Announce our identity
        await identify.announce()

        # Get known peer identities
        known = identify.get_known_peers()

        # Request specific peer's identity
        await identify.request_identity(peer_id)
    """

    def __init__(self, peers: "Peers"):
        """
        Initialize IdentifyProtocol.

        Args:
            peers: Peers instance for network access
        """
        self._peers = peers
        self._known_peers: Dict[str, PeerIdentity] = {}  # peer_id -> identity
        self._identity_timestamps: Dict[str, float] = {}  # peer_id -> last_seen
        self._started = False
        self._trio_token = None  # Store trio token for cross-thread calls

    async def start(self) -> None:
        """Start the identify protocol."""
        if self._started:
            return

        # Store trio token for callbacks that need to schedule async work
        import trio
        try:
            self._trio_token = trio.lowlevel.current_trio_token()
        except RuntimeError:
            logger.warning("Could not get trio token during identify start")

        # Subscribe to identity announcements
        self._peers.subscribe(IDENTIFY_TOPIC, self._on_identity)

        # Subscribe to identity requests
        self._peers.subscribe(IDENTIFY_REQUEST_TOPIC, self._on_identity_request)

        self._started = True
        logger.info("Identify protocol started")

        # Announce ourselves
        await self.announce()

    async def stop(self) -> None:
        """Stop the identify protocol."""
        if not self._started:
            return

        self._peers.unsubscribe(IDENTIFY_TOPIC)
        self._peers.unsubscribe(IDENTIFY_REQUEST_TOPIC)

        self._known_peers.clear()
        self._identity_timestamps.clear()
        self._started = False
        logger.info("Identify protocol stopped")

    async def announce(self) -> None:
        """Announce our identity to the network."""
        if not self._started:
            return

        identity = self._build_identity()
        if not identity:
            return

        try:
            await self._peers.broadcast(
                json.dumps(identity.to_dict()).encode(),
                stream_id=IDENTIFY_TOPIC.replace("satori/", "")
            )
            logger.debug(f"Announced identity: {identity.peer_id[:16]}...")
        except Exception as e:
            logger.warning(f"Failed to announce identity: {e}")

    async def request_identity(self, target_peer_id: Optional[str] = None) -> None:
        """
        Request identity from a specific peer or all peers.

        Args:
            target_peer_id: Specific peer, or None for broadcast request
        """
        if not self._started:
            return

        my_peer_id = self._peers.peer_id
        if not my_peer_id:
            return

        request = IdentifyRequest(
            requester_id=my_peer_id,
            target_id=target_peer_id,
        )

        try:
            await self._peers.broadcast(
                json.dumps(request.to_dict()).encode(),
                stream_id=IDENTIFY_REQUEST_TOPIC.replace("satori/", "")
            )
        except Exception as e:
            logger.debug(f"Failed to request identity: {e}")

    def get_known_peers(self) -> Dict[str, PeerIdentity]:
        """Get all known peer identities."""
        self._cleanup_stale()
        return dict(self._known_peers)

    def get_peer_identity(self, peer_id: str) -> Optional[PeerIdentity]:
        """Get identity for a specific peer."""
        self._cleanup_stale()
        return self._known_peers.get(peer_id)

    def get_peers_by_role(self, role: str) -> List[PeerIdentity]:
        """Get all peers with a specific role."""
        self._cleanup_stale()
        return [
            identity for identity in self._known_peers.values()
            if role in identity.roles
        ]

    def get_stats(self) -> dict:
        """Get identify protocol statistics."""
        self._cleanup_stale()
        return {
            "started": self._started,
            "known_peers": len(self._known_peers),
            "version": IDENTIFY_PROTOCOL_VERSION,
            "roles_seen": list(set(
                role for identity in self._known_peers.values()
                for role in identity.roles
            )),
        }

    def _build_identity(self) -> Optional[PeerIdentity]:
        """Build our own identity."""
        peer_id = self._peers.peer_id
        if not peer_id:
            return None

        # Determine our roles
        roles = ["predictor"]  # All nodes are predictors
        if self._peers.is_relay:
            roles.append("relay")

        return PeerIdentity(
            peer_id=peer_id,
            evrmore_address=self._peers.evrmore_address or "",
            public_key=self._peers.public_key or "",
            listen_addresses=self._peers.public_addresses or [],
            protocols=self._get_supported_protocols(),
            agent_version=self._get_agent_version(),
            roles=roles,
            timestamp=time.time(),
        )

    def _get_supported_protocols(self) -> List[str]:
        """Get list of supported protocols."""
        protocols = [
            "/satori/stream/1.0.0",
            "/satori/ping/1.0.0",
            "/satori/identify/1.0.0",
        ]

        if self._peers.enable_dht:
            protocols.append("/ipfs/kad/1.0.0")

        if self._peers.enable_pubsub:
            protocols.append("/meshsub/1.1.0")
            protocols.append("/floodsub/1.0.0")

        if self._peers.enable_relay:
            protocols.append("/libp2p/circuit/relay/0.2.0/hop")

        return protocols

    def _get_agent_version(self) -> str:
        """Get agent version string."""
        try:
            from importlib.metadata import version
            pkg_version = version("satorip2p")
            return f"satorip2p/{pkg_version}"
        except Exception:
            return "satorip2p/1.0.0"

    def _on_identity(self, stream_id: str, data: bytes) -> None:
        """Handle incoming identity announcement."""
        try:
            identity_data = json.loads(data.decode())
            identity = PeerIdentity.from_dict(identity_data)

            # Don't store our own identity
            if identity.peer_id == self._peers.peer_id:
                return

            # Store identity
            self._known_peers[identity.peer_id] = identity
            self._identity_timestamps[identity.peer_id] = time.time()

            logger.debug(
                f"Received identity from {identity.peer_id[:16]}... "
                f"(roles: {identity.roles})"
            )

        except Exception as e:
            logger.debug(f"Error handling identity: {e}")

    def _on_identity_request(self, stream_id: str, data: bytes) -> None:
        """Handle incoming identity request."""
        try:
            request_data = json.loads(data.decode())
            request = IdentifyRequest.from_dict(request_data)

            my_peer_id = self._peers.peer_id
            if not my_peer_id:
                return

            # Check if request is for us
            if request.target_id is not None and request.target_id != my_peer_id:
                return

            # Don't respond to our own requests
            if request.requester_id == my_peer_id:
                return

            # Respond with our identity - schedule async announce
            import trio
            try:
                # Check if we're already in a trio context
                trio.lowlevel.current_trio_token()
                # We're in trio, use nursery from peers if available
                if hasattr(self._peers, '_nursery') and self._peers._nursery:
                    self._peers._nursery.start_soon(self.announce)
                else:
                    logger.debug("Cannot announce: no nursery available")
            except RuntimeError:
                # Not in trio context, use from_thread with stored token
                if self._trio_token:
                    trio.from_thread.run(self.announce, trio_token=self._trio_token)
                else:
                    logger.debug("Cannot announce: no trio token available")

        except Exception as e:
            logger.debug(f"Error handling identity request: {e}")

    def _cleanup_stale(self) -> None:
        """Remove stale identities."""
        now = time.time()
        stale = [
            peer_id for peer_id, ts in self._identity_timestamps.items()
            if now - ts > IDENTITY_CACHE_TTL
        ]
        for peer_id in stale:
            self._known_peers.pop(peer_id, None)
            self._identity_timestamps.pop(peer_id, None)
