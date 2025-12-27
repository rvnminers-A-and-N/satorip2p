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
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING
from dataclasses import dataclass, asdict, field

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.identify")

# Protocol constants - topic names (prefixed with satori/stream/ by Peers)
IDENTIFY_TOPIC = "identify"
IDENTIFY_REQUEST_TOPIC = "identify/request"
IDENTIFY_PROTOCOL_VERSION = "1.0.0"
# Note: No TTL - known peers never auto-expire. Users can manually forget peers if needed.
IDENTITY_REANNOUNCE_INTERVAL = 300  # Re-announce every 5 minutes to keep peers informed


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
        self._reannounce_task_scope = None  # For cancelling reannounce loop

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

        # Subscribe to identity announcements - use subscribe_async for GossipSub
        await self._peers.subscribe_async(IDENTIFY_TOPIC, self._on_identity)

        # Subscribe to identity requests - use subscribe_async for GossipSub
        await self._peers.subscribe_async(IDENTIFY_REQUEST_TOPIC, self._on_identity_request)

        self._started = True
        logger.info("Identify protocol started")

        # Announce ourselves
        await self.announce()

        # Start periodic re-announce loop in background
        if hasattr(self._peers, '_nursery') and self._peers._nursery:
            self._peers._nursery.start_soon(self._reannounce_loop)

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
                IDENTIFY_TOPIC,
                json.dumps(identity.to_dict()).encode()
            )
            logger.debug(f"Announced identity: peer_id={identity.peer_id}")
        except Exception as e:
            logger.warning(f"Failed to announce identity: {e}")

    async def _reannounce_loop(self) -> None:
        """Periodically re-announce identity to keep peers fresh."""
        import trio
        logger.info("Identity re-announce loop started")
        try:
            while self._started:
                await trio.sleep(IDENTITY_REANNOUNCE_INTERVAL)
                if not self._started:
                    break
                try:
                    await self.announce()
                    logger.debug("Re-announced identity to network")
                except Exception as e:
                    logger.warning(f"Re-announce failed: {e}")
        except trio.Cancelled:
            pass
        logger.info("Identity re-announce loop stopped")

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
                IDENTIFY_REQUEST_TOPIC,
                json.dumps(request.to_dict()).encode()
            )
        except Exception as e:
            logger.debug(f"Failed to request identity: {e}")

    async def announce_to_peer(self, peer_id: str) -> bool:
        """
        Send our identity directly to a specific peer.

        Args:
            peer_id: Target peer's libp2p peer ID

        Returns:
            True if sent successfully, False otherwise
        """
        if not self._started:
            return False

        identity = self._build_identity()
        if not identity:
            return False

        try:
            message = {
                "type": "identify_announce",
                "identity": identity.to_dict(),
            }
            success = await self._peers._send_direct(peer_id, message)
            if success:
                logger.debug(f"Sent identity to peer: {peer_id}")
            return success
        except Exception as e:
            logger.warning(f"Failed to send identity to {peer_id}: {e}")
            return False

    async def announce_to_known_peers(self) -> int:
        """
        Send our identity to all known peers.

        Returns:
            Number of peers successfully sent to
        """
        if not self._started:
            return 0

        success_count = 0
        for peer_id in list(self._known_peers.keys()):
            if await self.announce_to_peer(peer_id):
                success_count += 1

        logger.info(f"Announced identity to {success_count}/{len(self._known_peers)} known peers")
        return success_count

    async def request_identity_from_peer(self, peer_id: str) -> bool:
        """
        Request identity directly from a specific peer.

        Args:
            peer_id: Target peer's libp2p peer ID

        Returns:
            True if request sent successfully, False otherwise
        """
        if not self._started:
            return False

        my_peer_id = self._peers.peer_id
        if not my_peer_id:
            return False

        try:
            message = {
                "type": "identify_request",
                "requester_id": my_peer_id,
                "target_id": peer_id,
            }
            success = await self._peers._send_direct(peer_id, message)
            if success:
                logger.debug(f"Sent identity request to peer: {peer_id}")
            return success
        except Exception as e:
            logger.warning(f"Failed to request identity from {peer_id}: {e}")
            return False

    async def request_identity_from_known_peers(self) -> int:
        """
        Request identity from all known peers.

        Returns:
            Number of peers successfully requested from
        """
        if not self._started:
            return 0

        success_count = 0
        for peer_id in list(self._known_peers.keys()):
            if await self.request_identity_from_peer(peer_id):
                success_count += 1

        logger.info(f"Requested identity from {success_count}/{len(self._known_peers)} known peers")
        return success_count

    def get_known_peers(self) -> Dict[str, PeerIdentity]:
        """Get all known peer identities."""
        return dict(self._known_peers)

    def get_peer_identity(self, peer_id: str) -> Optional[PeerIdentity]:
        """Get identity for a specific peer."""
        return self._known_peers.get(peer_id)

    def get_peers_by_role(self, role: str) -> List[PeerIdentity]:
        """Get all peers with a specific role."""
        return [
            identity for identity in self._known_peers.values()
            if role in identity.roles
        ]

    def forget_peer(self, peer_id: str) -> bool:
        """
        Remove a peer from the known peers list.

        Args:
            peer_id: libp2p peer ID to forget

        Returns:
            True if peer was found and removed, False otherwise
        """
        if peer_id in self._known_peers:
            del self._known_peers[peer_id]
            logger.info(f"Forgot peer: {peer_id}")
            return True
        return False

    def get_stats(self) -> dict:
        """Get identify protocol statistics."""
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

    def _on_identity(self, stream_id: str, data: Any) -> None:
        """Handle incoming identity announcement."""
        try:
            # Data may be bytes (raw) or already deserialized dict
            if isinstance(data, bytes):
                identity_data = json.loads(data.decode())
            elif isinstance(data, dict):
                identity_data = data
            else:
                logger.warning(f"Unexpected identity data type: {type(data)}")
                return

            identity = PeerIdentity.from_dict(identity_data)

            # Don't store our own identity
            if identity.peer_id == self._peers.peer_id:
                return

            # Store identity
            self._known_peers[identity.peer_id] = identity
            self._identity_timestamps[identity.peer_id] = time.time()

            logger.info(
                f"Received identity from peer_id={identity.peer_id} "
                f"(roles: {identity.roles}, evrmore_address: {identity.evrmore_address})"
            )

        except Exception as e:
            logger.warning(f"Error handling identity: {e}")

    def _on_identity_request(self, stream_id: str, data: Any) -> None:
        """Handle incoming identity request."""
        try:
            # Data may be bytes (raw) or already deserialized dict
            if isinstance(data, bytes):
                request_data = json.loads(data.decode())
            elif isinstance(data, dict):
                request_data = data
            else:
                logger.warning(f"Unexpected identity request data type: {type(data)}")
                return

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

    # Note: _cleanup_stale removed - known peers never auto-expire.
    # Users can manually forget peers using forget_peer() if needed.
