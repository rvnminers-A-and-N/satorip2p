"""
satorip2p/protocol/stream_registry.py

Decentralized stream registry via DHT.

Replaces central server stream assignment with:
- DHT-based stream definitions (anyone can create)
- Predictor slot claiming (first-come-first-serve or staked)
- Stream discovery by source/target/datatype

Works alongside central server in hybrid mode:
- Central mode: Not used (central server assigns streams)
- Hybrid mode: Queries both P2P and central server
- P2P mode: Only P2P stream discovery

Usage:
    from satorip2p.protocol.stream_registry import StreamRegistry

    registry = StreamRegistry(peers)
    await registry.start()

    # Discover available streams
    streams = await registry.discover_streams(source="exchange/binance")

    # Claim a slot on a stream
    await registry.claim_stream(stream_id, slot_index=0)

    # Get my claimed streams
    my_streams = await registry.get_my_streams()
"""

import logging
import time
import json
import hashlib
import trio
from typing import Dict, List, Optional, Callable, TYPE_CHECKING, Any
from dataclasses import dataclass, field, asdict

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.stream_registry")


@dataclass
class StreamDefinition:
    """
    Definition of a data stream that can be predicted.

    A stream represents a source of time-series data (e.g., BTC price)
    that predictors can make predictions on.

    In decentralized mode, streams are registered in the DHT
    and predictors can claim slots to make predictions.
    """
    stream_id: str              # Unique identifier (hash of source+stream+target)
    source: str                 # Data source (e.g., "exchange/binance")
    stream: str                 # Stream name (e.g., "BTCUSDT")
    target: str                 # Target field (e.g., "close_price")
    datatype: str               # Data type (e.g., "price", "volume")
    cadence: int                # Update frequency in seconds
    predictor_slots: int        # Max predictors allowed (0 = unlimited)
    creator: str                # Evrmore address of creator
    timestamp: int              # Creation timestamp
    description: str = ""       # Human-readable description
    tags: List[str] = field(default_factory=list)  # Searchable tags
    metadata: Dict[str, Any] = field(default_factory=dict)  # Additional data

    @staticmethod
    def generate_stream_id(source: str, stream: str, target: str) -> str:
        """Generate deterministic stream ID from components."""
        data = f"{source}:{stream}:{target}"
        return hashlib.sha256(data.encode()).hexdigest()[:32]

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "StreamDefinition":
        """Create from dictionary."""
        # Handle missing fields with defaults
        data.setdefault("description", "")
        data.setdefault("tags", [])
        data.setdefault("metadata", {})
        return cls(**data)


@dataclass
class StreamClaim:
    """
    A predictor's claim on a stream slot.

    Claims represent a predictor's commitment to provide predictions
    for a specific stream.
    """
    stream_id: str              # Which stream
    slot_index: int             # Which slot (0-based)
    predictor: str              # Evrmore address of predictor
    peer_id: str                # libp2p peer ID
    timestamp: int              # When claimed
    signature: str = ""         # Signed by predictor's wallet
    expires: int = 0            # Expiration timestamp (0 = no expiry)
    stake: float = 0.0          # Optional stake amount (for priority)

    def is_expired(self) -> bool:
        """Check if claim has expired."""
        if self.expires == 0:
            return False
        return time.time() > self.expires

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "StreamClaim":
        """Create from dictionary."""
        return cls(**data)

    def get_signing_message(self) -> str:
        """Get the message that should be signed."""
        return f"{self.stream_id}:{self.slot_index}:{self.predictor}:{self.timestamp}"


class StreamRegistry:
    """
    Decentralized stream registry using DHT and Rendezvous.

    Allows:
    - Stream discovery without central server
    - Predictor slot claiming
    - Stream creation (governance TBD)

    Architecture:
    - Stream definitions stored via Rendezvous namespaces
    - Claims broadcast via GossipSub
    - Local cache for fast lookups
    """

    # Namespace prefixes
    STREAM_NS_PREFIX = "satori/streams/"
    CLAIM_NS_PREFIX = "satori/claims/"

    # GossipSub topics
    STREAM_TOPIC = "satori/stream-registry"
    CLAIM_TOPIC = "satori/stream-claims"

    # Default claim TTL (24 hours)
    DEFAULT_CLAIM_TTL = 86400

    def __init__(self, peers: "Peers"):
        """
        Initialize StreamRegistry.

        Args:
            peers: Peers instance for P2P operations
        """
        self.peers = peers
        self._streams: Dict[str, StreamDefinition] = {}  # stream_id -> definition
        self._claims: Dict[str, Dict[int, StreamClaim]] = {}  # stream_id -> {slot: claim}
        self._my_claims: Dict[str, StreamClaim] = {}  # stream_id -> my claim
        self._stream_callbacks: List[Callable] = []
        self._claim_callbacks: List[Callable] = []
        self._started = False

    @property
    def evrmore_address(self) -> str:
        """Get our Evrmore address."""
        if self.peers._identity_bridge:
            return self.peers._identity_bridge.evrmore_address
        return ""

    @property
    def peer_id(self) -> str:
        """Get our peer ID."""
        return self.peers.peer_id or ""

    async def start(self) -> bool:
        """
        Start the stream registry.

        Subscribes to stream and claim topics.

        Returns:
            True if started successfully
        """
        if self._started:
            return True

        try:
            # Subscribe to stream announcements with full network registration
            if self.peers._pubsub:
                await self.peers.subscribe_async(
                    self.STREAM_TOPIC,
                    self._on_stream_received
                )
                await self.peers.subscribe_async(
                    self.CLAIM_TOPIC,
                    self._on_claim_received
                )
                logger.debug(f"Subscribed to {self.STREAM_TOPIC} and {self.CLAIM_TOPIC}")

            self._started = True
            logger.info("StreamRegistry started")
            return True

        except Exception as e:
            logger.error(f"Failed to start StreamRegistry: {e}")
            return False

    async def stop(self) -> None:
        """Stop the stream registry."""
        if self.peers._pubsub:
            try:
                await self.peers.unsubscribe(self.STREAM_TOPIC)
                await self.peers.unsubscribe(self.CLAIM_TOPIC)
            except Exception:
                pass

        self._started = False
        logger.info("StreamRegistry stopped")

    # ========== Stream Discovery ==========

    async def discover_streams(
        self,
        source: Optional[str] = None,
        datatype: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 100
    ) -> List[StreamDefinition]:
        """
        Discover available streams.

        Args:
            source: Filter by source (e.g., "exchange/binance")
            datatype: Filter by datatype (e.g., "price")
            tags: Filter by tags
            limit: Maximum results

        Returns:
            List of matching StreamDefinitions
        """
        # Query Rendezvous for streams
        if self.peers._rendezvous:
            try:
                # Discover from base namespace
                namespace = self.STREAM_NS_PREFIX
                if source:
                    namespace += source.replace("/", "_")

                peer_ids = await self.peers._rendezvous.discover(namespace, limit=limit)
                logger.debug(f"Discovered {len(peer_ids)} peers with streams")
            except Exception as e:
                logger.debug(f"Rendezvous discovery failed: {e}")

        # Filter cached streams
        results = []
        for stream in self._streams.values():
            if source and not stream.source.startswith(source):
                continue
            if datatype and stream.datatype != datatype:
                continue
            if tags and not any(t in stream.tags for t in tags):
                continue
            results.append(stream)
            if len(results) >= limit:
                break

        return results

    async def get_stream(self, stream_id: str) -> Optional[StreamDefinition]:
        """
        Get a specific stream by ID.

        Args:
            stream_id: Stream identifier

        Returns:
            StreamDefinition if found
        """
        # Check cache
        if stream_id in self._streams:
            return self._streams[stream_id]

        # Query network
        if self.peers._rendezvous:
            try:
                namespace = f"{self.STREAM_NS_PREFIX}{stream_id}"
                peer_ids = await self.peers._rendezvous.discover(namespace, limit=1)
                # Would need to fetch stream data from discovered peer
            except Exception:
                pass

        return None

    async def register_stream(
        self,
        source: str,
        stream: str,
        target: str,
        datatype: str = "numeric",
        cadence: int = 60,
        predictor_slots: int = 10,
        description: str = "",
        tags: Optional[List[str]] = None
    ) -> Optional[StreamDefinition]:
        """
        Register a new stream definition.

        Args:
            source: Data source (e.g., "exchange/binance")
            stream: Stream name (e.g., "BTCUSDT")
            target: Target field (e.g., "close")
            datatype: Data type (e.g., "price")
            cadence: Update frequency in seconds
            predictor_slots: Max predictors (0 = unlimited)
            description: Human-readable description
            tags: Searchable tags

        Returns:
            StreamDefinition if registered successfully
        """
        if not self.evrmore_address:
            logger.warning("Cannot register stream: no evrmore address")
            return None

        stream_id = StreamDefinition.generate_stream_id(source, stream, target)

        definition = StreamDefinition(
            stream_id=stream_id,
            source=source,
            stream=stream,
            target=target,
            datatype=datatype,
            cadence=cadence,
            predictor_slots=predictor_slots,
            creator=self.evrmore_address,
            timestamp=int(time.time()),
            description=description,
            tags=tags or [],
        )

        # Store locally
        self._streams[stream_id] = definition

        # Register via Rendezvous
        await self._register_stream_rendezvous(definition)

        # Broadcast via GossipSub
        await self._broadcast_stream(definition)

        logger.info(f"Registered stream: {source}/{stream}/{target}")
        return definition

    async def _register_stream_rendezvous(self, definition: StreamDefinition) -> bool:
        """Register stream via Rendezvous protocol."""
        if not self.peers._rendezvous:
            return False

        try:
            namespace = f"{self.STREAM_NS_PREFIX}{definition.stream_id}"
            return await self.peers._rendezvous.register(namespace, ttl=3600)
        except Exception as e:
            logger.debug(f"Stream Rendezvous registration failed: {e}")
            return False

    async def _broadcast_stream(self, definition: StreamDefinition) -> bool:
        """Broadcast stream definition via GossipSub."""
        try:
            await self.peers.broadcast(
                self.STREAM_TOPIC,
                {"type": "stream", "data": definition.to_dict()}
            )
            return True
        except Exception as e:
            logger.debug(f"Stream broadcast failed: {e}")
            return False

    async def _on_stream_received(self, stream_id: str, data: dict) -> None:
        """Handle received stream definition."""
        try:
            if data.get("type") != "stream":
                return

            definition = StreamDefinition.from_dict(data.get("data", {}))
            self._streams[definition.stream_id] = definition

            logger.debug(f"Received stream: {definition.source}/{definition.stream}")

            # Notify callbacks
            for callback in self._stream_callbacks:
                try:
                    callback(definition)
                except Exception:
                    pass

        except Exception as e:
            logger.debug(f"Failed to process stream: {e}")

    # ========== Stream Claiming ==========

    async def claim_stream(
        self,
        stream_id: str,
        slot_index: Optional[int] = None,
        ttl: int = None
    ) -> Optional[StreamClaim]:
        """
        Claim a predictor slot on a stream.

        Args:
            stream_id: Stream to claim
            slot_index: Specific slot (None = first available)
            ttl: Claim TTL in seconds

        Returns:
            StreamClaim if successful
        """
        if not self.peer_id or not self.evrmore_address:
            logger.warning("Cannot claim stream: missing identity")
            return None

        # Check if stream exists
        stream = await self.get_stream(stream_id)
        if not stream:
            # Stream might not be in cache yet, continue anyway
            pass

        # Find available slot
        if slot_index is None:
            slot_index = self._find_available_slot(stream_id)

        ttl = ttl or self.DEFAULT_CLAIM_TTL

        claim = StreamClaim(
            stream_id=stream_id,
            slot_index=slot_index,
            predictor=self.evrmore_address,
            peer_id=self.peer_id,
            timestamp=int(time.time()),
            expires=int(time.time()) + ttl if ttl > 0 else 0,
        )

        # Sign the claim
        try:
            message = claim.get_signing_message()
            if self.peers._identity_bridge:
                signature = self.peers._identity_bridge.sign(message.encode())
                claim.signature = signature if isinstance(signature, str) else signature.decode()
        except Exception as e:
            logger.warning(f"Failed to sign claim: {e}")
            claim.signature = "unsigned"

        # Store locally
        self._my_claims[stream_id] = claim
        if stream_id not in self._claims:
            self._claims[stream_id] = {}
        self._claims[stream_id][slot_index] = claim

        # Broadcast claim
        await self._broadcast_claim(claim)

        logger.info(f"Claimed stream {stream_id} slot {slot_index}")
        return claim

    def _find_available_slot(self, stream_id: str) -> int:
        """Find first available slot for a stream."""
        claims = self._claims.get(stream_id, {})

        # Find first unclaimed slot
        slot = 0
        while slot in claims:
            claim = claims[slot]
            if claim.is_expired():
                break  # Expired claim, can reuse slot
            slot += 1

        return slot

    async def release_claim(self, stream_id: str) -> bool:
        """
        Release our claim on a stream.

        Args:
            stream_id: Stream to release

        Returns:
            True if released successfully
        """
        if stream_id not in self._my_claims:
            return False

        claim = self._my_claims.pop(stream_id)

        # Remove from claims dict
        if stream_id in self._claims:
            slot = claim.slot_index
            if slot in self._claims[stream_id]:
                del self._claims[stream_id][slot]

        # Broadcast release
        await self.peers.broadcast(
            self.CLAIM_TOPIC,
            {"type": "release", "stream_id": stream_id, "slot": claim.slot_index}
        )

        logger.info(f"Released claim on {stream_id}")
        return True

    async def _broadcast_claim(self, claim: StreamClaim) -> bool:
        """Broadcast stream claim via GossipSub."""
        try:
            await self.peers.broadcast(
                self.CLAIM_TOPIC,
                {"type": "claim", "data": claim.to_dict()}
            )
            return True
        except Exception as e:
            logger.debug(f"Claim broadcast failed: {e}")
            return False

    async def _on_claim_received(self, stream_id: str, data: dict) -> None:
        """Handle received stream claim."""
        try:
            msg_type = data.get("type")

            if msg_type == "claim":
                claim = StreamClaim.from_dict(data.get("data", {}))

                # Don't process our own claims
                if claim.predictor == self.evrmore_address:
                    return

                # Verify signature
                if not await self._verify_claim(claim):
                    logger.debug(f"Invalid claim signature from {claim.predictor}")
                    return

                # Store claim
                stream_id = claim.stream_id
                if stream_id not in self._claims:
                    self._claims[stream_id] = {}
                self._claims[stream_id][claim.slot_index] = claim

                logger.debug(
                    f"Received claim for {stream_id} "
                    f"slot {claim.slot_index} from {claim.predictor}"
                )

                # Notify callbacks
                for callback in self._claim_callbacks:
                    try:
                        callback(claim)
                    except Exception:
                        pass

            elif msg_type == "release":
                stream_id = data.get("stream_id")
                slot = data.get("slot", 0)
                if stream_id in self._claims and slot in self._claims[stream_id]:
                    del self._claims[stream_id][slot]
                    logger.debug(f"Slot {slot} released on {stream_id}")

        except Exception as e:
            logger.debug(f"Failed to process claim: {e}")

    async def _verify_claim(self, claim: StreamClaim) -> bool:
        """Verify a claim's signature."""
        if claim.signature == "unsigned":
            return True  # Development mode

        if not claim.signature:
            return False

        try:
            message = claim.get_signing_message()
            if self.peers._identity_bridge:
                return self.peers._identity_bridge.verify(
                    message.encode(),
                    claim.signature.encode() if isinstance(claim.signature, str) else claim.signature,
                    claim.predictor
                )
        except Exception:
            pass

        return False

    # ========== Query Methods ==========

    async def get_my_streams(self) -> List[StreamClaim]:
        """Get list of streams we've claimed."""
        return list(self._my_claims.values())

    async def get_stream_claims(self, stream_id: str) -> List[StreamClaim]:
        """Get all claims for a stream."""
        claims = self._claims.get(stream_id, {})
        return [c for c in claims.values() if not c.is_expired()]

    async def get_predictors_for_stream(self, stream_id: str) -> List[str]:
        """Get list of predictor addresses for a stream."""
        claims = await self.get_stream_claims(stream_id)
        return [c.predictor for c in claims]

    def get_cached_streams(self) -> List[StreamDefinition]:
        """Get all cached stream definitions."""
        return list(self._streams.values())

    # ========== Callbacks ==========

    def on_stream_registered(self, callback: Callable[[StreamDefinition], None]) -> None:
        """Register callback for new stream registrations."""
        self._stream_callbacks.append(callback)

    def on_stream_claimed(self, callback: Callable[[StreamClaim], None]) -> None:
        """Register callback for new stream claims."""
        self._claim_callbacks.append(callback)

    # ========== Statistics ==========

    def get_stats(self) -> dict:
        """Get registry statistics."""
        return {
            "known_streams": len(self._streams),
            "my_claims": len(self._my_claims),
            "total_claims": sum(len(c) for c in self._claims.values()),
            "started": self._started,
        }
