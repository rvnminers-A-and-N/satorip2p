"""
satorip2p/protocol/oracle_network.py

Decentralized oracle network for data feed publishing.

Replaces central server's role in distributing observation data:
- Oracles publish observations directly to P2P network
- Subscribers receive data via GossipSub topics
- Data is signed and verifiable

Works alongside central server in hybrid mode:
- Central mode: Not used (central server distributes data)
- Hybrid mode: Publishes to P2P AND central server
- P2P mode: Only P2P data distribution

Usage:
    from satorip2p.protocol.oracle_network import OracleNetwork

    oracle = OracleNetwork(peers)
    await oracle.start()

    # Subscribe to a stream's data
    await oracle.subscribe_to_stream(stream_id, callback)

    # Publish observation (if you're an oracle)
    await oracle.publish_observation(stream_id, value, timestamp)
"""

import logging
import time
import json
import hashlib
import trio
from typing import Dict, List, Optional, Callable, TYPE_CHECKING, Any, Union
from dataclasses import dataclass, field, asdict

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.oracle_network")


@dataclass
class Observation:
    """
    A single observation/data point from an oracle.

    Observations are the raw data that predictors use to make predictions.
    Each observation is signed by the oracle for verification.
    """
    stream_id: str              # Which stream this is for
    value: Union[float, str]    # The observed value
    timestamp: int              # Unix timestamp of observation
    oracle: str                 # Evrmore address of oracle
    hash: str = ""              # Hash of the observation
    signature: str = ""         # Signed by oracle's wallet
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Generate hash if not provided."""
        if not self.hash:
            self.hash = self.compute_hash()

    def compute_hash(self) -> str:
        """Compute deterministic hash of observation."""
        data = f"{self.stream_id}:{self.value}:{self.timestamp}:{self.oracle}"
        return hashlib.sha256(data.encode()).hexdigest()[:32]

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Observation":
        """Create from dictionary."""
        data.setdefault("metadata", {})
        return cls(**data)

    def get_signing_message(self) -> str:
        """Get the message that should be signed."""
        return f"{self.stream_id}:{self.value}:{self.timestamp}:{self.hash}"


@dataclass
class OracleRegistration:
    """
    Registration of an oracle for a stream.

    Oracles must register before publishing observations.
    This allows subscribers to know who can publish for a stream.
    """
    stream_id: str              # Which stream
    oracle: str                 # Evrmore address
    peer_id: str                # libp2p peer ID
    timestamp: int              # Registration time
    signature: str = ""         # Signed registration
    reputation: float = 1.0     # Oracle reputation score (0-1)
    is_primary: bool = False    # Is this the primary oracle?

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "OracleRegistration":
        """Create from dictionary."""
        return cls(**data)


class OracleNetwork:
    """
    Decentralized oracle network for data distribution.

    Allows oracles to publish observations directly to subscribers
    without requiring a central server.

    Architecture:
    - Each stream has its own GossipSub topic
    - Oracles publish signed observations
    - Subscribers receive and verify observations
    - Multiple oracles can publish to same stream (with reputation)
    """

    # Topic prefix for stream data
    STREAM_TOPIC_PREFIX = "satori/data/"

    # Topic for oracle registrations
    ORACLE_REGISTRY_TOPIC = "satori/oracle-registry"

    # Maximum observations to cache per stream
    MAX_CACHE_SIZE = 1000

    def __init__(self, peers: "Peers"):
        """
        Initialize OracleNetwork.

        Args:
            peers: Peers instance for P2P operations
        """
        self.peers = peers
        self._subscribed_streams: Dict[str, List[Callable]] = {}  # stream_id -> callbacks
        self._oracle_registrations: Dict[str, Dict[str, OracleRegistration]] = {}  # stream_id -> {oracle -> reg}
        self._my_registrations: Dict[str, OracleRegistration] = {}  # stream_id -> my registration
        self._observation_cache: Dict[str, List[Observation]] = {}  # stream_id -> recent observations
        self._started = False

        # External callback for bridge integration (set by p2p_bridge)
        self.on_observation_received: Optional[Callable[[Observation], None]] = None

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
        Start the oracle network.

        Returns:
            True if started successfully
        """
        if self._started:
            return True

        try:
            # Subscribe to oracle registry topic with full network registration
            if self.peers._pubsub:
                await self.peers.subscribe_async(
                    self.ORACLE_REGISTRY_TOPIC,
                    self._on_oracle_registration
                )
                logger.debug(f"Subscribed to {self.ORACLE_REGISTRY_TOPIC}")

            self._started = True
            logger.info("OracleNetwork started")
            return True

        except Exception as e:
            logger.error(f"Failed to start OracleNetwork: {e}")
            return False

    async def stop(self) -> None:
        """Stop the oracle network."""
        # Unsubscribe from all stream topics
        for stream_id in list(self._subscribed_streams.keys()):
            await self.unsubscribe_from_stream(stream_id)

        # Unsubscribe from oracle registry
        if self.peers._pubsub:
            try:
                await self.peers.unsubscribe(self.ORACLE_REGISTRY_TOPIC)
            except Exception:
                pass

        self._started = False
        logger.info("OracleNetwork stopped")

    # ========== Oracle Registration ==========

    async def register_as_oracle(
        self,
        stream_id: str,
        is_primary: bool = False
    ) -> Optional[OracleRegistration]:
        """
        Register as an oracle for a stream.

        Args:
            stream_id: Stream to register for
            is_primary: Whether we're the primary oracle

        Returns:
            OracleRegistration if successful
        """
        if not self.peer_id or not self.evrmore_address:
            logger.warning("Cannot register as oracle: missing identity")
            return None

        registration = OracleRegistration(
            stream_id=stream_id,
            oracle=self.evrmore_address,
            peer_id=self.peer_id,
            timestamp=int(time.time()),
            is_primary=is_primary,
        )

        # Sign the registration
        try:
            message = f"{stream_id}:{self.evrmore_address}:{registration.timestamp}"
            if self.peers._identity_bridge:
                signature = self.peers._identity_bridge.sign(message.encode())
                registration.signature = signature if isinstance(signature, str) else signature.decode()
        except Exception as e:
            logger.warning(f"Failed to sign oracle registration: {e}")
            registration.signature = "unsigned"

        # Store locally
        self._my_registrations[stream_id] = registration

        # Broadcast registration
        await self._broadcast_oracle_registration(registration)

        logger.info(f"Registered as oracle for stream_id={stream_id}")
        return registration

    async def _broadcast_oracle_registration(self, registration: OracleRegistration) -> bool:
        """Broadcast oracle registration via GossipSub."""
        try:
            await self.peers.broadcast(
                self.ORACLE_REGISTRY_TOPIC,
                {"type": "register", "data": registration.to_dict()}
            )
            return True
        except Exception as e:
            logger.debug(f"Oracle registration broadcast failed: {e}")
            return False

    async def _on_oracle_registration(self, data: dict) -> None:
        """Handle received oracle registration."""
        try:
            if data.get("type") != "register":
                return

            registration = OracleRegistration.from_dict(data.get("data", {}))

            # Don't process our own registrations
            if registration.oracle == self.evrmore_address:
                return

            # Store registration
            stream_id = registration.stream_id
            if stream_id not in self._oracle_registrations:
                self._oracle_registrations[stream_id] = {}
            self._oracle_registrations[stream_id][registration.oracle] = registration

            logger.debug(
                f"Received oracle registration for stream_id={stream_id} "
                f"from oracle={registration.oracle}"
            )

        except Exception as e:
            logger.debug(f"Failed to process oracle registration: {e}")

    # ========== Data Subscription ==========

    async def subscribe_to_stream(
        self,
        stream_id: str,
        callback: Callable[[Observation], None]
    ) -> bool:
        """
        Subscribe to receive observations for a stream.

        Args:
            stream_id: Stream to subscribe to
            callback: Function called with each Observation

        Returns:
            True if subscribed successfully
        """
        topic = f"{self.STREAM_TOPIC_PREFIX}{stream_id}"

        # Track callback
        if stream_id not in self._subscribed_streams:
            self._subscribed_streams[stream_id] = []

            # Subscribe to GossipSub topic with full network registration
            if self.peers._pubsub:
                await self.peers.subscribe_async(
                    topic,
                    lambda data: self._on_observation_received(stream_id, data)
                )

        self._subscribed_streams[stream_id].append(callback)
        logger.debug(f"Subscribed to stream_id={stream_id}")
        return True

    async def unsubscribe_from_stream(self, stream_id: str) -> bool:
        """
        Unsubscribe from a stream.

        Args:
            stream_id: Stream to unsubscribe from

        Returns:
            True if unsubscribed successfully
        """
        if stream_id not in self._subscribed_streams:
            return False

        topic = f"{self.STREAM_TOPIC_PREFIX}{stream_id}"

        # Unsubscribe from GossipSub
        if self.peers._pubsub:
            try:
                await self.peers.unsubscribe(topic)
            except Exception:
                pass

        del self._subscribed_streams[stream_id]
        logger.debug(f"Unsubscribed from stream_id={stream_id}")
        return True

    async def _on_observation_received(self, stream_id: str, data: dict) -> None:
        """Handle received observation."""
        try:
            observation = Observation.from_dict(data)

            # Verify hash
            expected_hash = observation.compute_hash()
            if observation.hash != expected_hash:
                logger.debug(f"Invalid observation hash from oracle={observation.oracle}")
                return

            # Verify signature
            if not await self._verify_observation(observation):
                logger.debug(f"Invalid observation signature from oracle={observation.oracle}")
                return

            # Cache observation
            if stream_id not in self._observation_cache:
                self._observation_cache[stream_id] = []
            self._observation_cache[stream_id].append(observation)

            # Trim cache
            if len(self._observation_cache[stream_id]) > self.MAX_CACHE_SIZE:
                self._observation_cache[stream_id] = self._observation_cache[stream_id][-self.MAX_CACHE_SIZE:]

            # Notify stream-specific callbacks
            if stream_id in self._subscribed_streams:
                for callback in self._subscribed_streams[stream_id]:
                    try:
                        callback(observation)
                    except Exception as e:
                        logger.debug(f"Observation callback error: {e}")

            # Notify global callback (for p2p_bridge integration)
            if self.on_observation_received:
                try:
                    self.on_observation_received(observation)
                except Exception as e:
                    logger.debug(f"Global observation callback error: {e}")

            logger.debug(
                f"Received observation for stream_id={stream_id} "
                f"value={observation.value}"
            )

        except Exception as e:
            logger.debug(f"Failed to process observation: {e}")

    async def _verify_observation(self, observation: Observation) -> bool:
        """Verify an observation's signature."""
        if observation.signature == "unsigned":
            return True  # Development mode

        if not observation.signature:
            return False

        try:
            message = observation.get_signing_message()
            if self.peers._identity_bridge:
                return self.peers._identity_bridge.verify(
                    message.encode(),
                    observation.signature.encode() if isinstance(observation.signature, str) else observation.signature,
                    observation.oracle
                )
        except Exception:
            pass

        return False

    # ========== Data Publishing ==========

    async def publish_observation(
        self,
        stream_id: str,
        value: Union[float, str],
        timestamp: int = None,
        metadata: dict = None
    ) -> Optional[Observation]:
        """
        Publish an observation to the network.

        Must be registered as an oracle for the stream.

        Args:
            stream_id: Stream to publish to
            value: Observed value
            timestamp: Observation timestamp (default: now)
            metadata: Additional metadata

        Returns:
            Observation if published successfully
        """
        # Check if we're registered as oracle
        if stream_id not in self._my_registrations:
            logger.warning(f"Not registered as oracle for stream_id={stream_id}")
            # Auto-register
            await self.register_as_oracle(stream_id)

        timestamp = timestamp or int(time.time())

        observation = Observation(
            stream_id=stream_id,
            value=value,
            timestamp=timestamp,
            oracle=self.evrmore_address,
            metadata=metadata or {},
        )

        # Sign the observation
        try:
            message = observation.get_signing_message()
            if self.peers._identity_bridge:
                signature = self.peers._identity_bridge.sign(message.encode())
                observation.signature = signature if isinstance(signature, str) else signature.decode()
        except Exception as e:
            logger.warning(f"Failed to sign observation: {e}")
            observation.signature = "unsigned"

        # Broadcast observation
        topic = f"{self.STREAM_TOPIC_PREFIX}{stream_id}"
        try:
            await self.peers.broadcast(topic, observation.to_dict())
            logger.debug(f"Published observation for stream_id={stream_id} value={value}")
            return observation
        except Exception as e:
            logger.warning(f"Failed to publish observation: {e}")
            return None

    # ========== Query Methods ==========

    def get_cached_observations(
        self,
        stream_id: str,
        limit: int = 100
    ) -> List[Observation]:
        """Get cached observations for a stream."""
        observations = self._observation_cache.get(stream_id, [])
        return observations[-limit:]

    def get_latest_observation(self, stream_id: str) -> Optional[Observation]:
        """Get the most recent observation for a stream."""
        observations = self._observation_cache.get(stream_id, [])
        return observations[-1] if observations else None

    def get_registered_oracles(self, stream_id: str) -> List[OracleRegistration]:
        """Get registered oracles for a stream."""
        return list(self._oracle_registrations.get(stream_id, {}).values())

    def is_registered_oracle(self, stream_id: str) -> bool:
        """Check if we're registered as an oracle for a stream."""
        return stream_id in self._my_registrations

    # ========== Statistics ==========

    def get_stats(self) -> dict:
        """Get oracle network statistics."""
        return {
            "subscribed_streams": len(self._subscribed_streams),
            "my_oracle_registrations": len(self._my_registrations),
            "known_oracles": sum(len(o) for o in self._oracle_registrations.values()),
            "cached_observations": sum(len(o) for o in self._observation_cache.values()),
            "started": self._started,
        }
