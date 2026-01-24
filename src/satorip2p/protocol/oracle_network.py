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

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None

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
    peer_id: str = ""           # libp2p peer ID of oracle
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
        data.setdefault("peer_id", "")  # Backward compatibility
        return cls(**data)

    def get_signing_message(self) -> str:
        """Get the message that should be signed."""
        return f"{self.stream_id}:{self.value}:{self.timestamp}:{self.hash}"


@dataclass
class OracleDataSource:
    """
    Configuration for how a primary oracle fetches data.

    Primary oracles need to know how to retrieve data from external sources.
    This can be a template (predefined source) or custom configuration.
    """
    # Template-based (use predefined source)
    template: str = ""          # Template name (e.g., "binance", "coingecko", "custom")

    # Custom API configuration
    api_url: str = ""           # Full URL or URL template with {symbol} placeholder
    api_method: str = "GET"     # HTTP method
    api_headers: Dict[str, str] = field(default_factory=dict)  # Headers (e.g., API key)
    api_body: str = ""          # Request body for POST requests

    # Response parsing
    json_path: str = ""         # JSON path to extract value (e.g., "data.price" or "0.close")
    value_type: str = "float"   # Expected type: "float", "int", "string"

    # Schedule (optional override of stream cadence)
    fetch_interval: int = 0     # Override fetch interval (0 = use stream cadence)

    # Template-specific parameters
    symbol: str = ""            # Trading pair symbol (e.g., "BTCUSDT")
    base_asset: str = ""        # Base asset (e.g., "BTC")
    quote_asset: str = ""       # Quote asset (e.g., "USD")

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "OracleDataSource":
        """Create from dictionary."""
        # Handle missing fields with defaults
        data.setdefault("template", "")
        data.setdefault("api_url", "")
        data.setdefault("api_method", "GET")
        data.setdefault("api_headers", {})
        data.setdefault("api_body", "")
        data.setdefault("json_path", "")
        data.setdefault("value_type", "float")
        data.setdefault("fetch_interval", 0)
        data.setdefault("symbol", "")
        data.setdefault("base_asset", "")
        data.setdefault("quote_asset", "")
        return cls(**data)

    def is_template(self) -> bool:
        """Check if using a predefined template."""
        return bool(self.template and self.template != "custom")


# Predefined data source templates
ORACLE_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "binance": {
        "name": "Binance",
        "description": "Binance exchange spot prices",
        "api_url": "https://api.binance.com/api/v3/ticker/price?symbol={symbol}",
        "json_path": "price",
        "value_type": "float",
        "requires": ["symbol"],  # e.g., "BTCUSDT"
        "example_symbol": "BTCUSDT",
    },
    "binance_kline": {
        "name": "Binance Klines",
        "description": "Binance OHLCV candlestick data",
        "api_url": "https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit=1",
        "json_path": "0.4",  # Close price is index 4 in kline array
        "value_type": "float",
        "requires": ["symbol"],
        "example_symbol": "BTCUSDT",
    },
    "coingecko": {
        "name": "CoinGecko",
        "description": "CoinGecko cryptocurrency prices (free, no API key)",
        "api_url": "https://api.coingecko.com/api/v3/simple/price?ids={base_asset}&vs_currencies={quote_asset}",
        "json_path": "{base_asset}.{quote_asset}",
        "value_type": "float",
        "requires": ["base_asset", "quote_asset"],  # e.g., "bitcoin", "usd"
        "example_base": "bitcoin",
        "example_quote": "usd",
    },
    "coinmarketcap": {
        "name": "CoinMarketCap",
        "description": "CoinMarketCap prices (requires API key)",
        "api_url": "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol={symbol}",
        "api_headers": {"X-CMC_PRO_API_KEY": "{api_key}"},
        "json_path": "data.{symbol}.quote.USD.price",
        "value_type": "float",
        "requires": ["symbol", "api_key"],
        "example_symbol": "BTC",
    },
    "kraken": {
        "name": "Kraken",
        "description": "Kraken exchange prices",
        "api_url": "https://api.kraken.com/0/public/Ticker?pair={symbol}",
        "json_path": "result.{symbol}.c.0",  # Last trade close price
        "value_type": "float",
        "requires": ["symbol"],
        "example_symbol": "XBTUSD",
    },
    "coinbase": {
        "name": "Coinbase",
        "description": "Coinbase exchange prices",
        "api_url": "https://api.coinbase.com/v2/prices/{symbol}/spot",
        "json_path": "data.amount",
        "value_type": "float",
        "requires": ["symbol"],  # e.g., "BTC-USD"
        "example_symbol": "BTC-USD",
    },
    "custom": {
        "name": "Custom API",
        "description": "Configure your own API endpoint",
        "api_url": "",
        "json_path": "",
        "value_type": "float",
        "requires": ["api_url", "json_path"],
    },
}


def get_template(template_name: str) -> Optional[Dict[str, Any]]:
    """Get a data source template by name."""
    return ORACLE_TEMPLATES.get(template_name.lower())


def list_templates() -> List[Dict[str, Any]]:
    """List all available data source templates."""
    return [
        {"id": k, **v}
        for k, v in ORACLE_TEMPLATES.items()
    ]


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
    data_source: Optional[OracleDataSource] = None  # Data source config (primary only)

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        # Handle nested dataclass
        if self.data_source:
            result['data_source'] = self.data_source.to_dict()
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "OracleRegistration":
        """Create from dictionary."""
        # Handle nested data_source
        if 'data_source' in data and data['data_source'] is not None:
            if isinstance(data['data_source'], dict):
                data['data_source'] = OracleDataSource.from_dict(data['data_source'])
        else:
            data['data_source'] = None
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
        self._my_published_observations: List[Observation] = []  # Our published observations
        self._started = False

        # External callback for bridge integration (set by p2p_bridge)
        self.on_observation_received: Optional[Callable[[Observation], None]] = None

        # Reference to stream registry for activity tracking (set by neuron startup)
        self._stream_registry: Optional["StreamRegistry"] = None

        # Data fetching for primary oracles
        self._fetch_cancel_scopes: Dict[str, trio.CancelScope] = {}  # stream_id -> cancel scope
        self._http_client: Optional["httpx.AsyncClient"] = None

    def set_stream_registry(self, registry: "StreamRegistry") -> None:
        """Set stream registry reference for activity tracking."""
        self._stream_registry = registry

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
        is_primary: bool = False,
        data_source: Optional[OracleDataSource] = None
    ) -> Optional[OracleRegistration]:
        """
        Register as an oracle for a stream.

        Args:
            stream_id: Stream to register for
            is_primary: Whether we're the primary oracle
            data_source: Data source configuration (required for primary oracles)

        Returns:
            OracleRegistration if successful
        """
        if not self.peer_id or not self.evrmore_address:
            logger.warning("Cannot register as oracle: missing identity")
            return None

        # Primary oracles should have data source config
        if is_primary and not data_source:
            logger.warning(f"Primary oracle registration without data source for {stream_id}")
            # Allow it but log warning - data source can be added later

        registration = OracleRegistration(
            stream_id=stream_id,
            oracle=self.evrmore_address,
            peer_id=self.peer_id,
            timestamp=int(time.time()),
            is_primary=is_primary,
            data_source=data_source,
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

        # Start data fetching loop for primary oracles
        if is_primary and data_source and data_source.api_url:
            await self.start_primary_oracle_fetching(stream_id, data_source)

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

    async def _on_oracle_registration(self, stream_id: str, data: dict) -> None:
        """Handle received oracle registration."""
        try:
            logger.info(f"_on_oracle_registration called: stream_id={stream_id}, data_type={data.get('type')}")

            if data.get("type") != "register":
                logger.debug(f"Ignoring non-register message: type={data.get('type')}")
                return

            registration = OracleRegistration.from_dict(data.get("data", {}))

            # Don't process our own registrations
            if registration.oracle == self.evrmore_address:
                logger.debug(f"Ignoring own registration for stream_id={registration.stream_id}")
                return

            # Store registration
            reg_stream_id = registration.stream_id
            if reg_stream_id not in self._oracle_registrations:
                self._oracle_registrations[reg_stream_id] = {}
            self._oracle_registrations[reg_stream_id][registration.oracle] = registration

            logger.info(
                f"Stored oracle registration: stream_id={reg_stream_id} "
                f"oracle={registration.oracle} (total known: {sum(len(v) for v in self._oracle_registrations.values())})"
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
                    lambda sid, data, s=stream_id: self._on_observation_received(s, data)
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
            logger.info(f"_on_observation_received: stream_id={stream_id}, data_keys={list(data.keys()) if isinstance(data, dict) else type(data)}")
            observation = Observation.from_dict(data)

            # Verify hash
            expected_hash = observation.compute_hash()
            if observation.hash != expected_hash:
                logger.warning(f"Invalid observation hash from oracle={observation.oracle}: got {observation.hash}, expected {expected_hash}")
                return

            # Verify signature
            if not await self._verify_observation(observation):
                logger.warning(f"Invalid observation signature from oracle={observation.oracle}")
                return

            # Cache observation
            if stream_id not in self._observation_cache:
                self._observation_cache[stream_id] = []
            self._observation_cache[stream_id].append(observation)
            logger.info(f"Cached observation: stream_id={stream_id}, value={observation.value}, oracle={observation.oracle} (cache size: {len(self._observation_cache[stream_id])})")

            # Trim cache
            if len(self._observation_cache[stream_id]) > self.MAX_CACHE_SIZE:
                self._observation_cache[stream_id] = self._observation_cache[stream_id][-self.MAX_CACHE_SIZE:]

            # Update stream activity in registry (for filtering active oracles)
            if self._stream_registry:
                try:
                    self._stream_registry.update_stream_activity(stream_id)
                except Exception as e:
                    logger.debug(f"Failed to update stream activity: {e}")

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
            logger.debug(f"Accepting unsigned observation from oracle={observation.oracle}")
            return True  # Development mode - unsigned observations allowed

        if not observation.signature:
            logger.debug(f"No signature on observation from oracle={observation.oracle}")
            return False

        try:
            message = observation.get_signing_message()
            if self.peers._identity_bridge:
                # Pass the oracle's address as 'address' parameter (not 'public_key')
                # The signature contains enough info to recover pubkey and verify against address
                result = self.peers._identity_bridge.verify(
                    message.encode(),
                    observation.signature.encode() if isinstance(observation.signature, str) else observation.signature,
                    public_key=None,  # Let verification recover pubkey from signature
                    address=observation.oracle  # Verify against oracle's Evrmore address
                )
                if not result:
                    logger.warning(f"Signature verification failed for oracle={observation.oracle}, sig_len={len(observation.signature)}")
                else:
                    logger.debug(f"Signature verified for oracle={observation.oracle}")
                return result
        except Exception as e:
            logger.warning(f"Signature verification exception for oracle={observation.oracle}: {e}")
            return False

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
            logger.info(f"Auto-registering as primary oracle for stream_id={stream_id}")
            # Auto-register as primary since we're the one publishing
            await self.register_as_oracle(stream_id, is_primary=True)

        timestamp = timestamp or int(time.time())

        observation = Observation(
            stream_id=stream_id,
            value=value,
            timestamp=timestamp,
            oracle=self.evrmore_address,
            peer_id=self.peer_id,
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

            # Store in our published observations cache
            self._my_published_observations.append(observation)
            # Keep only recent observations (max 100)
            if len(self._my_published_observations) > 100:
                self._my_published_observations = self._my_published_observations[-100:]

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

    def get_recent_observations(self, limit: int = 20, include_own: bool = True) -> List[Observation]:
        """Get recent observations across all streams, sorted by timestamp.

        Args:
            limit: Maximum number of observations to return
            include_own: If True, include our own published observations (default: True)

        Returns:
            List of recent observations sorted by timestamp (newest first)
        """
        all_observations = []
        for stream_observations in self._observation_cache.values():
            all_observations.extend(stream_observations)

        # Include our own published observations if requested
        if include_own and self._my_published_observations:
            all_observations.extend(self._my_published_observations)

        # Sort by timestamp descending and limit
        all_observations.sort(key=lambda o: o.timestamp, reverse=True)
        return all_observations[:limit]

    def get_my_published_observations(self) -> List[Observation]:
        """Get our own published observations, sorted by timestamp (newest first)."""
        return list(reversed(self._my_published_observations))

    def get_registered_oracles(self, stream_id: str) -> List[OracleRegistration]:
        """Get registered oracles for a stream."""
        return list(self._oracle_registrations.get(stream_id, {}).values())

    def is_registered_oracle(self, stream_id: str) -> bool:
        """Check if we're registered as an oracle for a stream."""
        return stream_id in self._my_registrations

    # ========== Primary/Secondary Oracle Methods ==========

    def get_primary_oracle(self, stream_id: str) -> Optional[OracleRegistration]:
        """
        Get the primary oracle for a stream.

        Returns:
            The primary OracleRegistration, or None if no primary is registered
        """
        registrations = self._oracle_registrations.get(stream_id, {})
        for reg in registrations.values():
            if reg.is_primary:
                return reg
        return None

    def get_secondary_oracles(self, stream_id: str) -> List[OracleRegistration]:
        """
        Get all secondary oracles for a stream.

        Returns:
            List of secondary OracleRegistrations
        """
        registrations = self._oracle_registrations.get(stream_id, {})
        return [reg for reg in registrations.values() if not reg.is_primary]

    def is_primary_oracle(self, stream_id: str) -> bool:
        """Check if we're the primary oracle for a stream."""
        reg = self._my_registrations.get(stream_id)
        return reg is not None and reg.is_primary

    def is_secondary_oracle(self, stream_id: str) -> bool:
        """Check if we're a secondary oracle for a stream."""
        reg = self._my_registrations.get(stream_id)
        return reg is not None and not reg.is_primary

    async def register_as_secondary_oracle(self, stream_id: str) -> Optional[OracleRegistration]:
        """
        Register as a secondary oracle for a stream.

        Secondary oracles relay data from primary oracles, providing
        redundancy and helping with network distribution.

        Args:
            stream_id: Stream to register for

        Returns:
            OracleRegistration if successful
        """
        return await self.register_as_oracle(stream_id, is_primary=False)

    async def relay_observation(
        self,
        observation: Observation,
        verify: bool = True
    ) -> bool:
        """
        Relay an observation from another oracle (Secondary oracle function).

        Secondary oracles receive observations from primary oracles and
        re-broadcast them to help with network distribution.

        Args:
            observation: The observation to relay
            verify: Whether to verify the observation signature first

        Returns:
            True if relayed successfully
        """
        stream_id = observation.stream_id

        # Check if we're registered as secondary for this stream
        if not self.is_secondary_oracle(stream_id):
            logger.debug(f"Cannot relay: not a secondary oracle for {stream_id}")
            return False

        # Optionally verify the original signature
        if verify:
            if not await self._verify_observation(observation):
                logger.warning(f"Cannot relay: invalid observation signature")
                return False

        # Re-broadcast the original observation (keeping original oracle's signature)
        topic = f"{self.STREAM_TOPIC_PREFIX}{stream_id}"
        try:
            await self.peers.broadcast(topic, observation.to_dict())
            logger.debug(f"Relayed observation for stream_id={stream_id} from oracle={observation.oracle}")
            return True
        except Exception as e:
            logger.warning(f"Failed to relay observation: {e}")
            return False

    def get_oracle_role(self, stream_id: str) -> str:
        """
        Get our role for a stream.

        Returns:
            'primary', 'secondary', or 'none'
        """
        if self.is_primary_oracle(stream_id):
            return 'primary'
        elif self.is_secondary_oracle(stream_id):
            return 'secondary'
        return 'none'

    def get_my_oracle_summary(self) -> dict:
        """
        Get summary of our oracle registrations.

        Returns:
            Dict with counts and details of primary/secondary registrations
        """
        primary_streams = []
        secondary_streams = []

        for stream_id, reg in self._my_registrations.items():
            if reg.is_primary:
                primary_streams.append(stream_id)
            else:
                secondary_streams.append(stream_id)

        return {
            'primary_count': len(primary_streams),
            'secondary_count': len(secondary_streams),
            'total_count': len(self._my_registrations),
            'primary_streams': primary_streams,
            'secondary_streams': secondary_streams,
        }

    # ========== Statistics ==========

    def get_stats(self) -> dict:
        """Get oracle network statistics."""
        # Count unique oracles (not registrations) by collecting unique oracle addresses
        unique_oracles = set()
        for stream_regs in self._oracle_registrations.values():
            unique_oracles.update(stream_regs.keys())  # keys are oracle addresses

        return {
            "subscribed_streams": len(self._subscribed_streams),
            "my_oracle_registrations": len(self._my_registrations),
            "known_oracles": len(unique_oracles),
            "cached_observations": sum(len(o) for o in self._observation_cache.values()),
            "my_published_observations": len(self._my_published_observations),
            "started": self._started,
        }

    # ========== Primary Oracle Data Fetching ==========

    def _extract_json_value(self, data: Any, json_path: str) -> Any:
        """
        Extract a value from JSON data using a dot-notation path.

        Supports:
        - Dot notation: "data.price" or "result.XBTUSD.c.0"
        - Array indices: "0.4" (first element, then index 4)
        - Nested paths: "satori-network.usd"

        Args:
            data: The JSON data (dict or list)
            json_path: Dot-separated path to the value

        Returns:
            The extracted value, or None if not found
        """
        if not json_path:
            return data

        parts = json_path.split('.')
        current = data

        for part in parts:
            if current is None:
                return None

            # Try as array index first
            try:
                idx = int(part)
                if isinstance(current, (list, tuple)) and 0 <= idx < len(current):
                    current = current[idx]
                    continue
            except ValueError:
                pass

            # Try as dict key
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

        return current

    async def _fetch_data_from_api(
        self,
        data_source: OracleDataSource
    ) -> Optional[Any]:
        """
        Fetch data from an API endpoint.

        Args:
            data_source: Data source configuration

        Returns:
            The extracted value, or None on error
        """
        if not HTTPX_AVAILABLE:
            logger.error("httpx not available - cannot fetch data from API")
            return None

        if not data_source.api_url:
            logger.warning("No API URL configured")
            return None

        # Initialize HTTP client if needed
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)

        try:
            # Prepare request
            method = data_source.api_method.upper() if data_source.api_method else "GET"
            headers = data_source.api_headers or {}

            # Make request
            if method == "GET":
                response = await self._http_client.get(
                    data_source.api_url,
                    headers=headers
                )
            elif method == "POST":
                body = data_source.api_body or ""
                response = await self._http_client.post(
                    data_source.api_url,
                    headers=headers,
                    content=body
                )
            else:
                logger.warning(f"Unsupported HTTP method: {method}")
                return None

            response.raise_for_status()
            json_data = response.json()

            # Extract value using json_path
            value = self._extract_json_value(json_data, data_source.json_path)

            if value is None:
                logger.warning(f"Could not extract value from path: {data_source.json_path}")
                return None

            # Convert to appropriate type
            value_type = data_source.value_type or "float"
            if value_type == "float":
                return float(value)
            elif value_type == "int":
                return int(float(value))
            elif value_type == "string":
                return str(value)
            else:
                return float(value)

        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP error fetching data: {e.response.status_code}")
            return None
        except httpx.RequestError as e:
            logger.warning(f"Request error fetching data: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error: {e}")
            return None
        except (ValueError, TypeError) as e:
            logger.warning(f"Value conversion error: {e}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error fetching data: {e}")
            return None

    async def _primary_oracle_fetch_loop(
        self,
        stream_id: str,
        data_source: OracleDataSource,
        interval: int = 60
    ) -> None:
        """
        Fetch data periodically and publish observations.

        This runs in the background for each primary oracle registration.

        Args:
            stream_id: Stream to publish to
            data_source: Data source configuration
            interval: Fetch interval in seconds
        """
        logger.info(f"Starting primary oracle fetch loop for {stream_id} (interval={interval}s)")

        while True:
            try:
                # Fetch data from API
                value = await self._fetch_data_from_api(data_source)

                if value is not None:
                    # Publish observation
                    timestamp = int(time.time())
                    await self.publish_observation(stream_id, value, timestamp)
                    logger.info(f"Published observation for {stream_id}: {value}")
                else:
                    logger.warning(f"Failed to fetch data for {stream_id}")

                # Wait for next fetch
                await trio.sleep(interval)

            except trio.Cancelled:
                logger.info(f"Fetch loop cancelled for {stream_id}")
                raise
            except Exception as e:
                logger.error(f"Error in fetch loop for {stream_id}: {e}")
                # Wait before retry on error
                await trio.sleep(min(interval, 30))

    async def start_primary_oracle_fetching(
        self,
        stream_id: str,
        data_source: OracleDataSource
    ) -> bool:
        """
        Start the data fetching loop for a primary oracle.

        Args:
            stream_id: Stream to fetch for
            data_source: Data source configuration

        Returns:
            True if started successfully
        """
        if not HTTPX_AVAILABLE:
            logger.error("httpx not available - cannot start data fetching")
            return False

        if stream_id in self._fetch_cancel_scopes:
            logger.warning(f"Fetch loop already running for {stream_id}")
            return False

        # Determine fetch interval
        interval = data_source.fetch_interval if data_source.fetch_interval > 0 else 60

        # Start the fetch loop in background
        async def run_fetch_loop():
            with trio.CancelScope() as scope:
                self._fetch_cancel_scopes[stream_id] = scope
                try:
                    await self._primary_oracle_fetch_loop(stream_id, data_source, interval)
                finally:
                    self._fetch_cancel_scopes.pop(stream_id, None)

        # Schedule the fetch loop to run
        if hasattr(self.peers, '_nursery') and self.peers._nursery:
            self.peers._nursery.start_soon(run_fetch_loop)
            logger.info(f"Started fetch loop for {stream_id}")
            return True
        else:
            logger.warning(f"No nursery available to start fetch loop for {stream_id}")
            return False

    async def stop_primary_oracle_fetching(self, stream_id: str) -> bool:
        """
        Stop the data fetching loop for a primary oracle.

        Args:
            stream_id: Stream to stop fetching for

        Returns:
            True if stopped successfully
        """
        scope = self._fetch_cancel_scopes.get(stream_id)
        if scope:
            scope.cancel()
            logger.info(f"Stopped fetch loop for {stream_id}")
            return True
        return False

    async def stop_all_fetching(self) -> None:
        """Stop all primary oracle fetch loops."""
        for stream_id in list(self._fetch_cancel_scopes.keys()):
            await self.stop_primary_oracle_fetching(stream_id)

        # Close HTTP client
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
