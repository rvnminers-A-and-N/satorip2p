"""
satorip2p/config.py

Configuration constants and data classes for satorip2p.
"""

from dataclasses import dataclass, field
from typing import Set, List, Optional
import time


# Default listening port (same as existing DataServer)
DEFAULT_PORT = 24600

# Bootstrap peers (will be replaced with actual Satori bootstrap nodes)
# Format: /dns4/{hostname}/tcp/{port}/p2p/{peer_id}
BOOTSTRAP_PEERS: List[str] = [
    # Satori bootstrap nodes (to be deployed)
    # "/dns4/bootstrap1.satorinet.io/tcp/4001/p2p/...",
    # "/dns4/bootstrap2.satorinet.io/tcp/4001/p2p/...",

    # IPFS bootstrap nodes (for initial testing/connectivity)
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
]

# GossipSub topic prefix for Satori streams
STREAM_TOPIC_PREFIX = "satori/stream/"

# Protocol IDs
SATORI_PROTOCOL_ID = "/satori/1.0.0"
SATORI_STORE_PROTOCOL_ID = "/satori/store/1.0.0"

# GossipSub parameters (tuned for Satori's use case)
GOSSIP_PARAMS = {
    "heartbeat_interval": 1.0,      # seconds
    "gossip_factor": 0.25,          # fraction of peers to gossip to
    "D": 6,                         # target mesh degree
    "D_low": 4,                     # low watermark for mesh
    "D_high": 12,                   # high watermark for mesh
    "D_lazy": 6,                    # lazy propagation degree
    "fanout_ttl": 60,               # seconds before pruning fanout
    "mcache_len": 5,                # history windows to keep
    "mcache_gossip": 3,             # windows to use for gossip
}

# DHT parameters
DHT_PARAMS = {
    "bucket_size": 20,              # Kademlia k parameter
    "alpha": 3,                     # Parallel queries
    "query_timeout": 10.0,          # seconds
    "record_ttl": 24 * 60 * 60,     # 24 hours
}

# Message store settings
MESSAGE_STORE_PARAMS = {
    "max_age_hours": 24,            # Drop messages older than this
    "max_queue_size": 1000,         # Per-peer queue limit
    "delivery_retry_seconds": 30,   # Retry interval
    "replication_factor": 3,        # Number of peers to replicate to
}

# AutoNAT settings
AUTONAT_PARAMS = {
    "min_confidence": 3,            # Confirmations before confident
    "refresh_interval": 15 * 60,    # 15 minutes
}

# UPnP settings
UPNP_PARAMS = {
    "discovery_timeout": 5,         # seconds
    "lease_duration": 0,            # 0 = permanent until removed
}


@dataclass
class PeerInfo:
    """Information about a peer in the network."""
    peer_id: str
    evrmore_address: str = ""
    public_key: str = ""
    addresses: List[str] = field(default_factory=list)
    nat_type: str = "UNKNOWN"
    is_relay: bool = False
    last_seen: float = field(default_factory=time.time)
    subscriptions: Set[str] = field(default_factory=set)
    publications: Set[str] = field(default_factory=set)

    def update_last_seen(self):
        """Update the last seen timestamp."""
        self.last_seen = time.time()

    def is_stale(self, max_age_seconds: float = 3600) -> bool:
        """Check if peer info is stale (not seen recently)."""
        return time.time() - self.last_seen > max_age_seconds


@dataclass
class StreamInfo:
    """Information about a data stream."""
    stream_id: str
    publisher: str = ""
    subscribers: Set[str] = field(default_factory=set)
    last_data: Optional[bytes] = None
    last_update: float = 0.0

    def update(self, data: bytes):
        """Update stream with new data."""
        self.last_data = data
        self.last_update = time.time()


@dataclass
class MessageEnvelope:
    """Envelope for messages being stored/forwarded."""
    message_id: str
    target_peer: str
    source_peer: str
    stream_id: Optional[str]
    payload: bytes
    timestamp: float = field(default_factory=time.time)
    ttl_hours: int = 24
    delivery_attempts: int = 0

    def is_expired(self) -> bool:
        """Check if message has exceeded TTL."""
        age_hours = (time.time() - self.timestamp) / 3600
        return age_hours > self.ttl_hours

    def increment_attempts(self):
        """Increment delivery attempt counter."""
        self.delivery_attempts += 1
