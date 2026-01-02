"""
Protocol Registry - Centralized protocol definitions and metadata.

This module provides a single source of truth for all protocol identifiers,
versions, and descriptions used in the Satori P2P network.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class ProtocolInfo:
    """Protocol metadata."""
    id: str                    # Full protocol ID (e.g., "/satori/ping/1.0.0")
    name: str                  # Short name (e.g., "ping")
    version: str               # Version string (e.g., "1.0.0")
    description: str           # Human-readable description
    category: str              # Category: "core", "satori", "libp2p"
    required: bool = False     # Is this protocol required for basic operation?


# ============================================================================
# SATORI CUSTOM PROTOCOLS
# ============================================================================

SATORI_PROTOCOLS: Dict[str, ProtocolInfo] = {
    # Core Satori protocols
    "/satori/ping/1.0.0": ProtocolInfo(
        id="/satori/ping/1.0.0",
        name="satori-ping",
        version="1.0.0",
        description="Satori Ping - Custom ping with node metadata and latency measurement",
        category="satori",
        required=True,
    ),
    "/satori/identify/1.0.0": ProtocolInfo(
        id="/satori/identify/1.0.0",
        name="satori-identify",
        version="1.0.0",
        description="Satori Identify - Extended identity with roles, titles, and capabilities",
        category="satori",
        required=True,
    ),
    "/satori/heartbeat/1.0.0": ProtocolInfo(
        id="/satori/heartbeat/1.0.0",
        name="heartbeat",
        version="1.0.0",
        description="Heartbeat Protocol - Uptime tracking and node liveness verification",
        category="satori",
        required=True,
    ),

    # Prediction & Oracle protocols
    "/satori/prediction/1.0.0": ProtocolInfo(
        id="/satori/prediction/1.0.0",
        name="prediction",
        version="1.0.0",
        description="Prediction Protocol - Submit predictions and receive consensus results",
        category="satori",
        required=True,
    ),
    "/satori/oracle/1.0.0": ProtocolInfo(
        id="/satori/oracle/1.0.0",
        name="oracle",
        version="1.0.0",
        description="Oracle Protocol - External observation submission and validation",
        category="satori",
    ),

    # Rewards & Governance protocols
    "/satori/rewards/1.0.0": ProtocolInfo(
        id="/satori/rewards/1.0.0",
        name="rewards",
        version="1.0.0",
        description="Rewards Protocol - Reward calculation and distribution coordination",
        category="satori",
    ),
    "/satori/governance/1.0.0": ProtocolInfo(
        id="/satori/governance/1.0.0",
        name="governance",
        version="1.0.0",
        description="Governance Protocol - Proposals, voting, and community decisions",
        category="satori",
    ),

    # Signer-specific protocols (curation & archiving are signer duties)
    "/satori/curator/1.0.0": ProtocolInfo(
        id="/satori/curator/1.0.0",
        name="curator",
        version="1.0.0",
        description="Curator Protocol - Data quality validation and oracle curation (signers)",
        category="satori",
    ),
    "/satori/archiver/1.0.0": ProtocolInfo(
        id="/satori/archiver/1.0.0",
        name="archiver",
        version="1.0.0",
        description="Archiver Protocol - Historical data storage and retrieval (signers)",
        category="satori",
    ),
    "/satori/signer/1.0.0": ProtocolInfo(
        id="/satori/signer/1.0.0",
        name="signer",
        version="1.0.0",
        description="Signer Protocol - Multi-sig coordination and consensus signing",
        category="satori",
    ),

    # Pool & Delegation protocols
    "/satori/pool/1.0.0": ProtocolInfo(
        id="/satori/pool/1.0.0",
        name="pool",
        version="1.0.0",
        description="Pool Protocol - Staking pool operations and reward distribution",
        category="satori",
    ),
    "/satori/delegation/1.0.0": ProtocolInfo(
        id="/satori/delegation/1.0.0",
        name="delegation",
        version="1.0.0",
        description="Delegation Protocol - Stake delegation to pool operators",
        category="satori",
    ),

    # Donation & Treasury protocols
    "/satori/donation/1.0.0": ProtocolInfo(
        id="/satori/donation/1.0.0",
        name="donation",
        version="1.0.0",
        description="Donation Protocol - Treasury donations and donor tier tracking",
        category="satori",
    ),
    "/satori/treasury/1.0.0": ProtocolInfo(
        id="/satori/treasury/1.0.0",
        name="treasury",
        version="1.0.0",
        description="Treasury Protocol - Treasury management and alerts",
        category="satori",
    ),

    # Referral protocol
    "/satori/referral/1.0.0": ProtocolInfo(
        id="/satori/referral/1.0.0",
        name="referral",
        version="1.0.0",
        description="Referral Protocol - Referral tracking and bonus coordination",
        category="satori",
    ),
}

# ============================================================================
# LIBP2P CORE PROTOCOLS
# ============================================================================

LIBP2P_PROTOCOLS: Dict[str, ProtocolInfo] = {
    # Identify
    "/ipfs/id/1.0.0": ProtocolInfo(
        id="/ipfs/id/1.0.0",
        name="identify",
        version="1.0.0",
        description="Identify - Exchange peer information and supported protocols",
        category="libp2p",
        required=True,
    ),
    "/ipfs/id/push/1.0.0": ProtocolInfo(
        id="/ipfs/id/push/1.0.0",
        name="identify-push",
        version="1.0.0",
        description="Identify Push - Proactively share identity updates to peers",
        category="libp2p",
    ),

    # Ping
    "/ipfs/ping/1.0.0": ProtocolInfo(
        id="/ipfs/ping/1.0.0",
        name="ping",
        version="1.0.0",
        description="Ping - Measure round-trip latency and check peer liveness",
        category="libp2p",
        required=True,
    ),

    # Kademlia DHT
    "/ipfs/kad/1.0.0": ProtocolInfo(
        id="/ipfs/kad/1.0.0",
        name="kad-dht",
        version="1.0.0",
        description="Kademlia DHT - Distributed hash table for peer and content discovery",
        category="libp2p",
        required=True,
    ),

    # Circuit Relay (NAT traversal)
    "/libp2p/circuit/relay/0.2.0/hop": ProtocolInfo(
        id="/libp2p/circuit/relay/0.2.0/hop",
        name="relay-hop",
        version="0.2.0",
        description="Circuit Relay Hop - Relay traffic for peers behind NAT",
        category="libp2p",
    ),
    "/libp2p/circuit/relay/0.2.0/stop": ProtocolInfo(
        id="/libp2p/circuit/relay/0.2.0/stop",
        name="relay-stop",
        version="0.2.0",
        description="Circuit Relay Stop - Receive connections via relay nodes",
        category="libp2p",
    ),

    # DCUtR (Direct Connection Upgrade through Relay)
    "/libp2p/dcutr": ProtocolInfo(
        id="/libp2p/dcutr",
        name="dcutr",
        version="1.0.0",
        description="DCUtR - Direct Connection Upgrade through Relay (NAT hole-punching)",
        category="libp2p",
    ),

    # AutoNAT
    "/libp2p/autonat/1.0.0": ProtocolInfo(
        id="/libp2p/autonat/1.0.0",
        name="autonat",
        version="1.0.0",
        description="AutoNAT - Automatic NAT detection and reachability testing",
        category="libp2p",
    ),

    # GossipSub
    "/meshsub/1.0.0": ProtocolInfo(
        id="/meshsub/1.0.0",
        name="gossipsub",
        version="1.0.0",
        description="GossipSub v1.0 - Efficient pub/sub message propagation",
        category="libp2p",
        required=True,
    ),
    "/meshsub/1.1.0": ProtocolInfo(
        id="/meshsub/1.1.0",
        name="gossipsub",
        version="1.1.0",
        description="GossipSub v1.1 - Enhanced pub/sub with peer scoring and flood protection",
        category="libp2p",
        required=True,
    ),
    "/meshsub/1.2.0": ProtocolInfo(
        id="/meshsub/1.2.0",
        name="gossipsub",
        version="1.2.0",
        description="GossipSub v1.2 - Latest pub/sub with IDONTWANT, improved peer scoring and protocol negotiation",
        category="libp2p",
        required=True,
    ),
    "/floodsub/1.0.0": ProtocolInfo(
        id="/floodsub/1.0.0",
        name="floodsub",
        version="1.0.0",
        description="FloodSub - Basic flooding pub/sub protocol (fallback)",
        category="libp2p",
    ),
}

# ============================================================================
# COMBINED REGISTRY
# ============================================================================

# All protocols combined
ALL_PROTOCOLS: Dict[str, ProtocolInfo] = {**SATORI_PROTOCOLS, **LIBP2P_PROTOCOLS}


def get_protocol_info(protocol_id: str) -> Optional[ProtocolInfo]:
    """
    Get protocol info by ID.

    Args:
        protocol_id: Full protocol ID (e.g., "/satori/ping/1.0.0")

    Returns:
        ProtocolInfo or None if not found
    """
    return ALL_PROTOCOLS.get(protocol_id)


def get_protocol_description(protocol_id: str) -> str:
    """
    Get protocol description by ID.

    Args:
        protocol_id: Full protocol ID or short name

    Returns:
        Description string, or generic description if not found
    """
    # Try exact match first
    if protocol_id in ALL_PROTOCOLS:
        return ALL_PROTOCOLS[protocol_id].description

    # Try matching by short name
    for proto in ALL_PROTOCOLS.values():
        if proto.name == protocol_id or protocol_id in proto.id:
            return proto.description

    return "Network protocol"


def get_all_protocol_descriptions() -> Dict[str, str]:
    """
    Get all protocol descriptions as a simple dict.

    Returns:
        Dict mapping protocol ID to description
    """
    return {proto_id: proto.description for proto_id, proto in ALL_PROTOCOLS.items()}


def get_required_protocols() -> List[str]:
    """
    Get list of required protocol IDs.

    Returns:
        List of protocol IDs that are required for basic operation
    """
    return [proto_id for proto_id, proto in ALL_PROTOCOLS.items() if proto.required]


def get_satori_protocols() -> List[str]:
    """
    Get list of Satori-specific protocol IDs.

    Returns:
        List of Satori protocol IDs
    """
    return list(SATORI_PROTOCOLS.keys())


def get_protocols_by_category(category: str) -> Dict[str, ProtocolInfo]:
    """
    Get protocols filtered by category.

    Args:
        category: "satori", "libp2p", or "core"

    Returns:
        Dict of matching protocols
    """
    return {
        proto_id: proto
        for proto_id, proto in ALL_PROTOCOLS.items()
        if proto.category == category
    }
