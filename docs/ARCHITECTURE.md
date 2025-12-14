# satorip2p Architecture

This document describes the architecture of satorip2p, a peer-to-peer networking module for the Satori Network.

## Table of Contents

1. [Overview](#overview)
2. [Component Architecture](#component-architecture)
3. [Protocol Stack](#protocol-stack)
4. [Message Flow](#message-flow)
5. [Identity System](#identity-system)
6. [NAT Traversal](#nat-traversal)
7. [Hybrid Mode Architecture](#hybrid-mode-architecture)

---

## Overview

satorip2p provides decentralized peer-to-peer networking for the Satori Network, replacing centralized pubsub servers with a distributed system based on libp2p.

### Design Goals

- **Decentralization**: No single point of failure
- **Backward Compatibility**: Support existing Satori protocols during transition
- **NAT Traversal**: Work behind firewalls and NAT without configuration
- **Scalability**: Support thousands of peers with efficient message routing
- **Identity**: Leverage existing Evrmore wallet keys for authentication

---

## Component Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              satorip2p                                      │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐           │
│  │     Peers       │   │   HybridPeers   │   │    PeersAPI     │           │
│  │  (Core P2P)     │   │ (Central+P2P)   │   │   (REST API)    │           │
│  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘           │
│           │                     │                     │                     │
│  ┌────────┴─────────────────────┴─────────────────────┴────────┐           │
│  │                       Protocol Layer                         │           │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │           │
│  │  │ Subscription │  │ MessageStore │  │  Rendezvous  │       │           │
│  │  │   Manager    │  │              │  │   Manager    │       │           │
│  │  └──────────────┘  └──────────────┘  └──────────────┘       │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │                      Identity Layer                          │           │
│  │  ┌──────────────────────────────────────────────────────┐   │           │
│  │  │             EvrmoreIdentityBridge                     │   │           │
│  │  │  - Evrmore wallet keys → libp2p keypairs             │   │           │
│  │  │  - Message signing/verification                       │   │           │
│  │  │  - ECDH shared secret derivation                      │   │           │
│  │  └──────────────────────────────────────────────────────┘   │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │                       NAT Layer                              │           │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐             │           │
│  │  │  AutoNAT   │  │   DCUtR    │  │   UPnP     │             │           │
│  │  │            │  │ (Hole Punch│  │  Manager   │             │           │
│  │  └────────────┘  └────────────┘  └────────────┘             │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │                    Transport Layer (libp2p)                  │           │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐             │           │
│  │  │    TCP     │  │    QUIC    │  │  Circuit   │             │           │
│  │  │            │  │            │  │   Relay    │             │           │
│  │  └────────────┘  └────────────┘  └────────────┘             │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                             │
└────────────────────────────────────────────────────────────────────────────┘
```

### Core Components

#### Peers (`satorip2p.peers`)
The main entry point for P2P networking. Manages:
- Peer connections and discovery
- GossipSub publish/subscribe
- DHT operations
- Message routing

#### HybridPeers (`satorip2p.hybrid`)
Extended Peers class supporting dual-mode operation:
- P2P_ONLY: Pure decentralized mode
- HYBRID: Both P2P and Central servers
- CENTRAL_ONLY: Legacy mode for compatibility

#### PeersAPI (`satorip2p.api`)
REST API for external integrations:
- Health checks
- Peer management
- Subscription management
- Message publishing

---

## Protocol Stack

### libp2p Protocols Used

| Protocol | Purpose | Implementation |
|----------|---------|----------------|
| **GossipSub 1.1** | Pub/sub messaging | Topic-based message propagation |
| **Kademlia DHT** | Peer discovery | Distributed hash table for finding peers |
| **Circuit Relay v2** | NAT traversal | Relay through public peers |
| **DCUtR** | Hole punching | Direct Connection Upgrade through Relay |
| **AutoNAT** | NAT detection | Automatic NAT type detection |
| **Noise** | Security | Encrypted connections |
| **Yamux** | Multiplexing | Multiple streams per connection |

### Transport Protocols

```
┌─────────────────────────────────────────────────────────┐
│                    Application                          │
├─────────────────────────────────────────────────────────┤
│  GossipSub  │  DHT Query  │  Direct Message  │  Relay  │
├─────────────────────────────────────────────────────────┤
│                    Yamux (Multiplexing)                 │
├─────────────────────────────────────────────────────────┤
│                    Noise (Encryption)                   │
├─────────────────────────────────────────────────────────┤
│      TCP (Port 4001)      │    QUIC (Port 4001/UDP)    │
└─────────────────────────────────────────────────────────┘
```

---

## Message Flow

### Publish Flow

```
Publisher                    GossipSub                    Subscribers
    │                            │                             │
    │  publish(stream_id, data)  │                             │
    │───────────────────────────►│                             │
    │                            │                             │
    │                            │  Message validation         │
    │                            │  (signature, dedup)         │
    │                            │                             │
    │                            │  Forward to mesh peers      │
    │                            │────────────────────────────►│
    │                            │                             │
    │                            │                    Callback │
    │                            │                             │
```

### Subscribe Flow

```
Subscriber                   DHT                         GossipSub
    │                         │                              │
    │  subscribe(stream_id)   │                              │
    │────────────────────────►│                              │
    │                         │                              │
    │                         │  Register at rendezvous     │
    │                         │  point for stream_id        │
    │                         │                              │
    │                         │  Find existing publishers   │
    │                         │                              │
    │                                     JOIN topic         │
    │─────────────────────────────────────────────────────►│
    │                                                        │
    │                              GRAFT to mesh             │
    │◄─────────────────────────────────────────────────────│
```

### Discovery Flow

```
New Peer                     Bootstrap                    DHT
    │                            │                          │
    │  Connect to bootstrap      │                          │
    │───────────────────────────►│                          │
    │                            │                          │
    │  Get initial peer list     │                          │
    │◄───────────────────────────│                          │
    │                            │                          │
    │  Bootstrap DHT routing table                          │
    │──────────────────────────────────────────────────────►│
    │                            │                          │
    │  Announce our streams (rendezvous)                    │
    │──────────────────────────────────────────────────────►│
    │                            │                          │
```

---

## Identity System

### Evrmore to libp2p Key Bridge

satorip2p uses existing Satori wallet keys for P2P identity:

```
┌─────────────────────────────────────────────────────────────┐
│                   Evrmore Wallet                            │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Private Key (secp256k1)                           │    │
│  │  └─► Address: EXxxx...                             │    │
│  └────────────────────────────────────────────────────┘    │
└───────────────────────────┬─────────────────────────────────┘
                            │
                   EvrmoreIdentityBridge
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    libp2p Identity                          │
│  ┌────────────────────────────────────────────────────┐    │
│  │  PeerID: 12D3Koo...                                │    │
│  │  └─► Derived from same secp256k1 key              │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Key Functions

- **Address Generation**: Same private key → same Evrmore address AND PeerID
- **Message Signing**: Sign with Evrmore key, verify with PeerID
- **ECDH Key Exchange**: Derive shared secrets for encrypted DMs

---

## NAT Traversal

### Strategy Hierarchy

satorip2p uses multiple strategies in order of preference:

```
┌─────────────────────────────────────────────────────────────┐
│                  NAT Traversal Strategy                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. Direct Connection (no NAT)                              │
│     └─► Public IP, ports open                               │
│                                                             │
│  2. UPnP Port Mapping                                       │
│     └─► Automatic router configuration                      │
│                                                             │
│  3. Hole Punching (DCUtR)                                   │
│     └─► UDP hole punch via relay coordination               │
│                                                             │
│  4. Circuit Relay                                           │
│     └─► Traffic relayed through public peers                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### AutoNAT Detection

```
Client                    AutoNAT Server                   Result
   │                            │                             │
   │  "Can you reach me?"       │                             │
   │───────────────────────────►│                             │
   │                            │                             │
   │                    Try direct connection                 │
   │◄───────────────────────────│                             │
   │                            │                             │
   │  NAT Status: Public/Private/Unknown                      │
   │◄─────────────────────────────────────────────────────────│
```

### Circuit Relay Flow

```
Peer A (behind NAT)         Relay Node          Peer B (behind NAT)
        │                       │                       │
        │  Reserve relay slot   │                       │
        │──────────────────────►│                       │
        │                       │                       │
        │                       │  Reserve relay slot   │
        │                       │◄──────────────────────│
        │                       │                       │
        │           Messages relayed bidirectionally    │
        │◄─────────────────────►│◄─────────────────────►│
        │                       │                       │
        │         Attempt hole punch (DCUtR)            │
        │◄──────────────────────────────────────────────│
        │                       │                       │
        │      Direct connection (if successful)        │
        │◄──────────────────────────────────────────────│
```

---

## Hybrid Mode Architecture

### Mode Comparison

| Feature | P2P_ONLY | HYBRID | CENTRAL_ONLY |
|---------|----------|--------|--------------|
| Central Server | No | Yes | Yes |
| P2P Network | Yes | Yes | No |
| Failover | N/A | Automatic | N/A |
| Message Bridge | N/A | Yes | N/A |
| Latency | Lower | Variable | Higher |

### Hybrid Bridge Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           HybridBridge                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────────┐           ┌──────────────────────┐            │
│  │    P2P GossipSub     │           │   Central WebSocket  │            │
│  │                      │           │                      │            │
│  │  ┌────────────────┐  │           │  ┌────────────────┐  │            │
│  │  │  Topic: stream │  │  Bridge   │  │  ws://pubsub   │  │            │
│  │  │  Messages      │◄─┼───────────┼─►│  Messages      │  │            │
│  │  └────────────────┘  │           │  └────────────────┘  │            │
│  │                      │           │                      │            │
│  └──────────────────────┘           └──────────────────────┘            │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    MessageDeduplicator                            │   │
│  │  - Prevents routing loops                                         │   │
│  │  - Content hash tracking                                          │   │
│  │  - TTL-based expiration                                           │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    FailoverManager                                │   │
│  │  - Health monitoring                                              │   │
│  │  - Automatic failover                                             │   │
│  │  - Configurable strategies                                        │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Failover Strategies

```
PREFER_P2P:
  Primary: P2P GossipSub
  Fallback: Central Server (on P2P failure)

PREFER_CENTRAL:
  Primary: Central Server
  Fallback: P2P GossipSub (on Central failure)

PARALLEL:
  Both: P2P + Central simultaneously
  Deduplication: MessageDeduplicator prevents duplicates
```

---

## Directory Structure

```
satorip2p/
├── src/satorip2p/
│   ├── __init__.py           # Package exports
│   ├── peers.py              # Core Peers class
│   ├── config.py             # Configuration classes
│   │
│   ├── identity/             # Identity management
│   │   └── evrmore_bridge.py # Evrmore → libp2p bridge
│   │
│   ├── nat/                  # NAT traversal
│   │   ├── docker.py         # Docker NAT detection
│   │   └── upnp.py           # UPnP port mapping
│   │
│   ├── protocol/             # Protocol implementations
│   │   ├── messages.py       # Message types
│   │   ├── subscriptions.py  # Subscription manager
│   │   ├── message_store.py  # Offline message storage
│   │   └── rendezvous.py     # Rendezvous discovery
│   │
│   ├── compat/               # Backward compatibility
│   │   ├── pubsub.py         # SatoriPubSubConn compat
│   │   ├── centrifugo.py     # Centrifugo compat
│   │   ├── server.py         # Central server REST compat
│   │   └── datamanager.py    # DataManager compat
│   │
│   ├── hybrid/               # Hybrid mode
│   │   ├── config.py         # Hybrid configuration
│   │   ├── central_client.py # Central pubsub client
│   │   ├── bridge.py         # Message bridge
│   │   └── hybrid_peers.py   # HybridPeers class
│   │
│   ├── api.py                # REST API
│   └── metrics.py            # Prometheus metrics
│
├── tests/                    # Test suite
├── docker/                   # Docker configurations
├── docs/                     # Documentation
└── examples/                 # Usage examples
```

---

## Performance Considerations

### Message Propagation

- **GossipSub mesh**: 6-12 peers per topic (configurable)
- **Heartbeat interval**: 1 second
- **Message cache**: 5 minutes (deduplication window)
- **Max message size**: 1 MB (configurable)

### DHT Performance

- **Bucket size**: 20 peers (k-value)
- **Concurrency**: 3 parallel queries (alpha)
- **Query timeout**: 30 seconds
- **Refresh interval**: 1 hour

### Resource Usage

| Component | Memory | CPU | Network |
|-----------|--------|-----|---------|
| Base Peers | ~50 MB | Low | Variable |
| + DHT | +20 MB | Low | Moderate |
| + Relay | +10 MB | Low | High (if relay) |
| + Hybrid | +30 MB | Moderate | Higher |

---

## Security Model

See [SECURITY.md](SECURITY.md) for detailed security documentation.

Key points:
- All connections encrypted (Noise protocol)
- Messages signed with Evrmore keys
- PeerID derived from wallet keys (verifiable)
- ECDH for encrypted direct messages
- Challenge-response authentication for compatibility APIs
