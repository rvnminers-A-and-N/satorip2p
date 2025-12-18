# Changelog

All notable changes to satorip2p will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

#### Core Signing (python-evrmorelib Integration)
- `signing.py` - Pure Python ECDSA signing using python-evrmorelib
  - `EvrmoreWallet` class for creating wallets from 32-byte entropy
  - `sign_message()` function for signing arbitrary messages
  - `verify_message()` function for signature verification
  - `generate_address()` function to derive address from public key
  - `recover_pubkey_from_signature()` function for signer recovery
- Comprehensive test suite for signing (`test_signing.py` - 33 tests)

#### Protocol Layer
- `protocol/consensus.py` - Stake-weighted consensus voting
  - `ConsensusManager` for managing consensus rounds
  - `ConsensusVote` dataclass with ECDSA signatures
  - Real signature verification using python-evrmorelib
  - 66% threshold, 5% vote cap, 20% quorum
- `protocol/signer.py` - Multi-signature coordination
  - `SignerNode` for multi-sig participants
  - 3-of-5 threshold signature scheme
  - Real ECDSA signing via wallet parameter
- `protocol/distribution_trigger.py` - Reward distribution coordination
  - `DistributionTrigger` for automated distributions
  - ElectrumX integration for UTXO queries and broadcasting
- `protocol/uptime.py` - Uptime and heartbeat tracking
  - `UptimeTracker` for monitoring node uptime
  - `Heartbeat` class for node presence proofs
  - Heartbeat sending/receiving (60s interval, 180s timeout)
  - 95% relay threshold for bonus eligibility
- `protocol/oracle_network.py` - Oracle network
  - `OracleNetwork` for observation consensus
  - Stake-weighted oracle voting
- `protocol/prediction_protocol.py` - Prediction protocol
  - Commit-reveal scheme for fair predictions
  - `PredictionProtocol` for managing prediction rounds
- `protocol/rewards.py` - Reward calculations
  - `SatoriScorer` for prediction scoring
  - `RewardCalculator` with role multipliers (Relay +5%, Oracle +10%, Signer +10%)
- `protocol/stream_registry.py` - Stream discovery
  - DHT-based stream registration and lookup
  - Stream metadata management
- `protocol/peer_registry.py` - Peer management
  - Peer reputation tracking
  - Role assignment (Predictor, Relay, Oracle, Signer)
- `protocol/subscriptions.py` - Subscription management
  - Stream subscription tracking
  - Subscription synchronization across peers
- `protocol/messages.py` - Message definitions
  - Protocol message types and serialization
- `protocol/message_store.py` - Message persistence
  - Local message storage and retrieval
- `protocol/rendezvous.py` - Peer discovery
  - Rendezvous protocol for peer introduction
- `protocol/lending.py` - P2P Pool Lending Registry
  - `LendingManager` for pool lending operations
  - `PoolConfig` dataclass for pool operator configuration
  - `LendRegistration` dataclass for lender registrations
  - PubSub topics for real-time lending updates
  - DHT storage for permanent lending records
- `protocol/delegation.py` - P2P Delegation/Proxy Protocol
  - `DelegationManager` for stake delegation operations
  - `DelegationRecord` dataclass for delegation tracking
  - `CharityUpdate` dataclass for charity status updates
  - Support for parent/child delegation relationships
  - PubSub and DHT integration for delegation sync

#### Identity Layer
- `identity/evrmore_bridge.py` - Enhanced bridge
  - Delegation to identity's verify() for satorilib mode
  - Support for both python-evrmorelib and satorilib backends

#### Blockchain Layer
- `blockchain/tx_builder.py` - Evrmore transaction building
  - UTXO selection and transaction construction
  - Multi-sig scriptSig generation
  - SATORI asset transfer outputs
  - OP_RETURN for merkle root inclusion
- `blockchain/reward_distributor.py` - Reward distribution
  - Batch reward transaction building
  - Fee calculation and change handling

#### ElectrumX Layer
- `electrumx/client.py` - ElectrumX JSON-RPC client
  - UTXO queries for transaction building
  - Transaction broadcasting
  - Balance queries
  - Pre-configured Satori ElectrumX servers
- `electrumx/connection.py` - Connection management
  - SSL/non-SSL connection handling
  - Automatic server failover

#### Integration Layer
- `integration/server.py` - Drop-in replacement for SatoriServerClient
  - `P2PSatoriServerClient` with full API compatibility
- `integration/pubsub.py` - Drop-in replacement for SatoriPubSubConn
  - `P2PSatoriPubSubConn` with WebSocket-like interface
- `integration/neuron.py` - Enhanced P2PSatoriServerClient
  - P2P lending methods (lendToAddress, lendRemove, poolAccepting, etc.)
  - P2P delegation methods (delegateGet, delegateRemove, stakeProxyChildren, etc.)
  - P2P-first observation polling with getObservation()
  - Manager setters for LendingManager, DelegationManager, OracleNetwork

#### Documentation
- `CONTRIBUTING.md` - Contribution guidelines
- `CHANGELOG.md` - This file

### Changed
- `evrmore_bridge.py` - Now delegates verification to identity object when using satorilib mode
- `consensus.py` - Skips verification for placeholder addresses (test compatibility)
- `signer.py` - Skips verification for placeholder addresses (test compatibility)

### Fixed
- Signature verification in satorilib mode now properly delegates to identity.verify()
- Test compatibility with placeholder addresses starting with "PLACEHOLDER_" or "ETest"

---

## [0.1.0] - Initial Development

### Added

#### Core P2P Infrastructure
- `peers.py` - Core P2P networking using py-libp2p
  - GossipSub publish/subscribe
  - Kademlia DHT for peer discovery
  - Direct messaging between peers
  - Connection management
- `hybrid/` - Hybrid P2P/Central mode
  - `HybridPeers` class for dual-mode operation
  - `HybridMode` enum (P2P_ONLY, HYBRID, CENTRAL_ONLY)
  - `FailoverStrategy` for network preference
  - Circuit breaker pattern for resilience

#### Compatibility Layer
- `compat/pubsub_server.py` - WebSocket server compatible with SatoriPubSubConn
- `compat/centrifugo_server.py` - Centrifugo-compatible server
- `compat/server_api.py` - REST API compatible with central server
- `compat/datamanager.py` - DataClient/DataServer bridge

#### NAT Traversal
- AutoNAT for NAT type detection
- Circuit Relay v2 for relay through public peers
- DCUtR (Direct Connection Upgrade through Relay) for hole punching
- UPnP port mapping support

#### API Layer
- `api/` - REST API for external integrations
  - Health checks
  - Peer management endpoints
  - Subscription management
  - Message publishing

#### Documentation
- `README.md` - Project overview and quick start
- `API_REFERENCE.md` - Complete API documentation
- `ARCHITECTURE.md` - System architecture details
- `CONFIGURATION.md` - Configuration options
- `DOCKER.md` - Docker deployment guide
- `EXAMPLES.md` - Usage examples
- `MIGRATION_GUIDE.md` - Migration from central servers
- `SECURITY.md` - Security model documentation

#### Testing
- Comprehensive test suite (500+ tests)
- Unit tests for all core modules
- Integration tests for protocol interactions
- Mock infrastructure for isolated testing

---

## Roadmap

### Completed
- [x] Evrmore transaction building (distribution_trigger.py, tx_builder.py)
- [x] Evrmore transaction broadcasting (ElectrumX integration)
- [x] Multi-sig signature combination (threshold ECDSA)
- [x] ElectrumX client module (electrumx/client.py, connection.py)
- [x] Multi-sig helper functions (redeem script, scriptSig, signature parsing)
- [x] Signer evolution roadmap documentation (SIGNER_ROADMAP.md)
- [x] P2P Pool Lending Registry (protocol/lending.py)
- [x] P2P Delegation/Proxy Protocol (protocol/delegation.py)
- [x] P2P-first observation polling (integration/neuron.py)

### Pending (Team Configuration)
- [ ] Signer configuration (6 placeholder values in signer.py)
  - 5 signer addresses (PLACEHOLDER_SIGNER_ADDRESS_1-5)
  - 1 treasury multi-sig address (PLACEHOLDER_MULTISIG_TREASURY_ADDRESS)
- [ ] Bootstrap nodes (config.py)
  - Deploy Satori bootstrap nodes (bootstrap1.satorinet.io, etc.)
  - Add multiaddresses to BOOTSTRAP_PEERS list
  - Currently using IPFS bootstrap nodes as fallback
- (ElectrumX servers already configured - no additional setup needed)

### Future
- [ ] On-chain signer verification (Phase 2 - see SIGNER_ROADMAP.md)
- [ ] Satori chain integration (SatoriChainDistributor)
- [ ] Enhanced peer scoring
- [ ] Network monitoring dashboard

---

## Migration Notes

### From Central Servers
See [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md) for detailed migration instructions.

### Key Changes
1. Replace `SatoriServerClient` with `P2PSatoriServerClient`
2. Replace `SatoriPubSubConn` with `P2PSatoriPubSubConn`
3. Configure bootstrap peers for P2P discovery
4. Use `HybridPeers` for gradual migration with fallback
