# satorip2p Security Model

Comprehensive security documentation for the satorip2p module.

## Table of Contents

1. [Security Overview](#security-overview)
2. [Identity and Authentication](#identity-and-authentication)
3. [Transport Security](#transport-security)
4. [Message Security](#message-security)
5. [Encryption](#encryption)
6. [Best Practices](#best-practices)
7. [Threat Model](#threat-model)

---

## Security Overview

satorip2p implements multiple layers of security:

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
│  - Message signing (secp256k1)                              │
│  - Content authentication                                   │
│  - Challenge-response auth                                  │
├─────────────────────────────────────────────────────────────┤
│                    Encryption Layer                         │
│  - ECDH key exchange                                        │
│  - AES-GCM encryption (direct messages)                     │
│  - Per-message nonces                                       │
├─────────────────────────────────────────────────────────────┤
│                    Transport Layer                          │
│  - Noise protocol (XX handshake)                            │
│  - Authenticated encryption                                 │
│  - Perfect forward secrecy                                  │
├─────────────────────────────────────────────────────────────┤
│                    Network Layer                            │
│  - TCP/QUIC transports                                      │
│  - TLS 1.3 (QUIC)                                           │
└─────────────────────────────────────────────────────────────┘
```

---

## Identity and Authentication

### Evrmore Wallet Identity

satorip2p uses existing Satori/Evrmore wallet keys for identity:

```
Evrmore Private Key (secp256k1)
        │
        ├──► Evrmore Address (base58check)
        │    └─► Used for: Wallet operations, challenge-response
        │
        └──► libp2p PeerID (multihash)
             └─► Used for: P2P identification, message routing
```

### PeerID Generation

The libp2p PeerID is derived from the same secp256k1 key:

```python
from satorip2p import EvrmoreIdentityBridge
from satorilib.wallet.evrmore.identity import EvrmoreIdentity

# Load wallet
evrmore_identity = EvrmoreIdentity('/path/to/wallet.yaml')

# Create bridge
bridge = EvrmoreIdentityBridge(evrmore_identity)

# Same key → verifiable identity
print(f"Evrmore Address: {bridge.address}")
print(f"libp2p PeerID: {bridge.peer_id}")
```

### Verification

Any peer can verify that a PeerID corresponds to an Evrmore address:

```python
# Peer A signs a message
message = b"Hello, World!"
signature = bridge.sign(message)

# Peer B can verify using PeerID
is_valid = bridge.verify(message, signature, peer_id=peer_a_id)
```

---

## Transport Security

### Noise Protocol

All libp2p connections use the Noise XX handshake pattern:

```
Initiator                                          Responder
    │                                                   │
    │  → e (ephemeral key)                              │
    │                                                   │
    │                   ← e, ee, s, es                  │
    │                   (ephemeral + static key)        │
    │                                                   │
    │  → s, se                                          │
    │  (static key, authenticated)                      │
    │                                                   │
    │              [Encrypted channel established]      │
```

**Properties:**
- Mutual authentication
- Perfect forward secrecy
- Identity hiding from passive observers
- Resistance to key compromise impersonation

### QUIC Transport

When using QUIC (UDP), additional security via TLS 1.3:

- 0-RTT connection resumption
- Built-in encryption
- Multiplexed streams
- Connection migration

---

## Message Security

### Message Signing

All published messages are signed by default:

```python
# Publishing with signature (default)
await peers.publish("stream-uuid", {"value": 42.0})

# Message structure
{
    "topic": "stream-uuid",
    "data": {"value": 42.0},
    "timestamp": "2024-01-01T00:00:00Z",
    "sender": "12D3KooW...",  # PeerID
    "signature": "304402..."  # secp256k1 signature
}
```

### Signature Verification

Receivers verify signatures automatically:

```python
def on_message(stream_id, data):
    # Data is already verified if it reached here
    # Invalid signatures are rejected at GossipSub layer
    print(f"Verified message: {data}")

await peers.subscribe("stream-uuid", on_message)
```

### GossipSub Message Validation

GossipSub provides additional validation:

1. **Deduplication**: Message cache prevents replay
2. **Sender validation**: Messages must come from valid peers
3. **Topic validation**: Messages must match subscribed topics
4. **Score-based filtering**: Low-score peers are deprioritized

---

## Encryption

### ECDH Key Exchange

For encrypted direct messages:

```python
# Derive shared secret
shared_secret = bridge.derive_shared_secret(peer_public_key)

# Shared secret is identical on both sides
# secret_A == secret_B (ECDH property)
```

### AES-GCM Encryption

Direct messages use AES-256-GCM:

```python
from satorip2p.compat.datamanager import SecurityPolicy

# Encrypt message
policy = SecurityPolicy(
    encrypt=True,
    sign=True,
)

encrypted_data = policy.encrypt(
    plaintext=message_bytes,
    shared_secret=shared_secret,
)

# Structure: nonce (12 bytes) + ciphertext + tag (16 bytes)
```

### Key Derivation

```
ECDH Shared Secret (32 bytes)
        │
        ▼
    HKDF-SHA256
        │
        ├──► AES Key (32 bytes)
        │
        └──► Additional derived keys as needed
```

---

## Best Practices

### Wallet Security

1. **Protect wallet files**:
   ```bash
   chmod 600 wallet.yaml
   ```

2. **Use encrypted storage**:
   ```bash
   # Store on encrypted partition or use LUKS
   ```

3. **Backup securely**:
   ```bash
   # Offline backup, never in cloud storage
   ```

### Network Security

1. **Use relay for NAT traversal**:
   ```python
   peers = Peers(enable_relay=True)  # Default
   ```

2. **Enable DHT for decentralized discovery**:
   ```python
   peers = Peers(enable_dht=True)  # Default
   ```

3. **Configure firewall**:
   ```bash
   # Only allow necessary ports
   ufw allow 4001/tcp
   ufw allow 4001/udp
   ```

### Application Security

1. **Validate incoming data**:
   ```python
   def on_message(stream_id, data):
       # Validate data structure
       if not isinstance(data, dict):
           logger.warning("Invalid message format")
           return

       # Validate expected fields
       if "value" not in data:
           logger.warning("Missing required field")
           return

       # Process validated data
       process(data)
   ```

2. **Rate limiting**:
   ```python
   from collections import defaultdict
   import time

   rate_limits = defaultdict(list)

   def rate_limited_handler(stream_id, data):
       sender = data.get("sender")
       now = time.time()

       # Clean old entries
       rate_limits[sender] = [t for t in rate_limits[sender] if now - t < 60]

       # Check rate
       if len(rate_limits[sender]) > 100:  # 100 msgs/min
           logger.warning(f"Rate limit exceeded for {sender}")
           return

       rate_limits[sender].append(now)
       process(data)
   ```

3. **Input sanitization**:
   ```python
   import bleach

   def sanitize_input(data):
       if isinstance(data, str):
           return bleach.clean(data)
       return data
   ```

---

## Threat Model

### Threats Mitigated

| Threat | Mitigation |
|--------|------------|
| **Eavesdropping** | Noise protocol encryption |
| **Man-in-the-middle** | Authenticated connections |
| **Replay attacks** | Message deduplication, timestamps |
| **Message tampering** | secp256k1 signatures |
| **Identity spoofing** | Wallet-derived PeerID |
| **Sybil attacks** | GossipSub peer scoring |
| **Eclipse attacks** | Multiple bootstrap peers |

### Threats Partially Mitigated

| Threat | Notes |
|--------|-------|
| **DDoS** | Rate limiting helps, but no complete protection |
| **Traffic analysis** | Encrypted content, but metadata visible |
| **Censorship** | DHT provides resilience, but not guaranteed |

### Threats NOT Mitigated

| Threat | Notes |
|--------|-------|
| **Compromised wallet** | If private key stolen, identity compromised |
| **Compromised endpoint** | Application-level security required |
| **51% network attack** | Possible with enough malicious nodes |

### Security Assumptions

1. **secp256k1 is secure**: No practical attacks on curve
2. **Noise protocol is secure**: Well-audited, widely used
3. **Wallet private key is secret**: User responsibility
4. **Bootstrap peers are honest**: At least some must be honest

---

## Challenge-Response Authentication

For compatibility APIs (REST/WebSocket):

```
Client                                              Server
   │                                                   │
   │  Request resource                                 │
   │──────────────────────────────────────────────────►│
   │                                                   │
   │                   Challenge (random nonce)        │
   │◄──────────────────────────────────────────────────│
   │                                                   │
   │  Sign(nonce + wallet_address)                     │
   │──────────────────────────────────────────────────►│
   │                                                   │
   │                   Verify signature                │
   │                   Grant access                    │
   │◄──────────────────────────────────────────────────│
```

```python
from satorip2p.compat.datamanager import ChallengeResponse

# Server generates challenge
challenge = ChallengeResponse.generate_challenge()

# Client signs
response = ChallengeResponse.sign_challenge(
    challenge=challenge,
    identity=bridge,
)

# Server verifies
is_valid = ChallengeResponse.verify_response(
    challenge=challenge,
    response=response,
    expected_address=client_address,
)
```

---

## Reporting Security Issues

If you discover a security vulnerability:

1. **Do NOT** open a public issue
2. Contact maintainers privately
3. Allow reasonable time for fix before disclosure
4. Credit will be given for responsible disclosure

---

## Audit Status

| Component | Status |
|-----------|--------|
| libp2p (py-libp2p) | Community audited |
| Noise protocol | Formally verified |
| secp256k1 | Widely audited |
| satorip2p application layer | Community review |

**Note:** satorip2p is open source. Security researchers are encouraged to review the code.
