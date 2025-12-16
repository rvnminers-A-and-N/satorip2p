# Signer Node Evolution Roadmap

This document describes the current signer node architecture and the roadmap for evolving to fully decentralized, on-chain verification.

## Current Architecture (Phase 1)

### Hardcoded Authorized Signers

Currently, signer nodes are identified via a hardcoded list in `signer.py`:

```python
AUTHORIZED_SIGNERS = [
    "PLACEHOLDER_SIGNER_ADDRESS_1",
    "PLACEHOLDER_SIGNER_ADDRESS_2",
    "PLACEHOLDER_SIGNER_ADDRESS_3",
    "PLACEHOLDER_SIGNER_ADDRESS_4",
    "PLACEHOLDER_SIGNER_ADDRESS_5",
]

TREASURY_MULTISIG_ADDRESS = "PLACEHOLDER_MULTISIG_TREASURY_ADDRESS"
```

### Configuration Before Deployment

The Satori team must:

1. **Generate 5 signer wallets** - Each with a unique Evrmore address
2. **Create the multi-sig treasury address** - Using the 5 public keys
3. **Replace placeholders** - Update `signer.py` with actual addresses
4. **Deploy signer nodes** - Each signer runs their node with `is_authorized_signer=True`

### Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Current Signer Flow                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Consensus Reached (66% stake agrees on merkle root)         │
│              │                                                  │
│              ▼                                                  │
│  2. Distribution Trigger builds unsigned TX                     │
│     - Queries UTXOs from ElectrumX                             │
│     - Creates SATORI transfer outputs                           │
│     - Includes OP_RETURN with merkle root                       │
│              │                                                  │
│              ▼                                                  │
│  3. Signature Request broadcast to signer nodes                 │
│              │                                                  │
│              ▼                                                  │
│  4. Each signer node:                                           │
│     - Verifies their address is in AUTHORIZED_SIGNERS           │
│     - Verifies merkle root matches their local calculation      │
│     - Signs the transaction                                     │
│     - Broadcasts signature response                             │
│              │                                                  │
│              ▼                                                  │
│  5. When 3-of-5 signatures collected:                           │
│     - Combine into multi-sig scriptSig                          │
│     - Broadcast via ElectrumX                                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Security Considerations

**Pros:**
- Simple and deterministic
- No on-chain queries needed for signer identification
- Fast verification

**Cons:**
- Requires code changes to rotate signers
- Centralized trust in code maintainers
- All nodes must update to recognize new signers

---

## Future Architecture (Phase 2): On-Chain Verification

### Overview

Move signer authorization to the blockchain itself, eliminating the need for code changes when rotating signers.

### Option A: Administrator Token

The SATORI Administrator Token (`SATORI!`) could designate signer authority:

```
SATORI Token: https://cryptoscope.io/evrmore/asset/?asset_id=081cab6cd370fa387035b9fb5e67e736d7493453
SATORI Administrator Token: https://cryptoscope.io/evrmore/asset/?asset_id=b0615a9beb5ad1483f2fe4c8d5f546fce5e47fc0
```

**Implementation:**
1. Issue 5 "SATORI_SIGNER" sub-assets or unique NFTs
2. Each signer holds their authorization token
3. Verification queries blockchain for token holders
4. Token transfer = signer rotation

### Option B: On-Chain Multi-Sig Registration

Store the current signer public keys in an on-chain OP_RETURN:

```
SIGNER_REGISTRY TX:
  OP_RETURN: "SATORI_SIGNERS:" + <pubkey1> + <pubkey2> + ... + <pubkey5>
```

**Implementation:**
1. Create a registry transaction with signer pubkeys
2. Nodes query for the latest registry TX
3. Signer rotation = new registry TX

### Option C: Smart Contract (Future Satori Chain)

If Satori develops its own chain, implement signer management in a smart contract:

```python
# Pseudo-code for signer management contract
class SignerRegistry:
    signers: Set[Address]
    threshold: int = 3

    def add_signer(self, address: Address, admin_sig: Signature):
        # Requires admin token holder signature
        ...

    def remove_signer(self, address: Address, admin_sig: Signature):
        ...

    def is_authorized(self, address: Address) -> bool:
        return address in self.signers
```

---

## Migration Path

### Phase 1 → Phase 2 Migration

1. **Deploy Phase 1** with hardcoded signers
2. **Issue authorization tokens** to the 5 signers
3. **Update satorip2p** to check both:
   - Hardcoded list (backward compatibility)
   - On-chain token ownership (new method)
4. **Deprecate hardcoded list** once all nodes updated
5. **Enable token-based rotation** for future changes

### Code Changes Required

```python
# signer.py evolution

# Phase 1 (Current)
def is_authorized_signer(address: str) -> bool:
    return address in AUTHORIZED_SIGNERS

# Phase 2 (Future)
async def is_authorized_signer(address: str, electrumx: ElectrumXClient) -> bool:
    # Check hardcoded list first (backward compatibility)
    if address in AUTHORIZED_SIGNERS:
        return True

    # Check on-chain token ownership
    try:
        balance = await electrumx.get_asset_balance(address, "SATORI_SIGNER")
        return balance > 0
    except Exception:
        return False
```

---

## Configuration Checklist

### Before Production Deployment

- [ ] Generate 5 signer wallet addresses
- [ ] Create multi-sig redeem script with 5 pubkeys
- [ ] Derive treasury P2SH address from redeem script
- [ ] Replace `PLACEHOLDER_SIGNER_ADDRESS_1-5` in `signer.py`
- [ ] Replace `PLACEHOLDER_MULTISIG_TREASURY_ADDRESS` in `signer.py`
- [ ] Fund treasury with SATORI for distribution
- [ ] Fund treasury with EVR for transaction fees
- [ ] Deploy 5 signer nodes with proper configuration
- [ ] Test distribution with small amounts first

### Signer Node Configuration

Each signer node must be started with:

```python
from satorip2p.protocol.signer import SignerNode

signer = SignerNode(
    peers=peers,
    signer_address="E...",  # Their address
    is_authorized_signer=True,
    wallet=evrmore_wallet,  # For signing
)
```

---

## Security Best Practices

1. **Geographic Distribution** - Signers should be in different locations
2. **Hardware Security** - Consider hardware wallets for signer keys
3. **Monitoring** - Alert on failed signing attempts
4. **Rotation Schedule** - Plan for periodic signer rotation
5. **Backup Procedure** - Document recovery process if signer unavailable

---

## Related Files

- `src/satorip2p/protocol/signer.py` - Signer node implementation
- `src/satorip2p/protocol/distribution_trigger.py` - Distribution coordination
- `src/satorip2p/blockchain/tx_builder.py` - Transaction building
- `src/satorip2p/electrumx/client.py` - ElectrumX client

---

## References

- [SATORI Token on Cryptoscope](https://cryptoscope.io/evrmore/asset/?asset_id=081cab6cd370fa387035b9fb5e67e736d7493453)
- [Evrmore Multi-sig Documentation](https://evrmore.com/docs/multisig)
- [BIP 11: M-of-N Standard Transactions](https://github.com/bitcoin/bips/blob/master/bip-0011.mediawiki)
