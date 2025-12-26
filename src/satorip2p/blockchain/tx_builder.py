"""
satorip2p/blockchain/tx_builder.py

Transaction builder using ElectrumX + python-evrmorelib.

Builds Evrmore transactions client-side without requiring a full node.
Supports:
- SATORI asset transfers
- Multi-recipient batching
- OP_RETURN data embedding
- Multi-sig (P2SH) inputs

This is used by the distribution system to build unsigned transactions
that are then signed by the multi-sig signer nodes.
"""

import logging
import hashlib
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING
from dataclasses import dataclass

logger = logging.getLogger("satorip2p.blockchain.tx_builder")

# Check for python-evrmorelib
try:
    from evrmore.core import (
        CMutableTransaction,
        CMutableTxIn,
        CMutableTxOut,
        COutPoint,
        CScript,
        CTxInWitness,
        b2x,
        x,
        lx,
    )
    from evrmore.core.script import (
        OP_RETURN,
        OP_DUP,
        OP_HASH160,
        OP_EQUALVERIFY,
        OP_CHECKSIG,
        OP_EVR_ASSET,
        SignatureHash,
        SIGHASH_ALL,
    )
    from evrmore.wallet import (
        P2PKHEvrmoreAddress,
        P2SHEvrmoreAddress,
        CEvrmoreSecret,
    )
    from evrmore.core.assets import RvnAssetData
    EVRMORELIB_AVAILABLE = True
except ImportError:
    EVRMORELIB_AVAILABLE = False
    logger.warning("python-evrmorelib not available - transaction building disabled")

if TYPE_CHECKING:
    from ..electrumx.client import UTXO, ElectrumXClient


# ============================================================================
# CONSTANTS
# ============================================================================

SATORI_ASSET_NAME = "SATORI"
SATOSHI_PER_EVR = 100_000_000  # 1 EVR = 100M satoshis
MIN_RELAY_FEE = 1000  # Minimum relay fee in satoshis
DUST_THRESHOLD = 546  # Dust threshold in satoshis
OP_RETURN_MAX_SIZE = 80  # Max OP_RETURN data size


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class TransactionOutput:
    """A transaction output."""
    address: str
    amount: int  # Satoshis for EVR, or asset units for SATORI
    is_asset: bool = False
    asset_name: str = ""


@dataclass
class UnsignedTransaction:
    """An unsigned transaction ready for signing."""
    tx_hex: str
    tx_hash: str
    inputs: List[Dict[str, Any]]
    outputs: List[Dict[str, Any]]
    total_evr_in: int
    total_evr_out: int
    total_satori_in: float
    total_satori_out: float
    fee: int

    def to_dict(self) -> dict:
        return {
            "tx_hex": self.tx_hex,
            "tx_hash": self.tx_hash,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "total_evr_in": self.total_evr_in,
            "total_evr_out": self.total_evr_out,
            "total_satori_in": self.total_satori_in,
            "total_satori_out": self.total_satori_out,
            "fee": self.fee,
        }


# ============================================================================
# TRANSACTION BUILDER
# ============================================================================

class TransactionBuilder:
    """
    Builds Evrmore transactions for SATORI distribution.

    Uses ElectrumX for UTXO queries and python-evrmorelib for
    transaction construction.

    Example:
        from satorip2p.electrumx import ElectrumXClient
        from satorip2p.blockchain.tx_builder import TransactionBuilder

        client = ElectrumXClient()
        client.connect()

        builder = TransactionBuilder(client, treasury_address)
        unsigned_tx = builder.build_distribution_tx(
            recipients={"Eaddr1": 100.0, "Eaddr2": 50.0},
            merkle_root="abc123..."
        )

        # Sign with multi-sig...
        # Broadcast with client.broadcast(signed_tx_hex)
    """

    def __init__(
        self,
        electrumx_client: "ElectrumXClient",
        treasury_address: str,
        fee_rate: float = 0.00001,  # EVR per byte
    ):
        """
        Initialize transaction builder.

        Args:
            electrumx_client: Connected ElectrumX client
            treasury_address: Treasury address holding SATORI
            fee_rate: Fee rate in EVR per byte
        """
        if not EVRMORELIB_AVAILABLE:
            raise ImportError("python-evrmorelib required for transaction building")

        self.client = electrumx_client
        self.treasury = treasury_address
        self.fee_rate = fee_rate

    def build_distribution_tx(
        self,
        recipients: Dict[str, float],
        merkle_root: str,
        round_id: Optional[str] = None,
    ) -> UnsignedTransaction:
        """
        Build an unsigned distribution transaction.

        Creates a transaction that:
        1. Spends SATORI from treasury
        2. Sends specified amounts to recipients
        3. Includes OP_RETURN with merkle root
        4. Returns change to treasury

        Args:
            recipients: {address: satori_amount} mapping
            merkle_root: Merkle root to embed in OP_RETURN
            round_id: Optional round ID for logging

        Returns:
            UnsignedTransaction ready for signing
        """
        logger.info(f"Building distribution TX for {len(recipients)} recipients")

        # Get UTXOs from treasury
        evr_utxos = self.client.get_unspent(self.treasury)
        satori_utxos = self.client.get_asset_unspent(self.treasury, SATORI_ASSET_NAME)

        # Calculate totals needed
        total_satori_needed = sum(recipients.values())
        num_outputs = len(recipients) + 2  # recipients + change + OP_RETURN
        estimated_size = 180 + (num_outputs * 34) + (len(evr_utxos) + len(satori_utxos)) * 148
        estimated_fee = max(MIN_RELAY_FEE, int(estimated_size * self.fee_rate * SATOSHI_PER_EVR))

        logger.debug(f"Need {total_satori_needed} SATORI, est fee: {estimated_fee} sat")

        # Select SATORI UTXOs
        selected_satori_utxos = []
        selected_satori_amount = 0.0

        for utxo in sorted(satori_utxos, key=lambda u: u.asset_amount or 0, reverse=True):
            if selected_satori_amount >= total_satori_needed:
                break
            selected_satori_utxos.append(utxo)
            selected_satori_amount += utxo.asset_amount or 0

        if selected_satori_amount < total_satori_needed:
            raise ValueError(
                f"Insufficient SATORI: have {selected_satori_amount}, need {total_satori_needed}"
            )

        # Select EVR UTXOs for fees
        selected_evr_utxos = []
        selected_evr_amount = 0

        for utxo in sorted(evr_utxos, key=lambda u: u.value, reverse=True):
            if selected_evr_amount >= estimated_fee + DUST_THRESHOLD:
                break
            selected_evr_utxos.append(utxo)
            selected_evr_amount += utxo.value

        if selected_evr_amount < estimated_fee:
            raise ValueError(
                f"Insufficient EVR for fees: have {selected_evr_amount} sat, need {estimated_fee} sat"
            )

        # Build transaction
        tx = CMutableTransaction()

        # Add inputs
        input_info = []

        for utxo in selected_evr_utxos:
            outpoint = COutPoint(lx(utxo.txid), utxo.vout)
            txin = CMutableTxIn(outpoint)
            tx.vin.append(txin)
            input_info.append({
                "txid": utxo.txid,
                "vout": utxo.vout,
                "value": utxo.value,
                "type": "evr",
            })

        for utxo in selected_satori_utxos:
            outpoint = COutPoint(lx(utxo.txid), utxo.vout)
            txin = CMutableTxIn(outpoint)
            tx.vin.append(txin)
            input_info.append({
                "txid": utxo.txid,
                "vout": utxo.vout,
                "value": utxo.value,
                "asset": SATORI_ASSET_NAME,
                "asset_amount": utxo.asset_amount,
                "type": "asset",
            })

        # Build outputs
        output_info = []

        # 1. OP_RETURN with merkle root
        op_return_data = self._encode_op_return(merkle_root, round_id)
        op_return_script = CScript([OP_RETURN, op_return_data])
        tx.vout.append(CMutableTxOut(0, op_return_script))
        output_info.append({
            "type": "op_return",
            "data": op_return_data.hex(),
        })

        # 2. SATORI outputs to recipients
        for address, amount in recipients.items():
            script = self._create_asset_transfer_script(address, SATORI_ASSET_NAME, amount)
            tx.vout.append(CMutableTxOut(0, script))  # 0 EVR for asset outputs
            output_info.append({
                "type": "satori_transfer",
                "address": address,
                "amount": amount,
            })

        # 3. SATORI change back to treasury
        satori_change = selected_satori_amount - total_satori_needed
        if satori_change > 0.00000001:  # Minimum asset amount
            script = self._create_asset_transfer_script(
                self.treasury, SATORI_ASSET_NAME, satori_change
            )
            tx.vout.append(CMutableTxOut(0, script))
            output_info.append({
                "type": "satori_change",
                "address": self.treasury,
                "amount": satori_change,
            })

        # 4. EVR change back to treasury
        evr_change = selected_evr_amount - estimated_fee
        if evr_change > DUST_THRESHOLD:
            addr = P2PKHEvrmoreAddress(self.treasury)
            script = addr.to_scriptPubKey()
            tx.vout.append(CMutableTxOut(evr_change, script))
            output_info.append({
                "type": "evr_change",
                "address": self.treasury,
                "amount": evr_change,
            })

        # Serialize
        tx_hex = b2x(tx.serialize())
        tx_hash = hashlib.sha256(hashlib.sha256(tx.serialize()).digest()).digest()[::-1].hex()

        logger.info(f"Built unsigned TX: {tx_hash} ({len(tx_hex)//2} bytes)")

        return UnsignedTransaction(
            tx_hex=tx_hex,
            tx_hash=tx_hash,
            inputs=input_info,
            outputs=output_info,
            total_evr_in=selected_evr_amount,
            total_evr_out=evr_change if evr_change > DUST_THRESHOLD else 0,
            total_satori_in=selected_satori_amount,
            total_satori_out=total_satori_needed,
            fee=estimated_fee,
        )

    def _encode_op_return(
        self,
        merkle_root: str,
        round_id: Optional[str] = None
    ) -> bytes:
        """
        Encode data for OP_RETURN output.

        Format: "SATORI:" + merkle_root (truncated to fit)

        Args:
            merkle_root: 64-char hex merkle root
            round_id: Optional round identifier

        Returns:
            Encoded bytes (max 80 bytes)
        """
        prefix = b"SATORI:"

        # Merkle root as bytes (32 bytes from 64 hex chars)
        try:
            merkle_bytes = bytes.fromhex(merkle_root[:64])
        except ValueError:
            merkle_bytes = merkle_root.encode()[:32]

        data = prefix + merkle_bytes

        # Truncate if needed
        if len(data) > OP_RETURN_MAX_SIZE:
            data = data[:OP_RETURN_MAX_SIZE]

        return data

    def _create_asset_transfer_script(
        self,
        address: str,
        asset_name: str,
        amount: float
    ) -> CScript:
        """
        Create script for asset transfer output.

        Evrmore asset transfers use a special script format:
        OP_DUP OP_HASH160 <pubkeyhash> OP_EQUALVERIFY OP_CHECKSIG
        OP_EVR_ASSET <asset_transfer_data>

        Args:
            address: Destination address
            asset_name: Asset name (e.g., "SATORI")
            amount: Amount to transfer

        Returns:
            CScript for the output
        """
        # Get address pubkey hash
        addr = P2PKHEvrmoreAddress(address)
        pubkey_hash = addr.to_bytes()[1:-4]  # Strip version and checksum

        # Create asset transfer
        # Amount is in satoshis (8 decimal places)
        amount_sats = int(amount * SATOSHI_PER_EVR)

        # Build standard P2PKH script + asset transfer
        # The exact format depends on python-evrmorelib's asset support
        try:
            asset_transfer = CAssetTransfer(asset_name, amount_sats)
            script = CScript([
                OP_DUP, OP_HASH160, pubkey_hash, OP_EQUALVERIFY, OP_CHECKSIG,
                OP_EVR_ASSET, asset_transfer.serialize()
            ])
        except Exception:
            # Fallback: manual construction if CAssetTransfer not available
            # Asset transfer format: name (null-terminated) + amount (8 bytes LE)
            asset_data = asset_name.encode() + b'\x00' + amount_sats.to_bytes(8, 'little')
            script = CScript([
                OP_DUP, OP_HASH160, pubkey_hash, OP_EQUALVERIFY, OP_CHECKSIG,
                OP_EVR_ASSET, asset_data
            ])

        return script

    def estimate_fee(self, num_inputs: int, num_outputs: int) -> int:
        """
        Estimate transaction fee.

        Args:
            num_inputs: Number of inputs
            num_outputs: Number of outputs

        Returns:
            Estimated fee in satoshis
        """
        # Rough size estimation
        # Header: ~10 bytes
        # Input: ~148 bytes (P2PKH) or ~297 bytes (P2SH multisig)
        # Output: ~34 bytes (P2PKH) or ~43 bytes (asset transfer)
        estimated_size = 10 + (num_inputs * 180) + (num_outputs * 40)
        return max(MIN_RELAY_FEE, int(estimated_size * self.fee_rate * SATOSHI_PER_EVR))


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def build_distribution_transaction(
    electrumx_client: "ElectrumXClient",
    treasury_address: str,
    recipients: Dict[str, float],
    merkle_root: str,
    round_id: Optional[str] = None,
) -> UnsignedTransaction:
    """
    Convenience function to build a distribution transaction.

    Args:
        electrumx_client: Connected ElectrumX client
        treasury_address: Treasury address
        recipients: {address: amount} mapping
        merkle_root: Merkle root for OP_RETURN
        round_id: Optional round ID

    Returns:
        UnsignedTransaction
    """
    builder = TransactionBuilder(electrumx_client, treasury_address)
    return builder.build_distribution_tx(recipients, merkle_root, round_id)
