"""
satorip2p/blockchain/reward_distributor.py

Blockchain-agnostic reward distribution system.

Supports:
- Evrmore (current - SATORI is an EVR asset)
- Future SAT chain (when available)
- Other backends (extensible)

Architecture:
    RewardDistributor (abstract)
    ├── EvrmoreDistributor (current - SATORI asset on EVR)
    └── SatoriChainDistributor (future - native SAT chain)

Usage:
    from satorip2p.blockchain.reward_distributor import EvrmoreDistributor

    distributor = EvrmoreDistributor(rpc_client, treasury_address)
    result = await distributor.distribute_round(round_summary)
"""

import logging
import struct
import hashlib
import time
import json
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from ..protocol.rewards import RoundSummary, RewardEntry
    from ..peers import Peers

logger = logging.getLogger("satorip2p.blockchain.reward_distributor")


# ============================================================================
# MERKLE TREE
# ============================================================================

class MerkleTree:
    """
    Build and verify merkle trees for reward verification.

    Used to create on-chain proofs that rewards are correct.
    """

    def __init__(self, leaves: Optional[List[str]] = None):
        """
        Initialize MerkleTree.

        Args:
            leaves: List of leaf hashes (hex strings)
        """
        self.leaves = leaves or []
        self.tree: List[str] = []
        self.root: str = ""

        if self.leaves:
            self._build()

    def _build(self) -> None:
        """Build the merkle tree from leaves."""
        if not self.leaves:
            self.root = ""
            self.tree = []
            return

        # Initialize with leaves
        self.tree = list(self.leaves)
        current_level = list(self.leaves)

        # Build tree bottom-up
        while len(current_level) > 1:
            next_level = []
            for i in range(0, len(current_level), 2):
                left = current_level[i]
                # If odd number, duplicate last element
                right = current_level[i + 1] if i + 1 < len(current_level) else left
                combined = hashlib.sha256((left + right).encode()).hexdigest()
                next_level.append(combined)
                self.tree.append(combined)
            current_level = next_level

        self.root = current_level[0] if current_level else ""

    def add_leaf(self, data: str) -> str:
        """
        Add a leaf to the tree.

        Args:
            data: Data to hash and add as leaf

        Returns:
            Leaf hash
        """
        leaf_hash = hashlib.sha256(data.encode()).hexdigest()
        self.leaves.append(leaf_hash)
        self._build()
        return leaf_hash

    def get_proof(self, leaf_index: int) -> List[Tuple[str, str]]:
        """
        Get merkle proof for a leaf.

        Args:
            leaf_index: Index of leaf in leaves list

        Returns:
            List of (direction, sibling_hash) tuples
        """
        if leaf_index >= len(self.leaves):
            return []

        proof = []
        current_level = list(self.leaves)
        idx = leaf_index

        while len(current_level) > 1:
            if idx % 2 == 0:
                # We're on the left, sibling is on the right
                sibling_idx = idx + 1
                direction = "right"
            else:
                # We're on the right, sibling is on the left
                sibling_idx = idx - 1
                direction = "left"

            if sibling_idx < len(current_level):
                proof.append((direction, current_level[sibling_idx]))
            else:
                # Odd case: duplicate ourselves
                proof.append((direction, current_level[idx]))

            # Move to next level
            next_level = []
            for i in range(0, len(current_level), 2):
                left = current_level[i]
                right = current_level[i + 1] if i + 1 < len(current_level) else left
                combined = hashlib.sha256((left + right).encode()).hexdigest()
                next_level.append(combined)

            current_level = next_level
            idx = idx // 2

        return proof

    @staticmethod
    def verify_proof(
        leaf_hash: str,
        merkle_root: str,
        proof: List[Tuple[str, str]]
    ) -> bool:
        """
        Verify a merkle proof.

        Args:
            leaf_hash: Hash of the leaf being verified
            merkle_root: Expected root hash
            proof: List of (direction, sibling_hash) tuples

        Returns:
            True if proof is valid
        """
        current_hash = leaf_hash

        for direction, sibling_hash in proof:
            if direction == 'left':
                combined = sibling_hash + current_hash
            else:
                combined = current_hash + sibling_hash
            current_hash = hashlib.sha256(combined.encode()).hexdigest()

        return current_hash == merkle_root

    @staticmethod
    def hash_reward_entry(
        address: str,
        amount: float,
        score: float,
        rank: int
    ) -> str:
        """Create deterministic hash for a reward entry."""
        leaf_data = f"{address}:{amount:.8f}:{score:.6f}:{rank}"
        return hashlib.sha256(leaf_data.encode()).hexdigest()


# ============================================================================
# OP_RETURN ENCODING
# ============================================================================

def encode_round_summary_for_opreturn(summary: Dict[str, Any]) -> bytes:
    """
    Encode round summary for OP_RETURN (max ~80 bytes).

    Format (69 bytes total):
    - 1 byte:  Version
    - 4 bytes: Epoch number
    - 8 bytes: Round ID hash (truncated)
    - 32 bytes: Merkle root of all rewards
    - 8 bytes: Total reward pool (satoshi precision)
    - 4 bytes: Number of predictors
    - 8 bytes: Observation value (float64)
    - 4 bytes: Observation timestamp (truncated)

    Args:
        summary: Round summary dict

    Returns:
        Encoded bytes (69 bytes)
    """
    VERSION = 1

    round_id_hash = hashlib.sha256(
        summary['round_id'].encode()
    ).digest()[:8]

    merkle_root_bytes = bytes.fromhex(summary['merkle_root']) if summary.get('merkle_root') else b'\x00' * 32

    data = struct.pack(
        '<B I 8s 32s Q I d I',
        VERSION,
        summary.get('epoch', 0),
        round_id_hash,
        merkle_root_bytes,
        int(summary.get('total_reward_pool', 0) * 1e8),
        summary.get('num_predictors', len(summary.get('rewards', []))),
        summary.get('observation_value', 0.0),
        summary.get('observation_time', 0) & 0xFFFFFFFF
    )

    return data  # 69 bytes


def decode_round_summary_from_opreturn(data: bytes) -> Dict[str, Any]:
    """
    Decode OP_RETURN data back to summary.

    Args:
        data: 69 bytes of encoded data

    Returns:
        Decoded summary dict
    """
    if len(data) != 69:
        raise ValueError(f"Expected 69 bytes, got {len(data)}")

    unpacked = struct.unpack('<B I 8s 32s Q I d I', data)

    return {
        'version': unpacked[0],
        'epoch': unpacked[1],
        'round_id_hash': unpacked[2].hex(),
        'merkle_root': unpacked[3].hex(),
        'total_reward_pool': unpacked[4] / 1e8,
        'num_predictors': unpacked[5],
        'observation_value': unpacked[6],
        'observation_time': unpacked[7],
    }


# ============================================================================
# ABSTRACT REWARD DISTRIBUTOR
# ============================================================================

class RewardDistributor(ABC):
    """
    Abstract base class for reward distribution.

    Subclass this to implement different blockchain backends.
    """

    @abstractmethod
    async def distribute_round(
        self,
        round_summary: "RoundSummary"
    ) -> Dict[str, Any]:
        """
        Distribute rewards for a completed round.

        Args:
            round_summary: Complete round data with rewards

        Returns:
            Result dict with tx_hash, status, etc.
        """
        pass

    @abstractmethod
    async def get_balance(self, address: str) -> float:
        """
        Get SATORI balance for an address.

        Args:
            address: Blockchain address

        Returns:
            Balance in SATORI
        """
        pass

    @abstractmethod
    async def verify_distribution(
        self,
        tx_hash: str,
        expected_merkle_root: str
    ) -> bool:
        """
        Verify a distribution transaction is valid.

        Args:
            tx_hash: Transaction hash
            expected_merkle_root: Expected merkle root from round

        Returns:
            True if transaction matches expectations
        """
        pass


# ============================================================================
# EVRMORE DISTRIBUTOR
# ============================================================================

class EvrmoreDistributor(RewardDistributor):
    """
    Distribute SATORI rewards via Evrmore asset transfers.

    SATORI is already an Evrmore asset, so we use direct transfers.
    """

    SATORI_ASSET = "SATORI"

    def __init__(
        self,
        rpc_client: Any,
        treasury_address: str,
        peers: Optional["Peers"] = None,
        dry_run: bool = False
    ):
        """
        Initialize EvrmoreDistributor.

        Args:
            rpc_client: Evrmore RPC client
            treasury_address: Address holding treasury SATORI
            peers: Optional Peers instance for DHT/PubSub
            dry_run: If True, don't actually send transactions
        """
        self.rpc = rpc_client
        self.treasury = treasury_address
        self.peers = peers
        self.dry_run = dry_run

    async def distribute_round(
        self,
        round_summary: "RoundSummary"
    ) -> Dict[str, Any]:
        """
        Complete reward distribution for a round.

        1. Build merkle tree (should already be in summary)
        2. Store full data in DHT (via peers if available)
        3. Create Evrmore TX (SATORI transfers + OP_RETURN)
        4. Broadcast via PubSub (via peers if available)

        Args:
            round_summary: Complete round data

        Returns:
            Result dict with status, tx_hash, etc.
        """
        rewards = round_summary.rewards

        if not rewards:
            logger.info(f"No rewards to distribute for round {round_summary.round_id}")
            return {
                'status': 'no_rewards',
                'round_id': round_summary.round_id,
            }

        # Build outputs for Evrmore transaction
        outputs: Dict[str, float] = {}
        for r in rewards:
            if r.amount > 0:
                addr = r.address
                outputs[addr] = outputs.get(addr, 0) + r.amount

        if not outputs:
            return {
                'status': 'no_outputs',
                'round_id': round_summary.round_id,
            }

        # Create OP_RETURN data
        op_return_data = encode_round_summary_for_opreturn(round_summary.to_dict())

        # Send batch transfer
        if self.dry_run:
            tx_hash = f"dry_run_{round_summary.round_id}"
            logger.info(f"DRY RUN: Would distribute {sum(outputs.values())} SATORI to {len(outputs)} addresses")
        else:
            tx_hash = await self._send_batch_transfer(outputs, op_return_data)

        return {
            'status': 'success',
            'tx_hash': tx_hash,
            'round_id': round_summary.round_id,
            'merkle_root': round_summary.merkle_root,
            'total_distributed': sum(outputs.values()),
            'num_recipients': len(outputs),
            'dry_run': self.dry_run,
        }

    async def _send_batch_transfer(
        self,
        outputs: Dict[str, float],
        op_return_data: bytes
    ) -> str:
        """
        Send SATORI to multiple recipients in single TX.

        Uses createrawtransaction for batch asset transfers since the
        simple 'transfer' command only supports one recipient.

        Process:
        1. Get UTXOs for treasury (EVR for fees + SATORI asset)
        2. Build raw transaction with multiple transfer outputs + OP_RETURN
        3. Sign and broadcast

        Args:
            outputs: {address: amount} mapping
            op_return_data: Data for OP_RETURN output

        Returns:
            Transaction hash
        """
        try:
            # Get UTXOs for the treasury address
            utxos = self.rpc.call('listunspent', 1, 9999999, [self.treasury])

            # Separate EVR UTXOs and SATORI asset UTXOs
            evr_utxos = []
            asset_utxos = []

            for utxo in utxos:
                if 'asset' in utxo and utxo.get('asset') == self.SATORI_ASSET:
                    asset_utxos.append(utxo)
                elif utxo.get('amount', 0) > 0:  # EVR
                    evr_utxos.append(utxo)

            # Calculate total SATORI needed
            total_satori_needed = sum(outputs.values())

            # Select SATORI UTXOs to cover the amount
            selected_asset_utxos = []
            selected_satori = 0.0
            for utxo in asset_utxos:
                if selected_satori >= total_satori_needed:
                    break
                selected_asset_utxos.append(utxo)
                selected_satori += utxo.get('amount', 0)

            if selected_satori < total_satori_needed:
                raise ValueError(
                    f"Insufficient SATORI: have {selected_satori}, need {total_satori_needed}"
                )

            # Select EVR UTXO for fees (estimate ~0.01 EVR per output)
            fee_estimate = 0.01 * (len(outputs) + 2)  # outputs + change + OP_RETURN
            selected_evr_utxo = None
            for utxo in evr_utxos:
                if utxo.get('amount', 0) >= fee_estimate:
                    selected_evr_utxo = utxo
                    break

            if not selected_evr_utxo:
                raise ValueError(f"Insufficient EVR for fees: need ~{fee_estimate}")

            # Build inputs
            inputs = [{"txid": selected_evr_utxo['txid'], "vout": selected_evr_utxo['vout']}]
            for utxo in selected_asset_utxos:
                inputs.append({"txid": utxo['txid'], "vout": utxo['vout']})

            # Build outputs
            # First: EVR change (coins must come before asset operations)
            evr_change = selected_evr_utxo['amount'] - fee_estimate
            tx_outputs = {}

            if evr_change > 0.0001:  # Dust threshold
                tx_outputs[self.treasury] = evr_change

            # Add OP_RETURN for merkle root proof
            tx_outputs["data"] = op_return_data.hex()

            # Add SATORI transfer outputs
            for address, amount in outputs.items():
                if address == self.treasury:
                    # Don't overwrite change address, use a different key approach
                    tx_outputs[address] = {
                        "transfer": {
                            self.SATORI_ASSET: amount
                        }
                    }
                else:
                    tx_outputs[address] = {
                        "transfer": {
                            self.SATORI_ASSET: amount
                        }
                    }

            # Add SATORI change if needed
            satori_change = selected_satori - total_satori_needed
            if satori_change > 0.00000001:  # SATORI dust threshold
                # Merge with existing treasury entry or create new
                if self.treasury in tx_outputs and isinstance(tx_outputs[self.treasury], dict):
                    tx_outputs[self.treasury]["transfer"][self.SATORI_ASSET] = (
                        tx_outputs[self.treasury]["transfer"].get(self.SATORI_ASSET, 0) + satori_change
                    )
                else:
                    # Treasury address needs both EVR change and asset change
                    # This is handled by asset_change_address in simpler transfers
                    pass  # Change handled automatically

            # Create raw transaction
            raw_tx = self.rpc.call('createrawtransaction', inputs, tx_outputs)

            # Sign the transaction
            signed = self.rpc.call('signrawtransaction', raw_tx)
            if not signed.get('complete'):
                raise ValueError(f"Failed to sign transaction: {signed.get('errors', [])}")

            # Broadcast
            tx_hash = self.rpc.call('sendrawtransaction', signed['hex'])

            logger.info(f"Distributed rewards in TX: {tx_hash}")
            return tx_hash

        except Exception as e:
            logger.error(f"Failed to send batch transfer: {e}")
            raise

    async def get_balance(self, address: str) -> float:
        """
        Get SATORI balance for an address.

        Args:
            address: Evrmore address

        Returns:
            Balance in SATORI
        """
        try:
            balances = self.rpc.call('listassetbalancesbyaddress', address)
            return balances.get(self.SATORI_ASSET, 0.0)
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            return 0.0

    async def verify_distribution(
        self,
        tx_hash: str,
        expected_merkle_root: str
    ) -> bool:
        """
        Verify a distribution transaction is valid.

        Checks:
        1. Transaction exists
        2. OP_RETURN contains expected merkle root
        3. Outputs match expected rewards

        Args:
            tx_hash: Transaction hash to verify
            expected_merkle_root: Expected merkle root

        Returns:
            True if valid
        """
        try:
            # Get transaction
            tx = self.rpc.call('getrawtransaction', tx_hash, True)

            # Find OP_RETURN output
            for vout in tx.get('vout', []):
                script = vout.get('scriptPubKey', {})
                if script.get('type') == 'nulldata':
                    # Decode OP_RETURN data
                    hex_data = script.get('hex', '')
                    # Skip OP_RETURN opcode (6a) and length byte
                    if hex_data.startswith('6a'):
                        data_hex = hex_data[4:]  # Skip 6a and length
                        data = bytes.fromhex(data_hex)
                        decoded = decode_round_summary_from_opreturn(data)

                        if decoded['merkle_root'] == expected_merkle_root:
                            return True

            return False

        except Exception as e:
            logger.error(f"Failed to verify distribution: {e}")
            return False

    async def get_treasury_balance(self) -> float:
        """Get treasury SATORI balance."""
        return await self.get_balance(self.treasury)


# ============================================================================
# ASSET BADGE ISSUER
# ============================================================================

class AssetBadgeIssuer:
    """
    Issue gamification badges as Evrmore unique assets.

    Evrmore unique assets are created using 'issueunique' command
    which creates sub-assets under a root asset (e.g., SATORI#tag).

    Requires ownership of the root asset's owner token (SATORI!).
    Each unique asset costs 5 EVR to create.
    """

    ROOT_ASSET = "SATORI"  # Root asset for badges (requires SATORI! owner token)

    def __init__(self, rpc_client: Any, dry_run: bool = False):
        """
        Initialize AssetBadgeIssuer.

        Args:
            rpc_client: Evrmore RPC client
            dry_run: If True, don't actually issue assets
        """
        self.rpc = rpc_client
        self.dry_run = dry_run

    async def issue_rank_badge(
        self,
        epoch: int,
        rank: int,
        recipient: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Issue unique asset badge to top performer.

        Badge naming: SATORI#E{epoch}_R{rank}
        Example: SATORI#E42_R1 = Epoch 42, Rank 1

        Uses 'issueunique' command which creates unique sub-assets.
        Requires ownership of SATORI! (the root asset owner token).

        Args:
            epoch: Epoch number
            rank: Rank in epoch (1 = first place)
            recipient: Address to receive badge
            metadata: Optional metadata dict (can include IPFS hash)

        Returns:
            Transaction hash if successful
        """
        badge_tag = f"E{epoch}_R{rank}"

        if self.dry_run:
            logger.info(f"DRY RUN: Would issue {self.ROOT_ASSET}#{badge_tag} to {recipient}")
            return f"dry_run_{self.ROOT_ASSET}#{badge_tag}"

        try:
            # Build IPFS hashes array if metadata provided
            ipfs_hashes = []
            if metadata and metadata.get('ipfs_hash'):
                ipfs_hashes = [metadata['ipfs_hash']]

            # Issue unique sub-asset using issueunique command
            # Signature: issueunique "root_name" [asset_tags] ( [ipfs_hashes] ) "to_address" "change_address"
            tx_hash = self.rpc.call(
                'issueunique',
                self.ROOT_ASSET,      # Root asset name (SATORI)
                [badge_tag],          # Array of unique tags to issue
                ipfs_hashes or [],    # Optional IPFS hashes (same length as tags)
                recipient,            # Recipient address
                "",                   # Change address (empty = auto)
            )

            logger.info(f"Issued badge {self.ROOT_ASSET}#{badge_tag} to {recipient}: {tx_hash}")
            return tx_hash

        except Exception as e:
            logger.error(f"Failed to issue badge {self.ROOT_ASSET}#{badge_tag}: {e}")
            return None

    async def issue_achievement_badge(
        self,
        achievement: str,
        recipient: str,
        ipfs_hash: Optional[str] = None
    ) -> Optional[str]:
        """
        Issue achievement badge.

        Examples:
        - SATORI#FOUNDER_PREDICTOR
        - SATORI#PERFECT_ROUND
        - SATORI#10_STREAK

        Args:
            achievement: Achievement tag name (without SATORI# prefix)
            recipient: Address to receive badge
            ipfs_hash: Optional IPFS hash for metadata

        Returns:
            Transaction hash if successful
        """
        if self.dry_run:
            logger.info(f"DRY RUN: Would issue {self.ROOT_ASSET}#{achievement} to {recipient}")
            return f"dry_run_{self.ROOT_ASSET}#{achievement}"

        try:
            ipfs_hashes = [ipfs_hash] if ipfs_hash else []

            tx_hash = self.rpc.call(
                'issueunique',
                self.ROOT_ASSET,
                [achievement],
                ipfs_hashes,
                recipient,
                "",
            )

            logger.info(f"Issued achievement {self.ROOT_ASSET}#{achievement} to {recipient}: {tx_hash}")
            return tx_hash

        except Exception as e:
            logger.error(f"Failed to issue achievement {self.ROOT_ASSET}#{achievement}: {e}")
            return None

    async def issue_multiple_badges(
        self,
        tags: List[str],
        recipient: str,
        ipfs_hashes: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Issue multiple unique badges in a single transaction.

        More efficient than issuing one at a time (single TX fee).

        Args:
            tags: List of badge tags to issue
            recipient: Address to receive all badges
            ipfs_hashes: Optional list of IPFS hashes (must match tags length)

        Returns:
            Transaction hash if successful
        """
        if not tags:
            return None

        if self.dry_run:
            logger.info(f"DRY RUN: Would issue {len(tags)} badges to {recipient}")
            return f"dry_run_batch_{len(tags)}"

        try:
            tx_hash = self.rpc.call(
                'issueunique',
                self.ROOT_ASSET,
                tags,
                ipfs_hashes or [],
                recipient,
                "",
            )

            logger.info(f"Issued {len(tags)} badges to {recipient}: {tx_hash}")
            return tx_hash

        except Exception as e:
            logger.error(f"Failed to issue {len(tags)} badges: {e}")
            return None

    async def issue_round_badges(
        self,
        epoch: int,
        results: List[Dict[str, Any]],
        top_n_ranks: int = 10
    ) -> Dict[str, Any]:
        """
        Issue all badges for a completed round efficiently.

        Combines rank badges (Option 2: separate calls per recipient) with
        achievement badges (Option 1: batched per recipient).

        Args:
            epoch: Round/epoch number
            results: List of participant results, each containing:
                {
                    'address': str,           # Recipient address
                    'rank': int or None,      # Rank (1-N) or None if not in top N
                    'achievements': list,     # List of achievement codes earned
                }
            top_n_ranks: Only issue rank badges for top N (default 10)

        Achievement codes (examples):
            - 'PERFECT'    → Perfect prediction accuracy
            - 'FAST'       → Fastest commit time
            - 'STREAK_N'   → N consecutive rounds of participation
            - 'CALIBRATED' → Best calibration score
            - 'COMEBACK'   → Biggest rank improvement
            - 'CONSISTENT' → Lowest score variance over N rounds
            - 'WHALE'      → Highest stake in round
            - 'EARLY'      → Among first N predictors

        Returns:
            {
                'epoch': int,
                'rank_badges_issued': int,
                'achievement_badges_issued': int,
                'total_badges': int,
                'tx_hashes': list,
                'badges_by_recipient': {address: [badge_tags]},
                'errors': list,
            }

        Example:
            results = [
                {'address': 'EAlice', 'rank': 1, 'achievements': ['PERFECT', 'FAST']},
                {'address': 'EBob', 'rank': 2, 'achievements': ['STREAK_10']},
                {'address': 'ECarol', 'rank': 3, 'achievements': []},
                {'address': 'EDave', 'rank': None, 'achievements': ['CALIBRATED']},
            ]
            await issuer.issue_round_badges(epoch=42, results=results)

            # Results in:
            # EAlice receives: SATORI#E42_R1, SATORI#E42_PERFECT, SATORI#E42_FAST
            # EBob receives:   SATORI#E42_R2, SATORI#E42_STREAK_10
            # ECarol receives: SATORI#E42_R3
            # EDave receives:  SATORI#E42_CALIBRATED
        """
        tx_hashes = []
        errors = []
        badges_by_recipient: Dict[str, List[str]] = {}
        rank_badges_issued = 0
        achievement_badges_issued = 0

        for result in results:
            address = result.get('address')
            if not address:
                errors.append({'error': 'Missing address in result', 'result': result})
                continue

            rank = result.get('rank')
            achievements = result.get('achievements', [])

            recipient_badges = []

            # Issue rank badge (separate call - different recipients)
            if rank is not None and 1 <= rank <= top_n_ranks:
                rank_tag = f"E{epoch}_R{rank}"
                try:
                    tx = await self.issue_rank_badge(epoch, rank, address)
                    if tx:
                        tx_hashes.append(tx)
                        recipient_badges.append(f"{self.ROOT_ASSET}#{rank_tag}")
                        rank_badges_issued += 1
                except Exception as e:
                    errors.append({
                        'error': str(e),
                        'address': address,
                        'badge_type': 'rank',
                        'rank': rank,
                    })

            # Issue achievement badges (batched - one call for all achievements per recipient)
            if achievements:
                # Build achievement tags with epoch prefix
                achievement_tags = [f"E{epoch}_{ach}" for ach in achievements]

                try:
                    tx = await self.issue_multiple_badges(achievement_tags, address)
                    if tx:
                        tx_hashes.append(tx)
                        for tag in achievement_tags:
                            recipient_badges.append(f"{self.ROOT_ASSET}#{tag}")
                        achievement_badges_issued += len(achievements)
                except Exception as e:
                    errors.append({
                        'error': str(e),
                        'address': address,
                        'badge_type': 'achievements',
                        'achievements': achievements,
                    })

            if recipient_badges:
                badges_by_recipient[address] = recipient_badges

        total_badges = rank_badges_issued + achievement_badges_issued

        logger.info(
            f"Round {epoch} badges: {rank_badges_issued} rank + "
            f"{achievement_badges_issued} achievement = {total_badges} total"
        )

        return {
            'epoch': epoch,
            'rank_badges_issued': rank_badges_issued,
            'achievement_badges_issued': achievement_badges_issued,
            'total_badges': total_badges,
            'tx_hashes': tx_hashes,
            'badges_by_recipient': badges_by_recipient,
            'errors': errors,
        }

    # Predefined achievement types for reference
    ACHIEVEMENT_TYPES = {
        'PERFECT': 'Perfect prediction accuracy (error < 0.1%)',
        'FAST': 'Fastest commit time in round',
        'EARLY': 'Among first 10 predictors in round',
        'CALIBRATED': 'Best calibration score (confidence matched accuracy)',
        'CONSISTENT': 'Lowest score variance over past 10 rounds',
        'STREAK_5': '5 consecutive rounds of participation',
        'STREAK_10': '10 consecutive rounds of participation',
        'STREAK_25': '25 consecutive rounds of participation',
        'STREAK_50': '50 consecutive rounds of participation',
        'STREAK_100': '100 consecutive rounds of participation',
        'COMEBACK': 'Biggest rank improvement from previous round',
        'WHALE': 'Highest stake committed in round',
        'UNDERDOG': 'Won with minimum stake',
        'FIRST_WIN': 'First time in top 10',
        'VETERAN': 'Participated in 100+ rounds',
        'FOUNDER': 'Participated in epoch 1',
    }

    async def get_badge_holders(self, badge_tag: str) -> List[str]:
        """
        Get all holders of a specific badge.

        Args:
            badge_tag: Badge tag (e.g., "E42_R1") or full name (e.g., "SATORI#E42_R1")

        Returns:
            List of addresses holding the badge
        """
        try:
            # Normalize badge name - add prefix if not present
            if not badge_tag.startswith(f"{self.ROOT_ASSET}#"):
                badge_name = f"{self.ROOT_ASSET}#{badge_tag}"
            else:
                badge_name = badge_tag

            holders = self.rpc.call('listaddressesbyasset', badge_name)
            return list(holders.keys()) if isinstance(holders, dict) else []
        except Exception as e:
            logger.error(f"Failed to get badge holders: {e}")
            return []

    async def get_badges_for_address(self, address: str) -> List[str]:
        """
        Get all SATORI badges held by an address.

        Args:
            address: Evrmore address

        Returns:
            List of badge names (e.g., ["SATORI#E42_R1", "SATORI#FOUNDER"])
        """
        try:
            all_assets = self.rpc.call('listassetbalancesbyaddress', address)
            # Filter to only SATORI# unique assets (badges)
            return [
                name for name in all_assets.keys()
                if name.startswith(f"{self.ROOT_ASSET}#")
            ]
        except Exception as e:
            logger.error(f"Failed to get badges for address: {e}")
            return []


# ============================================================================
# CENTRALIZED DISTRIBUTOR (Transition Phase)
# ============================================================================

class CentralizedDistributor(RewardDistributor):
    """
    Centralized reward distribution for transition phase.

    Used when the team runs the distribution service.
    Stores distribution records locally and logs for manual processing.
    """

    def __init__(
        self,
        output_file: str = "pending_distributions.json",
        auto_approve: bool = False
    ):
        """
        Initialize CentralizedDistributor.

        Args:
            output_file: File to store pending distributions
            auto_approve: If True, skip manual approval
        """
        self.output_file = output_file
        self.auto_approve = auto_approve
        self._pending: List[Dict[str, Any]] = []

    async def distribute_round(
        self,
        round_summary: "RoundSummary"
    ) -> Dict[str, Any]:
        """
        Record distribution for manual processing.

        Args:
            round_summary: Complete round data

        Returns:
            Result dict
        """
        distribution = {
            'round_id': round_summary.round_id,
            'stream_id': round_summary.stream_id,
            'epoch': round_summary.epoch,
            'total_reward_pool': round_summary.total_reward_pool,
            'merkle_root': round_summary.merkle_root,
            'rewards': [r.to_dict() for r in round_summary.rewards],
            'created_at': int(time.time()),
            'status': 'pending',
        }

        self._pending.append(distribution)

        # Save to file
        try:
            with open(self.output_file, 'w') as f:
                json.dump(self._pending, f, indent=2)
            logger.info(f"Saved distribution for round {round_summary.round_id}")
        except Exception as e:
            logger.error(f"Failed to save distribution: {e}")

        return {
            'status': 'pending_manual_approval',
            'round_id': round_summary.round_id,
            'output_file': self.output_file,
        }

    async def get_balance(self, address: str) -> float:
        """Not applicable for centralized mode."""
        return 0.0

    async def verify_distribution(
        self,
        tx_hash: str,
        expected_merkle_root: str
    ) -> bool:
        """Not applicable for centralized mode."""
        return False

    def get_pending_distributions(self) -> List[Dict[str, Any]]:
        """Get all pending distributions."""
        return self._pending

    def mark_distributed(self, round_id: str, tx_hash: str) -> bool:
        """
        Mark a distribution as completed.

        Args:
            round_id: Round that was distributed
            tx_hash: Transaction hash of distribution

        Returns:
            True if marked successfully
        """
        for dist in self._pending:
            if dist['round_id'] == round_id:
                dist['status'] = 'distributed'
                dist['tx_hash'] = tx_hash
                dist['distributed_at'] = int(time.time())

                # Save to file
                try:
                    with open(self.output_file, 'w') as f:
                        json.dump(self._pending, f, indent=2)
                    return True
                except Exception:
                    pass

        return False


# ============================================================================
# FUTURE: SATORI CHAIN DISTRIBUTOR
# ============================================================================

class SatoriChainDistributor(RewardDistributor):
    """
    Placeholder for future SAT chain native distribution.

    Will be implemented when the SAT chain is available.
    """

    def __init__(self, *args, **kwargs):
        """Initialize SatoriChainDistributor."""
        raise NotImplementedError(
            "SAT chain not yet available. Use EvrmoreDistributor for now."
        )

    async def distribute_round(
        self,
        round_summary: "RoundSummary"
    ) -> Dict[str, Any]:
        """Not yet implemented."""
        raise NotImplementedError()

    async def get_balance(self, address: str) -> float:
        """Not yet implemented."""
        raise NotImplementedError()

    async def verify_distribution(
        self,
        tx_hash: str,
        expected_merkle_root: str
    ) -> bool:
        """Not yet implemented."""
        raise NotImplementedError()
