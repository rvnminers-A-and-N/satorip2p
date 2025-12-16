"""
satorip2p/blockchain/

Blockchain integration for Satori P2P network.

Currently supports Evrmore (SATORI is an EVR asset).
Designed for future SAT chain or other backends.
"""

from .reward_distributor import (
    RewardDistributor,
    EvrmoreDistributor,
    MerkleTree,
    AssetBadgeIssuer,
    encode_round_summary_for_opreturn,
    decode_round_summary_from_opreturn,
)

from .tx_builder import (
    TransactionBuilder,
    UnsignedTransaction,
    TransactionOutput,
    build_distribution_transaction,
    SATORI_ASSET_NAME,
)

__all__ = [
    # Reward distribution
    "RewardDistributor",
    "EvrmoreDistributor",
    "MerkleTree",
    "AssetBadgeIssuer",
    "encode_round_summary_for_opreturn",
    "decode_round_summary_from_opreturn",
    # Transaction building (ElectrumX-based)
    "TransactionBuilder",
    "UnsignedTransaction",
    "TransactionOutput",
    "build_distribution_transaction",
    "SATORI_ASSET_NAME",
]
