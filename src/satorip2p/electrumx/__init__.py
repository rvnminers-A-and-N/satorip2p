"""
satorip2p/electrumx - ElectrumX client for Evrmore blockchain interaction.

Provides UTXO queries, transaction broadcasting, and blockchain state queries
for the Satori P2P network's reward distribution system.
"""

from .client import ElectrumXClient, ElectrumXError
from .connection import ElectrumXConnection

__all__ = [
    "ElectrumXClient",
    "ElectrumXConnection",
    "ElectrumXError",
]
