"""
satorip2p/electrumx/client.py

ElectrumX JSON-RPC client for Evrmore blockchain interaction.

Provides methods for:
- UTXO queries (for transaction building)
- Transaction broadcasting
- Balance queries
- Transaction lookups
"""

import hashlib
import logging
import time
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

from .connection import ElectrumXConnection

logger = logging.getLogger("satorip2p.electrumx.client")


# ============================================================================
# CONFIGURATION
# ============================================================================

# Satori ElectrumX servers (SSL on port 50002)
ELECTRUMX_SERVERS = [
    ("electrumx1.satorinet.io", 50002),
    ("electrumx2.satorinet.io", 50002),
    ("electrumx3.satorinet.io", 50002),
]

# Fallback servers (non-SSL on port 50001)
ELECTRUMX_SERVERS_NO_SSL = [
    ("electrumx1.satorinet.io", 50001),
    ("electrumx2.satorinet.io", 50001),
    ("electrumx3.satorinet.io", 50001),
]

# Client identification
CLIENT_NAME = "satorip2p"
CLIENT_VERSION = "1.0"
PROTOCOL_VERSION = "1.10"


# ============================================================================
# DATA STRUCTURES
# ============================================================================

class ElectrumXError(Exception):
    """Exception raised for ElectrumX errors."""
    pass


@dataclass
class UTXO:
    """Unspent transaction output."""
    txid: str
    vout: int
    value: int  # In satoshis
    height: int
    asset: Optional[str] = None  # Asset name if asset UTXO
    asset_amount: Optional[float] = None  # Asset amount if asset UTXO

    def to_dict(self) -> dict:
        return {
            "txid": self.txid,
            "vout": self.vout,
            "value": self.value,
            "height": self.height,
            "asset": self.asset,
            "asset_amount": self.asset_amount,
        }


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def address_to_scripthash(address: str) -> str:
    """
    Convert an Evrmore address to ElectrumX scripthash format.

    ElectrumX uses SHA256(scriptPubKey) reversed as hex.

    Args:
        address: Evrmore address (P2PKH starting with 'E')

    Returns:
        Scripthash as hex string
    """
    try:
        # Import here to avoid circular imports
        from evrmore.wallet import P2PKHEvrmoreAddress
        from evrmore.core.script import CScript

        # Parse address and get scriptPubKey
        addr = P2PKHEvrmoreAddress(address)
        script = addr.to_scriptPubKey()

        # SHA256 hash of script, reversed
        script_hash = hashlib.sha256(bytes(script)).digest()
        return script_hash[::-1].hex()

    except ImportError:
        logger.error("python-evrmorelib not available for address conversion")
        raise ElectrumXError("python-evrmorelib required for address conversion")
    except Exception as e:
        logger.error(f"Failed to convert address to scripthash: {e}")
        raise ElectrumXError(f"Invalid address: {address}")


# ============================================================================
# ELECTRUMX CLIENT
# ============================================================================

class ElectrumXClient:
    """
    ElectrumX JSON-RPC client for Evrmore.

    Provides high-level methods for blockchain interaction:
    - UTXO queries for transaction building
    - Transaction broadcasting
    - Balance queries

    Example:
        client = ElectrumXClient()
        client.connect()

        # Get UTXOs for an address
        utxos = client.get_unspent("ExxxxxxxxxxxxxxxxxxxxxxxxxxxxxxXX")

        # Broadcast a transaction
        txid = client.broadcast("0100000001...")

        client.close()
    """

    def __init__(
        self,
        servers: Optional[List[Tuple[str, int]]] = None,
        use_ssl: bool = True,
        timeout: float = 30.0,
    ):
        """
        Initialize the client.

        Args:
            servers: List of (host, port) tuples. Uses defaults if None.
            use_ssl: Whether to use SSL connections
            timeout: Connection timeout in seconds
        """
        if servers is None:
            servers = ELECTRUMX_SERVERS if use_ssl else ELECTRUMX_SERVERS_NO_SSL

        self.servers = servers
        self.use_ssl = use_ssl
        self.timeout = timeout

        self._connection: Optional[ElectrumXConnection] = None
        self._request_id = 0
        self._server_version: Optional[str] = None

    @property
    def connected(self) -> bool:
        """Check if connected to a server."""
        return self._connection is not None and self._connection.connected

    def connect(self, server_index: int = 0) -> bool:
        """
        Connect to an ElectrumX server.

        Tries servers in order until one connects successfully.

        Args:
            server_index: Starting server index

        Returns:
            True if connected successfully
        """
        # Try each server
        for i in range(len(self.servers)):
            idx = (server_index + i) % len(self.servers)
            host, port = self.servers[idx]

            logger.info(f"Connecting to ElectrumX server {host}:{port}...")

            self._connection = ElectrumXConnection(
                host=host,
                port=port,
                use_ssl=self.use_ssl,
                timeout=self.timeout,
            )

            if self._connection.connect():
                # Perform handshake
                if self._handshake():
                    logger.info(f"Connected to {host}:{port} (version: {self._server_version})")
                    return True
                else:
                    logger.warning(f"Handshake failed with {host}:{port}")
                    self._connection.close()
            else:
                logger.warning(f"Failed to connect to {host}:{port}")

        logger.error("Failed to connect to any ElectrumX server")
        return False

    def close(self) -> None:
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
        self._server_version = None

    def _handshake(self) -> bool:
        """
        Perform protocol handshake with server.

        Returns:
            True if handshake successful
        """
        try:
            result = self._call("server.version", CLIENT_NAME, PROTOCOL_VERSION)
            if result and isinstance(result, list) and len(result) >= 1:
                self._server_version = result[0]
                return True
            return False
        except Exception as e:
            logger.error(f"Handshake failed: {e}")
            return False

    def _next_id(self) -> int:
        """Get next request ID."""
        self._request_id += 1
        return self._request_id

    def _call(self, method: str, *params) -> Any:
        """
        Make a JSON-RPC call.

        Args:
            method: RPC method name
            *params: Method parameters

        Returns:
            Result from server

        Raises:
            ElectrumXError: On communication or server error
        """
        if not self._connection or not self._connection.connected:
            raise ElectrumXError("Not connected to server")

        request = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": method,
            "params": list(params),
        }

        if not self._connection.send(request):
            raise ElectrumXError("Failed to send request")

        response = self._connection.receive()
        if response is None:
            raise ElectrumXError("No response from server")

        if "error" in response and response["error"]:
            error = response["error"]
            msg = error.get("message", str(error)) if isinstance(error, dict) else str(error)
            raise ElectrumXError(f"Server error: {msg}")

        return response.get("result")

    # ========================================================================
    # PUBLIC API
    # ========================================================================

    def ping(self) -> bool:
        """
        Ping the server to check connection.

        Returns:
            True if server responds
        """
        try:
            self._call("server.ping")
            return True
        except Exception:
            return False

    def get_balance(self, address: str) -> Dict[str, int]:
        """
        Get confirmed and unconfirmed balance for an address.

        Args:
            address: Evrmore address

        Returns:
            Dict with 'confirmed' and 'unconfirmed' balances in satoshis
        """
        scripthash = address_to_scripthash(address)
        result = self._call("blockchain.scripthash.get_balance", scripthash)
        return {
            "confirmed": result.get("confirmed", 0),
            "unconfirmed": result.get("unconfirmed", 0),
        }

    def get_unspent(self, address: str) -> List[UTXO]:
        """
        Get unspent transaction outputs for an address.

        Args:
            address: Evrmore address

        Returns:
            List of UTXO objects
        """
        scripthash = address_to_scripthash(address)
        result = self._call("blockchain.scripthash.listunspent", scripthash)

        utxos = []
        for item in result:
            utxo = UTXO(
                txid=item["tx_hash"],
                vout=item["tx_pos"],
                value=item["value"],
                height=item.get("height", 0),
            )
            utxos.append(utxo)

        return utxos

    def get_asset_unspent(
        self,
        address: str,
        asset: str = "SATORI"
    ) -> List[UTXO]:
        """
        Get unspent asset outputs for an address.

        Args:
            address: Evrmore address
            asset: Asset name (default: SATORI)

        Returns:
            List of UTXO objects with asset information
        """
        scripthash = address_to_scripthash(address)

        # Query with asset filter
        result = self._call("blockchain.scripthash.listunspent", scripthash)

        # Filter for asset UTXOs
        # Note: ElectrumX returns asset info in the UTXO data
        utxos = []
        for item in result:
            # Check if this is an asset UTXO
            if "asset" in item or item.get("value", 0) == 0:
                item_asset = item.get("asset", {})
                if isinstance(item_asset, dict):
                    asset_name = item_asset.get("name", "")
                    asset_amount = item_asset.get("amount", 0)
                else:
                    asset_name = str(item_asset) if item_asset else ""
                    asset_amount = item.get("asset_amount", 0)

                if asset_name == asset:
                    utxo = UTXO(
                        txid=item["tx_hash"],
                        vout=item["tx_pos"],
                        value=item.get("value", 0),
                        height=item.get("height", 0),
                        asset=asset_name,
                        asset_amount=asset_amount,
                    )
                    utxos.append(utxo)

        return utxos

    def get_transaction(self, txid: str, verbose: bool = True) -> Dict[str, Any]:
        """
        Get transaction details.

        Args:
            txid: Transaction hash
            verbose: If True, return decoded transaction

        Returns:
            Transaction data
        """
        result = self._call("blockchain.transaction.get", txid, verbose)
        return result

    def broadcast(self, tx_hex: str) -> str:
        """
        Broadcast a signed transaction to the network.

        Args:
            tx_hex: Hex-encoded signed transaction

        Returns:
            Transaction ID (txid) if successful

        Raises:
            ElectrumXError: If broadcast fails
        """
        result = self._call("blockchain.transaction.broadcast", tx_hex)

        if isinstance(result, str) and len(result) == 64:
            logger.info(f"Transaction broadcast successful: {result}")
            return result
        else:
            raise ElectrumXError(f"Broadcast failed: {result}")

    def get_block_height(self) -> int:
        """
        Get current blockchain height.

        Returns:
            Current block height
        """
        result = self._call("blockchain.headers.subscribe")
        return result.get("height", 0)

    def get_fee_estimate(self, blocks: int = 6) -> float:
        """
        Get estimated fee rate.

        Args:
            blocks: Target confirmation blocks

        Returns:
            Fee rate in EVR/kB
        """
        try:
            result = self._call("blockchain.estimatefee", blocks)
            return float(result) if result and result > 0 else 0.00001
        except Exception:
            # Return minimum fee if estimation fails
            return 0.00001

    # ========================================================================
    # CONTEXT MANAGER
    # ========================================================================

    def __enter__(self) -> "ElectrumXClient":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_client(use_ssl: bool = True, timeout: float = 30.0) -> ElectrumXClient:
    """
    Create and connect an ElectrumX client.

    Args:
        use_ssl: Whether to use SSL
        timeout: Connection timeout

    Returns:
        Connected ElectrumXClient

    Raises:
        ElectrumXError: If connection fails
    """
    client = ElectrumXClient(use_ssl=use_ssl, timeout=timeout)
    if not client.connect():
        raise ElectrumXError("Failed to connect to any ElectrumX server")
    return client
