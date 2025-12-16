"""
satorip2p/electrumx/connection.py

Low-level SSL/TCP connection handling for ElectrumX servers.
"""

import socket
import ssl
import json
import logging
from typing import Optional, Any

logger = logging.getLogger("satorip2p.electrumx.connection")


class ElectrumXConnection:
    """
    Manages socket connection to an ElectrumX server.

    Supports both SSL (port 50002) and TCP (port 50001) connections.
    """

    DEFAULT_TIMEOUT = 30  # seconds
    BUFFER_SIZE = 4096

    def __init__(
        self,
        host: str,
        port: int = 50002,
        use_ssl: bool = True,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        """
        Initialize connection parameters.

        Args:
            host: ElectrumX server hostname
            port: Server port (50002 for SSL, 50001 for TCP)
            use_ssl: Whether to use SSL encryption
            timeout: Connection timeout in seconds
        """
        self.host = host
        self.port = port
        self.use_ssl = use_ssl
        self.timeout = timeout

        self._socket: Optional[socket.socket] = None
        self._ssl_socket: Optional[ssl.SSLSocket] = None
        self._connected = False
        self._buffer = ""

    @property
    def connected(self) -> bool:
        """Check if connection is active."""
        return self._connected

    def connect(self) -> bool:
        """
        Establish connection to the server.

        Returns:
            True if connection successful
        """
        try:
            # Create base socket
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(self.timeout)

            if self.use_ssl:
                # Create SSL context (permissive for self-signed certs)
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE

                self._ssl_socket = context.wrap_socket(
                    self._socket,
                    server_hostname=self.host
                )
                self._ssl_socket.connect((self.host, self.port))
                logger.debug(f"SSL connection established to {self.host}:{self.port}")
            else:
                self._socket.connect((self.host, self.port))
                logger.debug(f"TCP connection established to {self.host}:{self.port}")

            self._connected = True
            return True

        except Exception as e:
            logger.error(f"Connection failed to {self.host}:{self.port}: {e}")
            self._connected = False
            self.close()
            return False

    def close(self) -> None:
        """Close the connection."""
        self._connected = False

        if self._ssl_socket:
            try:
                self._ssl_socket.close()
            except Exception:
                pass
            self._ssl_socket = None

        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

        self._buffer = ""

    def send(self, data: dict) -> bool:
        """
        Send JSON-RPC request to server.

        Args:
            data: Dictionary to send as JSON

        Returns:
            True if sent successfully
        """
        if not self._connected:
            return False

        try:
            payload = json.dumps(data) + "\n"
            sock = self._ssl_socket if self.use_ssl else self._socket
            sock.sendall(payload.encode("utf-8"))
            return True
        except Exception as e:
            logger.error(f"Send failed: {e}")
            self._connected = False
            return False

    def receive(self) -> Optional[dict]:
        """
        Receive JSON-RPC response from server.

        Returns:
            Parsed JSON response or None on error
        """
        if not self._connected:
            return None

        sock = self._ssl_socket if self.use_ssl else self._socket

        try:
            while "\n" not in self._buffer:
                chunk = sock.recv(self.BUFFER_SIZE)
                if not chunk:
                    logger.warning("Connection closed by server")
                    self._connected = False
                    return None
                self._buffer += chunk.decode("utf-8")

            # Extract first complete message
            line, self._buffer = self._buffer.split("\n", 1)
            return json.loads(line)

        except socket.timeout:
            logger.warning("Receive timeout")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response: {e}")
            return None
        except Exception as e:
            logger.error(f"Receive failed: {e}")
            self._connected = False
            return None

    def __enter__(self) -> "ElectrumXConnection":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
