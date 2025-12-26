"""
satorip2p.compat.centrifugo - Centrifugo Compatible WebSocket/REST Server

Provides a WebSocket and REST server that implements the Centrifugo protocol
used by Satori Network (satorilib/centrifugo/client.py), allowing legacy
clients to connect to the P2P network without modification.

Protocol:
    - WebSocket: wss://host:port/connection/websocket
    - Auth: JWT token (HS256) with user ID
    - Channels: "streams:{uuid}" format
    - REST: POST /api/publish for publishing

Usage:
    from satorip2p import Peers
    from satorip2p.compat import CentrifugoServer

    peers = Peers(identity=identity)
    await peers.start()

    centrifugo = CentrifugoServer(
        peers,
        port=8000,
        jwt_secret="your_secret",
    )
    await centrifugo.start()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(peers.run_forever)
        nursery.start_soon(centrifugo.run_forever)
"""

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set
import trio
import json
import logging
import time
import hmac
import hashlib
import base64

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.compat.centrifugo")


class CentrifugoClient:
    """Represents a connected Centrifugo WebSocket client."""

    def __init__(
        self,
        client_id: str,
        user_id: str,
        websocket,
        send_channel: trio.MemorySendChannel,
    ):
        self.client_id = client_id
        self.user_id = user_id
        self.websocket = websocket
        self.send_channel = send_channel
        self.subscriptions: Dict[str, Any] = {}  # channel -> subscription_info
        self.connected = True
        self.connected_at = time.time()

    async def send(self, message: dict) -> bool:
        """Send JSON message to client."""
        if not self.connected:
            return False
        try:
            await self.send_channel.send(json.dumps(message))
            return True
        except trio.ClosedResourceError:
            self.connected = False
            return False


class CentrifugoServer:
    """
    Centrifugo Compatible WebSocket/REST Server.

    Bridges legacy Satori Centrifugo clients to the P2P network by:
    1. Accepting WebSocket connections with JWT authentication
    2. Handling subscribe/publish in Centrifugo protocol format
    3. Providing REST API for publishing (/api/publish)
    4. Routing messages between WebSocket clients and libp2p GossipSub

    Attributes:
        peers: The Peers instance for P2P communication
        host: Host address to bind to
        port: Port to listen on
        jwt_secret: Secret key for JWT token verification
    """

    def __init__(
        self,
        peers: "Peers",
        host: str = "0.0.0.0",
        port: int = 8000,
        jwt_secret: Optional[str] = None,
        skip_jwt_verify: bool = False,
    ):
        """
        Initialize CentrifugoServer.

        Args:
            peers: Peers instance for P2P communication
            host: Host address to bind (default: all interfaces)
            port: Server port (default: 8000)
            jwt_secret: Secret for JWT verification (HS256)
            skip_jwt_verify: If True, skip JWT verification (for testing)
        """
        self.peers = peers
        self.host = host
        self.port = port
        self.jwt_secret = jwt_secret
        self.skip_jwt_verify = skip_jwt_verify

        # Client management
        self._clients: Dict[str, CentrifugoClient] = {}  # client_id -> client
        self._user_clients: Dict[str, Set[str]] = {}  # user_id -> set of client_ids
        self._channel_clients: Dict[str, Set[str]] = {}  # channel -> set of client_ids

        # ID generation
        self._client_counter = 0

        # State
        self._started = False
        self._nursery: Optional[trio.Nursery] = None

    @property
    def client_count(self) -> int:
        """Number of connected clients."""
        return len(self._clients)

    @property
    def channel_count(self) -> int:
        """Number of active channels."""
        return len(self._channel_clients)

    def _generate_client_id(self) -> str:
        """Generate unique client ID."""
        self._client_counter += 1
        return f"p2p-{self._client_counter}-{int(time.time() * 1000)}"

    async def start(self) -> bool:
        """
        Start the Centrifugo-compatible server.

        Returns:
            True if started successfully
        """
        if self._started:
            logger.warning("CentrifugoServer already started")
            return True

        try:
            from trio_websocket import serve_websocket

            logger.info(f"Starting CentrifugoServer on {self.host}:{self.port}")
            self._started = True
            return True

        except ImportError:
            logger.error("trio-websocket not installed. Install with: pip install trio-websocket")
            return False
        except Exception as e:
            logger.error(f"Failed to start CentrifugoServer: {e}")
            return False

    async def stop(self) -> None:
        """Stop the server."""
        if not self._started:
            return

        logger.info("Stopping CentrifugoServer...")

        # Disconnect all clients
        for client in list(self._clients.values()):
            client.connected = False

        self._clients.clear()
        self._user_clients.clear()
        self._channel_clients.clear()
        self._started = False

        logger.info("CentrifugoServer stopped")

    async def run_forever(self) -> None:
        """
        Run the server until cancelled.

        Starts both WebSocket server and HTTP server for REST API.
        """
        if not self._started:
            if not await self.start():
                return

        try:
            from trio_websocket import serve_websocket

            async with trio.open_nursery() as nursery:
                self._nursery = nursery

                # Start WebSocket server for /connection/websocket
                ws_port = self.port
                nursery.start_soon(self._run_websocket_server, ws_port)

                # Start HTTP server for REST API
                http_port = self.port + 1
                nursery.start_soon(self._run_http_server, http_port)

                logger.info(f"CentrifugoServer listening:")
                logger.info(f"  WebSocket: ws://{self.host}:{ws_port}/connection/websocket")
                logger.info(f"  REST API:  http://{self.host}:{http_port}/api/publish")

                while True:
                    await trio.sleep(1)

        except trio.Cancelled:
            logger.info("CentrifugoServer cancelled")
            raise
        except Exception as e:
            logger.error(f"CentrifugoServer error: {e}")
            import traceback
            traceback.print_exc()

    async def _run_websocket_server(self, port: int) -> None:
        """Run WebSocket server."""
        from trio_websocket import serve_websocket

        await serve_websocket(
            self._handle_websocket,
            self.host,
            port,
            None,  # ssl_context
        )

    async def _run_http_server(self, port: int) -> None:
        """Run simple HTTP server for REST API."""
        # Use trio's TCP listener for basic HTTP
        listeners = await trio.open_tcp_listeners(port, host=self.host)
        async with listeners[0] as listener:
            while True:
                conn = await listener.accept()
                self._nursery.start_soon(self._handle_http_request, conn)

    async def _handle_http_request(self, conn) -> None:
        """Handle HTTP request for REST API."""
        try:
            async with conn:
                # Read request
                data = b""
                while b"\r\n\r\n" not in data:
                    chunk = await conn.receive_some(4096)
                    if not chunk:
                        return
                    data += chunk

                # Parse request
                header_end = data.index(b"\r\n\r\n")
                headers = data[:header_end].decode("utf-8")
                body = data[header_end + 4:]

                # Get Content-Length if present
                content_length = 0
                for line in headers.split("\r\n"):
                    if line.lower().startswith("content-length:"):
                        content_length = int(line.split(":", 1)[1].strip())
                        break

                # Read remaining body if needed
                while len(body) < content_length:
                    chunk = await conn.receive_some(4096)
                    if not chunk:
                        break
                    body += chunk

                # Parse request line
                first_line = headers.split("\r\n")[0]
                method, path, _ = first_line.split(" ", 2)

                # Route request
                if method == "POST" and path == "/api/publish":
                    response = await self._handle_publish_api(headers, body)
                elif method == "OPTIONS":
                    response = self._cors_response()
                else:
                    response = self._error_response(404, "Not Found")

                await conn.send_all(response.encode("utf-8"))

        except Exception as e:
            logger.debug(f"HTTP request error: {e}")

    async def _handle_publish_api(self, headers: str, body: bytes) -> str:
        """Handle POST /api/publish REST endpoint."""
        try:
            # Check authorization
            auth_header = None
            for line in headers.split("\r\n"):
                if line.lower().startswith("authorization:"):
                    auth_header = line.split(":", 1)[1].strip()
                    break

            if auth_header and not self.skip_jwt_verify:
                # Verify Bearer token
                if not auth_header.startswith("Bearer "):
                    return self._error_response(401, "Invalid authorization header")
                token = auth_header[7:]
                if not self._verify_jwt(token):
                    return self._error_response(401, "Invalid token")

            # Parse body
            payload = json.loads(body.decode("utf-8"))
            channel = payload.get("channel", "")
            data = payload.get("data", {})

            # Extract stream ID from channel (format: "streams:{uuid}")
            if channel.startswith("streams:"):
                stream_id = channel[8:]
            else:
                stream_id = channel

            # Publish via P2P
            await self.peers.publish(stream_id, data)

            # Broadcast to WebSocket clients
            await self._broadcast_to_channel(channel, data)

            # Success response
            return self._json_response({"result": {}})

        except json.JSONDecodeError:
            return self._error_response(400, "Invalid JSON")
        except Exception as e:
            logger.error(f"Publish API error: {e}")
            return self._error_response(500, str(e))

    def _json_response(self, data: dict, status: int = 200) -> str:
        """Create JSON HTTP response."""
        body = json.dumps(data)
        return (
            f"HTTP/1.1 {status} OK\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Access-Control-Allow-Origin: *\r\n"
            f"\r\n"
            f"{body}"
        )

    def _error_response(self, status: int, message: str) -> str:
        """Create error HTTP response."""
        body = json.dumps({"error": {"code": status, "message": message}})
        status_text = {400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 500: "Internal Server Error"}
        return (
            f"HTTP/1.1 {status} {status_text.get(status, 'Error')}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Access-Control-Allow-Origin: *\r\n"
            f"\r\n"
            f"{body}"
        )

    def _cors_response(self) -> str:
        """Create CORS preflight response."""
        return (
            "HTTP/1.1 204 No Content\r\n"
            "Access-Control-Allow-Origin: *\r\n"
            "Access-Control-Allow-Methods: POST, OPTIONS\r\n"
            "Access-Control-Allow-Headers: Content-Type, Authorization\r\n"
            "Access-Control-Max-Age: 86400\r\n"
            "\r\n"
        )

    async def _handle_websocket(self, request) -> None:
        """Handle WebSocket connection."""
        try:
            # Accept connection
            ws = await request.accept()

            # Create client with temporary ID (will be updated on connect message)
            client_id = self._generate_client_id()
            send_channel, receive_channel = trio.open_memory_channel(100)

            client = CentrifugoClient(client_id, "", ws, send_channel)
            self._clients[client_id] = client

            try:
                async with trio.open_nursery() as client_nursery:
                    client_nursery.start_soon(
                        self._send_loop, client, receive_channel
                    )
                    client_nursery.start_soon(
                        self._receive_loop, client
                    )

            except Exception as e:
                logger.debug(f"Client {client_id} disconnected: {e}")

            finally:
                client.connected = False
                self._unregister_client(client_id)
                try:
                    await send_channel.aclose()
                    await receive_channel.aclose()
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"WebSocket handler error: {e}")

    async def _send_loop(
        self,
        client: CentrifugoClient,
        receive_channel: trio.MemoryReceiveChannel,
    ) -> None:
        """Send messages to WebSocket."""
        try:
            async for message in receive_channel:
                if not client.connected:
                    break
                try:
                    await client.websocket.send_message(message)
                except Exception:
                    break
        except trio.ClosedResourceError:
            pass

    async def _receive_loop(self, client: CentrifugoClient) -> None:
        """Receive and process Centrifugo protocol messages."""
        try:
            while client.connected:
                try:
                    message = await client.websocket.get_message()
                    await self._process_message(client, message)
                except Exception as e:
                    logger.debug(f"Receive error: {e}")
                    break
        except trio.ClosedResourceError:
            pass
        finally:
            client.connected = False

    async def _process_message(self, client: CentrifugoClient, message: str) -> None:
        """
        Process Centrifugo protocol message.

        Centrifugo uses JSON messages with format:
        {
            "id": <request_id>,
            "method": <method_name>,
            "params": {...}
        }
        """
        try:
            data = json.loads(message)

            # Handle batched commands (array of commands)
            if isinstance(data, list):
                for cmd in data:
                    await self._handle_command(client, cmd)
            else:
                await self._handle_command(client, data)

        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON from client: {e}")
        except Exception as e:
            logger.error(f"Message processing error: {e}")

    async def _handle_command(self, client: CentrifugoClient, cmd: dict) -> None:
        """Handle single Centrifugo command."""
        request_id = cmd.get("id", 1)
        method = cmd.get("method", "")
        params = cmd.get("params", {})

        if method == "connect":
            await self._handle_connect(client, request_id, params)
        elif method == "subscribe":
            await self._handle_subscribe(client, request_id, params)
        elif method == "unsubscribe":
            await self._handle_unsubscribe(client, request_id, params)
        elif method == "publish":
            await self._handle_publish(client, request_id, params)
        elif method == "ping":
            await self._handle_ping(client, request_id)
        else:
            logger.debug(f"Unknown method: {method}")
            await client.send({
                "id": request_id,
                "error": {"code": 102, "message": f"unknown method: {method}"}
            })

    async def _handle_connect(self, client: CentrifugoClient, request_id: int, params: dict) -> None:
        """Handle connect command with JWT authentication."""
        token = params.get("token", "")

        if not self.skip_jwt_verify and self.jwt_secret:
            payload = self._verify_jwt(token)
            if not payload:
                await client.send({
                    "id": request_id,
                    "error": {"code": 101, "message": "unauthorized"}
                })
                return
            user_id = payload.get("sub", "")
        else:
            # Skip verification - use token as user_id or generate one
            user_id = params.get("name", "") or f"user-{client.client_id}"

        # Update client with user info
        client.user_id = user_id

        # Track user -> clients mapping
        if user_id not in self._user_clients:
            self._user_clients[user_id] = set()
        self._user_clients[user_id].add(client.client_id)

        logger.info(f"Client connected: {client.client_id} (user: {user_id})")

        # Send connect response
        await client.send({
            "id": request_id,
            "result": {
                "client": client.client_id,
                "version": "0.0.1",
                "expires": False,
                "ttl": 0,
            }
        })

    async def _handle_subscribe(self, client: CentrifugoClient, request_id: int, params: dict) -> None:
        """Handle subscribe command."""
        channel = params.get("channel", "")

        if not channel:
            await client.send({
                "id": request_id,
                "error": {"code": 103, "message": "channel required"}
            })
            return

        # Add to channel tracking
        if channel not in self._channel_clients:
            self._channel_clients[channel] = set()
            # First subscriber - set up P2P subscription
            await self._setup_p2p_subscription(channel)

        self._channel_clients[channel].add(client.client_id)
        client.subscriptions[channel] = {"subscribed_at": time.time()}

        logger.debug(f"Client {client.client_id} subscribed to {channel}")

        # Send subscribe response
        await client.send({
            "id": request_id,
            "result": {
                "recoverable": False,
                "epoch": "0",
                "publications": [],
            }
        })

    async def _handle_unsubscribe(self, client: CentrifugoClient, request_id: int, params: dict) -> None:
        """Handle unsubscribe command."""
        channel = params.get("channel", "")

        if channel in client.subscriptions:
            del client.subscriptions[channel]
            if channel in self._channel_clients:
                self._channel_clients[channel].discard(client.client_id)
                if not self._channel_clients[channel]:
                    del self._channel_clients[channel]

        await client.send({
            "id": request_id,
            "result": {}
        })

    async def _handle_publish(self, client: CentrifugoClient, request_id: int, params: dict) -> None:
        """Handle publish command."""
        channel = params.get("channel", "")
        data = params.get("data", {})

        if not channel:
            await client.send({
                "id": request_id,
                "error": {"code": 103, "message": "channel required"}
            })
            return

        # Extract stream ID
        if channel.startswith("streams:"):
            stream_id = channel[8:]
        else:
            stream_id = channel

        # Publish via P2P
        await self.peers.publish(stream_id, data)

        # Broadcast to other WebSocket clients
        await self._broadcast_to_channel(channel, data, exclude_client=client.client_id)

        await client.send({
            "id": request_id,
            "result": {}
        })

    async def _handle_ping(self, client: CentrifugoClient, request_id: int) -> None:
        """Handle ping command."""
        await client.send({
            "id": request_id,
            "result": {}
        })

    def _unregister_client(self, client_id: str) -> None:
        """Unregister client and cleanup."""
        client = self._clients.pop(client_id, None)
        if not client:
            return

        # Remove from user tracking
        if client.user_id in self._user_clients:
            self._user_clients[client.user_id].discard(client_id)
            if not self._user_clients[client.user_id]:
                del self._user_clients[client.user_id]

        # Remove from channel tracking
        for channel in client.subscriptions:
            if channel in self._channel_clients:
                self._channel_clients[channel].discard(client_id)
                if not self._channel_clients[channel]:
                    del self._channel_clients[channel]

        logger.info(f"Client disconnected: {client_id}")

    async def _setup_p2p_subscription(self, channel: str) -> None:
        """Set up P2P subscription for a channel."""
        # Extract stream ID
        if channel.startswith("streams:"):
            stream_id = channel[8:]
        else:
            stream_id = channel

        def on_message(sid: str, data: Any) -> None:
            """Route P2P messages to WebSocket clients."""
            if self._nursery:
                self._nursery.start_soon(
                    self._broadcast_to_channel, f"streams:{sid}", data
                )

        await self.peers.subscribe_async(stream_id, on_message)
        logger.debug(f"P2P subscription set up for channel {channel}")

    async def _broadcast_to_channel(
        self,
        channel: str,
        data: Any,
        exclude_client: Optional[str] = None,
    ) -> None:
        """Broadcast data to all clients subscribed to a channel."""
        client_ids = self._channel_clients.get(channel, set())

        # Create publication message
        pub_message = {
            "push": {
                "channel": channel,
                "pub": {
                    "data": data,
                    "offset": int(time.time() * 1000),
                }
            }
        }

        for client_id in client_ids:
            if client_id == exclude_client:
                continue

            client = self._clients.get(client_id)
            if client and client.connected:
                await client.send(pub_message)

    def _verify_jwt(self, token: str) -> Optional[dict]:
        """
        Verify JWT token and return payload.

        Supports HS256 algorithm.
        """
        if not self.jwt_secret:
            return None

        try:
            # Split token
            parts = token.split(".")
            if len(parts) != 3:
                return None

            header_b64, payload_b64, signature_b64 = parts

            # Verify signature
            message = f"{header_b64}.{payload_b64}".encode()
            expected_sig = base64.urlsafe_b64encode(
                hmac.new(
                    self.jwt_secret.encode(),
                    message,
                    hashlib.sha256
                ).digest()
            ).rstrip(b"=").decode()

            # Compare signatures (remove padding from incoming)
            actual_sig = signature_b64.rstrip("=")
            if not hmac.compare_digest(expected_sig, actual_sig):
                return None

            # Decode payload
            # Add padding if needed
            payload_b64 += "=" * (4 - len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))

            # Check expiration
            if "exp" in payload and payload["exp"] < time.time():
                return None

            return payload

        except Exception as e:
            logger.debug(f"JWT verification failed: {e}")
            return None

    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        return {
            "host": self.host,
            "port": self.port,
            "running": self._started,
            "client_count": self.client_count,
            "channel_count": self.channel_count,
            "channels": {
                ch: len(ids)
                for ch, ids in self._channel_clients.items()
            },
        }

    def __repr__(self) -> str:
        status = "running" if self._started else "stopped"
        return f"CentrifugoServer(status={status}, clients={self.client_count}, channels={self.channel_count})"
