"""
satorip2p.compat.server - Central Server REST API Compatibility Layer

Provides REST endpoints compatible with the Satori central server
(satorilib/server/server.py), allowing the P2P network to handle
registrations and checkins that would normally go to central.satorinet.io.

Endpoints:
    - POST /checkin - Node checkin (returns keys, subscriptions, publications)
    - POST /register/stream - Register a new data stream
    - POST /register/subscription - Subscribe to a stream
    - POST /register/wallet - Register wallet
    - GET /centrifugo/token - Get Centrifugo JWT token
    - POST /get/streams - Query stream metadata
    - POST /my/streams - Get user's streams

Usage:
    from satorip2p import Peers
    from satorip2p.compat.server import ServerAPI

    peers = Peers(identity=identity)
    await peers.start()

    server_api = ServerAPI(peers, port=8080)
    await server_api.start()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(peers.run_forever)
        nursery.start_soon(server_api.run_forever)
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union
import trio
import json
import time
import logging
import hashlib
import hmac
import base64
import uuid

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.compat.server")


class CheckinDetails:
    """
    Response object for /checkin endpoint.

    Matches the structure from satorilib/server/api.py
    """

    def __init__(
        self,
        key: str,
        oracle_key: str = "",
        id_key: str = "",
        subscription_keys: Optional[List[str]] = None,
        publication_keys: Optional[List[str]] = None,
        subscriptions: Optional[List[dict]] = None,
        publications: Optional[List[dict]] = None,
        wallet: Optional[dict] = None,
        stake_required: str = "0",
        reward_address: str = "",
    ):
        self.key = key
        self.oracle_key = oracle_key
        self.id_key = id_key
        self.subscription_keys = subscription_keys or []
        self.publication_keys = publication_keys or []
        self.subscriptions = subscriptions or []
        self.publications = publications or []
        self.wallet = wallet or {}
        self.stake_required = stake_required
        self.reward_address = reward_address

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "oracleKey": self.oracle_key,
            "idKey": self.id_key,
            "subscriptionKeys": self.subscription_keys,
            "publicationKeys": self.publication_keys,
            "subscriptions": json.dumps(self.subscriptions),
            "publications": json.dumps(self.publications),
            "wallet": self.wallet,
            "stakeRequired": self.stake_required,
            "rewardaddress": self.reward_address,
        }


class StreamRegistration:
    """Represents a registered stream."""

    def __init__(
        self,
        stream_id: str,
        source: str,
        author: str,
        stream: str,
        target: str = "",
        cadence: float = 0,
        datatype: str = "float",
        description: str = "",
        registered_at: float = 0,
    ):
        self.stream_id = stream_id
        self.source = source
        self.author = author
        self.stream = stream
        self.target = target
        self.cadence = cadence
        self.datatype = datatype
        self.description = description
        self.registered_at = registered_at or time.time()

    def to_dict(self) -> dict:
        return {
            "uuid": self.stream_id,
            "source": self.source,
            "author": self.author,
            "stream": self.stream,
            "target": self.target,
            "cadence": self.cadence,
            "datatype": self.datatype,
            "description": self.description,
        }


class ServerAPI:
    """
    Central Server REST API Compatibility Layer.

    Provides REST endpoints that replicate the Satori central server,
    allowing nodes to register and discover streams via P2P instead
    of central servers.

    Attributes:
        peers: The Peers instance for P2P communication
        host: Host address to bind to
        port: Port to listen on
        jwt_secret: Secret for generating Centrifugo tokens
    """

    def __init__(
        self,
        peers: "Peers",
        host: str = "0.0.0.0",
        port: int = 8080,
        jwt_secret: Optional[str] = None,
    ):
        """
        Initialize ServerAPI.

        Args:
            peers: Peers instance for P2P communication
            host: Host address to bind (default: all interfaces)
            port: REST API port (default: 8080)
            jwt_secret: Secret for Centrifugo JWT generation
        """
        self.peers = peers
        self.host = host
        self.port = port
        self.jwt_secret = jwt_secret or self._generate_secret()

        # In-memory registrations (would be DHT in production)
        self._registered_wallets: Dict[str, dict] = {}
        self._registered_streams: Dict[str, StreamRegistration] = {}
        self._subscriptions: Dict[str, Set[str]] = {}  # wallet -> set of stream_ids
        self._publications: Dict[str, Set[str]] = {}   # wallet -> set of stream_ids

        # Session keys for authenticated clients
        self._session_keys: Dict[str, str] = {}  # wallet_address -> session_key

        # State
        self._started = False
        self._nursery: Optional[trio.Nursery] = None

    def _generate_secret(self) -> str:
        """Generate a random JWT secret."""
        return base64.b64encode(hashlib.sha256(
            str(time.time()).encode() + str(uuid.uuid4()).encode()
        ).digest()).decode()

    def _generate_session_key(self, wallet_address: str) -> str:
        """Generate a session key for a wallet."""
        key = base64.b64encode(hashlib.sha256(
            f"{wallet_address}:{time.time()}:{uuid.uuid4()}".encode()
        ).digest()).decode()[:32]
        self._session_keys[wallet_address] = key
        return key

    def _generate_stream_uuid(self, source: str, author: str, stream: str, target: str = "") -> str:
        """Generate UUID for a stream (matches Satori's UUID generation)."""
        # UUID v5 using a namespace
        namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # URL namespace
        name = f"{source}:{author}:{stream}:{target}"
        return str(uuid.uuid5(namespace, name))

    async def start(self) -> bool:
        """Start the REST API server."""
        if self._started:
            logger.warning("ServerAPI already started")
            return True

        logger.info(f"Starting ServerAPI on http://{self.host}:{self.port}")
        self._started = True
        return True

    async def stop(self) -> None:
        """Stop the REST API server."""
        if not self._started:
            return

        logger.info("Stopping ServerAPI...")
        self._started = False
        logger.info("ServerAPI stopped")

    async def run_forever(self) -> None:
        """Run the REST API server until cancelled."""
        if not self._started:
            if not await self.start():
                return

        try:
            async with trio.open_nursery() as nursery:
                self._nursery = nursery

                # Start HTTP server
                listeners = await trio.open_tcp_listeners(self.port, host=self.host)

                async with listeners[0] as listener:
                    logger.info(f"ServerAPI listening on http://{self.host}:{self.port}")

                    while True:
                        conn = await listener.accept()
                        nursery.start_soon(self._handle_request, conn)

        except trio.Cancelled:
            logger.info("ServerAPI cancelled")
            raise
        except Exception as e:
            logger.error(f"ServerAPI error: {e}")
            import traceback
            traceback.print_exc()

    async def _handle_request(self, conn) -> None:
        """Handle incoming HTTP request."""
        try:
            async with conn:
                # Read request
                data = b""
                while b"\r\n\r\n" not in data:
                    chunk = await conn.receive_some(4096)
                    if not chunk:
                        return
                    data += chunk

                # Parse headers
                header_end = data.index(b"\r\n\r\n")
                headers_raw = data[:header_end].decode("utf-8")
                body = data[header_end + 4:]

                # Get Content-Length
                content_length = 0
                headers = {}
                for line in headers_raw.split("\r\n")[1:]:
                    if ":" in line:
                        key, value = line.split(":", 1)
                        headers[key.lower().strip()] = value.strip()
                        if key.lower().strip() == "content-length":
                            content_length = int(value.strip())

                # Read remaining body
                while len(body) < content_length:
                    chunk = await conn.receive_some(4096)
                    if not chunk:
                        break
                    body += chunk

                # Parse request line
                first_line = headers_raw.split("\r\n")[0]
                parts = first_line.split(" ")
                method = parts[0]
                path = parts[1] if len(parts) > 1 else "/"

                # Route request
                response = await self._route_request(method, path, headers, body)
                await conn.send_all(response.encode("utf-8"))

        except Exception as e:
            logger.debug(f"Request handler error: {e}")

    async def _route_request(
        self,
        method: str,
        path: str,
        headers: dict,
        body: bytes,
    ) -> str:
        """Route request to appropriate handler."""

        # CORS preflight
        if method == "OPTIONS":
            return self._cors_response()

        # Parse path (remove query string)
        path = path.split("?")[0]

        try:
            # Authenticated endpoints
            if path == "/checkin" and method == "POST":
                return await self._handle_checkin(headers, body)

            elif path == "/register/wallet" and method == "POST":
                return await self._handle_register_wallet(headers, body)

            elif path == "/register/stream" and method == "POST":
                return await self._handle_register_stream(headers, body)

            elif path == "/register/subscription" and method == "POST":
                return await self._handle_register_subscription(headers, body)

            elif path == "/centrifugo/token" and method == "GET":
                return await self._handle_centrifugo_token(headers)

            elif path == "/get/streams" and method == "POST":
                return await self._handle_get_streams(headers, body)

            elif path == "/my/streams" and method == "POST":
                return await self._handle_my_streams(headers, body)

            # Unauthenticated endpoints
            elif path == "/streams/search" and method == "POST":
                return await self._handle_search_streams(body)

            elif path == "/health" and method == "GET":
                return self._json_response({"status": "healthy", "peers": self.peers.connected_peers})

            else:
                return self._error_response(404, f"Not Found: {path}")

        except Exception as e:
            logger.error(f"Request error for {path}: {e}")
            import traceback
            traceback.print_exc()
            return self._error_response(500, str(e))

    def _verify_auth(self, headers: dict) -> Optional[str]:
        """
        Verify wallet authentication from headers.

        Returns wallet address if valid, None otherwise.
        """
        public_key = headers.get("publickey")
        signature = headers.get("signature")
        address = headers.get("address")

        if not all([public_key, signature, address]):
            return None

        # In production, verify signature against challenge
        # For now, trust the address if all headers present
        return address

    async def _handle_checkin(self, headers: dict, body: bytes) -> str:
        """
        Handle POST /checkin - Main node checkin.

        Returns session key, subscriptions, and publications.
        """
        wallet_address = self._verify_auth(headers)
        if not wallet_address:
            return self._error_response(401, "Unauthorized")

        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except json.JSONDecodeError:
            payload = {}

        # Generate session key
        session_key = self._generate_session_key(wallet_address)

        # Get wallet's subscriptions and publications
        subscriptions = []
        publications = []

        sub_ids = self._subscriptions.get(wallet_address, set())
        pub_ids = self._publications.get(wallet_address, set())

        for stream_id in sub_ids:
            if stream_id in self._registered_streams:
                subscriptions.append(self._registered_streams[stream_id].to_dict())

        for stream_id in pub_ids:
            if stream_id in self._registered_streams:
                publications.append(self._registered_streams[stream_id].to_dict())

        # Also discover via P2P
        p2p_subs = self.peers.get_my_subscriptions()
        p2p_pubs = self.peers.get_my_publications()

        # Create checkin response
        details = CheckinDetails(
            key=session_key,
            oracle_key=f"oracle-{wallet_address[:8]}",
            id_key=f"id-{wallet_address[:8]}",
            subscription_keys=[f"sub-{s[:8]}" for s in sub_ids],
            publication_keys=[f"pub-{p[:8]}" for p in pub_ids],
            subscriptions=subscriptions,
            publications=publications,
            wallet={
                "address": wallet_address,
                "publicKey": headers.get("publickey", ""),
            },
            reward_address=wallet_address,
        )

        logger.info(f"Checkin successful for {wallet_address[:16]}...")
        return self._json_response(details.to_dict())

    async def _handle_register_wallet(self, headers: dict, body: bytes) -> str:
        """Handle POST /register/wallet - Register a wallet."""
        wallet_address = self._verify_auth(headers)
        if not wallet_address:
            return self._error_response(401, "Unauthorized")

        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except json.JSONDecodeError:
            payload = {}

        self._registered_wallets[wallet_address] = {
            "address": wallet_address,
            "publicKey": headers.get("publickey", ""),
            "registered_at": time.time(),
            **payload,
        }

        logger.info(f"Wallet registered: {wallet_address[:16]}...")
        return self._json_response({"status": "success", "address": wallet_address})

    async def _handle_register_stream(self, headers: dict, body: bytes) -> str:
        """
        Handle POST /register/stream - Register a data stream.

        Expected payload: {"source": "...", "name": "...", "target": "..."}
        """
        wallet_address = self._verify_auth(headers)
        if not wallet_address:
            return self._error_response(401, "Unauthorized")

        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except json.JSONDecodeError:
            return self._error_response(400, "Invalid JSON")

        source = payload.get("source", "satori")
        stream_name = payload.get("name") or payload.get("stream", "")
        target = payload.get("target", "")

        if not stream_name:
            return self._error_response(400, "Stream name required")

        # Generate stream UUID
        stream_id = self._generate_stream_uuid(source, wallet_address, stream_name, target)

        # Register stream
        registration = StreamRegistration(
            stream_id=stream_id,
            source=source,
            author=wallet_address,
            stream=stream_name,
            target=target,
            cadence=payload.get("cadence", 0),
            datatype=payload.get("datatype", "float"),
            description=payload.get("description", ""),
        )

        self._registered_streams[stream_id] = registration

        # Track as publication for this wallet
        if wallet_address not in self._publications:
            self._publications[wallet_address] = set()
        self._publications[wallet_address].add(stream_id)

        # Register with P2P network
        # The actual publishing will happen when data is sent

        logger.info(f"Stream registered: {stream_id[:16]}... by {wallet_address[:16]}...")
        return self._json_response({
            "status": "success",
            "uuid": stream_id,
            **registration.to_dict(),
        })

    async def _handle_register_subscription(self, headers: dict, body: bytes) -> str:
        """
        Handle POST /register/subscription - Subscribe to a stream.

        Expected payload: {"uuid": "stream-uuid"} or {"source": "...", "stream": "..."}
        """
        wallet_address = self._verify_auth(headers)
        if not wallet_address:
            return self._error_response(401, "Unauthorized")

        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except json.JSONDecodeError:
            return self._error_response(400, "Invalid JSON")

        # Get stream ID (either direct UUID or generate from components)
        stream_id = payload.get("uuid")
        if not stream_id:
            source = payload.get("source", "satori")
            author = payload.get("author", "")
            stream_name = payload.get("stream", "")
            target = payload.get("target", "")

            if stream_name:
                stream_id = self._generate_stream_uuid(source, author, stream_name, target)
            else:
                return self._error_response(400, "Stream UUID or stream name required")

        # Track subscription
        if wallet_address not in self._subscriptions:
            self._subscriptions[wallet_address] = set()
        self._subscriptions[wallet_address].add(stream_id)

        # Subscribe via P2P
        # Note: Actual callback setup would be done by the caller

        logger.info(f"Subscription registered: {stream_id[:16]}... for {wallet_address[:16]}...")
        return self._json_response({
            "status": "success",
            "uuid": stream_id,
        })

    async def _handle_centrifugo_token(self, headers: dict) -> str:
        """
        Handle GET /centrifugo/token - Get Centrifugo JWT token.

        Returns JWT token for Centrifugo WebSocket connection.
        """
        wallet_address = self._verify_auth(headers)
        if not wallet_address:
            return self._error_response(401, "Unauthorized")

        # Create JWT token
        token = self._create_jwt(wallet_address)

        return self._json_response({
            "token": token,
            "ws_url": f"ws://{self.host}:{self.port + 1}/connection/websocket",
            "expires_at": time.time() + 3600,
            "user_id": wallet_address,
        })

    def _create_jwt(self, user_id: str, expires_in: int = 3600) -> str:
        """Create JWT token for Centrifugo."""
        header = base64.urlsafe_b64encode(
            json.dumps({"alg": "HS256", "typ": "JWT"}).encode()
        ).rstrip(b"=").decode()

        payload_data = {
            "sub": user_id,
            "exp": int(time.time()) + expires_in,
            "iat": int(time.time()),
        }
        payload = base64.urlsafe_b64encode(
            json.dumps(payload_data).encode()
        ).rstrip(b"=").decode()

        message = f"{header}.{payload}"
        signature = base64.urlsafe_b64encode(
            hmac.new(self.jwt_secret.encode(), message.encode(), hashlib.sha256).digest()
        ).rstrip(b"=").decode()

        return f"{header}.{payload}.{signature}"

    async def _handle_get_streams(self, headers: dict, body: bytes) -> str:
        """Handle POST /get/streams - Query stream metadata."""
        wallet_address = self._verify_auth(headers)
        if not wallet_address:
            return self._error_response(401, "Unauthorized")

        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except json.JSONDecodeError:
            payload = {}

        # Filter streams based on query
        results = []
        for stream_id, reg in self._registered_streams.items():
            results.append(reg.to_dict())

        return self._json_response(results)

    async def _handle_my_streams(self, headers: dict, body: bytes) -> str:
        """Handle POST /my/streams - Get user's streams."""
        wallet_address = self._verify_auth(headers)
        if not wallet_address:
            return self._error_response(401, "Unauthorized")

        # Get subscriptions and publications
        subscriptions = []
        publications = []

        for stream_id in self._subscriptions.get(wallet_address, set()):
            if stream_id in self._registered_streams:
                subscriptions.append(self._registered_streams[stream_id].to_dict())

        for stream_id in self._publications.get(wallet_address, set()):
            if stream_id in self._registered_streams:
                publications.append(self._registered_streams[stream_id].to_dict())

        return self._json_response({
            "subscriptions": subscriptions,
            "publications": publications,
        })

    async def _handle_search_streams(self, body: bytes) -> str:
        """Handle POST /streams/search - Search available streams."""
        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except json.JSONDecodeError:
            payload = {}

        search_text = payload.get("search", "").lower()

        results = []
        for stream_id, reg in self._registered_streams.items():
            # Simple search matching
            if not search_text or search_text in reg.stream.lower() or search_text in reg.description.lower():
                results.append({
                    **reg.to_dict(),
                    "stream_id": stream_id,
                    "oracle_address": reg.author,
                    "sanctioned": 0,
                    "total_vote": 0,
                    "vote": 0,
                })

        return self._json_response(results)

    def _json_response(self, data: Any, status: int = 200) -> str:
        """Create JSON HTTP response."""
        body = json.dumps(data)
        return (
            f"HTTP/1.1 {status} OK\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Access-Control-Allow-Origin: *\r\n"
            f"Access-Control-Allow-Headers: Content-Type, publicKey, signature, address\r\n"
            f"\r\n"
            f"{body}"
        )

    def _error_response(self, status: int, message: str) -> str:
        """Create error HTTP response."""
        body = json.dumps({"error": message})
        status_texts = {
            400: "Bad Request",
            401: "Unauthorized",
            404: "Not Found",
            500: "Internal Server Error",
        }
        return (
            f"HTTP/1.1 {status} {status_texts.get(status, 'Error')}\r\n"
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
            "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
            "Access-Control-Allow-Headers: Content-Type, publicKey, signature, address\r\n"
            "Access-Control-Max-Age: 86400\r\n"
            "\r\n"
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        return {
            "host": self.host,
            "port": self.port,
            "running": self._started,
            "registered_wallets": len(self._registered_wallets),
            "registered_streams": len(self._registered_streams),
            "total_subscriptions": sum(len(s) for s in self._subscriptions.values()),
            "total_publications": sum(len(p) for p in self._publications.values()),
        }

    def __repr__(self) -> str:
        status = "running" if self._started else "stopped"
        return f"ServerAPI(status={status}, streams={len(self._registered_streams)})"
