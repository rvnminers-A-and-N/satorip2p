"""
satorip2p/api.py

REST API server for satorip2p external integrations.

Provides HTTP endpoints for monitoring, control, and integration
with external systems.
"""

import json
import logging
import time
import trio
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

if TYPE_CHECKING:
    from .peers import Peers

from .metrics import MetricsCollector

logger = logging.getLogger("satorip2p.api")


@dataclass
class Request:
    """HTTP request representation."""
    method: str
    path: str
    query: Dict[str, List[str]]
    headers: Dict[str, str]
    body: bytes


@dataclass
class Response:
    """HTTP response representation."""
    status: int
    headers: Dict[str, str]
    body: bytes

    @classmethod
    def json(cls, data: Any, status: int = 200) -> "Response":
        """Create JSON response."""
        body = json.dumps(data, indent=2).encode("utf-8")
        return cls(
            status=status,
            headers={"Content-Type": "application/json"},
            body=body,
        )

    @classmethod
    def text(cls, text: str, status: int = 200, content_type: str = "text/plain") -> "Response":
        """Create text response."""
        return cls(
            status=status,
            headers={"Content-Type": content_type},
            body=text.encode("utf-8"),
        )

    @classmethod
    def error(cls, message: str, status: int = 400) -> "Response":
        """Create error response."""
        return cls.json({"error": message}, status=status)


class PeersAPI:
    """
    REST API server for satorip2p.

    Provides HTTP endpoints for monitoring, control, and external integrations.

    Usage:
        from satorip2p import Peers
        from satorip2p.api import PeersAPI

        peers = Peers(identity=identity)
        await peers.start()

        api = PeersAPI(peers, host="0.0.0.0", port=8080)
        await api.start()

        # API available at http://localhost:8080
    """

    def __init__(
        self,
        peers: "Peers",
        host: str = "127.0.0.1",
        port: int = 8080,
        enable_metrics: bool = True,
    ):
        """
        Initialize REST API server.

        Args:
            peers: Peers instance to expose via API
            host: Host to bind to (default: localhost)
            port: Port to listen on (default: 8080)
            enable_metrics: Enable Prometheus metrics endpoint
        """
        self.peers = peers
        self.host = host
        self.port = port
        self.enable_metrics = enable_metrics

        # Initialize metrics collector
        self.metrics = MetricsCollector(peers) if enable_metrics else None

        # Server state
        self._server = None
        self._running = False
        self._start_time = time.time()

        # Route handlers
        self._routes: Dict[Tuple[str, str], Callable] = {
            ("GET", "/"): self._handle_root,
            ("GET", "/health"): self._handle_health,
            ("GET", "/status"): self._handle_status,
            ("GET", "/peers"): self._handle_get_peers,
            ("POST", "/peers/{peer_id}/ping"): self._handle_ping_peer,
            ("GET", "/network"): self._handle_network,
            ("GET", "/subscriptions"): self._handle_get_subscriptions,
            ("POST", "/subscriptions"): self._handle_subscribe,
            ("DELETE", "/subscriptions/{stream_id}"): self._handle_unsubscribe,
            ("POST", "/publish/{stream_id}"): self._handle_publish,
            ("GET", "/metrics"): self._handle_metrics,
        }

    async def start(self) -> None:
        """Start the API server."""
        if self._running:
            logger.warning("API server already running")
            return

        self._running = True
        logger.info(f"Starting REST API server on {self.host}:{self.port}")

        try:
            await trio.serve_tcp(
                self._handle_connection,
                self.port,
                host=self.host,
            )
        except Exception as e:
            logger.error(f"API server error: {e}")
            self._running = False
            raise

    async def stop(self) -> None:
        """Stop the API server."""
        self._running = False
        logger.info("REST API server stopped")

    async def _handle_connection(self, stream: trio.SocketStream) -> None:
        """Handle incoming TCP connection."""
        try:
            # Read HTTP request
            request = await self._read_request(stream)
            if not request:
                return

            # Route request
            response = await self._route_request(request)

            # Send response
            await self._send_response(stream, response)

        except Exception as e:
            logger.error(f"Connection error: {e}")
            try:
                error_response = Response.error(str(e), status=500)
                await self._send_response(stream, error_response)
            except Exception:
                pass
        finally:
            await stream.aclose()

    async def _read_request(self, stream: trio.SocketStream) -> Optional[Request]:
        """Read and parse HTTP request."""
        try:
            # Read request line and headers
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = await stream.receive_some(4096)
                if not chunk:
                    return None
                data += chunk

            # Split headers and body
            header_end = data.index(b"\r\n\r\n")
            header_data = data[:header_end].decode("utf-8")
            body = data[header_end + 4:]

            # Parse request line
            lines = header_data.split("\r\n")
            request_line = lines[0].split(" ")
            method = request_line[0]
            path_with_query = request_line[1] if len(request_line) > 1 else "/"

            # Parse path and query string
            parsed = urlparse(path_with_query)
            path = parsed.path
            query = parse_qs(parsed.query)

            # Parse headers
            headers = {}
            for line in lines[1:]:
                if ":" in line:
                    key, value = line.split(":", 1)
                    headers[key.strip().lower()] = value.strip()

            # Read remaining body if Content-Length specified
            content_length = int(headers.get("content-length", 0))
            while len(body) < content_length:
                chunk = await stream.receive_some(4096)
                if not chunk:
                    break
                body += chunk

            return Request(
                method=method,
                path=path,
                query=query,
                headers=headers,
                body=body[:content_length] if content_length else body,
            )

        except Exception as e:
            logger.error(f"Error reading request: {e}")
            return None

    async def _send_response(self, stream: trio.SocketStream, response: Response) -> None:
        """Send HTTP response."""
        status_text = {
            200: "OK",
            201: "Created",
            204: "No Content",
            400: "Bad Request",
            404: "Not Found",
            500: "Internal Server Error",
        }.get(response.status, "Unknown")

        # Build response
        lines = [f"HTTP/1.1 {response.status} {status_text}"]

        # Add headers
        response.headers["Content-Length"] = str(len(response.body))
        response.headers["Connection"] = "close"
        response.headers["Server"] = "satorip2p/0.1.0"

        for key, value in response.headers.items():
            lines.append(f"{key}: {value}")

        lines.append("")
        header_bytes = "\r\n".join(lines).encode("utf-8") + b"\r\n"

        await stream.send_all(header_bytes + response.body)

    async def _route_request(self, request: Request) -> Response:
        """Route request to appropriate handler."""
        # Try exact match first
        handler = self._routes.get((request.method, request.path))
        if handler:
            return await handler(request)

        # Try pattern matching for path parameters
        for (method, pattern), handler in self._routes.items():
            if method != request.method:
                continue

            match, params = self._match_path(pattern, request.path)
            if match:
                request.path_params = params
                return await handler(request)

        return Response.error("Not Found", status=404)

    def _match_path(self, pattern: str, path: str) -> Tuple[bool, Dict[str, str]]:
        """Match path against pattern with parameters."""
        pattern_parts = pattern.split("/")
        path_parts = path.split("/")

        if len(pattern_parts) != len(path_parts):
            return False, {}

        params = {}
        for p_part, path_part in zip(pattern_parts, path_parts):
            if p_part.startswith("{") and p_part.endswith("}"):
                param_name = p_part[1:-1]
                params[param_name] = path_part
            elif p_part != path_part:
                return False, {}

        return True, params

    # ========== Route Handlers ==========

    async def _handle_root(self, request: Request) -> Response:
        """Handle root endpoint."""
        return Response.json({
            "name": "satorip2p",
            "version": "0.1.0",
            "endpoints": list(f"{m} {p}" for (m, p) in self._routes.keys()),
        })

    async def _handle_health(self, request: Request) -> Response:
        """Handle health check."""
        is_healthy = self.peers._started and self.peers.is_connected
        return Response.json({
            "status": "healthy" if is_healthy else "unhealthy",
            "started": self.peers._started,
            "connected": self.peers.is_connected,
            "uptime_seconds": time.time() - self._start_time,
        }, status=200 if is_healthy else 503)

    async def _handle_status(self, request: Request) -> Response:
        """Handle status endpoint."""
        return Response.json({
            "peer_id": self.peers.peer_id,
            "evrmore_address": self.peers.evrmore_address,
            "public_key": self.peers.public_key,
            "addresses": self.peers.public_addresses,
            "nat_type": self.peers.nat_type,
            "is_relay": self.peers.is_relay,
            "connected_peers": self.peers.connected_peers,
            "subscriptions": self.peers.get_my_subscriptions(),
            "publications": self.peers.get_my_publications(),
            "features": {
                "dht": self.peers.enable_dht,
                "pubsub": self.peers.enable_pubsub,
                "relay": self.peers.enable_relay,
                "rendezvous": self.peers.enable_rendezvous,
                "mdns": self.peers.enable_mdns,
                "ping": self.peers.enable_ping,
                "autonat": self.peers.enable_autonat,
                "identify": self.peers.enable_identify,
                "quic": self.peers.enable_quic,
                "websocket": self.peers.enable_websocket,
            },
        })

    async def _handle_get_peers(self, request: Request) -> Response:
        """Handle get peers endpoint."""
        connected = self.peers.get_connected_peers()
        peers_info = []

        for peer_id in connected:
            info = {"peer_id": peer_id}

            # Add subscription info if available
            subs = self.peers.get_peer_subscriptions(peer_id)
            if subs:
                info["subscriptions"] = subs

            peers_info.append(info)

        return Response.json({
            "count": len(connected),
            "peers": peers_info,
        })

    async def _handle_ping_peer(self, request: Request) -> Response:
        """Handle ping peer endpoint."""
        peer_id = getattr(request, 'path_params', {}).get('peer_id')
        if not peer_id:
            return Response.error("peer_id is required", status=400)

        # Get count from query or body
        count = 3
        if request.body:
            try:
                body = json.loads(request.body)
                count = body.get("count", 3)
            except json.JSONDecodeError:
                pass

        count_param = request.query.get("count", [])
        if count_param:
            try:
                count = int(count_param[0])
            except ValueError:
                pass

        latencies = await self.peers.ping_peer(peer_id, count=count)

        if latencies is None:
            return Response.json({
                "peer_id": peer_id,
                "success": False,
                "error": "Ping failed or peer not connected",
            }, status=404)

        # Record in metrics
        if self.metrics:
            for lat in latencies:
                self.metrics.record_ping_latency(lat)

        return Response.json({
            "peer_id": peer_id,
            "success": True,
            "count": len(latencies),
            "latencies_ms": [round(lat * 1000, 2) for lat in latencies],
            "avg_ms": round(sum(latencies) / len(latencies) * 1000, 2) if latencies else 0,
            "min_ms": round(min(latencies) * 1000, 2) if latencies else 0,
            "max_ms": round(max(latencies) * 1000, 2) if latencies else 0,
        })

    async def _handle_network(self, request: Request) -> Response:
        """Handle network map endpoint."""
        return Response.json(self.peers.get_network_map())

    async def _handle_get_subscriptions(self, request: Request) -> Response:
        """Handle get subscriptions endpoint."""
        my_subs = self.peers.get_my_subscriptions()
        my_pubs = self.peers.get_my_publications()

        result = {
            "subscriptions": [],
            "publications": [],
        }

        for stream_id in my_subs:
            result["subscriptions"].append({
                "stream_id": stream_id,
                "subscribers": self.peers.get_subscribers(stream_id),
                "publishers": self.peers.get_publishers(stream_id),
            })

        for stream_id in my_pubs:
            result["publications"].append({
                "stream_id": stream_id,
                "subscribers": self.peers.get_subscribers(stream_id),
            })

        return Response.json(result)

    async def _handle_subscribe(self, request: Request) -> Response:
        """Handle subscribe endpoint."""
        if not request.body:
            return Response.error("Request body required", status=400)

        try:
            body = json.loads(request.body)
            stream_id = body.get("stream_id")

            if not stream_id:
                return Response.error("stream_id is required", status=400)

            # Subscribe (without callback - API consumers should poll or use webhooks)
            def noop_callback(sid, data):
                pass

            await self.peers.subscribe_async(stream_id, noop_callback)

            return Response.json({
                "success": True,
                "stream_id": stream_id,
                "message": f"Subscribed to {stream_id}",
            }, status=201)

        except json.JSONDecodeError:
            return Response.error("Invalid JSON", status=400)
        except Exception as e:
            return Response.error(str(e), status=500)

    async def _handle_unsubscribe(self, request: Request) -> Response:
        """Handle unsubscribe endpoint."""
        stream_id = getattr(request, 'path_params', {}).get('stream_id')
        if not stream_id:
            return Response.error("stream_id is required", status=400)

        try:
            await self.peers.unsubscribe_async(stream_id)
            return Response.json({
                "success": True,
                "stream_id": stream_id,
                "message": f"Unsubscribed from {stream_id}",
            })
        except Exception as e:
            return Response.error(str(e), status=500)

    async def _handle_publish(self, request: Request) -> Response:
        """Handle publish endpoint."""
        stream_id = getattr(request, 'path_params', {}).get('stream_id')
        if not stream_id:
            return Response.error("stream_id is required", status=400)

        if not request.body:
            return Response.error("Request body required", status=400)

        try:
            # Try to parse as JSON, otherwise use raw bytes
            try:
                data = json.loads(request.body)
            except json.JSONDecodeError:
                data = request.body

            await self.peers.publish(stream_id, data)

            # Record in metrics
            if self.metrics:
                self.metrics.record_message_sent(len(request.body))

            return Response.json({
                "success": True,
                "stream_id": stream_id,
                "message": "Published successfully",
            })

        except Exception as e:
            return Response.error(str(e), status=500)

    async def _handle_metrics(self, request: Request) -> Response:
        """Handle Prometheus metrics endpoint."""
        if not self.metrics:
            return Response.error("Metrics not enabled", status=404)

        metrics_output = self.metrics.collect()
        return Response.text(
            metrics_output,
            content_type="text/plain; version=0.0.4; charset=utf-8",
        )
