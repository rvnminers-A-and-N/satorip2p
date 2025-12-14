"""
satorip2p.compat.pubsub - SatoriPubSubConn Compatible WebSocket Server

Provides a WebSocket server that implements the same protocol as the
central Satori pubsub server (satorilib/pubsub/pubsub.py), allowing
legacy Neuron clients to connect to the P2P network without modification.

Protocol:
    - Connect: ws://host:port?uid=<wallet_address>
    - Subscribe: send "key:<json_payload>" or "subscribe:<stream_uuid>"
    - Publish: send "publish:{"topic":"...", "data":"...", "time":"...", "hash":"..."}"
    - Receive: messages are routed from P2P GossipSub to connected clients

Usage:
    from satorip2p import Peers
    from satorip2p.compat import PubSubServer

    peers = Peers(identity=identity)
    await peers.start()

    pubsub = PubSubServer(peers, port=24603)
    await pubsub.start()

    # Run both
    async with trio.open_nursery() as nursery:
        nursery.start_soon(peers.run_forever)
        nursery.start_soon(pubsub.run_forever)
"""

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set
import trio
import json
import logging
from urllib.parse import parse_qs, urlparse

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.compat.pubsub")


class PubSubClient:
    """Represents a connected WebSocket client."""

    def __init__(
        self,
        uid: str,
        websocket,
        send_channel: trio.MemorySendChannel,
    ):
        self.uid = uid
        self.websocket = websocket
        self.send_channel = send_channel
        self.subscriptions: Set[str] = set()
        self.connected = True

    async def send(self, message: str) -> bool:
        """Send message to client."""
        if not self.connected:
            return False
        try:
            await self.send_channel.send(message)
            return True
        except trio.ClosedResourceError:
            self.connected = False
            return False


class PubSubServer:
    """
    SatoriPubSubConn Compatible WebSocket Server.

    Bridges legacy Satori Neuron clients to the P2P network by:
    1. Accepting WebSocket connections with ?uid= authentication
    2. Handling subscribe/publish commands in command:payload format
    3. Routing messages between WebSocket clients and libp2p GossipSub

    Attributes:
        peers: The Peers instance for P2P communication
        host: Host address to bind to
        port: Port to listen on (default 24603, same as central server)
        clients: Connected clients by uid
    """

    def __init__(
        self,
        peers: "Peers",
        host: str = "0.0.0.0",
        port: int = 24603,
    ):
        """
        Initialize PubSubServer.

        Args:
            peers: Peers instance for P2P communication
            host: Host address to bind (default: all interfaces)
            port: WebSocket port (default: 24603 - same as central pubsub)
        """
        self.peers = peers
        self.host = host
        self.port = port

        # Client management
        self._clients: Dict[str, PubSubClient] = {}
        self._stream_clients: Dict[str, Set[str]] = {}  # stream_id -> set of uids

        # State
        self._started = False
        self._server = None
        self._nursery: Optional[trio.Nursery] = None

    @property
    def client_count(self) -> int:
        """Number of connected clients."""
        return len(self._clients)

    @property
    def stream_count(self) -> int:
        """Number of active streams."""
        return len(self._stream_clients)

    async def start(self) -> bool:
        """
        Start the WebSocket server.

        Returns:
            True if started successfully
        """
        if self._started:
            logger.warning("PubSubServer already started")
            return True

        try:
            # Import trio-websocket for WebSocket server
            from trio_websocket import serve_websocket

            logger.info(f"Starting PubSubServer on ws://{self.host}:{self.port}")
            self._started = True
            return True

        except ImportError:
            logger.error("trio-websocket not installed. Install with: pip install trio-websocket")
            return False
        except Exception as e:
            logger.error(f"Failed to start PubSubServer: {e}")
            return False

    async def stop(self) -> None:
        """Stop the WebSocket server."""
        if not self._started:
            return

        logger.info("Stopping PubSubServer...")

        # Disconnect all clients
        for uid, client in list(self._clients.items()):
            try:
                client.connected = False
            except Exception:
                pass

        self._clients.clear()
        self._stream_clients.clear()
        self._started = False

        logger.info("PubSubServer stopped")

    async def run_forever(self) -> None:
        """
        Run the WebSocket server until cancelled.

        Use in a trio nursery alongside peers.run_forever():
            async with trio.open_nursery() as nursery:
                nursery.start_soon(peers.run_forever)
                nursery.start_soon(pubsub_server.run_forever)
        """
        if not self._started:
            if not await self.start():
                return

        try:
            from trio_websocket import serve_websocket

            async with trio.open_nursery() as nursery:
                self._nursery = nursery

                # Start WebSocket server
                await nursery.start(
                    serve_websocket,
                    self._handle_connection,
                    self.host,
                    self.port,
                    None,  # ssl_context
                )

                logger.info(f"PubSubServer listening on ws://{self.host}:{self.port}")

                # Keep running until cancelled
                while True:
                    await trio.sleep(1)

        except trio.Cancelled:
            logger.info("PubSubServer cancelled")
            raise
        except Exception as e:
            logger.error(f"PubSubServer error: {e}")
            import traceback
            traceback.print_exc()

    async def _handle_connection(self, request) -> None:
        """
        Handle incoming WebSocket connection.

        Expected URL format: ws://host:port?uid=<wallet_address>
        """
        try:
            from trio_websocket import WebSocketConnection

            # Parse uid from query string
            path = request.path
            query_string = path.split("?", 1)[1] if "?" in path else ""
            params = parse_qs(query_string)
            uid = params.get("uid", [None])[0]

            if not uid:
                logger.warning(f"Connection rejected: no uid provided")
                # Can't reject cleanly before accept, so accept then close
                ws = await request.accept()
                await ws.aclose()
                return

            # Accept connection
            ws = await request.accept()
            logger.info(f"Client connected: {uid[:16]}...")

            # Create send channel for this client
            send_channel, receive_channel = trio.open_memory_channel(100)

            # Register client
            client = PubSubClient(uid, ws, send_channel)
            self._clients[uid] = client

            try:
                async with trio.open_nursery() as client_nursery:
                    # Task to send messages to client
                    client_nursery.start_soon(
                        self._send_loop, client, receive_channel
                    )

                    # Task to receive messages from client
                    client_nursery.start_soon(
                        self._receive_loop, client
                    )

            except Exception as e:
                logger.debug(f"Client {uid[:16]}... disconnected: {e}")

            finally:
                # Cleanup
                client.connected = False
                self._unregister_client(uid)
                try:
                    await send_channel.aclose()
                    await receive_channel.aclose()
                except Exception:
                    pass
                logger.info(f"Client disconnected: {uid[:16]}...")

        except Exception as e:
            logger.error(f"Connection handler error: {e}")

    async def _send_loop(
        self,
        client: PubSubClient,
        receive_channel: trio.MemoryReceiveChannel,
    ) -> None:
        """Send messages from channel to WebSocket."""
        try:
            async for message in receive_channel:
                if not client.connected:
                    break
                try:
                    await client.websocket.send_message(message)
                except Exception as e:
                    logger.debug(f"Send error: {e}")
                    break
        except trio.ClosedResourceError:
            pass

    async def _receive_loop(self, client: PubSubClient) -> None:
        """Receive messages from WebSocket and process commands."""
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

    async def _process_message(self, client: PubSubClient, message: str) -> None:
        """
        Process incoming message from client.

        Message format: command:payload
        Commands:
            - key:json_payload - Initial subscription (legacy format)
            - subscribe:stream_uuid - Subscribe to stream
            - publish:json_payload - Publish data
            - notice:json_payload - Notice message (e.g., disconnect)
        """
        if not message or ":" not in message:
            logger.debug(f"Invalid message format from {client.uid[:16]}...")
            return

        try:
            # Split command:payload
            colon_pos = message.index(":")
            command = message[:colon_pos].lower()
            payload_str = message[colon_pos + 1:]

            if command == "key":
                # Legacy initial subscription format
                # Payload is typically a JSON with subscription info
                await self._handle_key_command(client, payload_str)

            elif command == "subscribe":
                # Subscribe to specific stream
                await self._handle_subscribe_command(client, payload_str)

            elif command == "publish":
                # Publish data to stream
                await self._handle_publish_command(client, payload_str)

            elif command == "notice":
                # Notice message (e.g., disconnect notification)
                await self._handle_notice_command(client, payload_str)

            else:
                logger.debug(f"Unknown command '{command}' from {client.uid[:16]}...")

        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error from {client.uid[:16]}...: {e}")
        except Exception as e:
            logger.error(f"Message processing error: {e}")

    async def _handle_key_command(self, client: PubSubClient, payload_str: str) -> None:
        """
        Handle 'key' command - legacy initial subscription.

        The payload is typically a JSON containing:
        - Subscription info (stream UUIDs to subscribe to)
        - Or just an identifier/key for authentication

        In the central server, this establishes the client's subscription set.
        Here we extract stream IDs and subscribe via P2P.
        """
        try:
            # Try to parse as JSON
            if payload_str.startswith("{"):
                payload = json.loads(payload_str)

                # Extract stream IDs from various possible formats
                stream_ids = []

                # Format 1: {"streams": ["uuid1", "uuid2"]}
                if "streams" in payload:
                    stream_ids.extend(payload["streams"])

                # Format 2: {"subscriptions": ["uuid1", "uuid2"]}
                if "subscriptions" in payload:
                    stream_ids.extend(payload["subscriptions"])

                # Format 3: Just a list of UUIDs
                if isinstance(payload, list):
                    stream_ids.extend(payload)

                # Subscribe to each stream
                for stream_id in stream_ids:
                    await self._subscribe_client_to_stream(client, stream_id)

            else:
                # Payload might just be a single stream ID
                if payload_str and len(payload_str) > 8:
                    await self._subscribe_client_to_stream(client, payload_str)

            logger.info(f"Client {client.uid[:16]}... key command processed, {len(client.subscriptions)} subscriptions")

        except json.JSONDecodeError:
            # Not JSON, might be a simple key/identifier
            logger.debug(f"Key payload not JSON: {payload_str[:50]}...")

    async def _handle_subscribe_command(self, client: PubSubClient, stream_id: str) -> None:
        """Handle 'subscribe' command - subscribe to a specific stream."""
        stream_id = stream_id.strip()
        if not stream_id:
            return

        await self._subscribe_client_to_stream(client, stream_id)
        logger.debug(f"Client {client.uid[:16]}... subscribed to {stream_id[:16]}...")

    async def _handle_publish_command(self, client: PubSubClient, payload_str: str) -> None:
        """
        Handle 'publish' command - publish data to a stream.

        Expected payload format:
        {
            "topic": "stream_uuid",
            "data": "the_data_value",
            "time": "observation_time",
            "hash": "observation_hash"
        }
        """
        try:
            payload = json.loads(payload_str)
            topic = payload.get("topic")
            data = payload.get("data")
            obs_time = payload.get("time")
            obs_hash = payload.get("hash")

            if not topic:
                logger.warning(f"Publish from {client.uid[:16]}... missing topic")
                return

            # Construct message for P2P broadcast
            message = {
                "type": "observation",
                "topic": topic,
                "data": data,
                "time": obs_time,
                "hash": obs_hash,
                "publisher": client.uid,
            }

            # Publish via P2P
            await self.peers.publish(topic, message)

            # Also broadcast to other WebSocket clients subscribed to this stream
            await self._broadcast_to_clients(topic, payload_str, exclude_uid=client.uid)

            logger.debug(f"Published to {topic[:16]}... from {client.uid[:16]}...")

        except json.JSONDecodeError as e:
            logger.warning(f"Invalid publish payload from {client.uid[:16]}...: {e}")

    async def _handle_notice_command(self, client: PubSubClient, payload_str: str) -> None:
        """Handle 'notice' command - notification messages."""
        try:
            payload = json.loads(payload_str)
            topic = payload.get("topic")
            data = payload.get("data")

            if topic == "connection" and data == "False":
                # Client is disconnecting gracefully
                logger.info(f"Client {client.uid[:16]}... sent disconnect notice")
                client.connected = False

        except json.JSONDecodeError:
            logger.debug(f"Notice payload not JSON: {payload_str[:50]}...")

    async def _subscribe_client_to_stream(self, client: PubSubClient, stream_id: str) -> None:
        """Subscribe a client to a stream and set up P2P subscription."""
        if stream_id in client.subscriptions:
            return  # Already subscribed

        # Add to client's subscription set
        client.subscriptions.add(stream_id)

        # Track which clients are subscribed to this stream
        if stream_id not in self._stream_clients:
            self._stream_clients[stream_id] = set()
            # First subscriber - set up P2P subscription
            await self._setup_p2p_subscription(stream_id)

        self._stream_clients[stream_id].add(client.uid)

    def _unregister_client(self, uid: str) -> None:
        """Unregister a client and clean up subscriptions."""
        client = self._clients.pop(uid, None)
        if not client:
            return

        # Remove from all stream subscriptions
        for stream_id in client.subscriptions:
            if stream_id in self._stream_clients:
                self._stream_clients[stream_id].discard(uid)
                # If no more clients, consider removing P2P subscription
                if not self._stream_clients[stream_id]:
                    del self._stream_clients[stream_id]
                    # Note: We don't unsubscribe from P2P immediately to avoid
                    # rapid subscribe/unsubscribe cycles. Could add a cleanup task.

    async def _setup_p2p_subscription(self, stream_id: str) -> None:
        """Set up P2P subscription for a stream and route messages to WebSocket clients."""
        def on_message(sid: str, data: Any) -> None:
            """Callback for P2P messages - route to WebSocket clients."""
            # This runs in the P2P message processing context
            # Schedule broadcast to clients
            if self._nursery:
                self._nursery.start_soon(
                    self._broadcast_to_clients_data, sid, data
                )

        # Subscribe to P2P stream
        await self.peers.subscribe_async(stream_id, on_message)

        logger.debug(f"P2P subscription set up for {stream_id[:16]}...")

    async def _broadcast_to_clients(
        self,
        stream_id: str,
        message: str,
        exclude_uid: Optional[str] = None,
    ) -> None:
        """Broadcast a message to all clients subscribed to a stream."""
        client_uids = self._stream_clients.get(stream_id, set())

        for uid in client_uids:
            if uid == exclude_uid:
                continue

            client = self._clients.get(uid)
            if client and client.connected:
                await client.send(message)

    async def _broadcast_to_clients_data(self, stream_id: str, data: Any) -> None:
        """Broadcast P2P data to WebSocket clients."""
        # Convert data to string format expected by legacy clients
        if isinstance(data, dict):
            # Reconstruct publish format
            message = json.dumps({
                "topic": stream_id,
                "data": data.get("data"),
                "time": data.get("time"),
                "hash": data.get("hash"),
            })
        elif isinstance(data, str):
            message = data
        else:
            message = json.dumps(data)

        await self._broadcast_to_clients(stream_id, message)

    async def send_emergency_stop(self) -> None:
        """Send emergency stop signal to all connected clients."""
        for client in self._clients.values():
            if client.connected:
                try:
                    await client.send("---STOP!---")
                except Exception:
                    pass

    def get_client_subscriptions(self, uid: str) -> List[str]:
        """Get subscriptions for a specific client."""
        client = self._clients.get(uid)
        if client:
            return list(client.subscriptions)
        return []

    def get_stream_subscribers(self, stream_id: str) -> List[str]:
        """Get clients subscribed to a specific stream."""
        return list(self._stream_clients.get(stream_id, set()))

    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        return {
            "host": self.host,
            "port": self.port,
            "running": self._started,
            "client_count": self.client_count,
            "stream_count": self.stream_count,
            "streams": {
                sid: len(uids)
                for sid, uids in self._stream_clients.items()
            },
        }

    def __repr__(self) -> str:
        status = "running" if self._started else "stopped"
        return f"PubSubServer(status={status}, clients={self.client_count}, streams={self.stream_count})"
