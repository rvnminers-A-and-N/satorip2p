"""
satorip2p.hybrid.central_client - Client for Central PubSub Server

Async client for connecting to the Satori central pubsub server.
Compatible with satorilib/pubsub/pubsub.py protocol.

Protocol:
    - Connect: ws://{url}?uid={uid}
    - Subscribe: send "key:{stream_id}" or "subscribe:{stream_id}"
    - Publish: send "publish:{json_payload}"
    - Messages: received as strings, parsed by router callback
"""

from typing import Any, Callable, Dict, List, Optional, Set, Union
import trio
import json
import logging
import time

logger = logging.getLogger("satorip2p.hybrid.central_client")


class CentralPubSubClient:
    """
    Async client for central Satori pubsub server.

    Implements the same protocol as SatoriPubSubConn but using
    trio for async operation.

    Attributes:
        uid: User ID (wallet address) for authentication
        url: WebSocket URL of central pubsub server
        connected: Whether currently connected
    """

    def __init__(
        self,
        uid: str,
        url: str = "ws://pubsub.satorinet.io:24603",
        on_message: Optional[Callable[[str, str], None]] = None,
        on_connect: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
        reconnect_delay: float = 60.0,
        socket_timeout: float = 3600.0,
    ):
        """
        Initialize CentralPubSubClient.

        Args:
            uid: User ID (wallet address) for authentication
            url: WebSocket URL (default: ws://pubsub.satorinet.io:24603)
            on_message: Callback for received messages (stream_id, data)
            on_connect: Callback when connected
            on_disconnect: Callback when disconnected
            reconnect_delay: Seconds between reconnection attempts
            socket_timeout: Socket timeout in seconds
        """
        self.uid = uid
        self.url = url
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.reconnect_delay = reconnect_delay
        self.socket_timeout = socket_timeout

        # Connection state
        self._ws = None
        self._connected = False
        self._should_reconnect = True
        self._connection_lock = trio.Lock()

        # Subscriptions
        self._subscriptions: Set[str] = set()
        self._pending_subscriptions: Set[str] = set()

        # Publishing rate limiting
        self._topic_times: Dict[str, float] = {}

        # Stats
        self._messages_received = 0
        self._messages_sent = 0
        self._reconnect_count = 0

    @property
    def connected(self) -> bool:
        """Whether connected to central server."""
        return self._connected and self._ws is not None

    async def start(self) -> bool:
        """
        Start the client and connect to central server.

        Returns:
            True if connected successfully
        """
        return await self.connect()

    async def stop(self) -> None:
        """Stop the client and disconnect."""
        self._should_reconnect = False
        await self.disconnect()

    async def connect(self) -> bool:
        """
        Connect to the central pubsub server.

        Returns:
            True if connected successfully
        """
        async with self._connection_lock:
            if self._connected:
                return True

            try:
                from trio_websocket import open_websocket_url

                # Connect with uid query parameter
                ws_url = f"{self.url}?uid={self.uid}"
                logger.info(f"Connecting to central server: {ws_url}")

                self._ws = await open_websocket_url(ws_url)
                self._connected = True

                # Call connect callback
                if self.on_connect:
                    try:
                        self.on_connect()
                    except Exception as e:
                        logger.warning(f"on_connect callback error: {e}")

                logger.info(f"Connected to central server as {self.uid}")

                # Re-subscribe to any pending subscriptions
                for stream_id in self._pending_subscriptions:
                    await self._send_subscribe(stream_id)
                self._subscriptions.update(self._pending_subscriptions)
                self._pending_subscriptions.clear()

                return True

            except ImportError:
                logger.error("trio-websocket not installed")
                return False
            except Exception as e:
                logger.warning(f"Failed to connect to central server: {e}")
                self._connected = False
                return False

    async def disconnect(self) -> None:
        """Disconnect from the central server."""
        async with self._connection_lock:
            if self._ws:
                try:
                    # Send disconnect notice
                    await self._send_raw("notice:{\"topic\":\"connection\",\"data\":\"False\"}")
                except Exception:
                    pass

                try:
                    await self._ws.aclose()
                except Exception:
                    pass

                self._ws = None

            self._connected = False

            if self.on_disconnect:
                try:
                    self.on_disconnect()
                except Exception as e:
                    logger.warning(f"on_disconnect callback error: {e}")

            logger.info("Disconnected from central server")

    async def run_forever(self) -> None:
        """
        Run the client with automatic reconnection.

        Use in a trio nursery:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(client.run_forever)
        """
        while self._should_reconnect:
            try:
                # Connect if not connected
                if not self._connected:
                    connected = await self.connect()
                    if not connected:
                        logger.debug(f"Reconnecting in {self.reconnect_delay}s...")
                        await trio.sleep(self.reconnect_delay)
                        self._reconnect_count += 1
                        continue

                # Listen for messages
                await self._listen_loop()

            except trio.Cancelled:
                break
            except Exception as e:
                logger.warning(f"Central client error: {e}")
                self._connected = False

                if self._should_reconnect:
                    await trio.sleep(self.reconnect_delay)
                    self._reconnect_count += 1

    async def _listen_loop(self) -> None:
        """Listen for messages from the server."""
        if not self._ws:
            return

        try:
            while self._connected:
                try:
                    message = await self._ws.get_message()
                    self._messages_received += 1

                    # Handle emergency stop
                    if message == "---STOP!---":
                        logger.warning("Received emergency stop from central server")
                        # Could trigger emergency restart callback here
                        continue

                    # Route message to callback
                    if self.on_message:
                        try:
                            # Try to extract stream_id from message
                            stream_id = self._extract_stream_id(message)
                            self.on_message(stream_id, message)
                        except Exception as e:
                            logger.debug(f"Message callback error: {e}")

                except Exception as e:
                    logger.debug(f"Listen error: {e}")
                    break

        finally:
            self._connected = False

    def _extract_stream_id(self, message: str) -> str:
        """Extract stream ID from message."""
        try:
            # Try to parse as JSON
            data = json.loads(message)
            return data.get("topic", "")
        except json.JSONDecodeError:
            # Not JSON, might be command:payload format
            if ":" in message:
                parts = message.split(":", 1)
                if parts[0] in ("publish", "key", "subscribe"):
                    try:
                        payload = json.loads(parts[1])
                        return payload.get("topic", "")
                    except json.JSONDecodeError:
                        return parts[1]  # Might be plain stream ID
            return ""

    async def subscribe(self, stream_id: str) -> bool:
        """
        Subscribe to a stream.

        Args:
            stream_id: Stream UUID to subscribe to

        Returns:
            True if subscription sent successfully
        """
        if stream_id in self._subscriptions:
            return True

        if self._connected:
            success = await self._send_subscribe(stream_id)
            if success:
                self._subscriptions.add(stream_id)
            return success
        else:
            # Queue for when connected
            self._pending_subscriptions.add(stream_id)
            return True

    async def _send_subscribe(self, stream_id: str) -> bool:
        """Send subscription command."""
        return await self._send_raw(f"key:{stream_id}")

    async def unsubscribe(self, stream_id: str) -> bool:
        """
        Unsubscribe from a stream.

        Args:
            stream_id: Stream UUID to unsubscribe from

        Returns:
            True if unsubscription processed
        """
        self._subscriptions.discard(stream_id)
        self._pending_subscriptions.discard(stream_id)
        # Note: Central server may not support explicit unsubscribe
        # Connection close/reconnect resets subscriptions
        return True

    async def publish(
        self,
        topic: str,
        data: Any,
        observation_time: Optional[str] = None,
        observation_hash: Optional[str] = None,
    ) -> bool:
        """
        Publish data to a stream.

        Args:
            topic: Stream UUID to publish to
            data: Data to publish (will be converted to string)
            observation_time: Timestamp of observation
            observation_hash: Hash of observation

        Returns:
            True if published successfully
        """
        # Rate limiting (55 seconds between publishes per topic)
        last_time = self._topic_times.get(topic, 0)
        if time.time() - last_time < 55:
            logger.debug(f"Rate limited: {topic}")
            return False

        if not self._connected:
            logger.warning("Cannot publish: not connected to central server")
            return False

        # Build payload
        payload = {
            "topic": topic,
            "data": str(data),
            "time": str(observation_time or time.time()),
            "hash": str(observation_hash or ""),
        }

        message = f"publish:{json.dumps(payload)}"
        success = await self._send_raw(message)

        if success:
            self._topic_times[topic] = time.time()
            self._messages_sent += 1

        return success

    async def _send_raw(self, message: str) -> bool:
        """Send raw message to server."""
        if not self._ws or not self._connected:
            return False

        try:
            await self._ws.send_message(message)
            return True
        except Exception as e:
            logger.warning(f"Send error: {e}")
            self._connected = False
            return False

    def get_subscriptions(self) -> List[str]:
        """Get list of current subscriptions."""
        return list(self._subscriptions)

    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        return {
            "connected": self._connected,
            "url": self.url,
            "uid": self.uid,
            "subscriptions": len(self._subscriptions),
            "pending_subscriptions": len(self._pending_subscriptions),
            "messages_received": self._messages_received,
            "messages_sent": self._messages_sent,
            "reconnect_count": self._reconnect_count,
        }

    def __repr__(self) -> str:
        status = "connected" if self._connected else "disconnected"
        return f"CentralPubSubClient({status}, subs={len(self._subscriptions)})"
