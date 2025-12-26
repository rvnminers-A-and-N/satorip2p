"""
satorip2p/protocol/ping.py

Custom Ping protocol for peer connectivity testing and latency measurement.

This is a Satori-native implementation that doesn't depend on libp2p's
optional ping module. Uses GossipSub for request/response.

Features:
- Round-trip time (RTT) measurement
- Multi-ping averaging
- Timeout handling
- Works over GossipSub mesh
"""

import logging
import time
import uuid
import json
from typing import Any, Dict, List, Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass, asdict

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.ping")

# Protocol constants - topic names (prefixed with satori/stream/ by Peers)
PING_TOPIC = "ping"
PONG_TOPIC = "pong"
PING_TIMEOUT = 10.0  # seconds
PING_PROTOCOL_VERSION = "1.0.0"


@dataclass
class PingRequest:
    """Ping request message."""
    ping_id: str           # Unique ping ID
    sender_id: str         # Sender's peer ID
    target_id: str         # Target peer ID
    timestamp: float       # Send timestamp (Unix epoch)
    version: str = PING_PROTOCOL_VERSION

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PingRequest":
        return cls(**data)


@dataclass
class PongResponse:
    """Pong response message."""
    ping_id: str           # Original ping ID
    sender_id: str         # Original sender (ping initiator)
    responder_id: str      # Responder's peer ID
    request_timestamp: float   # Original request timestamp
    response_timestamp: float  # Response timestamp
    version: str = PING_PROTOCOL_VERSION

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PongResponse":
        return cls(**data)


class PingProtocol:
    """
    Custom ping protocol for satorip2p.

    Uses GossipSub topics for request/response pattern.
    Measures round-trip time for network diagnostics.

    Usage:
        ping_protocol = PingProtocol(peers)
        await ping_protocol.start()

        # Ping a peer
        latencies = await ping_protocol.ping(target_peer_id, count=3)
        if latencies:
            avg_rtt = sum(latencies) / len(latencies)
            print(f"Average RTT: {avg_rtt * 1000:.2f} ms")
    """

    def __init__(self, peers: "Peers"):
        """
        Initialize PingProtocol.

        Args:
            peers: Peers instance for network access
        """
        self._peers = peers
        self._pending_pings: Dict[str, float] = {}  # ping_id -> send_timestamp
        self._received_pongs: Dict[str, PongResponse] = {}  # ping_id -> response
        self._started = False
        self._trio_token = None  # Store trio token for cross-thread calls

    async def start(self) -> None:
        """Start the ping protocol by subscribing to topics."""
        if self._started:
            return

        # Store trio token for callbacks that need to schedule async work
        import trio
        try:
            self._trio_token = trio.lowlevel.current_trio_token()
        except RuntimeError:
            logger.warning("Could not get trio token during ping start")

        # Subscribe to ping requests (to respond) - use subscribe_async for GossipSub
        await self._peers.subscribe_async(PING_TOPIC, self._on_ping_request)

        # Subscribe to pong responses (to receive) - use subscribe_async for GossipSub
        await self._peers.subscribe_async(PONG_TOPIC, self._on_pong_response)

        self._started = True
        logger.info("Ping protocol started")

    async def stop(self) -> None:
        """Stop the ping protocol."""
        if not self._started:
            return

        self._peers.unsubscribe(PING_TOPIC)
        self._peers.unsubscribe(PONG_TOPIC)

        self._pending_pings.clear()
        self._received_pongs.clear()
        self._started = False
        logger.info("Ping protocol stopped")

    async def ping(
        self,
        target_peer_id: str,
        count: int = 3,
        timeout: float = PING_TIMEOUT
    ) -> Optional[List[float]]:
        """
        Ping a peer and measure round-trip times.

        Args:
            target_peer_id: Target peer's ID
            count: Number of pings to send
            timeout: Timeout per ping in seconds

        Returns:
            List of RTT values in seconds, or None if all failed
        """
        if not self._started:
            logger.warning("Ping protocol not started")
            return None

        latencies = []
        my_peer_id = self._peers.peer_id

        if not my_peer_id:
            logger.warning("Peer ID not available")
            return None

        for i in range(count):
            ping_id = str(uuid.uuid4())
            send_time = time.time()

            # Create ping request
            request = PingRequest(
                ping_id=ping_id,
                sender_id=my_peer_id,
                target_id=target_peer_id,
                timestamp=send_time,
            )

            # Track pending ping
            self._pending_pings[ping_id] = send_time

            try:
                # Send ping via broadcast (stream_id first, then message)
                await self._peers.broadcast(
                    PING_TOPIC,
                    json.dumps(request.to_dict()).encode()
                )

                # Wait for pong
                import trio
                start_wait = time.time()
                while time.time() - start_wait < timeout:
                    if ping_id in self._received_pongs:
                        pong = self._received_pongs.pop(ping_id)
                        rtt = pong.response_timestamp - send_time
                        latencies.append(rtt)
                        logger.debug(f"Ping {i+1}/{count} to target_peer_id={target_peer_id}: {rtt*1000:.2f}ms")
                        break
                    await trio.sleep(0.1)
                else:
                    logger.debug(f"Ping {i+1}/{count} to target_peer_id={target_peer_id} timed out")

            except Exception as e:
                logger.debug(f"Ping {i+1}/{count} failed: {e}")
            finally:
                # Clean up pending ping
                self._pending_pings.pop(ping_id, None)

        return latencies if latencies else None

    def _on_ping_request(self, stream_id: str, data: Any) -> None:
        """Handle incoming ping request."""
        try:
            logger.info(f"_on_ping_request called with stream_id={stream_id}, data type={type(data)}")

            # Data may be bytes (raw) or already deserialized dict
            if isinstance(data, bytes):
                request_data = json.loads(data.decode())
            elif isinstance(data, dict):
                request_data = data
            else:
                logger.warning(f"Unexpected ping data type: {type(data)}")
                return

            request = PingRequest.from_dict(request_data)
            logger.info(f"Parsed ping request: ping_id={request.ping_id}, sender={request.sender_id}, target={request.target_id}")

            my_peer_id = self._peers.peer_id
            if not my_peer_id:
                logger.warning("Cannot respond to ping: my_peer_id is not available")
                return

            logger.info(f"Target check: request.target_id={request.target_id} vs my_peer_id={my_peer_id}")

            # Only respond if we're the target
            if request.target_id != my_peer_id:
                logger.info(f"Ignoring ping: not targeted at us (target={request.target_id}, me={my_peer_id})")
                return

            logger.info(f"Ping targeted at us from {request.sender_id}, responding with pong")

            # Create pong response
            response = PongResponse(
                ping_id=request.ping_id,
                sender_id=request.sender_id,
                responder_id=my_peer_id,
                request_timestamp=request.timestamp,
                response_timestamp=time.time(),
            )

            # Send pong response - schedule async work
            import trio
            try:
                # Check if we're already in a trio context
                trio.lowlevel.current_trio_token()
                logger.debug("In trio context, checking for nursery")
                # We're in trio, use nursery from peers if available
                if hasattr(self._peers, '_nursery') and self._peers._nursery:
                    self._peers._nursery.start_soon(self._send_pong, response)
                    logger.debug("Pong scheduled via nursery")
                else:
                    logger.warning("Cannot send pong: no nursery available (nursery is None or missing)")
            except RuntimeError:
                # Not in trio context, use from_thread with stored token
                logger.debug(f"Not in trio context, using from_thread (token available: {self._trio_token is not None})")
                if self._trio_token:
                    trio.from_thread.run(
                        self._send_pong,
                        response,
                        trio_token=self._trio_token
                    )
                    logger.debug("Pong sent via from_thread")
                else:
                    logger.warning("Cannot send pong: no trio token available")

        except Exception as e:
            logger.error(f"Error handling ping request: {e}", exc_info=True)

    async def _send_pong(self, response: PongResponse) -> None:
        """Send pong response."""
        try:
            logger.info(f"Sending pong response: ping_id={response.ping_id}, to sender={response.sender_id}")
            await self._peers.broadcast(
                PONG_TOPIC,
                json.dumps(response.to_dict()).encode()
            )
            logger.debug(f"Pong broadcast complete for ping_id={response.ping_id}")
        except Exception as e:
            logger.error(f"Error sending pong: {e}", exc_info=True)

    def _on_pong_response(self, stream_id: str, data: Any) -> None:
        """Handle incoming pong response."""
        try:
            # Data may be bytes (raw) or already deserialized dict
            if isinstance(data, bytes):
                response_data = json.loads(data.decode())
            elif isinstance(data, dict):
                response_data = data
            else:
                logger.warning(f"Unexpected pong data type: {type(data)}")
                return

            response = PongResponse.from_dict(response_data)

            my_peer_id = self._peers.peer_id
            if not my_peer_id:
                return

            # Only process if we're the original sender
            if response.sender_id != my_peer_id:
                return

            # Store response for pickup
            if response.ping_id in self._pending_pings:
                self._received_pongs[response.ping_id] = response

        except Exception as e:
            logger.debug(f"Error handling pong response: {e}")

    def get_stats(self) -> dict:
        """Get ping protocol statistics."""
        return {
            "started": self._started,
            "pending_pings": len(self._pending_pings),
            "version": PING_PROTOCOL_VERSION,
        }
