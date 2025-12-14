"""
satorip2p/docker/subscriber_node.py

Test subscriber node for Docker testing.
Run with: python -m satorip2p.docker.subscriber_node
"""

import trio
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [SUBSCRIBER] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


class MockEvrmoreIdentity:
    """Mock identity for testing."""
    def __init__(self, seed: int = 3):
        self.address = f"ESubscriber{seed:032d}"
        self.pubkey = "02" + f"{seed:02x}" * 32
        self._entropy = bytes([seed % 256] * 32)

    def sign(self, msg: str) -> bytes:
        return b"signature"

    def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3


async def main():
    """Run subscriber node."""
    from satorip2p import Peers
    from satorip2p.docker import detect_docker_environment

    # Detect Docker environment
    docker_info = detect_docker_environment()
    logger.info(f"Docker info: in_container={docker_info.in_container}, "
                f"mode={docker_info.network_mode}")

    if docker_info.needs_relay:
        logger.info("Bridge mode detected - will use Circuit Relay for NAT traversal")

    # Configuration from environment
    bootstrap = os.environ.get("SATORI_P2P_BOOTSTRAP", "")

    # Create subscriber peer
    identity = MockEvrmoreIdentity(seed=3)
    peers = Peers(
        identity=identity,
        enable_dht=True,
        enable_pubsub=True,
        enable_rendezvous=True,
        enable_relay=True,  # Use relay for bridge mode
        enable_upnp=False,  # No UPnP in Docker bridge mode
        bootstrap_peers=[bootstrap] if bootstrap else [],
    )

    # Track received messages
    received_messages = []

    def on_message(stream_id: str, data: bytes):
        """Callback for received messages."""
        msg = data.decode() if isinstance(data, bytes) else str(data)
        received_messages.append((stream_id, msg))
        logger.info(f"RECEIVED [{stream_id}]: {msg}")

    logger.info("Starting subscriber node...")
    await peers.start()

    peer_id = peers.peer_id
    logger.info(f"Subscriber started: {peer_id}")

    # Subscribe to test streams with full network registration
    stream_ids = ["test-stream-1", "test-stream-2"]
    for stream_id in stream_ids:
        await peers.subscribe_async(stream_id, on_message)
        logger.info(f"Subscribed to: {stream_id}")

    # Run message processing and stats loop in parallel using trio nursery
    async def stats_loop():
        """Log stats periodically."""
        try:
            while True:
                await trio.sleep(30)
                connected = peers.get_connected_peers()
                logger.info(
                    f"Stats: connected={len(connected)}, "
                    f"messages_received={len(received_messages)}"
                )
                subs = peers.get_my_subscriptions()
                logger.info(f"Active subscriptions: {list(subs)}")
        except trio.Cancelled:
            pass

    try:
        async with trio.open_nursery() as nursery:
            # Run P2P services (DHT, Pubsub, bootstrap connection) - CRITICAL!
            nursery.start_soon(peers.run_forever)

            # Start message processing for each stream
            for stream_id in stream_ids:
                nursery.start_soon(peers.process_messages, stream_id)

            # Start stats loop
            nursery.start_soon(stats_loop)

    except trio.Cancelled:
        logger.info("Shutting down subscriber...")
        logger.info(f"Total messages received: {len(received_messages)}")
        await peers.stop()


if __name__ == "__main__":
    try:
        trio.run(main)
    except KeyboardInterrupt:
        logger.info("Subscriber stopped")
        sys.exit(0)
