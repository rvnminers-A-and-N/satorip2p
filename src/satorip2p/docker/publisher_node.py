"""
satorip2p/docker/publisher_node.py

Test publisher node for Docker testing.
Run with: python -m satorip2p.docker.publisher_node
"""

import trio
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PUBLISHER] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


class MockEvrmoreIdentity:
    """Mock identity for testing."""
    def __init__(self, seed: int = 2):
        self.address = f"EPublisher{seed:032d}"
        self.pubkey = "02" + f"{seed:02x}" * 32
        self._entropy = bytes([seed % 256] * 32)

    def sign(self, msg: str) -> bytes:
        return b"signature"

    def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3


async def main():
    """Run publisher node."""
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

    # Create publisher peer
    identity = MockEvrmoreIdentity(seed=2)
    peers = Peers(
        identity=identity,
        enable_dht=True,
        enable_pubsub=True,
        enable_rendezvous=True,
        enable_relay=True,  # Use relay for bridge mode
        enable_upnp=False,  # No UPnP in Docker bridge mode
        bootstrap_peers=[bootstrap] if bootstrap else [],
    )

    logger.info("Starting publisher node...")
    await peers.start()

    peer_id = peers.peer_id
    logger.info(f"Publisher started: {peer_id}")

    # Run P2P services and publishing loop together
    async def publish_loop():
        """Publish data periodically."""
        # Wait a moment for connections to establish
        await trio.sleep(3)

        # Register as publisher for test streams
        stream_ids = ["test-stream-1", "test-stream-2"]
        for stream_id in stream_ids:
            await peers.publish(stream_id, b"initial data")
            logger.info(f"Registered as publisher for: {stream_id}")

        # Publish data periodically
        counter = 0
        while True:
            await trio.sleep(10)
            counter += 1

            for stream_id in stream_ids:
                data = f"Data update #{counter} for {stream_id}".encode()
                await peers.broadcast(stream_id, data)
                logger.info(f"Published to {stream_id}: update #{counter}")

            # Log stats
            connected = peers.get_connected_peers()
            logger.info(f"Stats: connected={len(connected)}")

    try:
        async with trio.open_nursery() as nursery:
            # Run P2P services (DHT, Pubsub, bootstrap connection)
            nursery.start_soon(peers.run_forever)
            # Run our publishing loop
            nursery.start_soon(publish_loop)
    except trio.Cancelled:
        logger.info("Shutting down publisher...")
    except BaseExceptionGroup as eg:
        logger.error(f"Publisher nursery ExceptionGroup with {len(eg.exceptions)} exception(s):")
        for i, exc in enumerate(eg.exceptions, 1):
            logger.error(f"  [{i}] {type(exc).__name__}: {exc}")
            if hasattr(exc, 'exceptions'):  # Nested ExceptionGroup
                for j, nested in enumerate(exc.exceptions, 1):
                    logger.error(f"    [{i}.{j}] {type(nested).__name__}: {nested}")
    except Exception as e:
        logger.error(f"Publisher nursery error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            await peers.stop()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        trio.run(main)
    except trio.TrioInternalError as e:
        # Known issue with py-libp2p async generator cleanup
        logger.warning(f"Trio cleanup error (non-critical): {e}")
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info("Publisher stopped")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Publisher error: {type(e).__name__}: {e}")
        sys.exit(1)
