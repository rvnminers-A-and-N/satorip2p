"""
satorip2p/docker/relay_node.py

Bootstrap/relay node for Docker testing.
Run with: python -m satorip2p.docker.relay_node
"""

import trio
import logging
import os
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Enable libp2p debug logging
logging.getLogger("libp2p").setLevel(logging.DEBUG)


class MockEvrmoreIdentity:
    """Mock identity for testing."""
    def __init__(self, seed: int = 1):
        self.address = f"ERelayNode{seed:032d}"
        self.pubkey = "02" + f"{seed:02x}" * 32
        self._entropy = bytes([seed % 256] * 32)

    def sign(self, msg: str) -> bytes:
        return b"signature"

    def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3


async def main():
    """Run relay node."""
    from satorip2p import Peers
    from satorip2p.docker import detect_docker_environment

    # Detect Docker environment
    docker_info = detect_docker_environment()
    logger.info(f"Docker info: in_container={docker_info.in_container}, "
                f"mode={docker_info.network_mode}, ip={docker_info.container_ip}")

    # Configuration from environment
    listen_port = int(os.environ.get("SATORI_P2P_LISTEN_PORT", "4001"))

    # Create relay peer
    identity = MockEvrmoreIdentity(seed=1)
    peers = Peers(
        identity=identity,
        listen_port=listen_port,
        enable_dht=True,
        enable_pubsub=True,
        enable_rendezvous=True,  # Re-enabled
        rendezvous_is_server=True,  # Relay runs as rendezvous server
        enable_relay=True,  # Enable relay service
        enable_upnp=False,  # No UPnP needed in Docker
        bootstrap_peers=[],  # This IS the bootstrap
    )

    logger.info("Starting relay node...")
    await peers.start()

    peer_id = peers.peer_id
    logger.info(f"Relay node started: {peer_id}")
    logger.info(f"Listening on port {listen_port}")
    logger.info("Ready to relay messages for peers in bridge mode")

    # Run with proper nursery for background tasks
    async def stats_loop():
        """Log stats periodically."""
        try:
            while True:
                await trio.sleep(30)
                network_map = peers.get_network_map()
                connected = network_map.get('connected_peers', [])
                known = network_map.get('known_peers', 0)
                known_count = known if isinstance(known, int) else len(known)
                logger.info(f"Stats: connected={len(connected)}, known={known_count}")
        except trio.Cancelled:
            pass

    try:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(stats_loop)
            nursery.start_soon(peers.run_forever)
    except trio.Cancelled:
        logger.info("Shutting down relay node...")
    except Exception as e:
        logger.error(f"Relay nursery error: {type(e).__name__}: {e}")
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
        logger.info("Relay node stopped")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Relay error: {type(e).__name__}: {e}")
        sys.exit(1)
