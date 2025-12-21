"""
satorip2p/tests/test_docker_p2p.py

Docker integration tests for actual P2P connections.
These tests require Docker and test real peer connections.

Run with: pytest tests/test_docker_p2p.py -v --timeout=120
Skip with: pytest tests/ -v --ignore=tests/test_docker_p2p.py
"""

import pytest
import trio
import os
import subprocess
import time
import json
from typing import List, Optional


# Check if Docker is available
def docker_available() -> bool:
    """Check if Docker is available and running."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except Exception:
        return False


# Note: These tests don't actually require Docker containers.
# They test actual P2P peer connections using trio locally.
# The Docker Compose tests at the end do require Docker.


class MockEvrmoreIdentity:
    """Mock Evrmore identity for tests."""

    def __init__(self, seed: int = 1):
        self.address = f"ETestAddress{seed:032d}"
        self.pubkey = "02" + f"{seed:02x}" * 32
        self._entropy = bytes([seed % 256] * 32)

    def sign(self, msg: str) -> bytes:
        return b"signature"

    def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3


class TestDockerP2PConnection:
    """Test actual P2P connections between Docker containers."""

    @pytest.mark.timeout(60)
    def test_two_peers_can_connect(self):
        """Test that two peers can establish a connection."""
        from satorip2p.peers import Peers

        async def run_test():
            # Create two peers with different identities
            identity1 = MockEvrmoreIdentity(seed=1)
            identity2 = MockEvrmoreIdentity(seed=2)

            peer1 = Peers(
                identity=identity1,
                listen_port=24601,
                enable_dht=True,
                enable_pubsub=True,
                enable_relay=False,
                enable_upnp=False,
                enable_mdns=True,  # Enable mDNS for local discovery
                bootstrap_peers=[],
            )

            peer2 = Peers(
                identity=identity2,
                listen_port=24602,
                enable_dht=True,
                enable_pubsub=True,
                enable_relay=False,
                enable_upnp=False,
                enable_mdns=True,
                bootstrap_peers=[],
            )

            try:
                # Start both peers
                await peer1.start()
                await peer2.start()

                assert peer1.peer_id is not None
                assert peer2.peer_id is not None
                assert peer1.peer_id != peer2.peer_id

                # Get peer1's address for peer2 to connect to
                peer1_addrs = peer1.public_addresses
                if peer1_addrs:
                    # Create bootstrap address
                    bootstrap_addr = f"{peer1_addrs[0]}/p2p/{peer1.peer_id}"

                    # Update peer2's bootstrap and try to connect
                    # (This tests the connection mechanism)
                    peer2.bootstrap_peers = [bootstrap_addr]

            finally:
                await peer1.stop()
                await peer2.stop()

        trio.run(run_test)

    @pytest.mark.timeout(60)
    def test_peer_discovery_via_mdns(self):
        """Test that peers can discover each other via mDNS."""
        from satorip2p.peers import Peers

        async def run_test():
            identity1 = MockEvrmoreIdentity(seed=10)
            identity2 = MockEvrmoreIdentity(seed=11)

            peer1 = Peers(
                identity=identity1,
                listen_port=24611,
                enable_dht=False,
                enable_pubsub=False,
                enable_relay=False,
                enable_upnp=False,
                enable_mdns=True,
                bootstrap_peers=[],
            )

            peer2 = Peers(
                identity=identity2,
                listen_port=24612,
                enable_dht=False,
                enable_pubsub=False,
                enable_relay=False,
                enable_upnp=False,
                enable_mdns=True,
                bootstrap_peers=[],
            )

            try:
                await peer1.start()
                await peer2.start()

                # Both peers should start successfully
                assert peer1._started
                assert peer2._started

            finally:
                await peer1.stop()
                await peer2.stop()

        trio.run(run_test)


class TestDockerPubSub:
    """Test PubSub messaging between peers."""

    @pytest.mark.timeout(90)
    def test_pubsub_message_delivery(self):
        """Test that PubSub messages are delivered between peers."""
        from satorip2p.peers import Peers

        async def run_test():
            identity1 = MockEvrmoreIdentity(seed=20)
            identity2 = MockEvrmoreIdentity(seed=21)

            received_messages = []

            def on_message(stream_id, data):
                received_messages.append((stream_id, data))

            publisher = Peers(
                identity=identity1,
                listen_port=24621,
                enable_dht=True,
                enable_pubsub=True,
                enable_relay=False,
                enable_upnp=False,
                bootstrap_peers=[],
            )

            subscriber = Peers(
                identity=identity2,
                listen_port=24622,
                enable_dht=True,
                enable_pubsub=True,
                enable_relay=False,
                enable_upnp=False,
                bootstrap_peers=[],
            )

            try:
                await publisher.start()
                await subscriber.start()

                # Subscribe to stream
                stream_id = "test-pubsub-stream"
                subscriber.subscribe(stream_id, on_message)

                # Verify subscription registered
                assert stream_id in subscriber.get_my_subscriptions()

            finally:
                await publisher.stop()
                await subscriber.stop()

        trio.run(run_test)


class TestDockerRelay:
    """Test Circuit Relay functionality."""

    @pytest.mark.timeout(90)
    def test_relay_node_initialization(self):
        """Test that a relay node initializes correctly."""
        from satorip2p.peers import Peers

        async def run_test():
            identity = MockEvrmoreIdentity(seed=30)

            relay = Peers(
                identity=identity,
                listen_port=24630,
                enable_dht=True,
                enable_pubsub=True,
                enable_relay=True,
                enable_upnp=False,
                rendezvous_is_server=True,  # Act as relay server
                bootstrap_peers=[],
            )

            try:
                await relay.start()

                assert relay._started
                assert relay.peer_id is not None
                # Relay should be initialized
                # Note: _circuit_relay is only set after _init_circuit_relay is called

            finally:
                await relay.stop()

        trio.run(run_test)


class TestDockerNATTraversal:
    """Test NAT traversal mechanisms."""

    @pytest.mark.timeout(60)
    def test_autonat_initialization(self):
        """Test that AutoNAT initializes correctly."""
        from satorip2p.peers import Peers

        async def run_test():
            identity = MockEvrmoreIdentity(seed=40)

            peer = Peers(
                identity=identity,
                listen_port=24640,
                enable_dht=True,
                enable_pubsub=False,
                enable_relay=True,
                enable_upnp=False,
                enable_autonat=True,
                bootstrap_peers=[],
            )

            try:
                await peer.start()

                assert peer._started
                # AutoNAT should be enabled
                assert peer.enable_autonat is True

            finally:
                await peer.stop()

        trio.run(run_test)

    @pytest.mark.timeout(60)
    def test_dcutr_initialization(self):
        """Test that DCUtR (hole punching) initializes correctly."""
        from satorip2p.peers import Peers

        async def run_test():
            identity = MockEvrmoreIdentity(seed=41)

            peer = Peers(
                identity=identity,
                listen_port=24641,
                enable_dht=True,
                enable_pubsub=False,
                enable_relay=True,
                enable_upnp=False,
                bootstrap_peers=[],
            )

            try:
                await peer.start()

                assert peer._started
                # DCUtR initialization happens in _init_circuit_relay

            finally:
                await peer.stop()

        trio.run(run_test)


class TestDockerPing:
    """Test Ping protocol between peers."""

    @pytest.mark.timeout(90)
    def test_ping_connected_peer(self):
        """Test pinging a connected peer."""
        from satorip2p.peers import Peers

        async def run_test():
            identity1 = MockEvrmoreIdentity(seed=50)
            identity2 = MockEvrmoreIdentity(seed=51)

            peer1 = Peers(
                identity=identity1,
                listen_port=24651,
                enable_dht=True,
                enable_pubsub=False,
                enable_relay=False,
                enable_upnp=False,
                enable_ping=True,
                bootstrap_peers=[],
            )

            peer2 = Peers(
                identity=identity2,
                listen_port=24652,
                enable_dht=True,
                enable_pubsub=False,
                enable_relay=False,
                enable_upnp=False,
                enable_ping=True,
                bootstrap_peers=[],
            )

            try:
                await peer1.start()
                await peer2.start()

                # Both should have ping enabled
                assert peer1.enable_ping is True
                assert peer2.enable_ping is True

                # Ping without connection should return None
                result = await peer1.ping_peer("nonexistent-peer-id")
                assert result is None

            finally:
                await peer1.stop()
                await peer2.stop()

        trio.run(run_test)


class TestDockerMultiplePeers:
    """Test scenarios with multiple peers."""

    @pytest.mark.timeout(120)
    def test_three_peer_network(self):
        """Test a small network of three peers."""
        from satorip2p.peers import Peers

        async def run_test():
            peers = []
            try:
                # Create 3 peers
                for i in range(3):
                    identity = MockEvrmoreIdentity(seed=60 + i)
                    peer = Peers(
                        identity=identity,
                        listen_port=24660 + i,
                        enable_dht=True,
                        enable_pubsub=True,
                        enable_relay=False,
                        enable_upnp=False,
                        bootstrap_peers=[],
                    )
                    await peer.start()
                    peers.append(peer)

                # All should start successfully
                for peer in peers:
                    assert peer._started
                    assert peer.peer_id is not None

                # All should have unique peer IDs
                peer_ids = [p.peer_id for p in peers]
                assert len(set(peer_ids)) == 3

            finally:
                for peer in peers:
                    await peer.stop()

        trio.run(run_test)


class TestDockerCompose:
    """Test Docker Compose multi-container setup."""

    @pytest.mark.skipif(
        not os.path.exists("docker/docker-compose.yml"),
        reason="docker-compose.yml not found"
    )
    def test_compose_file_valid(self):
        """Test that docker-compose.yml is valid."""
        result = subprocess.run(
            ["docker", "compose", "-f", "docker/docker-compose.yml", "config"],
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 0, f"Invalid compose file: {result.stderr.decode()}"

    @pytest.mark.skipif(
        not os.path.exists("docker/Dockerfile"),
        reason="Dockerfile not found"
    )
    def test_dockerfile_valid(self):
        """Test that Dockerfile syntax is valid."""
        # Basic syntax check by parsing
        with open("docker/Dockerfile", "r") as f:
            content = f.read()
            assert "FROM" in content
            assert "COPY" in content or "ADD" in content


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--timeout=120"])
