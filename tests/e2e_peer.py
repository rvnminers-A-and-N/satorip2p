#!/usr/bin/env python3
"""
Simple peer node for Docker E2E testing.
Connects to bootstrap and participates in the mesh.
"""

import trio
import sys
import os

sys.path.insert(0, '/app/src')
from satorip2p import Peers


class MockEvrmoreIdentity:
    """Mock Evrmore identity for tests."""

    def __init__(self, seed: int = 1):
        self.address = f"EPeer{seed:032d}"
        self.pubkey = "02" + f"{seed:02x}" * 32
        self._entropy = bytes([seed % 256] * 32)

    def sign(self, msg: str) -> bytes:
        return b"signature"

    def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3


async def run_peer():
    seed = int(os.environ.get("SATORI_P2P_SEED", "2"))
    port = int(os.environ.get("SATORI_P2P_PORT", "4002"))
    role = os.environ.get("SATORI_P2P_ROLE", "peer")

    print(f"Starting {role} peer (seed={seed}, port={port})...")
    identity = MockEvrmoreIdentity(seed=seed)

    peers = Peers(
        identity=identity,
        listen_port=port,
        enable_dht=True,
        enable_relay=True,
        enable_upnp=False,
        enable_mdns=True,
        enable_pubsub=True,
        enable_ping=True,
        enable_identify=True,
        bootstrap_peers=[],  # Use mDNS for discovery in Docker network
    )

    await peers.start()
    print(f"Peer ID: {peers.peer_id}")
    print(f"Listening on port {port}")

    # Subscribe to common test topics
    def on_message(stream_id: str, data: bytes):
        print(f"[{role}] Received on {stream_id}: {data[:50]}...")

    await peers.subscribe_async("satori/test/pubsub", on_message)
    await peers.subscribe_async("satori/predictions", on_message)
    await peers.subscribe_async("satori/stream-a", on_message)
    await peers.subscribe_async("satori/stream-b", on_message)

    print(f"[{role}] Peer running, waiting for messages...")

    try:
        # Run the peer services
        async with trio.open_nursery() as nursery:
            nursery.start_soon(peers.run_forever)
            if peers._pubsub_manager:
                nursery.start_soon(peers._pubsub_manager.process_messages)

            # Keep running and log status
            while True:
                await trio.sleep(10)
                print(f"[{role}] Connected peers: {peers.connected_peers}")
    except trio.Cancelled:
        print(f"[{role}] Shutting down...")
    finally:
        await peers.stop()


if __name__ == "__main__":
    trio.run(run_peer)
