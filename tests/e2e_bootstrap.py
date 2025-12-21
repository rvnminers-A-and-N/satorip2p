#!/usr/bin/env python3
"""Bootstrap node for Docker E2E testing."""

import trio
import sys
import os

sys.path.insert(0, '/app/src')
from satorip2p import Peers


class MockEvrmoreIdentity:
    """Mock Evrmore identity for tests."""

    def __init__(self, seed: int = 1):
        self.address = f"EBootstrap{seed:032d}"
        self.pubkey = "02" + f"{seed:02x}" * 32
        self._entropy = bytes([seed % 256] * 32)

    def sign(self, msg: str) -> bytes:
        return b"signature"

    def verify(self, msg: str, sig: bytes, pubkey=None, address=None) -> bool:
        return True

    def secret(self, pubkey: str) -> bytes:
        return b"sharedsecret" * 3


async def run_bootstrap():
    seed = int(os.environ.get("SATORI_P2P_SEED", "1"))
    port = int(os.environ.get("SATORI_P2P_PORT", "4001"))

    print(f"Starting bootstrap node (seed={seed}, port={port})...")
    identity = MockEvrmoreIdentity(seed=seed)

    peers = Peers(
        identity=identity,
        listen_port=port,
        enable_dht=True,
        enable_relay=True,
        enable_upnp=False,
        enable_mdns=True,
        enable_pubsub=True,
        bootstrap_peers=[],
    )

    await peers.start()
    print(f"Bootstrap peer ID: {peers.peer_id}")
    print(f"Listening on port {port}")

    # Keep running forever
    while True:
        await trio.sleep(1)


if __name__ == "__main__":
    trio.run(run_bootstrap)
