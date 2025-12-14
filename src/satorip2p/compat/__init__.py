"""
satorip2p.compat - Compatibility layer for legacy Satori protocols

This module provides WebSocket and REST API interfaces that are compatible
with the existing Satori Network infrastructure, allowing gradual migration
from centralized servers to P2P networking.

Components:
    - PubSubServer: SatoriPubSubConn compatible WebSocket server (port 24603)
    - CentrifugoServer: Centrifugo compatible WebSocket/REST server
    - ServerAPI: Central server REST API compat (/checkin, /register/stream, etc.)
    - DataManagerBridge: DataClient/DataServer compatible interface
    - Message: PyArrow IPC message format for DataManager protocol

Usage:
    from satorip2p import Peers
    from satorip2p.compat import PubSubServer, ServerAPI, DataManagerBridge

    peers = Peers(identity=identity)
    await peers.start()

    # Start compatibility servers
    pubsub = PubSubServer(peers, port=24603)
    server_api = ServerAPI(peers, port=8080)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(peers.run_forever)
        nursery.start_soon(pubsub.run_forever)
        nursery.start_soon(server_api.run_forever)

    # Use DataManager bridge for DataFrame transmission
    bridge = DataManagerBridge(peers)
    await bridge.publish_dataframe("stream-uuid", df)
"""

from .pubsub import PubSubServer
from .centrifugo import CentrifugoServer
from .server import ServerAPI, CheckinDetails, StreamRegistration
from .datamanager import (
    Message,
    SecurityPolicy,
    ChallengeResponse,
    DataManagerBridge,
    DataManagerAPI,
)

__all__ = [
    # WebSocket servers
    "PubSubServer",
    "CentrifugoServer",
    # REST API
    "ServerAPI",
    "CheckinDetails",
    "StreamRegistration",
    # DataManager
    "Message",
    "SecurityPolicy",
    "ChallengeResponse",
    "DataManagerBridge",
    "DataManagerAPI",
]
