"""
satorip2p/peers.py

Main P2P interface for Satori Network, built on py-libp2p.

Provides:
- Peer discovery via Kademlia DHT
- Pub/sub messaging via GossipSub
- NAT traversal via AutoNAT and Circuit Relay
- Offline message delivery via custom MessageStore

Usage (context manager - recommended):
    from satorip2p import Peers
    from satorilib.wallet.evrmore.identity import EvrmoreIdentity

    identity = EvrmoreIdentity('/path/to/wallet.yaml')

    async with Peers(identity=identity) as peers:
        # Subscribe to a stream
        await peers.subscribe_async("stream-uuid", callback)

        # Publish data
        await peers.publish("stream-uuid", data)

        # Run until cancelled
        await peers.run_forever()

Usage (manual lifecycle):
    peers = Peers(identity=identity)
    await peers.start()
    # ... use peers ...
    await peers.stop()
"""

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Union
import trio
import logging
import uuid

from .config import (
    PeerInfo,
    StreamInfo,
    DEFAULT_PORT,
    BOOTSTRAP_PEERS,
    STREAM_TOPIC_PREFIX,
    SATORI_PROTOCOL_ID,
    SATORI_STORE_PROTOCOL_ID,
)
from .protocol.subscriptions import SubscriptionManager
from .protocol.message_store import MessageStore
from .protocol.messages import serialize_message, deserialize_message
from .protocol.rendezvous import RendezvousManager
from .identity.evrmore_bridge import EvrmoreIdentityBridge
from .nat.upnp import UPnPManager
from .nat.docker import detect_docker_environment

if TYPE_CHECKING:
    from satorilib.wallet.evrmore.identity import EvrmoreIdentity

logger = logging.getLogger("satorip2p.peers")


class Peers:
    """
    Main P2P interface for Satori Network.

    Built on py-libp2p with Kademlia DHT for peer discovery,
    GossipSub for pub/sub messaging, and AutoNAT/Circuit Relay
    for NAT traversal.

    Attributes:
        peer_id: This node's libp2p peer ID
        evrmore_address: This node's Evrmore wallet address
        is_connected: Whether connected to the P2P network
        nat_type: Detected NAT type (PUBLIC, PRIVATE, UNKNOWN)
        connected_peers: Number of connected peers
    """

    def __init__(
        self,
        identity: "EvrmoreIdentity",
        listen_port: int = DEFAULT_PORT,
        bootstrap_peers: Optional[List[str]] = None,
        enable_upnp: bool = True,
        enable_relay: bool = True,
        enable_dht: bool = True,
        enable_pubsub: bool = True,
        enable_rendezvous: bool = True,
        enable_mdns: bool = True,
        enable_ping: bool = True,
        enable_autonat: bool = True,
        enable_identify: bool = True,
        enable_quic: bool = False,
        enable_websocket: bool = False,
        rendezvous_is_server: bool = False,
    ):
        """
        Initialize Peers instance.

        Args:
            identity: EvrmoreIdentity instance with loaded wallet
            listen_port: Port to listen on (default: 24600)
            bootstrap_peers: List of bootstrap peer multiaddresses
            enable_upnp: Attempt UPnP port mapping
            enable_relay: Enable Circuit Relay (client and service)
            enable_dht: Enable Kademlia DHT for peer discovery
            enable_pubsub: Enable GossipSub for pub/sub messaging
            enable_rendezvous: Enable Rendezvous protocol for stream discovery
            enable_mdns: Enable mDNS for local network peer discovery
            enable_ping: Enable Ping protocol for connectivity testing
            enable_autonat: Enable AutoNAT for NAT type detection
            enable_identify: Enable Identify protocol for peer info exchange
            enable_quic: Enable QUIC transport (experimental)
            enable_websocket: Enable WebSocket transport for browser compatibility
            rendezvous_is_server: If True, run as rendezvous server (for relay nodes)
        """
        self.identity = identity
        self.listen_port = listen_port
        # Use explicit None check - empty list means "no bootstrap peers"
        self.bootstrap_peers = BOOTSTRAP_PEERS if bootstrap_peers is None else bootstrap_peers
        self.enable_upnp = enable_upnp
        self.enable_relay = enable_relay
        self.enable_dht = enable_dht
        self.enable_pubsub = enable_pubsub
        self.enable_rendezvous = enable_rendezvous
        self.enable_mdns = enable_mdns
        self.enable_ping = enable_ping
        self.enable_autonat = enable_autonat
        self.enable_identify = enable_identify
        self.enable_quic = enable_quic
        self.enable_websocket = enable_websocket
        self.rendezvous_is_server = rendezvous_is_server

        # Core components (initialized in start())
        self._identity_bridge: Optional[EvrmoreIdentityBridge] = None
        self._host = None  # libp2p BasicHost
        self._host_context = None  # Host context manager
        self._listen_addrs = None  # Listen addresses
        self._dht = None   # Kademlia DHT
        self._pubsub = None  # GossipSub

        # Satori protocol layer
        self._subscriptions: Optional[SubscriptionManager] = None
        self._message_store: Optional[MessageStore] = None
        self._rendezvous: Optional[RendezvousManager] = None
        self._upnp: Optional[UPnPManager] = None

        # Rendezvous failover support
        self._rendezvous_backup_peers: List[str] = []  # Sorted by latency (best first)
        self._rendezvous_peers_by_latency: List[tuple] = []  # [(peer_id, latency_ms), ...]
        self._rendezvous_peer_infos: Dict[str, Any] = {}  # peer_id -> peer_info

        # State
        self._started = False
        self._peer_info: Dict[str, PeerInfo] = {}
        self._stream_info: Dict[str, StreamInfo] = {}
        self._callbacks: Dict[str, List[Callable]] = {}
        self._my_subscriptions: Set[str] = set()
        self._my_publications: Set[str] = set()

        # Background task management (trio)
        self._cancel_scope: Optional[trio.CancelScope] = None
        self._nursery: Optional[trio.Nursery] = None
        self._pending_responses: Dict[str, trio.Event] = {}
        self._response_data: Dict[str, Any] = {}
        self._pending_background_tasks: List[tuple] = []  # [(coro_func, args), ...]

        # Service managers (for proper cleanup)
        self._dht_manager = None  # TrioManager for DHT
        self._pubsub_manager = None  # TrioManager for Pubsub

        # Circuit Relay v2 components
        self._circuit_relay = None  # CircuitV2Protocol
        self._relay_discovery = None  # RelayDiscovery
        self._circuit_relay_manager = None  # TrioManager for Circuit Relay
        self._relay_discovery_manager = None  # TrioManager for Relay Discovery

        # mDNS discovery for local network
        self._mdns_discovery = None  # MDNSDiscovery

        # DCUtR for hole punching
        self._dcutr = None  # DCUtRProtocol
        self._dcutr_manager = None  # TrioManager for DCUtR

        # Additional protocols
        self._ping_service = None  # PingService for connectivity testing
        self._reachability_checker = None  # AutoNAT-like reachability checking
        self._identify_handler = None  # Identify protocol handler

        # Connection change callbacks for real-time UI updates
        self._connection_callbacks: List[Callable[[str, bool], None]] = []
        self._last_known_connections: Set[str] = set()

        # Peer latency tracking (peer_id -> latest RTT in ms)
        self._peer_latencies: Dict[str, float] = {}
        self._peer_latency_history: Dict[str, List[float]] = {}  # Last N measurements
        self._latency_history_size = 10  # Keep last 10 measurements per peer

        # Bandwidth tracking (optional, set via set_bandwidth_tracker)
        self._bandwidth_tracker = None

    # ========== Lifecycle ==========

    async def start(self) -> bool:
        """
        Initialize and start P2P networking.

        1. Convert Evrmore identity to libp2p identity
        2. Detect Docker environment
        3. Attempt UPnP port mapping
        4. Create libp2p host with transports
        5. Initialize DHT and GossipSub
        6. Connect to bootstrap peers
        7. Start protocol handlers

        Returns:
            True if started successfully
        """
        if self._started:
            logger.warning("Peers already started")
            return True

        try:
            logger.info("Starting P2P networking...")

            # 1. Bridge Evrmore identity to libp2p
            self._identity_bridge = EvrmoreIdentityBridge(self.identity)
            logger.debug(f"Evrmore address: {self._identity_bridge.evrmore_address}")

            # 2. Detect Docker environment
            docker_info = detect_docker_environment()
            if docker_info.in_container:
                logger.info(f"Running in Docker ({docker_info.network_mode} mode)")
                if docker_info.needs_relay:
                    logger.info("Bridge mode detected, relay will be used for connectivity")

            # 3. Attempt UPnP port mapping
            if self.enable_upnp:
                self._upnp = UPnPManager()
                upnp_success = await self._upnp.map_port(self.listen_port)
                if upnp_success:
                    logger.info(f"UPnP mapped port {self.listen_port}")
                else:
                    logger.info("UPnP not available, will use relay if needed")

            # 4. Create libp2p host
            await self._create_host()

            # 5. Start host (enter context manager)
            await self._start_host()

            # 6. Initialize DHT
            if self.enable_dht:
                await self._init_dht()

            # 7. Initialize GossipSub
            if self.enable_pubsub:
                await self._init_pubsub()

            # 8. Initialize Circuit Relay v2
            if self.enable_relay:
                await self._init_circuit_relay()

            # 9. Initialize mDNS for local network discovery
            if self.enable_mdns:
                await self._init_mdns()

            # 10. Initialize Ping protocol
            if self.enable_ping:
                await self._init_ping()

            # 11. Initialize AutoNAT (ReachabilityChecker)
            if self.enable_autonat:
                await self._init_autonat()

            # 12. Initialize Identify protocol
            if self.enable_identify:
                await self._init_identify()

            # 13. Initialize Satori protocol layer
            self._subscriptions = SubscriptionManager(self._dht)
            self._message_store = MessageStore(self._host, self._dht)

            # 11. Register protocol handlers
            await self._register_handlers()

            # NOTE: Bootstrap connection and rendezvous initialization are deferred
            # to run_forever() because the Pubsub and DHT services must be running
            # before any connections are made. Their stream handlers require
            # self.manager.is_running, which is only set when TrioManager runs them.

            # 12. Mark background tasks as deferred (started in run_forever)
            logger.debug("Background tasks deferred until run_forever() is called")

            self._started = True
            logger.info(f"P2P started. PeerID: {self.peer_id}")
            logger.info(f"Listening on port {self.listen_port}")
            return True

        except ImportError as e:
            logger.error(f"Missing dependency: {e}")
            logger.error("Install with: pip install libp2p trio")
            return False
        except Exception as e:
            logger.error(f"Failed to start P2P: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def stop(self) -> None:
        """Gracefully shutdown P2P networking."""
        if not self._started:
            return

        logger.info("Stopping P2P networking...")

        # Stop services first (DHT, Pubsub via TrioManager)
        await self._stop_services()

        # Cancel background tasks via cancel scope
        if self._cancel_scope:
            self._cancel_scope.cancel()

        # Stop Rendezvous
        if self._rendezvous:
            try:
                await self._rendezvous.stop()
            except Exception:
                pass

        # Remove UPnP mapping
        if self._upnp:
            try:
                await self._upnp.unmap_all()
            except Exception:
                pass

        # Stop host (exit context manager)
        await self._stop_host()

        self._started = False
        logger.info("P2P stopped")

    async def __aenter__(self) -> "Peers":
        """Async context manager entry - starts the P2P node."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit - stops the P2P node."""
        await self.stop()

    async def run_forever(self) -> None:
        """
        Run the P2P node with all background tasks until cancelled.

        This method runs:
        - KadDHT service (peer routing, content routing)
        - GossipSub/Pubsub service (message propagation)
        - Bootstrap connection (after services are ready)
        - Rendezvous initialization (after bootstrap)
        - Message cleanup (every 5 minutes)
        - Pending message retrieval
        - GossipSub message processing for all subscriptions

        Use in a trio nursery or call directly after start().
        """
        if not self._started:
            raise RuntimeError("Peers not started. Call start() first or use 'async with Peers(...)'")

        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            self._cancel_scope = nursery.cancel_scope

            # Spawn any queued background tasks first
            if self._pending_background_tasks:
                logger.info(f"Spawning {len(self._pending_background_tasks)} queued background task(s)")
                for coro_func, args in self._pending_background_tasks:
                    nursery.start_soon(coro_func, *args)
                    logger.debug(f"Queued task spawned: {coro_func.__name__}")
                self._pending_background_tasks.clear()

            # Start KadDHT service (CRITICAL: needed for peer discovery)
            if self._dht:
                nursery.start_soon(self._run_dht_service)
                logger.debug("KadDHT service started")

            # Start Pubsub service (CRITICAL: needed for message propagation)
            if self._pubsub:
                nursery.start_soon(self._run_pubsub_service)
                logger.debug("Pubsub service started")

            # Start Circuit Relay v2 service (for NAT traversal)
            if self._circuit_relay:
                nursery.start_soon(self._run_circuit_relay_service)
                logger.debug("Circuit Relay v2 service started")

            # Start Relay Discovery service (for finding relays)
            if self._relay_discovery:
                nursery.start_soon(self._run_relay_discovery_service)
                logger.debug("Relay Discovery service started")

            # Start DCUtR service (for hole punching)
            if self._dcutr:
                nursery.start_soon(self._run_dcutr_service)
                logger.debug("DCUtR (hole punching) service started")

            # Start mDNS discovery (for local network peer discovery)
            # Add random jitter to stagger mDNS starts and reduce simultaneous connection races
            if self._mdns_discovery:
                try:
                    import random
                    mdns_jitter = random.uniform(0.5, 2.5)
                    logger.debug(f"mDNS staggered start: waiting {mdns_jitter:.2f}s")
                    await trio.sleep(mdns_jitter)

                    self._mdns_discovery.start()  # Sync method
                    logger.info("mDNS discovery started")
                except Exception as e:
                    logger.warning(f"Failed to start mDNS: {e}")

            # Wait for services to initialize (pubsub needs to be ready for protocols)
            await trio.sleep(0.5)

            # Start custom Ping protocol (for connectivity testing)
            if self._ping_service:
                try:
                    await self._ping_service.start()
                    logger.debug("Custom Ping protocol started")
                except Exception as e:
                    logger.warning(f"Failed to start Ping protocol: {e}")

            # Start custom Identify protocol (for peer info exchange)
            if self._identify_handler:
                try:
                    await self._identify_handler.start()
                    logger.debug("Custom Identify protocol started")
                except Exception as e:
                    logger.warning(f"Failed to start Identify protocol: {e}")

            # Give services time to initialize their managers
            # This is needed because stream handlers check manager.is_running
            await trio.sleep(0.1)

            # NOW connect to bootstrap peers (services must be running first!)
            try:
                await self._connect_to_bootstrap()
            except Exception as e:
                logger.error(f"Bootstrap connection failed: {e}")
                # Continue anyway - we can still operate without bootstrap

            # Initialize Rendezvous for stream discovery (after bootstrap)
            if self.enable_rendezvous:
                try:
                    await self._init_rendezvous()
                except Exception as e:
                    logger.error(f"Rendezvous initialization failed: {e}")

            # Start background cleanup task
            nursery.start_soon(self._cleanup_task)

            # Start periodic latency measurement task
            nursery.start_soon(self._latency_measurement_task)

            # Start periodic subscription re-advertisement task
            # This ensures nodes that start before peers are available
            # will eventually advertise their subscriptions
            nursery.start_soon(self._subscription_readvertisement_task)

            # Start mesh repair task to fix py-libp2p GossipSub bug
            # where topics can end up with empty mesh due to race conditions
            nursery.start_soon(self._mesh_repair_task)

            # Retrieve pending messages
            nursery.start_soon(self._retrieve_pending_messages)

            # Start message processing for all current subscriptions
            for stream_id in list(self._my_subscriptions):
                nursery.start_soon(self.process_messages, stream_id)

            # Run until cancelled
            try:
                while True:
                    await trio.sleep(1)
            except trio.Cancelled:
                logger.info("run_forever cancelled, stopping services...")
                # Stop services gracefully before nursery exits
                await self._stop_services()
                raise

    async def _run_dht_service(self) -> None:
        """
        Run the KadDHT service.

        KadDHT extends Service from async_service and requires TrioManager
        to properly initialize its _manager attribute before run() can work.
        """
        if not self._dht:
            return

        try:
            from libp2p.tools.async_service import TrioManager
            logger.info("Starting KadDHT service via TrioManager...")
            self._dht_manager = TrioManager(self._dht)
            await self._dht_manager.run()
        except trio.Cancelled:
            logger.debug("KadDHT service cancelled")
            raise
        except Exception as e:
            logger.error(f"KadDHT service error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._dht_manager = None

    async def _run_pubsub_service(self) -> None:
        """
        Run the Pubsub/GossipSub service.

        Pubsub extends Service from async_service and requires TrioManager
        to properly initialize its _manager attribute before run() can work.
        """
        if not self._pubsub:
            return

        try:
            from libp2p.tools.async_service import TrioManager
            logger.info("Starting Pubsub service via TrioManager...")
            self._pubsub_manager = TrioManager(self._pubsub)
            await self._pubsub_manager.run()
        except trio.Cancelled:
            logger.debug("Pubsub service cancelled")
            raise
        except Exception as e:
            logger.error(f"Pubsub service error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._pubsub_manager = None

    async def _run_circuit_relay_service(self) -> None:
        """
        Run the Circuit Relay v2 protocol service.

        CircuitV2Protocol extends Service from async_service and requires
        TrioManager to properly initialize its _manager attribute.
        """
        if not self._circuit_relay:
            return

        try:
            from libp2p.tools.async_service import TrioManager
            logger.info("Starting Circuit Relay v2 service via TrioManager...")
            self._circuit_relay_manager = TrioManager(self._circuit_relay)
            await self._circuit_relay_manager.run()
        except trio.Cancelled:
            logger.debug("Circuit Relay v2 service cancelled")
            raise
        except Exception as e:
            logger.error(f"Circuit Relay v2 service error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._circuit_relay_manager = None

    async def _run_relay_discovery_service(self) -> None:
        """
        Run the Relay Discovery service.

        RelayDiscovery extends Service from async_service and requires
        TrioManager to properly initialize its _manager attribute.
        """
        if not self._relay_discovery:
            return

        try:
            from libp2p.tools.async_service import TrioManager
            logger.info("Starting Relay Discovery service via TrioManager...")
            self._relay_discovery_manager = TrioManager(self._relay_discovery)
            await self._relay_discovery_manager.run()
        except trio.Cancelled:
            logger.debug("Relay Discovery service cancelled")
            raise
        except Exception as e:
            logger.error(f"Relay Discovery service error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._relay_discovery_manager = None

    async def _run_dcutr_service(self) -> None:
        """
        Run the DCUtR (Direct Connection Upgrade through Relay) service.

        DCUtRProtocol extends Service from async_service and requires
        TrioManager to properly initialize its _manager attribute.
        """
        if not self._dcutr:
            return

        try:
            from libp2p.tools.async_service import TrioManager
            logger.info("Starting DCUtR (hole punching) service via TrioManager...")
            self._dcutr_manager = TrioManager(self._dcutr)
            await self._dcutr_manager.run()
        except trio.Cancelled:
            logger.debug("DCUtR service cancelled")
            raise
        except Exception as e:
            logger.error(f"DCUtR service error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._dcutr_manager = None

    async def _stop_services(self) -> None:
        """Stop all services (DHT, Pubsub, Relay, mDNS, DCUtR) gracefully."""
        # Stop mDNS first
        if self._mdns_discovery:
            logger.debug("Stopping mDNS discovery...")
            try:
                self._mdns_discovery.stop()  # Sync method
            except Exception:
                pass

        # Stop DCUtR
        if self._dcutr_manager and self._dcutr_manager.is_running:
            logger.debug("Stopping DCUtR service...")
            self._dcutr_manager.cancel()
            try:
                with trio.move_on_after(2):
                    await self._dcutr_manager.wait_finished()
            except Exception:
                pass

        # Stop Relay Discovery (it depends on relay)
        if self._relay_discovery_manager and self._relay_discovery_manager.is_running:
            logger.debug("Stopping Relay Discovery service...")
            self._relay_discovery_manager.cancel()
            try:
                with trio.move_on_after(2):
                    await self._relay_discovery_manager.wait_finished()
            except Exception:
                pass

        # Stop Circuit Relay
        if self._circuit_relay_manager and self._circuit_relay_manager.is_running:
            logger.debug("Stopping Circuit Relay v2 service...")
            self._circuit_relay_manager.cancel()
            try:
                with trio.move_on_after(2):
                    await self._circuit_relay_manager.wait_finished()
            except Exception:
                pass

        # Stop Pubsub
        if self._pubsub_manager and self._pubsub_manager.is_running:
            logger.debug("Stopping Pubsub service...")
            self._pubsub_manager.cancel()
            try:
                with trio.move_on_after(2):
                    await self._pubsub_manager.wait_finished()
            except Exception:
                pass

        # Stop DHT last
        if self._dht_manager and self._dht_manager.is_running:
            logger.debug("Stopping KadDHT service...")
            self._dht_manager.cancel()
            try:
                with trio.move_on_after(2):
                    await self._dht_manager.wait_finished()
            except Exception:
                pass

    async def _cleanup_task(self) -> None:
        """Background task to cleanup expired messages periodically."""
        while True:
            await trio.sleep(300)  # Every 5 minutes
            if self._message_store:
                self._message_store.cleanup_expired()
                logger.debug("Cleaned up expired messages")

    async def _latency_measurement_task(self) -> None:
        """
        Background task to periodically measure latency to all connected peers.

        Pings each connected peer every 30 seconds to maintain fresh RTT data.
        This enables the UI to show current latency values for all peers.
        """
        # Wait for initial startup before measuring
        await trio.sleep(10)

        while True:
            try:
                connected_peers = self.get_connected_peers()
                if connected_peers and self._ping_service:
                    for peer_id in connected_peers:
                        try:
                            # Single ping with short timeout to avoid blocking
                            await self.ping_peer(str(peer_id), count=1, timeout=5.0)
                            # Small delay between pings to avoid flooding
                            await trio.sleep(0.5)
                        except Exception as e:
                            logger.debug(f"Latency measurement to {peer_id} failed: {e}")

                    avg_latency = self.get_network_avg_latency()
                    if avg_latency is not None:
                        logger.debug(f"Network avg latency: {avg_latency:.1f}ms across {len(self._peer_latencies)} peers")

            except Exception as e:
                logger.debug(f"Latency measurement task error: {e}")

            # Measure every 30 seconds
            await trio.sleep(30)

    async def _subscription_readvertisement_task(self) -> None:
        """
        Background task to periodically re-advertise topic subscriptions.

        This ensures nodes that started before peers were available will
        eventually advertise their subscriptions once peers connect.
        Without this, the first node to start would not be discoverable
        for topics it subscribed to before any peers were available.

        Runs every 60 seconds after an initial 30-second delay.
        """
        # Wait for initial startup and peer connections
        await trio.sleep(30)

        while True:
            try:
                connected_count = len(self.get_connected_peers())
                subscriptions = list(self._my_subscriptions)

                if connected_count > 0 and subscriptions and self._subscriptions and self.peer_id:
                    logger.info(f"Re-advertising {len(subscriptions)} subscription(s) to {connected_count} connected peer(s)")

                    for stream_id in subscriptions:
                        try:
                            await self._subscriptions.announce_subscription(
                                self.peer_id,
                                stream_id,
                                self.evrmore_address
                            )
                        except Exception as e:
                            logger.debug(f"Failed to re-advertise subscription {stream_id}: {e}")

                    logger.debug("Subscription re-advertisement complete")
                elif subscriptions and connected_count == 0:
                    logger.debug(f"Skipping re-advertisement: no connected peers (have {len(subscriptions)} subscription(s))")

            except Exception as e:
                logger.debug(f"Subscription re-advertisement task error: {e}")

            # Re-advertise every 60 seconds
            await trio.sleep(60)

    async def _mesh_repair_task(self) -> None:
        """
        Background task to repair broken GossipSub meshes.

        This fixes a py-libp2p bug where topics can end up with empty or
        undersized mesh despite having peers available in peer_topics.
        This can happen due to race conditions during subscription when
        peers subscribe before others are available.

        The task checks all topic meshes and grafts peers when mesh size
        falls below D_low (minimum mesh size). It repairs up to D (target
        mesh size) peers.

        Runs every 60 seconds after an initial 45-second delay.
        """
        logger.info("Mesh repair task starting (45s initial delay)...")
        # Wait for initial mesh formation
        await trio.sleep(45)
        logger.info("Mesh repair task active - will check every 60s")

        while True:
            try:
                if self._pubsub and hasattr(self._pubsub, 'router'):
                    router = self._pubsub.router
                    repairs_made = 0

                    # Step 1: Try to add connected peers to pubsub if they're missing
                    # This fixes py-libp2p issue where pubsub doesn't open streams to all peers
                    # Get PeerID objects directly from network connections
                    try:
                        connected_peer_ids = list(self._host.get_network().connections.keys())
                    except Exception:
                        connected_peer_ids = []
                    pubsub_peers = set(self._pubsub.peers.keys()) if hasattr(self._pubsub, 'peers') else set()
                    missing_from_pubsub = set(connected_peer_ids) - pubsub_peers

                    if missing_from_pubsub:
                        logger.info(f"Mesh repair: {len(missing_from_pubsub)} connected peers missing from pubsub")
                        for peer_id in missing_from_pubsub:
                            try:
                                # Use pubsub's _handle_new_peer to properly setup the stream
                                # This sends hello packet and stores stream in pubsub.peers
                                if hasattr(self._pubsub, '_handle_new_peer'):
                                    await self._pubsub._handle_new_peer(peer_id)
                                    # Check if it worked
                                    if peer_id in self._pubsub.peers:
                                        logger.info(f"Mesh repair: successfully added peer {peer_id} to pubsub")
                                    else:
                                        logger.debug(f"Mesh repair: _handle_new_peer did not add {peer_id}")
                            except Exception as e:
                                logger.debug(f"Mesh repair: could not add {peer_id} to pubsub: {e}")

                    # Step 2: Graft connected peers to mesh if they're in peer_topics but not in mesh
                    # This ensures all connected peers that subscribe to a topic are in our mesh
                    connected_set = set(connected_peer_ids)
                    for topic in list(router.mesh.keys()):
                        mesh_peers = router.mesh.get(topic, set())
                        mesh_size = len(mesh_peers)
                        peer_topics = self._pubsub.peer_topics.get(topic, set())

                        # Find connected peers that are subscribed to this topic but not in our mesh
                        connected_subscribers = peer_topics & connected_set
                        missing = connected_subscribers - mesh_peers

                        if missing:
                            added = 0
                            for peer in list(missing):
                                try:
                                    router.mesh[topic].add(peer)
                                    await router.emit_graft(topic, peer)
                                    added += 1
                                except Exception as e:
                                    logger.debug(f"Failed to graft peer {peer} to {topic}: {e}")

                            if added > 0:
                                logger.info(f"Mesh repair: added {added} peer(s) to {topic} (was {mesh_size}, now {mesh_size + added})")
                                repairs_made += added

                    if repairs_made > 0:
                        logger.info(f"Mesh repair complete: {repairs_made} total peer(s) added across topics")

                    # Log current mesh status for visibility (get fresh counts)
                    total_mesh_peers = sum(len(router.mesh.get(t, set())) for t in router.mesh.keys())
                    total_topics = len(router.mesh)
                    current_pubsub_peers = len(self._pubsub.peers) if hasattr(self._pubsub, 'peers') else 0
                    connected_count = len(connected_peer_ids)
                    logger.info(f"Mesh status: connected={connected_count}, pubsub={current_pubsub_peers}, topics={total_topics}, mesh_peers={total_mesh_peers}")

            except Exception as e:
                logger.warning(f"Mesh repair task error: {e}")

            # Check every 60 seconds
            await trio.sleep(60)

    # ========== Properties ==========

    @property
    def peer_id(self) -> Optional[str]:
        """Get this node's peer ID."""
        if self._identity_bridge:
            return self._identity_bridge.get_peer_id()
        return None

    @property
    def evrmore_address(self) -> str:
        """Get this node's Evrmore address."""
        return self.identity.address

    @property
    def public_key(self) -> str:
        """Get this node's public key (hex)."""
        return self.identity.pubkey

    @property
    def is_connected(self) -> bool:
        """Check if connected to the P2P network."""
        if not self._host:
            return False
        try:
            return len(self._host.get_network().connections) > 0
        except:
            return False

    @property
    def nat_type(self) -> str:
        """
        Get detected NAT type.

        Returns:
            "PUBLIC" - Node has public connectivity (UPnP mapped or public IP)
            "PRIVATE" - Node is behind NAT without port mapping
            "UNKNOWN" - NAT status cannot be determined
        """
        # Check if UPnP successfully mapped a port
        if self._upnp and self._upnp.is_mapped:
            return "PUBLIC"

        # Check if we have public addresses
        addrs = self.public_addresses
        if addrs:
            from .nat.docker import detect_docker_environment
            docker_info = detect_docker_environment()

            # In Docker bridge mode, we're behind NAT
            if docker_info.in_container and docker_info.is_bridge_mode:
                return "PRIVATE"

            # Check if any address is a public IP (not 10.x, 172.16-31.x, 192.168.x)
            for addr in addrs:
                if self._is_public_address(addr):
                    return "PUBLIC"

        # If we have incoming connections, we might be publicly reachable
        if self._host:
            try:
                connections = self._host.get_network().connections
                # Check for inbound connections (simplified check)
                if len(connections) > 0:
                    return "UNKNOWN"  # Could be either, needs more info
            except:
                pass

        return "PRIVATE"

    def _is_public_address(self, multiaddr: str) -> bool:
        """Check if a multiaddress contains a public IP."""
        import re
        # Extract IP from multiaddr like /ip4/1.2.3.4/tcp/4001
        ip_match = re.search(r'/ip4/(\d+\.\d+\.\d+\.\d+)/', multiaddr)
        if not ip_match:
            return False

        ip = ip_match.group(1)
        octets = [int(o) for o in ip.split('.')]

        # Private IP ranges
        if octets[0] == 10:  # 10.0.0.0/8
            return False
        if octets[0] == 172 and 16 <= octets[1] <= 31:  # 172.16.0.0/12
            return False
        if octets[0] == 192 and octets[1] == 168:  # 192.168.0.0/16
            return False
        if octets[0] == 127:  # Loopback
            return False
        if octets[0] == 0:  # Invalid
            return False

        return True

    @property
    def public_addresses(self) -> List[str]:
        """Get public multiaddresses."""
        if self._host:
            try:
                return [str(addr) for addr in self._host.get_addrs()]
            except:
                pass
        return []

    @property
    def is_relay(self) -> bool:
        """Check if acting as a relay node."""
        return self.enable_relay and self.nat_type in ("PUBLIC", "UNKNOWN")

    @property
    def connected_peers(self) -> int:
        """Get number of connected peers."""
        if self._host:
            try:
                return len(self._host.get_network().connections)
            except:
                pass
        return 0

    # ========== Communication ==========

    async def send(
        self,
        peer_id: str,
        message: Any,
        reliable: bool = True
    ) -> bool:
        """
        Send message to specific peer.

        If peer is offline and reliable=True, message is queued
        for delivery when peer reconnects.

        Args:
            peer_id: Target peer ID
            message: Message payload (will be serialized)
            reliable: If True, queue for offline delivery

        Returns:
            True if sent (or queued) successfully
        """
        if not self._started:
            logger.warning("P2P not started")
            return False

        try:
            # Check if peer is connected
            if await self._is_peer_connected(peer_id):
                return await self._send_direct(peer_id, message)
            elif reliable and self._message_store:
                # Queue for later delivery
                payload = serialize_message(message)
                await self._message_store.store_for_peer(peer_id, payload)
                logger.debug(f"Queued message for offline peer {peer_id}")
                return True
            else:
                logger.debug(f"Peer {peer_id} not connected")
                return False

        except Exception as e:
            logger.error(f"Failed to send to {peer_id}: {e}")
            return False

    async def broadcast(
        self,
        stream_id_or_message: Any,
        message: Any = None
    ) -> int:
        """
        Broadcast message to all peers or stream subscribers.

        Args:
            stream_id_or_message: Stream ID (topic) if message provided, else message
            message: Message payload (if stream_id provided as first arg)

        Returns:
            Number of peers message was sent to

        Usage:
            # Broadcast to topic (common pattern)
            await peers.broadcast("satori/topic", {"data": "value"})

            # Broadcast to all peers (no topic)
            await peers.broadcast({"data": "value"})
        """
        if not self._started:
            return 0

        # Handle both calling conventions:
        # broadcast(topic, message) - new common pattern
        # broadcast(message) - no topic, broadcast to all
        if message is not None:
            stream_id = stream_id_or_message
        else:
            stream_id = None
            message = stream_id_or_message

        if stream_id and self._pubsub:
            # Publish to GossipSub topic
            topic = f"{STREAM_TOPIC_PREFIX}{stream_id}"
            data = serialize_message(message)
            try:
                logger.info(f"Broadcasting to topic {topic}")
                await self._pubsub.publish(topic, data)
                logger.info(f"Broadcast to {topic} successful")

                # Track bandwidth for outgoing message
                if self._bandwidth_tracker:
                    await self._bandwidth_tracker.account_publish(stream_id, len(data))

                # GossipSub handles mesh propagation
                subs = self._subscriptions.get_subscribers(stream_id) if self._subscriptions else []
                return len(subs)
            except Exception as e:
                logger.error(f"Broadcast to {topic} failed: {e}")
                return 0
        elif stream_id and not self._pubsub:
            logger.warning(f"Cannot broadcast to {stream_id}: pubsub not available")
        else:
            # Broadcast to all connected peers
            count = 0
            for peer_id in self.get_connected_peers():
                if await self.send(peer_id, message, reliable=False):
                    count += 1
            return count

    async def request(
        self,
        peer_id: str,
        message: Any,
        timeout: float = 30.0
    ) -> Optional[Any]:
        """
        Send request and wait for response.

        Args:
            peer_id: Target peer ID
            message: Request payload
            timeout: Timeout in seconds

        Returns:
            Response payload or None if timeout/error
        """
        if not self._started:
            return None

        request_id = str(uuid.uuid4())

        try:
            # Add request ID for correlation
            if isinstance(message, dict):
                message["_request_id"] = request_id
            else:
                message = {"_request_id": request_id, "data": message}

            # Create event for response waiting
            response_event = trio.Event()
            self._pending_responses[request_id] = response_event
            self._response_data[request_id] = None

            # Send request
            success = await self._send_direct(peer_id, message)
            if not success:
                self._pending_responses.pop(request_id, None)
                self._response_data.pop(request_id, None)
                return None

            # Wait for response with timeout
            with trio.move_on_after(timeout) as cancel_scope:
                await response_event.wait()

            # Cleanup and return result
            self._pending_responses.pop(request_id, None)
            response = self._response_data.pop(request_id, None)

            if cancel_scope.cancelled_caught:
                logger.warning(f"Request to {peer_id} timed out")
                return None

            return response

        except Exception as e:
            logger.error(f"Request to {peer_id} failed: {e}")
            # Cleanup on error
            self._pending_responses.pop(request_id, None)
            self._response_data.pop(request_id, None)
            return None

    def _handle_response(self, request_id: str, data: Any) -> bool:
        """
        Handle an incoming response for a pending request.

        Called by the protocol handler when a response message is received.

        Args:
            request_id: The request ID from the original request
            data: The response data

        Returns:
            True if the response was matched to a pending request
        """
        if request_id in self._pending_responses:
            self._response_data[request_id] = data
            self._pending_responses[request_id].set()
            return True
        return False

    # ========== Subscription Management ==========

    async def subscribe(
        self,
        stream_id: str,
        callback: Callable[[str, Any], None]
    ) -> None:
        """
        Subscribe to a data stream.

        Args:
            stream_id: Stream UUID to subscribe to
            callback: Function called with (stream_id, data) on new data

        Note: This is now async for compatibility with protocol classes.
        For full network registration, call subscribe_async() instead.
        """
        if stream_id not in self._callbacks:
            self._callbacks[stream_id] = []
        self._callbacks[stream_id].append(callback)
        self._my_subscriptions.add(stream_id)
        logger.debug(f"Subscribed to stream {stream_id}")

    async def subscribe_async(self, stream_id: str, callback: Callable[[str, Any], None]) -> None:
        """
        Subscribe to a data stream with full network registration.

        Args:
            stream_id: Stream UUID to subscribe to
            callback: Function called with (stream_id, data) on new data
        """
        # Local subscription
        await self.subscribe(stream_id, callback)

        # Subscribe to GossipSub topic
        if self._pubsub:
            topic = f"{STREAM_TOPIC_PREFIX}{stream_id}"
            await self._subscribe_to_topic(topic, stream_id)
            # Start message processor for this subscription
            if self._nursery:
                self._nursery.start_soon(self.process_messages, stream_id)

        # Announce subscription to DHT
        if self._subscriptions and self.peer_id:
            await self._subscriptions.announce_subscription(
                self.peer_id,
                stream_id,
                self.evrmore_address
            )

        # Register with Rendezvous for fast stream-specific discovery
        if self._rendezvous:
            await self._rendezvous.register_subscriber(stream_id)

    async def unsubscribe(self, stream_id: str) -> None:
        """Unsubscribe from a data stream (local only)."""
        self._callbacks.pop(stream_id, None)
        self._my_subscriptions.discard(stream_id)
        logger.debug(f"Unsubscribed from stream {stream_id}")

    async def unsubscribe_async(self, stream_id: str) -> None:
        """Unsubscribe from a data stream with network unregistration."""
        await self.unsubscribe(stream_id)

        if self._pubsub:
            topic = f"{STREAM_TOPIC_PREFIX}{stream_id}"
            await self._unsubscribe_from_topic(topic)

        # Unregister from Rendezvous
        if self._rendezvous:
            await self._rendezvous.unregister_subscriber(stream_id)

    async def publish(self, stream_id: str, data: Any) -> None:
        """
        Publish data to a stream.

        Args:
            stream_id: Stream UUID to publish to
            data: Data to publish (will be serialized)
        """
        self._my_publications.add(stream_id)
        await self.broadcast(data, stream_id=stream_id)

        # Announce as publisher via DHT
        if self._subscriptions and self.peer_id:
            await self._subscriptions.announce_publication(
                self.peer_id,
                stream_id,
                self.evrmore_address
            )

        # Register as publisher with Rendezvous
        if self._rendezvous:
            await self._rendezvous.register_publisher(stream_id)

    # ========== Peer Discovery ==========

    async def discover_peers(self, stream_id: Optional[str] = None) -> List[str]:
        """
        Discover peers via DHT and Rendezvous.

        Uses Rendezvous for fast stream-specific discovery, falls back to DHT.

        Args:
            stream_id: If provided, find peers subscribed to this stream

        Returns:
            List of discovered peer IDs
        """
        if stream_id:
            peers = set()

            # Try Rendezvous first (faster for stream-specific)
            if self._rendezvous:
                rv_peers = await self._rendezvous.discover_subscribers(stream_id)
                peers.update(rv_peers)

            # Also query DHT for completeness
            if self._subscriptions:
                dht_peers = await self._subscriptions.find_subscribers(stream_id)
                peers.update(dht_peers)

            return list(peers)
        else:
            # General peer discovery via DHT
            discovered = set()

            # Get peers from DHT routing table
            if self._dht:
                try:
                    # Refresh routing table to discover more peers
                    await self._dht.refresh_routing_table()

                    # Get routing table entries
                    routing_table_size = self._dht.get_routing_table_size()
                    logger.debug(f"DHT routing table size: {routing_table_size}")

                    # Also get currently connected peers as they're in the DHT network
                    if self._host:
                        connected = self._host.get_connected_peers()
                        for peer in connected:
                            discovered.add(str(peer))
                except Exception as e:
                    logger.debug(f"DHT peer discovery error: {e}")

            # Add known peers from our peer info store
            for peer_id in self._peer_info.keys():
                discovered.add(peer_id)

            return list(discovered)

    async def discover_publishers(self, stream_id: str) -> List[str]:
        """
        Discover peers publishing to a stream.

        Uses Rendezvous for fast discovery, falls back to DHT.

        Args:
            stream_id: Stream UUID to find publishers for

        Returns:
            List of publisher peer IDs
        """
        publishers = set()

        # Try Rendezvous first
        if self._rendezvous:
            rv_pubs = await self._rendezvous.discover_publishers(stream_id)
            publishers.update(rv_pubs)

        # Also query DHT
        if self._subscriptions:
            dht_pubs = await self._subscriptions.find_publishers(stream_id)
            publishers.update(dht_pubs)

        return list(publishers)

    def get_peers(self) -> List[PeerInfo]:
        """Get list of known peers with status."""
        return list(self._peer_info.values())

    def get_connected_peers(self) -> List[str]:
        """Get list of currently connected peer IDs."""
        if not self._host:
            return []
        try:
            # connections is a dict with peer ID as key
            return [str(peer_id) for peer_id in self._host.get_network().connections.keys()]
        except Exception:
            return []

    def get_peer_count(self) -> int:
        """Get count of currently connected peers."""
        return len(self.get_connected_peers())

    def set_bandwidth_tracker(self, tracker) -> None:
        """
        Set the bandwidth tracker for monitoring network traffic.

        Args:
            tracker: BandwidthTracker instance from satorip2p.protocol.bandwidth
        """
        self._bandwidth_tracker = tracker
        logger.debug("Bandwidth tracker attached to Peers")

    def on_connection_change(self, callback: Callable[[str, bool], None]) -> None:
        """
        Register a callback for connection changes.

        The callback will be called with (peer_id, connected) where:
        - peer_id: The peer ID that connected/disconnected
        - connected: True if connected, False if disconnected

        Usage:
            def my_callback(peer_id: str, connected: bool):
                if connected:
                    print(f"Peer {peer_id} connected")
                else:
                    print(f"Peer {peer_id} disconnected")

            peers.on_connection_change(my_callback)
        """
        self._connection_callbacks.append(callback)

    def check_connection_changes(self) -> List[tuple]:
        """
        Check for connection changes since last check.

        Returns list of (peer_id, connected) tuples for any changes.
        Also triggers registered callbacks.
        """
        current = set(self.get_connected_peers())
        previous = self._last_known_connections

        changes = []

        # Find new connections
        for peer_id in current - previous:
            changes.append((peer_id, True))

        # Find disconnections
        for peer_id in previous - current:
            changes.append((peer_id, False))

        # Update state
        self._last_known_connections = current

        # Trigger callbacks
        for peer_id, connected in changes:
            for callback in self._connection_callbacks:
                try:
                    callback(peer_id, connected)
                except Exception as e:
                    logger.warning(f"Connection callback error: {e}")

        return changes

    def get_pubsub_debug(self) -> Dict[str, Any]:
        """
        Get debug information about the GossipSub/Pubsub state.

        Returns dict with:
        - pubsub_peers: peers with open pubsub streams
        - peer_topics: which peers are subscribed to which topics
        - mesh: GossipSub mesh state (peers in mesh per topic)
        - my_topics: topics we are subscribed to
        """
        result = {
            "pubsub_available": self._pubsub is not None,
            "pubsub_peers": [],
            "peer_topics": {},
            "mesh": {},
            "my_topics": [],
        }

        if not self._pubsub:
            return result

        # Get peers with open pubsub streams
        if hasattr(self._pubsub, 'peers'):
            result["pubsub_peers"] = [str(pid) for pid in self._pubsub.peers.keys()]

        # Get peer_topics (which peers are subscribed to which topics)
        if hasattr(self._pubsub, 'peer_topics'):
            result["peer_topics"] = {
                topic: [str(pid) for pid in peers]
                for topic, peers in self._pubsub.peer_topics.items()
            }

        # Get our subscribed topics
        if hasattr(self._pubsub, 'topic_ids'):
            result["my_topics"] = list(self._pubsub.topic_ids)

        # Get GossipSub mesh state
        if hasattr(self._pubsub, 'router') and hasattr(self._pubsub.router, 'mesh'):
            result["mesh"] = {
                topic: [str(pid) for pid in peers]
                for topic, peers in self._pubsub.router.mesh.items()
            }

        # Get peer_protocol (which protocol each peer is using)
        if hasattr(self._pubsub, 'router') and hasattr(self._pubsub.router, 'peer_protocol'):
            result["peer_protocol"] = {
                str(pid): str(proto)
                for pid, proto in self._pubsub.router.peer_protocol.items()
            }

        # Get backoff state (peers temporarily excluded from mesh)
        if hasattr(self._pubsub, 'router') and hasattr(self._pubsub.router, 'back_off'):
            import time
            current_time = time.time()
            backoff_info = {}
            for (peer_id, topic), expiry in self._pubsub.router.back_off.items():
                key = f"{str(peer_id)}:{topic}"
                backoff_info[key] = {
                    "expires_in": round(expiry - current_time, 1),
                    "expired": expiry < current_time
                }
            result["backoff"] = backoff_info

        # Get fanout state
        if hasattr(self._pubsub, 'router') and hasattr(self._pubsub.router, 'fanout'):
            result["fanout"] = {
                topic: [str(pid) for pid in peers]
                for topic, peers in self._pubsub.router.fanout.items()
            }

        return result

    def get_subscribers(self, stream_id: str) -> List[str]:
        """Get peers subscribed to a stream."""
        if self._subscriptions:
            return self._subscriptions.get_subscribers(stream_id)
        return []

    def get_publishers(self, stream_id: str) -> List[str]:
        """Get peers publishing to a stream."""
        if self._subscriptions:
            return self._subscriptions.get_publishers(stream_id)
        return []

    def get_peer_subscriptions(self, peer_id: str) -> List[str]:
        """Get streams a peer is subscribed to."""
        if self._subscriptions:
            return self._subscriptions.get_peer_subscriptions(peer_id)
        return []

    def get_my_subscriptions(self) -> List[str]:
        """Get streams we are subscribed to."""
        return list(self._my_subscriptions)

    def get_my_publications(self) -> List[str]:
        """Get streams we publish to."""
        return list(self._my_publications)

    # ========== Connectivity & NAT Detection ==========

    async def connect_peer(self, multiaddr: str, timeout: float = 30.0) -> bool:
        """
        Connect to a peer by multiaddress.

        Implements simultaneous connect handling: when both peers try to connect
        to each other at the same time, uses peer ID comparison to determine
        which peer should initiate. The peer with the "higher" peer ID initiates.

        Args:
            multiaddr: Full multiaddress including peer ID
                      (e.g., /ip4/172.17.0.3/tcp/24600/p2p/16Uiu2HAk...)
            timeout: Connection timeout in seconds

        Returns:
            True if connection succeeded, False otherwise
        """
        if not self._host:
            logger.warning("Host not initialized")
            return False

        try:
            import trio
            from multiaddr import Multiaddr
            from libp2p.peer.peerinfo import info_from_p2p_addr

            # Resolve DNS if needed
            resolved_addr = self._resolve_multiaddr_dns(multiaddr)
            maddr = Multiaddr(resolved_addr)
            peer_info = info_from_p2p_addr(maddr)
            target_peer_id = str(peer_info.peer_id)

            # Check if already connected - reuse existing connection
            network = self._host.get_network()
            if peer_info.peer_id in network.connections:
                existing_conns = network.connections.get(peer_info.peer_id, [])
                if existing_conns:
                    logger.debug(f"Already connected to {target_peer_id}")
                    return True

            # Simultaneous connect handling using peer ID comparison
            # The peer with the "higher" peer ID (lexicographically) initiates
            my_peer_id = str(self._host.get_id())

            # Track pending connections to prevent duplicates
            if not hasattr(self, '_pending_connections'):
                self._pending_connections = set()

            if target_peer_id in self._pending_connections:
                logger.debug(f"Connection to {target_peer_id} already pending")
                # Wait briefly and check if connection succeeded
                await trio.sleep(1.0)
                if peer_info.peer_id in network.connections:
                    return True
                return False

            # Peer ID tiebreaker: lower ID waits, higher ID connects
            if my_peer_id < target_peer_id:
                # We have lower ID - wait briefly for them to connect to us
                logger.debug(f"Peer ID tiebreaker: waiting for {target_peer_id} to connect")
                await trio.sleep(0.5)
                # Check if they connected to us
                if peer_info.peer_id in network.connections:
                    logger.info(f"Peer {target_peer_id} connected to us")
                    return True
                # They didn't connect, proceed with our connection
                logger.debug(f"Proceeding with connection to {target_peer_id}")

            self._pending_connections.add(target_peer_id)
            try:
                logger.info(f"Connecting to peer {target_peer_id}")

                with trio.move_on_after(timeout) as cancel_scope:
                    await self._host.connect(peer_info)

                if cancel_scope.cancelled_caught:
                    logger.warning(f"Connection to {target_peer_id} timed out")
                    return False

                logger.info(f"Connected to peer: {target_peer_id}")
                return True
            finally:
                self._pending_connections.discard(target_peer_id)

        except Exception as e:
            logger.warning(f"Failed to connect to peer: {e}")
            return False

    async def disconnect_peer(self, peer_id: str) -> bool:
        """
        Disconnect from a peer.

        Args:
            peer_id: Peer ID to disconnect from

        Returns:
            True if disconnection succeeded, False otherwise
        """
        if not self._host:
            logger.warning("Host not initialized")
            return False

        try:
            from libp2p.peer.id import ID as PeerID

            # Parse peer ID
            pid = PeerID.from_base58(peer_id)

            # Close all connections to this peer
            network = self._host.get_network()
            if pid in network.connections:
                for conn in list(network.connections.get(pid, [])):
                    try:
                        await conn.close()
                    except Exception as e:
                        logger.debug(f"Error closing connection: {e}")

                # Remove from connections dict
                network.connections.pop(pid, None)
                logger.info(f"Disconnected from peer: {peer_id}")
                return True
            else:
                logger.info(f"Peer {peer_id} was not connected")
                return True  # Not an error if already disconnected

        except Exception as e:
            logger.warning(f"Failed to disconnect from peer: {e}")
            return False

    async def ping_peer(self, peer_id: str, count: int = 3, timeout: float = 10.0) -> Optional[List[float]]:
        """
        Ping a peer to test connectivity and measure latency.

        Uses our custom GossipSub-based Ping protocol.

        Args:
            peer_id: Target peer ID to ping
            count: Number of ping requests to send (default: 3)
            timeout: Timeout per ping in seconds (default: 10.0)

        Returns:
            List of round-trip times in seconds, or None if all pings failed
        """
        if not self._ping_service:
            logger.warning("Ping service not initialized")
            return None

        try:
            latencies = await self._ping_service.ping(peer_id, count, timeout)
            if latencies:
                avg_rtt = sum(latencies) / len(latencies)
                avg_rtt_ms = avg_rtt * 1000
                logger.debug(f"Ping to {peer_id}: avg={avg_rtt_ms:.2f}ms")

                # Store latency for this peer
                self._peer_latencies[peer_id] = avg_rtt_ms

                # Update history
                if peer_id not in self._peer_latency_history:
                    self._peer_latency_history[peer_id] = []
                self._peer_latency_history[peer_id].append(avg_rtt_ms)
                # Keep only last N measurements
                if len(self._peer_latency_history[peer_id]) > self._latency_history_size:
                    self._peer_latency_history[peer_id] = self._peer_latency_history[peer_id][-self._latency_history_size:]

            return latencies

        except Exception as e:
            logger.debug(f"Ping to {peer_id} failed: {e}")
            return None

    def get_peer_latency(self, peer_id: str) -> Optional[float]:
        """Get the latest latency measurement for a peer in milliseconds."""
        return self._peer_latencies.get(peer_id)

    def get_peer_avg_latency(self, peer_id: str) -> Optional[float]:
        """Get the average latency for a peer from recent history."""
        history = self._peer_latency_history.get(peer_id, [])
        if history:
            return sum(history) / len(history)
        return None

    def get_all_peer_latencies(self) -> Dict[str, float]:
        """Get all stored peer latencies."""
        return dict(self._peer_latencies)

    def get_network_avg_latency(self) -> Optional[float]:
        """Get average latency across all peers with measurements."""
        if not self._peer_latencies:
            return None
        return sum(self._peer_latencies.values()) / len(self._peer_latencies)

    def get_known_peer_identities(self) -> Dict[str, Any]:
        """
        Get identities of known peers from the Identify protocol.

        Returns:
            Dict mapping peer_id to PeerIdentity objects
        """
        if not self._identify_handler:
            return {}

        try:
            return self._identify_handler.get_known_peers()
        except Exception as e:
            logger.debug(f"Failed to get peer identities: {e}")
            return {}

    def get_peers_by_role(self, role: str) -> List[Any]:
        """
        Get peers with a specific role (predictor, relay, oracle, signer).

        Args:
            role: Role to filter by

        Returns:
            List of PeerIdentity objects for peers with that role
        """
        if not self._identify_handler:
            return []

        try:
            return self._identify_handler.get_peers_by_role(role)
        except Exception as e:
            logger.debug(f"Failed to get peers by role: {e}")
            return []

    async def announce_identity(self) -> None:
        """
        Announce our identity to the network.

        This broadcasts our peer ID, Evrmore address, supported protocols,
        and node roles to all connected peers.
        """
        if not self._identify_handler:
            logger.warning("Identify protocol not initialized")
            return

        try:
            await self._identify_handler.announce()
            logger.debug("Identity announced")
        except Exception as e:
            logger.warning(f"Failed to announce identity: {e}")

    def forget_peer(self, peer_id: str) -> bool:
        """
        Remove a peer from the known peers list.

        Args:
            peer_id: libp2p peer ID to forget

        Returns:
            True if peer was found and removed, False otherwise
        """
        if not self._identify_handler:
            logger.warning("Identify protocol not initialized")
            return False

        return self._identify_handler.forget_peer(peer_id)

    async def check_reachability(self) -> bool:
        """
        Check if this node is publicly reachable.

        Uses AutoNAT to have other peers attempt to connect back to us.

        Returns:
            True if publicly reachable, False otherwise
        """
        if not self._reachability_checker:
            logger.warning("ReachabilityChecker not initialized")
            return False

        try:
            is_reachable = await self._reachability_checker.check_self_reachability()
            logger.info(f"Reachability check: {'reachable' if is_reachable else 'not reachable'}")
            return is_reachable

        except Exception as e:
            logger.debug(f"Reachability check failed: {e}")
            return False

    async def get_public_addrs_autonat(self) -> List[str]:
        """
        Get our public addresses as determined by AutoNAT.

        Other peers report back what address they see us as,
        which helps determine our public-facing address.

        Returns:
            List of public multiaddresses
        """
        if not self._reachability_checker:
            return []

        try:
            addrs = await self._reachability_checker.get_public_addrs()
            return [str(addr) for addr in addrs]
        except Exception as e:
            logger.debug(f"Failed to get public addrs via AutoNAT: {e}")
            return []

    def is_address_public(self, addr: str) -> bool:
        """
        Check if an address is considered public by AutoNAT.

        Args:
            addr: Multiaddress string to check

        Returns:
            True if the address is public
        """
        if not self._reachability_checker:
            # Fall back to our local check
            return self._is_public_address(addr)

        try:
            from multiaddr import Multiaddr
            maddr = Multiaddr(addr)
            return self._reachability_checker.is_addr_public(maddr)
        except Exception:
            return self._is_public_address(addr)

    # ========== Network Map ==========

    def get_network_map(self) -> Dict[str, Any]:
        """
        Get current network topology.

        Returns:
            Dictionary with peer connections and stream mappings
        """
        return {
            "self": {
                "peer_id": self.peer_id,
                "evrmore_address": self.evrmore_address,
                "addresses": self.public_addresses,
                "nat_type": self.nat_type,
                "is_relay": self.is_relay,
            },
            "connected_peers": self.get_connected_peers(),
            "known_peers": len(self._peer_info),
            "my_subscriptions": list(self._my_subscriptions),
            "my_publications": list(self._my_publications),
            "streams": {
                sid: {
                    "subscribers": self.get_subscribers(sid),
                    "publishers": self.get_publishers(sid),
                }
                for sid in set(self._my_subscriptions) | set(self._my_publications)
            },
        }

    def get_subscription_map(self) -> Dict[str, List[str]]:
        """Get stream -> subscribers mapping."""
        if self._subscriptions:
            return self._subscriptions.get_all_subscriptions()
        return {}

    # ========== Private Methods ==========

    async def _create_host(self) -> None:
        """Create libp2p host with configured transports."""
        try:
            from libp2p import new_host
            from libp2p.utils.address_validation import get_available_interfaces

            key_pair = self._identity_bridge.to_libp2p_key()

            # Build transport configuration
            transports = []

            # QUIC transport (if enabled)
            if self.enable_quic:
                try:
                    from libp2p.transport.quic import QUICTransport, QUICTransportConfig
                    quic_config = QUICTransportConfig(
                        max_stream_receive_window=15 * 1024 * 1024,  # 15 MB
                        max_connection_receive_window=25 * 1024 * 1024,  # 25 MB
                        keep_alive_period=15.0,
                        handshake_timeout=10.0,
                    )
                    quic_transport = QUICTransport(quic_config)
                    transports.append(quic_transport)
                    logger.info("QUIC transport enabled")
                except ImportError as e:
                    logger.warning(f"QUIC transport not available: {e}")
                except Exception as e:
                    logger.warning(f"Failed to initialize QUIC transport: {e}")

            # WebSocket transport (if enabled)
            if self.enable_websocket:
                try:
                    from libp2p.transport.websocket import WebsocketTransport
                    # WebsocketTransport requires an upgrader, which new_host provides
                    # We'll configure it via host creation
                    logger.info("WebSocket transport will be enabled")
                except ImportError as e:
                    logger.warning(f"WebSocket transport not available: {e}")

            # Create host with key pair and extended negotiate timeout
            # Note: QUIC and WebSocket transports need to be passed during host creation
            # The default TCP transport is always included
            host_kwargs = {
                "key_pair": key_pair,
                "negotiate_timeout": 30,  # Extend from 5s default for Docker networking
            }

            # Add transports if configured
            if transports:
                host_kwargs["transports"] = transports

            # Add ResourceManager with connection limits to prevent simultaneous connection issues
            try:
                from libp2p.rcmgr import ResourceManager
                from libp2p.rcmgr.manager import ConnectionLimits
                connection_limits = ConnectionLimits(
                    max_established_per_peer=1,  # Only 1 connection per peer to avoid race conditions
                    max_pending_inbound=10,
                    max_pending_outbound=10,
                )
                resource_manager = ResourceManager(
                    connection_limits=connection_limits,
                    connections_per_peer_per_sec=2.0,  # Rate limit to prevent rapid reconnects
                )
                host_kwargs["resource_manager"] = resource_manager
                logger.info("ResourceManager configured with max 1 connection per peer")
            except ImportError as e:
                logger.warning(f"ResourceManager not available: {e}")

            self._host = new_host(**host_kwargs)

            # Get listen addresses for TCP
            self._listen_addrs = get_available_interfaces(self.listen_port)

            # Add QUIC listen addresses if enabled
            if self.enable_quic:
                quic_port = self.listen_port + 1  # Use next port for QUIC
                quic_addrs = []
                for addr in self._listen_addrs:
                    # Convert /ip4/.../tcp/PORT to /ip4/.../udp/PORT/quic-v1
                    quic_addr = str(addr).replace(
                        f"/tcp/{self.listen_port}",
                        f"/udp/{quic_port}/quic-v1"
                    )
                    try:
                        from multiaddr import Multiaddr
                        quic_addrs.append(Multiaddr(quic_addr))
                    except Exception:
                        pass
                self._listen_addrs.extend(quic_addrs)
                logger.debug(f"Added QUIC listen addresses on port {quic_port}")

            # Add WebSocket listen addresses if enabled
            if self.enable_websocket:
                ws_port = self.listen_port + 2  # Use port + 2 for WebSocket
                ws_addrs = []
                for addr in list(self._listen_addrs)[:len(get_available_interfaces(self.listen_port))]:
                    # Convert /ip4/.../tcp/PORT to /ip4/.../tcp/PORT/ws
                    ws_addr = str(addr).replace(
                        f"/tcp/{self.listen_port}",
                        f"/tcp/{ws_port}/ws"
                    )
                    try:
                        from multiaddr import Multiaddr
                        ws_addrs.append(Multiaddr(ws_addr))
                    except Exception:
                        pass
                self._listen_addrs.extend(ws_addrs)
                logger.debug(f"Added WebSocket listen addresses on port {ws_port}")

            logger.debug(f"Created libp2p host: {self._host.get_id()}")

        except ImportError:
            logger.error("libp2p not installed")
            raise
        except Exception as e:
            logger.error(f"Failed to create host: {e}")
            raise

    async def _start_host(self) -> None:
        """Start the libp2p host (enter context manager)."""
        if not self._host:
            return

        try:
            from contextlib import aclosing

            # Enter the host run context with proper cleanup tracking
            logger.info(f"Starting host with listen_addrs: {self._listen_addrs}")
            self._host_context = self._host.run(listen_addrs=self._listen_addrs)

            # Store the generator for proper cleanup
            self._host_gen = self._host_context.__aenter__()
            await self._host_gen

            # Log actual addresses after startup
            actual_addrs = self._host.get_addrs()
            logger.info(f"Host listening on: {actual_addrs}")

        except Exception as e:
            logger.error(f"Failed to start host: {e}")
            raise

    async def _stop_host(self) -> None:
        """Stop the libp2p host (exit context manager)."""
        if self._host_context:
            try:
                # Try graceful exit first
                await self._host_context.__aexit__(None, None, None)
            except GeneratorExit:
                # Expected when async generator is closed
                pass
            except trio.Cancelled:
                # Propagate cancellation
                raise
            except Exception as e:
                logger.debug(f"Error stopping host (non-critical): {e}")
            finally:
                self._host_context = None

    async def _init_dht(self) -> None:
        """Initialize Kademlia DHT."""
        if not self._host:
            return

        try:
            from libp2p.kad_dht.kad_dht import KadDHT, DHTMode

            # Use SERVER mode if we have public connectivity, CLIENT otherwise
            dht_mode = DHTMode.SERVER if self.enable_relay else DHTMode.CLIENT

            # Create DHT with random walk enabled for better peer discovery
            self._dht = KadDHT(
                self._host,
                mode=dht_mode,
                enable_random_walk=True,  # Enable for better discovery
            )
            logger.debug(f"Kademlia DHT initialized (mode={dht_mode}, random_walk=enabled)")

        except ImportError:
            logger.warning("DHT not available in this libp2p version")
        except Exception as e:
            logger.warning(f"Failed to initialize DHT: {e}")

    async def _init_pubsub(self) -> None:
        """Initialize GossipSub for pub/sub messaging."""
        if not self._host:
            return

        try:
            from libp2p.pubsub.gossipsub import (
                GossipSub,
                PROTOCOL_ID,
                PROTOCOL_ID_V11,
                PROTOCOL_ID_V12,
            )
            from libp2p.pubsub.pubsub import Pubsub

            # Use GossipSub v1.2 (latest) with fallbacks to v1.1 and v1.0
            protocols = [PROTOCOL_ID_V12, PROTOCOL_ID_V11, PROTOCOL_ID]

            # Create GossipSub router with parameters
            gossipsub = GossipSub(
                protocols=protocols,
                degree=6,           # Target mesh peers (D)
                degree_low=4,       # Minimum mesh peers (D_low)
                degree_high=12,     # Maximum mesh peers (D_high)
                time_to_live=60,    # Message TTL in seconds
                gossip_window=3,    # Gossip window (messages)
                gossip_history=5,   # Gossip history length
                heartbeat_interval=1.0,  # Heartbeat interval seconds
            )

            # Create Pubsub service with GossipSub router
            self._pubsub = Pubsub(self._host, gossipsub)
            logger.info("GossipSub v1.2 initialized")

        except ImportError as e:
            logger.warning(f"GossipSub not available: {e}")
        except Exception as e:
            logger.warning(f"Failed to initialize GossipSub: {e}")
            import traceback
            traceback.print_exc()

    async def _init_circuit_relay(self) -> None:
        """
        Initialize Circuit Relay v2 for NAT traversal.

        For relay nodes (rendezvous_is_server=True):
        - Enable HOP to allow other peers to relay through us

        For regular nodes:
        - Enable CLIENT and STOP to use and accept relay connections
        - Initialize RelayDiscovery to find relay nodes
        """
        if not self._host:
            return

        try:
            from libp2p.relay import (
                CircuitV2Protocol,
                RelayDiscovery,
                RelayLimits,
            )

            # Determine if we should act as a relay hop (relay server)
            allow_hop = self.rendezvous_is_server

            # Configure limits for relay connections
            limits = RelayLimits(
                duration=600,  # Max 10 minutes per circuit
                data=1024 * 1024 * 10,  # Max 10MB per circuit
                max_circuit_conns=20,  # Max concurrent circuit connections
                max_reservations=10,  # Max reservations (for hop nodes)
            )

            # Create Circuit Relay v2 protocol
            self._circuit_relay = CircuitV2Protocol(
                host=self._host,
                limits=limits,
                allow_hop=allow_hop,
            )

            mode = "HOP (relay server)" if allow_hop else "CLIENT/STOP"
            logger.info(f"Circuit Relay v2 initialized ({mode})")

            # For non-relay nodes, initialize relay discovery
            if not allow_hop:
                self._relay_discovery = RelayDiscovery(
                    host=self._host,
                    auto_reserve=True,  # Automatically make reservations
                    discovery_interval=300,  # Discover every 5 minutes
                    max_relays=5,  # Track up to 5 relays
                )
                logger.debug("Relay Discovery initialized")

            # Initialize DCUtR for hole punching (both relay and client nodes)
            from libp2p.relay import DCUtRProtocol
            self._dcutr = DCUtRProtocol(
                host=self._host,
                read_timeout=30,
                write_timeout=30,
                dial_timeout=10,
            )
            logger.info("DCUtR (hole punching) initialized")

        except ImportError as e:
            logger.warning(f"Circuit Relay v2 not available: {e}")
        except Exception as e:
            logger.warning(f"Failed to initialize Circuit Relay v2: {e}")
            import traceback
            traceback.print_exc()

    async def _init_mdns(self) -> None:
        """
        Initialize mDNS for local network peer discovery.

        mDNS allows peers on the same local network to discover each other
        without needing bootstrap peers or DHT connectivity.
        """
        if not self._host:
            return

        try:
            from libp2p.discovery.mdns.mdns import MDNSDiscovery

            # Get the network/swarm from the host
            network = self._host.get_network()

            # Create mDNS discovery with the network and listen port
            self._mdns_discovery = MDNSDiscovery(
                swarm=network,
                port=self.listen_port,
            )

            logger.info("mDNS discovery initialized")

        except ImportError as e:
            logger.warning(f"mDNS not available: {e}")
        except Exception as e:
            logger.warning(f"Failed to initialize mDNS: {e}")
            import traceback
            traceback.print_exc()

    async def _init_ping(self) -> None:
        """
        Initialize custom Ping protocol for connectivity testing.

        Uses our GossipSub-based PingProtocol for measuring round-trip
        latency. This is a Satori-native implementation that doesn't
        depend on libp2p's optional ping module.
        """
        try:
            from .protocol.ping import PingProtocol

            self._ping_service = PingProtocol(self)
            logger.info("Ping protocol initialized (Satori custom)")

        except Exception as e:
            logger.warning(f"Failed to initialize Ping protocol: {e}")

    async def _init_autonat(self) -> None:
        """
        Initialize AutoNAT (ReachabilityChecker) for NAT type detection.

        ReachabilityChecker allows the node to determine if it is
        publicly reachable by having other peers attempt to connect back.
        """
        if not self._host:
            return

        try:
            from libp2p.relay import ReachabilityChecker

            self._reachability_checker = ReachabilityChecker(self._host)
            logger.info("AutoNAT (ReachabilityChecker) initialized")

        except ImportError as e:
            logger.warning(f"AutoNAT not available: {e}")
        except Exception as e:
            logger.warning(f"Failed to initialize AutoNAT: {e}")

    async def _init_identify(self) -> None:
        """
        Initialize custom Identify protocol for peer information exchange.

        Uses our GossipSub-based IdentifyProtocol for exchanging peer
        information (protocols, addresses, roles). This is a Satori-native
        implementation that doesn't depend on libp2p's optional identify module.
        """
        try:
            from .protocol.identify import IdentifyProtocol

            self._identify_handler = IdentifyProtocol(self)
            logger.info("Identify protocol initialized (Satori custom)")

        except Exception as e:
            logger.warning(f"Failed to initialize Identify protocol: {e}")
            import traceback
            traceback.print_exc()

    async def _select_best_rendezvous_peer(self) -> tuple:
        """
        Select the best rendezvous server based on latency with failover support.

        Pings all bootstrap peers concurrently and returns:
        - The peer with lowest latency as primary
        - Sorted list of backup peers for failover

        Returns:
            Tuple of (best_peer_id: str, backup_peer_ids: List[str], latencies: Dict[str, float])
        """
        if not self.bootstrap_peers:
            return None, [], {}

        from multiaddr import Multiaddr
        from libp2p.peer.peerinfo import info_from_p2p_addr
        import time

        latencies = {}  # peer_id -> latency_ms
        peer_infos = {}  # peer_id -> peer_info

        async def measure_latency(addr: str) -> tuple:
            """Measure connection latency to a bootstrap peer."""
            try:
                resolved_addr = self._resolve_multiaddr_dns(addr)
                maddr = Multiaddr(resolved_addr)
                peer_info = info_from_p2p_addr(maddr)
                peer_id = str(peer_info.peer_id)

                # Check if already connected
                if peer_id in [str(p) for p in self._host.get_network().connections.keys()]:
                    # Already connected - measure with a ping-like operation
                    start = time.perf_counter()
                    # Try to open a stream as latency test
                    try:
                        with trio.move_on_after(5) as cancel_scope:
                            stream = await self._host.new_stream(peer_info.peer_id, ["/ipfs/ping/1.0.0"])
                            if stream:
                                await stream.close()
                        if cancel_scope.cancelled_caught:
                            return peer_id, peer_info, 5000.0  # Timeout = 5000ms
                        latency = (time.perf_counter() - start) * 1000
                    except Exception:
                        latency = 100.0  # Assume 100ms if connected but ping fails
                    return peer_id, peer_info, latency
                else:
                    # Not connected - measure connection time
                    start = time.perf_counter()
                    with trio.move_on_after(10) as cancel_scope:
                        await self._host.connect(peer_info)
                    if cancel_scope.cancelled_caught:
                        return peer_id, peer_info, 10000.0  # Timeout = 10000ms
                    latency = (time.perf_counter() - start) * 1000
                    return peer_id, peer_info, latency

            except Exception as e:
                logger.debug(f"Failed to measure latency to {addr[:50]}...: {e}")
                return None, None, float('inf')

        # Measure latency to all bootstrap peers concurrently
        logger.info(f"Measuring latency to {len(self.bootstrap_peers)} bootstrap peer(s) for rendezvous selection...")

        async with trio.open_nursery() as nursery:
            results = []

            async def collect_result(addr):
                result = await measure_latency(addr)
                results.append(result)

            for addr in self.bootstrap_peers:
                nursery.start_soon(collect_result, addr)

        # Process results
        for peer_id, peer_info, latency in results:
            if peer_id and latency < float('inf'):
                latencies[peer_id] = latency
                peer_infos[peer_id] = peer_info

        if not latencies:
            logger.warning("No bootstrap peers reachable for rendezvous")
            return None, [], {}

        # Sort by latency
        sorted_peers = sorted(latencies.items(), key=lambda x: x[1])
        best_peer_id = sorted_peers[0][0]
        best_latency = sorted_peers[0][1]
        backup_peer_ids = [p[0] for p in sorted_peers[1:]]

        logger.info(f"Selected rendezvous server: {best_peer_id[:16]}... (latency: {best_latency:.1f}ms)")
        if backup_peer_ids:
            logger.debug(f"Backup rendezvous servers: {len(backup_peer_ids)} available")

        # Store for potential failover
        self._rendezvous_peers_by_latency = sorted_peers
        self._rendezvous_peer_infos = peer_infos

        return best_peer_id, backup_peer_ids, latencies

    async def _init_rendezvous(self) -> None:
        """Initialize Rendezvous protocol for stream-specific discovery."""
        if not self._host:
            return

        # Select best rendezvous peer based on latency (for client mode)
        rendezvous_peer_id = None
        if not self.rendezvous_is_server and self.bootstrap_peers:
            try:
                best_peer_id, backup_peers, latencies = await self._select_best_rendezvous_peer()
                if best_peer_id:
                    rendezvous_peer_id = best_peer_id
                    # Store backups for failover
                    self._rendezvous_backup_peers = backup_peers
                else:
                    # Fallback to first bootstrap peer if latency measurement fails
                    from multiaddr import Multiaddr
                    from libp2p.peer.peerinfo import info_from_p2p_addr

                    maddr = Multiaddr(self.bootstrap_peers[0])
                    peer_info = info_from_p2p_addr(maddr)
                    rendezvous_peer_id = str(peer_info.peer_id)
                    logger.debug(f"Fallback to first bootstrap for rendezvous: {rendezvous_peer_id[:16]}...")
            except Exception as e:
                logger.debug(f"Failed to select rendezvous peer: {e}")

        self._rendezvous = RendezvousManager(
            self._host,
            rendezvous_peer_id=rendezvous_peer_id,
            is_server=self.rendezvous_is_server,
        )
        started = await self._rendezvous.start(nursery=self._nursery)

        if started:
            mode = "server" if self.rendezvous_is_server else "client"
            logger.info(f"Rendezvous protocol initialized ({mode} mode)")
        else:
            logger.debug("Rendezvous running in local-only mode")

    async def failover_rendezvous(self) -> bool:
        """
        Switch to the next available rendezvous server if current one fails.

        This is called when rendezvous operations fail repeatedly, allowing
        automatic recovery by switching to a backup server.

        Returns:
            True if successfully failed over to a new server, False if no backups available
        """
        if not self._rendezvous_backup_peers:
            logger.warning("No backup rendezvous servers available for failover")
            return False

        if self.rendezvous_is_server:
            logger.debug("This node is a rendezvous server, failover not applicable")
            return False

        # Get the next backup peer
        next_peer_id = self._rendezvous_backup_peers.pop(0)
        logger.info(f"Failing over rendezvous to: {next_peer_id[:16]}...")

        try:
            # Stop current rendezvous
            if self._rendezvous:
                await self._rendezvous.stop()

            # Create new rendezvous manager with backup peer
            self._rendezvous = RendezvousManager(
                self._host,
                rendezvous_peer_id=next_peer_id,
                is_server=False,
            )
            started = await self._rendezvous.start(nursery=self._nursery)

            if started:
                # Find latency for logging
                latency = "unknown"
                for peer_id, lat in self._rendezvous_peers_by_latency:
                    if peer_id == next_peer_id:
                        latency = f"{lat:.1f}ms"
                        break
                logger.info(f"Rendezvous failover successful to {next_peer_id[:16]}... (latency: {latency})")
                return True
            else:
                logger.warning(f"Rendezvous failover to {next_peer_id[:16]}... failed to start")
                # Try next backup recursively
                return await self.failover_rendezvous()

        except Exception as e:
            logger.error(f"Rendezvous failover failed: {e}")
            # Try next backup recursively
            return await self.failover_rendezvous()

    def get_rendezvous_status(self) -> dict:
        """
        Get current rendezvous server status and available backups.

        Returns:
            Dict with current server, backup count, and latency info
        """
        current_peer = None
        if self._rendezvous and hasattr(self._rendezvous, '_rendezvous_peer_id'):
            current_peer = self._rendezvous._rendezvous_peer_id

        return {
            "is_server": self.rendezvous_is_server,
            "current_peer": current_peer[:16] + "..." if current_peer else None,
            "backup_count": len(self._rendezvous_backup_peers),
            "peers_by_latency": [
                {"peer_id": p[:16] + "...", "latency_ms": round(l, 1)}
                for p, l in self._rendezvous_peers_by_latency[:5]  # Top 5
            ],
        }

    async def _register_handlers(self) -> None:
        """Register protocol stream handlers."""
        if not self._host:
            return

        # Main Satori protocol handler
        self._host.set_stream_handler(SATORI_PROTOCOL_ID, self._handle_stream)

        # Message store protocol handler
        if self._message_store:
            self._host.set_stream_handler(
                SATORI_STORE_PROTOCOL_ID,
                self._message_store.handle_store_request
            )

    async def _connect_to_bootstrap(self) -> None:
        """Connect to bootstrap peers."""
        if not self._host:
            return

        if not self.bootstrap_peers:
            logger.info("No bootstrap peers configured")
            return

        logger.info(f"Connecting to {len(self.bootstrap_peers)} bootstrap peer(s)...")

        for addr in self.bootstrap_peers:
            try:
                from multiaddr import Multiaddr
                from libp2p.peer.peerinfo import info_from_p2p_addr
                import socket

                # Resolve DNS in multiaddr if present
                resolved_addr = self._resolve_multiaddr_dns(addr)
                if resolved_addr != addr:
                    logger.info(f"Resolved DNS: {addr[:50]}... -> {resolved_addr[:50]}...")

                maddr = Multiaddr(resolved_addr)
                peer_info = info_from_p2p_addr(maddr)
                logger.info(f"Dialing bootstrap peer {peer_info.peer_id}...")

                # Add timeout to connect operation (must be > negotiate_timeout)
                # Using 60s to ensure no timeout race conditions
                with trio.move_on_after(60) as cancel_scope:
                    await self._host.connect(peer_info)

                if cancel_scope.cancelled_caught:
                    logger.warning(f"TIMEOUT connecting to bootstrap {peer_info.peer_id}")
                else:
                    logger.info(f"SUCCESS: Connected to bootstrap: {peer_info.peer_id}")

            except Exception as e:
                logger.warning(f"FAILED to connect to bootstrap {addr[:50]}...: {type(e).__name__}: {e}")

    def _resolve_multiaddr_dns(self, addr: str) -> str:
        """
        Resolve DNS names in multiaddress to IP addresses.

        py-libp2p's TCP transport doesn't automatically resolve DNS names,
        so we need to resolve them before dialing.

        Args:
            addr: Multiaddress string (e.g., /dns4/relay-node/tcp/4001/p2p/Qm...)

        Returns:
            Resolved multiaddress with IP instead of DNS name
        """
        import socket
        import re

        # Match /dns4/<hostname>/ or /dns/<hostname>/
        dns_pattern = r'/dns4?/([^/]+)/'
        match = re.search(dns_pattern, addr)

        if not match:
            return addr

        hostname = match.group(1)

        try:
            # Resolve DNS to IP
            ip_addr = socket.gethostbyname(hostname)
            # Replace /dns4/hostname/ with /ip4/ip/
            resolved = re.sub(
                dns_pattern,
                f'/ip4/{ip_addr}/',
                addr
            )
            return resolved
        except socket.gaierror as e:
            logger.warning(f"DNS resolution failed for {hostname}: {e}")
            return addr

    def _start_background_tasks(self) -> None:
        """Start background maintenance tasks.

        Background tasks run in the nursery created by run_forever().
        If run_forever() hasn't been called, tasks are deferred until it is.
        """
        if self._nursery:
            # Nursery available - start tasks immediately
            self._nursery.start_soon(self._cleanup_task)
            self._nursery.start_soon(self._retrieve_pending_messages)
            logger.debug("Background tasks started in existing nursery")
        else:
            # Tasks will be started when run_forever() is called
            logger.debug("Background tasks deferred until run_forever() is called")

    def spawn_background_task(self, coro_func, *args) -> bool:
        """
        Spawn a coroutine as a background task in the P2P nursery.

        If the nursery is not yet available (before run_forever() is called),
        the task is queued and will be started when run_forever() creates the nursery.

        Args:
            coro_func: Async function to run
            *args: Arguments to pass to the function

        Returns:
            True if task was spawned or queued successfully
        """
        if self._nursery:
            self._nursery.start_soon(coro_func, *args)
            logger.debug(f"Background task spawned immediately: {coro_func.__name__}")
            return True
        else:
            # Queue for later - will be spawned when run_forever() creates nursery
            self._pending_background_tasks.append((coro_func, args))
            logger.info(f"Background task queued (will start when nursery available): {coro_func.__name__}")
            return True

    async def _retrieve_pending_messages(self) -> None:
        """Retrieve messages stored for us while offline."""
        if not self._message_store:
            return

        await trio.sleep(5)  # Wait for connections to establish
        try:
            messages = await self._message_store.retrieve_pending(self.peer_id)
        except Exception as e:
            logger.warning(f"Failed to retrieve pending messages: {e}")
            return

        for payload in messages:
            try:
                message = deserialize_message(payload)
                stream_id = message.get("params", {}).get("uuid")
                if stream_id and stream_id in self._callbacks:
                    for callback in self._callbacks[stream_id]:
                        try:
                            callback(stream_id, message.get("data"))
                        except Exception as e:
                            logger.error(f"Callback error: {e}")
            except Exception as e:
                logger.warning(f"Failed to process pending message: {e}")

    async def _is_peer_connected(self, peer_id: str) -> bool:
        """Check if peer is currently connected."""
        return peer_id in self.get_connected_peers()

    async def _send_direct(self, peer_id: str, message: Any) -> bool:
        """Send message directly to connected peer."""
        if not self._host:
            return False

        try:
            from libp2p.peer.id import ID as PeerID
            from libp2p.peer.peerinfo import PeerInfo

            target_id = PeerID.from_base58(peer_id)
            peer_info = PeerInfo(target_id, [])
            stream = await self._host.new_stream(peer_info, [SATORI_PROTOCOL_ID])

            data = serialize_message(message)
            await stream.write(data)
            await stream.close()
            return True

        except Exception as e:
            logger.debug(f"Direct send failed: {e}")
            return False

    async def _handle_stream(self, stream) -> None:
        """Handle incoming protocol stream."""
        try:
            data = await stream.read()
            message = deserialize_message(data)

            # Route based on message type
            msg_type = message.get("type") or message.get("method", "unknown")

            if msg_type == "subscription_announce":
                self._handle_subscription_announce(message)
            elif msg_type == "peer_announce":
                self._handle_peer_announce(message)
            elif message.get("params", {}).get("uuid"):
                # Stream data message
                stream_id = message["params"]["uuid"]
                self._handle_stream_data(stream_id, message)
            else:
                logger.debug(f"Unknown message type: {msg_type}")

            await stream.close()

        except Exception as e:
            logger.error(f"Stream handler error: {e}")

    def _handle_subscription_announce(self, message: dict) -> None:
        """Handle subscription announcement from peer."""
        stream_id = message.get("stream_id")
        peer_id = message.get("peer_id")
        evrmore_address = message.get("evrmore_address", "")
        is_publisher = message.get("is_publisher", False)

        if self._subscriptions and stream_id and peer_id:
            if is_publisher:
                self._subscriptions.add_publisher(stream_id, peer_id, evrmore_address)
            else:
                self._subscriptions.add_subscriber(stream_id, peer_id, evrmore_address)

    def _handle_peer_announce(self, message: dict) -> None:
        """Handle peer announcement."""
        peer_info_data = message.get("peer_info", {})
        peer_id = peer_info_data.get("peer_id")

        if peer_id:
            self._peer_info[peer_id] = PeerInfo(
                peer_id=peer_id,
                evrmore_address=peer_info_data.get("evrmore_address", ""),
                public_key=peer_info_data.get("public_key", ""),
                addresses=peer_info_data.get("addresses", []),
                nat_type=peer_info_data.get("nat_type", "UNKNOWN"),
                is_relay=peer_info_data.get("is_relay", False),
            )

            # If peer is a relay, add to message store
            if peer_info_data.get("is_relay") and self._message_store:
                self._message_store.add_relay(peer_id)

    def _handle_stream_data(self, stream_id: str, message: dict) -> None:
        """Handle incoming stream data."""
        if stream_id in self._callbacks:
            data = message.get("data")
            for callback in self._callbacks[stream_id]:
                try:
                    callback(stream_id, data)
                except Exception as e:
                    logger.error(f"Callback error: {e}")

    async def _subscribe_to_topic(self, topic: str, stream_id: str) -> None:
        """Subscribe to a GossipSub topic."""
        if not self._pubsub:
            logger.warning(f"Cannot subscribe to {topic}: pubsub not initialized")
            return

        try:
            # Subscribe returns a subscription object
            subscription = await self._pubsub.subscribe(topic)

            # Store subscription for message processing
            if not hasattr(self, '_topic_subscriptions'):
                self._topic_subscriptions = {}
            self._topic_subscriptions[topic] = subscription

            logger.info(f"Subscribed to {topic}")

            # Note: Message processing is now handled by process_messages()
            # which should be called in a trio nursery by the caller

        except Exception as e:
            logger.warning(f"Failed to subscribe to topic {topic}: {e}")

    async def _unsubscribe_from_topic(self, topic: str) -> None:
        """Unsubscribe from a GossipSub topic."""
        if not self._pubsub:
            return

        try:
            await self._pubsub.unsubscribe(topic)
        except Exception as e:
            logger.debug(f"Failed to unsubscribe from topic: {e}")

    async def process_messages(self, stream_id: str) -> None:
        """
        Process incoming messages for a stream subscription.

        This should be called in a trio nursery to receive messages
        for a subscribed stream. Messages are dispatched to registered
        callbacks.

        Args:
            stream_id: The stream ID to process messages for

        Example:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(peers.process_messages, "stream-uuid")
        """
        topic = f"{STREAM_TOPIC_PREFIX}{stream_id}"

        if not hasattr(self, '_topic_subscriptions'):
            return

        subscription = self._topic_subscriptions.get(topic)
        if not subscription:
            logger.warning(f"No subscription found for stream {stream_id}")
            return

        logger.info(f"Starting message processing for {stream_id}")

        while stream_id in self._my_subscriptions:
            try:
                msg = await subscription.get()
                byte_size = len(msg.data)
                logger.info(f"Received message on {stream_id}: {byte_size} bytes")

                # Track bandwidth for incoming message
                if self._bandwidth_tracker:
                    # msg.from_id contains the original author peer ID
                    peer_id = str(msg.from_id) if hasattr(msg, 'from_id') and msg.from_id else None
                    await self._bandwidth_tracker.account_receive(stream_id, byte_size, peer_id)

                data = deserialize_message(msg.data)
                if stream_id in self._callbacks:
                    callbacks = self._callbacks[stream_id]
                    logger.info(f"Dispatching to {len(callbacks)} callback(s) for {stream_id}")
                    for callback in callbacks:
                        try:
                            callback(stream_id, data)
                        except Exception as e:
                            logger.error(f"Callback error for {stream_id}: {e}", exc_info=True)
                else:
                    logger.warning(f"No callbacks registered for stream {stream_id}")
            except Exception as e:
                logger.debug(f"Message loop error for {topic}: {e}")
                break

    def __repr__(self) -> str:
        status = "running" if self._started else "stopped"
        return (
            f"Peers(status={status}, "
            f"peer_id={self.peer_id if self.peer_id else 'N/A'}, "
            f"connected={self.connected_peers})"
        )
