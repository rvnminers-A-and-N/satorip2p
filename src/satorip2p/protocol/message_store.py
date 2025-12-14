"""
satorip2p/protocol/message_store.py

Distributed message storage for offline peer delivery.
Messages are stored on relay nodes and delivered when peers reconnect.

This is CUSTOM functionality - NOT provided by py-libp2p.
"""

import logging
import time
import json
import base64
import asyncio
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, field, asdict
from collections import defaultdict
import uuid

from ..config import MESSAGE_STORE_PARAMS, MessageEnvelope

logger = logging.getLogger("satorip2p.protocol.message_store")


class MessageStore:
    """
    Distributed message storage for offline peer delivery.

    When a target peer is offline:
    1. Message is stored locally
    2. Message is replicated to N relay nodes for redundancy
    3. We register in DHT as a message holder for that peer
    4. When target peer comes online, we deliver the message

    This runs on ALL nodes, but only nodes with open ports (relays)
    will be asked to store messages for others.

    Usage:
        store = MessageStore(host, dht)

        # Store message for offline peer
        await store.store_for_peer(target_peer_id, message_bytes)

        # When we come online, retrieve our messages
        messages = await store.retrieve_pending()

        # Handle incoming store requests (we're acting as relay)
        store.register_protocol_handler()
    """

    # DHT key prefix for message holder registration
    PENDING_KEY_PREFIX = "/satori/pending/"

    def __init__(
        self,
        host=None,
        dht=None,
        max_age_hours: int = None,
        max_queue_size: int = None,
        replication_factor: int = None,
    ):
        """
        Initialize message store.

        Args:
            host: py-libp2p host instance
            dht: py-libp2p DHT instance
            max_age_hours: Message TTL (default from config)
            max_queue_size: Max messages per peer (default from config)
            replication_factor: Number of relay nodes to replicate to
        """
        self.host = host
        self.dht = dht

        self.max_age_hours = max_age_hours or MESSAGE_STORE_PARAMS["max_age_hours"]
        self.max_queue_size = max_queue_size or MESSAGE_STORE_PARAMS["max_queue_size"]
        self.replication_factor = replication_factor or MESSAGE_STORE_PARAMS["replication_factor"]

        # Local message storage: target_peer_id -> list of MessageEnvelope
        self._local_store: Dict[str, List[MessageEnvelope]] = defaultdict(list)

        # Track which peers we've announced holding messages for
        self._announced_for: Set[str] = set()

        # Track connected relay nodes
        self._known_relays: Set[str] = set()

        # Protocol ID for store requests
        self.PROTOCOL_ID = "/satori/store/1.0.0"

    def set_host(self, host) -> None:
        """Set the libp2p host."""
        self.host = host

    def set_dht(self, dht) -> None:
        """Set the DHT instance."""
        self.dht = dht

    def add_relay(self, peer_id: str) -> None:
        """Add a known relay node."""
        self._known_relays.add(peer_id)

    def remove_relay(self, peer_id: str) -> None:
        """Remove a relay node."""
        self._known_relays.discard(peer_id)

    # ========== Store Methods ==========

    async def store_for_peer(
        self,
        target_peer: str,
        payload: bytes,
        stream_id: Optional[str] = None,
        source_peer: Optional[str] = None,
    ) -> bool:
        """
        Store a message for an offline peer.

        The message is stored locally and replicated to relay nodes.

        Args:
            target_peer: Peer ID of intended recipient
            payload: Message content (bytes)
            stream_id: Optional stream ID for routing
            source_peer: Sender peer ID (uses our ID if not provided)

        Returns:
            True if message was stored (locally or on relays)
        """
        if source_peer is None and self.host:
            source_peer = str(self.host.get_id())

        envelope = MessageEnvelope(
            message_id=str(uuid.uuid4()),
            target_peer=target_peer,
            source_peer=source_peer or "",
            stream_id=stream_id,
            payload=payload,
            ttl_hours=self.max_age_hours,
        )

        stored_locally = self._store_locally(envelope)
        replicated = await self._replicate_to_relays(envelope)

        if stored_locally:
            await self._announce_holding(target_peer)

        return stored_locally or replicated

    def _store_locally(self, envelope: MessageEnvelope) -> bool:
        """Store message in local queue."""
        target = envelope.target_peer

        # Enforce queue size limit
        if len(self._local_store[target]) >= self.max_queue_size:
            # Remove oldest message
            self._local_store[target].pop(0)
            logger.warning(f"Queue full for {target[:16]}..., dropped oldest message")

        self._local_store[target].append(envelope)
        logger.debug(f"Stored message {envelope.message_id[:8]}... for {target[:16]}...")
        return True

    async def _replicate_to_relays(self, envelope: MessageEnvelope) -> bool:
        """Replicate message to relay nodes for redundancy."""
        if not self.host or not self._known_relays:
            return False

        # Select relay nodes (excluding target peer)
        relays = [r for r in self._known_relays if r != envelope.target_peer]
        relays = relays[:self.replication_factor]

        if not relays:
            logger.debug("No relay nodes available for replication")
            return False

        replicated_count = 0
        for relay_id in relays:
            try:
                success = await self._send_store_request(relay_id, envelope)
                if success:
                    replicated_count += 1
            except Exception as e:
                logger.warning(f"Failed to replicate to {relay_id[:16]}...: {e}")

        logger.debug(f"Replicated to {replicated_count}/{len(relays)} relays")
        return replicated_count > 0

    async def _send_store_request(self, relay_id: str, envelope: MessageEnvelope) -> bool:
        """Send store request to a relay node."""
        if not self.host:
            return False

        try:
            from libp2p.peer.id import ID as PeerID

            target = PeerID.from_base58(relay_id)
            stream = await self.host.new_stream(target, [self.PROTOCOL_ID])

            request = {
                "action": "store",
                "envelope": {
                    "message_id": envelope.message_id,
                    "target_peer": envelope.target_peer,
                    "source_peer": envelope.source_peer,
                    "stream_id": envelope.stream_id,
                    "payload": base64.b64encode(envelope.payload).decode("ascii"),
                    "timestamp": envelope.timestamp,
                    "ttl_hours": envelope.ttl_hours,
                }
            }

            await stream.write(json.dumps(request).encode())
            response = json.loads(await stream.read())
            await stream.close()

            return response.get("status") == "ok"

        except Exception as e:
            logger.debug(f"Store request failed: {e}")
            return False

    async def _announce_holding(self, target_peer: str) -> None:
        """Announce in DHT that we hold messages for a peer."""
        if not self.dht:
            return

        if target_peer in self._announced_for:
            return

        try:
            key = f"{self.PENDING_KEY_PREFIX}{target_peer}"
            await self.dht.provide(key.encode())
            self._announced_for.add(target_peer)
            logger.debug(f"Announced holding messages for {target_peer[:16]}...")
        except Exception as e:
            logger.warning(f"Failed to announce in DHT: {e}")

    # ========== Retrieve Methods ==========

    async def retrieve_pending(self, my_peer_id: Optional[str] = None) -> List[bytes]:
        """
        Retrieve messages stored for us while we were offline.

        Queries DHT for nodes holding our messages, then fetches them.

        Args:
            my_peer_id: Our peer ID (uses host ID if not provided)

        Returns:
            List of message payloads (bytes)
        """
        if my_peer_id is None and self.host:
            my_peer_id = str(self.host.get_id())

        if not my_peer_id:
            return []

        messages = []

        # Query DHT for message holders
        if self.dht:
            try:
                key = f"{self.PENDING_KEY_PREFIX}{my_peer_id}"
                holders = await self.dht.find_providers(key.encode())

                for holder in holders:
                    holder_id = str(holder)
                    retrieved = await self._fetch_from_holder(holder_id, my_peer_id)
                    messages.extend(retrieved)

            except Exception as e:
                logger.warning(f"Failed to query DHT for pending messages: {e}")

        # Also check connected peers
        if self.host:
            # connections is a dict with peer ID as key
            for peer_id in self.host.get_network().connections.keys():
                holder_id = str(peer_id)
                if holder_id not in [str(h) for h in (holders if self.dht else [])]:
                    retrieved = await self._fetch_from_holder(holder_id, my_peer_id)
                    messages.extend(retrieved)

        logger.info(f"Retrieved {len(messages)} pending messages")
        return messages

    async def _fetch_from_holder(self, holder_id: str, my_peer_id: str) -> List[bytes]:
        """Fetch messages from a specific holder."""
        if not self.host:
            return []

        try:
            from libp2p.peer.id import ID as PeerID

            target = PeerID.from_base58(holder_id)
            stream = await self.host.new_stream(target, [self.PROTOCOL_ID])

            request = {
                "action": "retrieve",
                "target": my_peer_id,
            }

            await stream.write(json.dumps(request).encode())
            response = json.loads(await stream.read())
            await stream.close()

            messages = []
            for msg in response.get("messages", []):
                try:
                    payload = base64.b64decode(msg["payload"])
                    messages.append(payload)
                except Exception:
                    pass

            return messages

        except Exception as e:
            logger.debug(f"Failed to fetch from {holder_id[:16]}...: {e}")
            return []

    # ========== Delivery Methods ==========

    async def deliver_to_peer(self, peer_id: str) -> int:
        """
        Deliver all stored messages to a peer that just came online.

        Args:
            peer_id: Peer ID that came online

        Returns:
            Number of messages delivered
        """
        if peer_id not in self._local_store:
            return 0

        messages = self._local_store.pop(peer_id, [])
        self._announced_for.discard(peer_id)

        delivered = 0
        for envelope in messages:
            if envelope.is_expired():
                logger.debug(f"Message {envelope.message_id[:8]}... expired, dropping")
                continue

            try:
                success = await self._deliver_message(peer_id, envelope)
                if success:
                    delivered += 1
            except Exception as e:
                logger.warning(f"Failed to deliver message: {e}")
                # Re-queue for retry
                self._local_store[peer_id].append(envelope)

        logger.info(f"Delivered {delivered}/{len(messages)} messages to {peer_id[:16]}...")
        return delivered

    async def _deliver_message(self, peer_id: str, envelope: MessageEnvelope) -> bool:
        """Deliver a single message to a peer."""
        if not self.host:
            return False

        try:
            from libp2p.peer.id import ID as PeerID

            target = PeerID.from_base58(peer_id)
            stream = await self.host.new_stream(target, ["/satori/1.0.0"])

            # Send the original payload
            await stream.write(envelope.payload)
            await stream.close()
            return True

        except Exception as e:
            logger.debug(f"Delivery failed: {e}")
            envelope.increment_attempts()
            return False

    # ========== Protocol Handler ==========

    async def handle_store_request(self, stream) -> None:
        """
        Handle incoming store/retrieve requests.

        Called when another peer wants us to store a message (we're a relay)
        or retrieve their stored messages.
        """
        try:
            data = json.loads(await stream.read())
            action = data.get("action")

            if action == "store":
                await self._handle_store(stream, data)
            elif action == "retrieve":
                await self._handle_retrieve(stream, data)
            else:
                await stream.write(json.dumps({"status": "error", "message": "Unknown action"}).encode())

        except Exception as e:
            logger.error(f"Error handling store request: {e}")
            try:
                await stream.write(json.dumps({"status": "error", "message": str(e)}).encode())
            except:
                pass

    async def _handle_store(self, stream, data: dict) -> None:
        """Handle a store request (we're acting as relay)."""
        envelope_data = data.get("envelope", {})

        envelope = MessageEnvelope(
            message_id=envelope_data.get("message_id", str(uuid.uuid4())),
            target_peer=envelope_data.get("target_peer", ""),
            source_peer=envelope_data.get("source_peer", ""),
            stream_id=envelope_data.get("stream_id"),
            payload=base64.b64decode(envelope_data.get("payload", "")),
            timestamp=envelope_data.get("timestamp", time.time()),
            ttl_hours=envelope_data.get("ttl_hours", self.max_age_hours),
        )

        self._store_locally(envelope)
        await self._announce_holding(envelope.target_peer)

        await stream.write(json.dumps({"status": "ok"}).encode())
        logger.debug(f"Stored message for {envelope.target_peer[:16]}... (relay)")

    async def _handle_retrieve(self, stream, data: dict) -> None:
        """Handle a retrieve request."""
        target_peer = data.get("target")

        messages = []
        if target_peer in self._local_store:
            envelopes = self._local_store.pop(target_peer, [])
            self._announced_for.discard(target_peer)

            for env in envelopes:
                if not env.is_expired():
                    messages.append({
                        "message_id": env.message_id,
                        "payload": base64.b64encode(env.payload).decode("ascii"),
                        "stream_id": env.stream_id,
                        "source_peer": env.source_peer,
                        "timestamp": env.timestamp,
                    })

        await stream.write(json.dumps({"status": "ok", "messages": messages}).encode())
        logger.debug(f"Sent {len(messages)} messages to {target_peer[:16]}...")

    # ========== Maintenance ==========

    def cleanup_expired(self) -> int:
        """Remove expired messages from local store."""
        removed = 0
        for peer_id in list(self._local_store.keys()):
            original_count = len(self._local_store[peer_id])
            self._local_store[peer_id] = [
                env for env in self._local_store[peer_id]
                if not env.is_expired()
            ]
            removed += original_count - len(self._local_store[peer_id])

            # Clean up empty entries
            if not self._local_store[peer_id]:
                del self._local_store[peer_id]
                self._announced_for.discard(peer_id)

        if removed:
            logger.debug(f"Cleaned up {removed} expired messages")
        return removed

    def get_stats(self) -> Dict[str, Any]:
        """Get message store statistics."""
        total_messages = sum(len(msgs) for msgs in self._local_store.values())
        return {
            "peers_with_pending": len(self._local_store),
            "total_pending_messages": total_messages,
            "known_relays": len(self._known_relays),
            "announced_for": len(self._announced_for),
        }

    def clear(self) -> None:
        """Clear all stored messages."""
        self._local_store.clear()
        self._announced_for.clear()
