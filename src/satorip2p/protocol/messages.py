"""
satorip2p/protocol/messages.py

Message serialization for P2P communication.
Supports DataFrames for compatibility with existing Satori code.
"""

import json
import logging
from typing import Any, Dict, Optional, Union
from datetime import datetime
import uuid

logger = logging.getLogger("satorip2p.protocol.messages")


def serialize_message(message: Any) -> bytes:
    """
    Serialize a message for transmission over libp2p.

    Handles special types:
    - pandas DataFrame -> dict records with marker
    - datetime -> ISO format string
    - bytes -> base64 encoded with marker

    Args:
        message: Message to serialize (dict, list, or primitive)

    Returns:
        UTF-8 encoded JSON bytes
    """
    return json.dumps(message, cls=SatoriEncoder).encode("utf-8")


def deserialize_message(data: bytes) -> Any:
    """
    Deserialize a message received from libp2p.

    Reconstructs special types:
    - DataFrame from dict records
    - datetime from ISO strings
    - bytes from base64

    Args:
        data: UTF-8 encoded JSON bytes

    Returns:
        Deserialized message
    """
    return json.loads(data.decode("utf-8"), object_hook=satori_decoder)


class SatoriEncoder(json.JSONEncoder):
    """Custom JSON encoder for Satori message types."""

    def default(self, obj):
        # Handle pandas DataFrame
        try:
            import pandas as pd
            if isinstance(obj, pd.DataFrame):
                return {
                    "__satori_type__": "dataframe",
                    "data": obj.to_dict(orient="records"),
                    "columns": list(obj.columns),
                    "index": list(obj.index) if not isinstance(obj.index, pd.RangeIndex) else None,
                }
        except ImportError:
            pass

        # Handle datetime
        if isinstance(obj, datetime):
            return {
                "__satori_type__": "datetime",
                "value": obj.isoformat(),
            }

        # Handle bytes
        if isinstance(obj, bytes):
            import base64
            return {
                "__satori_type__": "bytes",
                "value": base64.b64encode(obj).decode("ascii"),
            }

        # Handle sets
        if isinstance(obj, set):
            return {
                "__satori_type__": "set",
                "value": list(obj),
            }

        return super().default(obj)


def satori_decoder(obj: dict) -> Any:
    """Custom JSON decoder hook for Satori message types."""
    if "__satori_type__" not in obj:
        return obj

    type_marker = obj["__satori_type__"]

    if type_marker == "dataframe":
        try:
            import pandas as pd
            df = pd.DataFrame(obj["data"], columns=obj.get("columns"))
            if obj.get("index"):
                df.index = obj["index"]
            return df
        except ImportError:
            logger.warning("pandas not available, returning raw dict")
            return obj["data"]

    if type_marker == "datetime":
        return datetime.fromisoformat(obj["value"])

    if type_marker == "bytes":
        import base64
        return base64.b64decode(obj["value"])

    if type_marker == "set":
        return set(obj["value"])

    return obj


def create_message(
    method: str,
    params: Optional[Dict[str, Any]] = None,
    data: Any = None,
    stream_id: Optional[str] = None,
    is_subscription: bool = False,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a standardized Satori P2P message.

    Compatible with existing Satori Message format where possible.

    Args:
        method: Message type/method (e.g., "subscribe", "publish", "data")
        params: Additional parameters
        data: Message payload
        stream_id: Associated stream UUID
        is_subscription: Whether this is a subscription message
        request_id: Request ID for request/response correlation

    Returns:
        Message dictionary ready for serialization
    """
    return {
        "id": request_id or str(uuid.uuid4()),
        "method": method,
        "params": {
            "uuid": stream_id,
            **(params or {}),
        },
        "data": data,
        "sub": is_subscription,
        "timestamp": datetime.utcnow().isoformat(),
    }


def create_response(
    request_id: str,
    status: str = "success",
    data: Any = None,
    message: Optional[str] = None,
    stream_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a response message.

    Args:
        request_id: ID of the request being responded to
        status: "success" or "error"
        data: Response payload
        message: Human-readable status message
        stream_id: Associated stream UUID

    Returns:
        Response message dictionary
    """
    return {
        "id": request_id,
        "status": status,
        "message": message,
        "params": {"uuid": stream_id} if stream_id else {},
        "data": data,
        "timestamp": datetime.utcnow().isoformat(),
    }


def create_subscription_announce(
    stream_id: str,
    peer_id: str,
    evrmore_address: str,
    is_publisher: bool = False,
) -> Dict[str, Any]:
    """
    Create a subscription announcement message.

    Broadcast when subscribing to or publishing a stream.

    Args:
        stream_id: Stream UUID
        peer_id: libp2p peer ID
        evrmore_address: Evrmore wallet address
        is_publisher: True if announcing as publisher

    Returns:
        Announcement message dictionary
    """
    return {
        "type": "subscription_announce",
        "stream_id": stream_id,
        "peer_id": peer_id,
        "evrmore_address": evrmore_address,
        "is_publisher": is_publisher,
        "timestamp": datetime.utcnow().isoformat(),
    }


def create_peer_announce(
    peer_id: str,
    evrmore_address: str,
    public_key: str,
    addresses: list,
    nat_type: str = "UNKNOWN",
    is_relay: bool = False,
    subscriptions: Optional[list] = None,
    publications: Optional[list] = None,
) -> Dict[str, Any]:
    """
    Create a peer announcement message.

    Broadcast periodically to announce presence on the network.

    Args:
        peer_id: libp2p peer ID
        evrmore_address: Evrmore wallet address
        public_key: Evrmore public key (hex)
        addresses: List of multiaddresses
        nat_type: Detected NAT type
        is_relay: Whether this peer can act as a relay
        subscriptions: List of subscribed stream IDs
        publications: List of published stream IDs

    Returns:
        Announcement message dictionary
    """
    return {
        "type": "peer_announce",
        "peer_info": {
            "peer_id": peer_id,
            "evrmore_address": evrmore_address,
            "public_key": public_key,
            "addresses": addresses,
            "nat_type": nat_type,
            "is_relay": is_relay,
        },
        "subscriptions": subscriptions or [],
        "publications": publications or [],
        "timestamp": datetime.utcnow().isoformat(),
    }


def create_store_request(
    target_peer: str,
    message_id: str,
    payload: bytes,
    ttl_hours: int = 24,
) -> Dict[str, Any]:
    """
    Create a message store request (for offline peer delivery).

    Args:
        target_peer: Peer ID of intended recipient
        message_id: Unique message identifier
        payload: Message content (bytes)
        ttl_hours: How long to store the message

    Returns:
        Store request message dictionary
    """
    import base64
    return {
        "action": "store",
        "target": target_peer,
        "message_id": message_id,
        "payload": base64.b64encode(payload).decode("ascii"),
        "ttl_hours": ttl_hours,
        "timestamp": datetime.utcnow().isoformat(),
    }


def create_retrieve_request(peer_id: str) -> Dict[str, Any]:
    """
    Create a message retrieval request.

    Used to fetch messages stored while we were offline.

    Args:
        peer_id: Our peer ID

    Returns:
        Retrieve request message dictionary
    """
    return {
        "action": "retrieve",
        "target": peer_id,
        "timestamp": datetime.utcnow().isoformat(),
    }
