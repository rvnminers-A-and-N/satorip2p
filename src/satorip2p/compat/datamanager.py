"""
satorip2p.compat.datamanager - DataManager Message Compatibility Layer

Provides PyArrow IPC serialization and message format compatibility with
Satori's DataManager (satorilib/datamanager/).

Features:
    - Apache Arrow IPC serialization for DataFrames
    - Message class compatible with DataManager protocol
    - Challenge-response authentication support
    - ECDH + AES encryption for secure peer communication

Usage:
    from satorip2p.compat.datamanager import Message, DataManagerBridge

    # Serialize DataFrame to P2P message
    msg = Message.from_dataframe(df, stream_id="uuid", method="stream/data/insert")
    data = msg.to_bytes()

    # Deserialize from P2P
    msg = Message.from_bytes(data)
    df = msg.get_dataframe()
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
import json
import time
import logging
import hashlib
import os

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.compat.datamanager")

# Optional imports - gracefully handle missing dependencies
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    pa = None


class Message:
    """
    DataManager Message Format.

    Compatible with satorilib/datamanager/helper/datastruct.py Message class.
    Supports PyArrow IPC serialization for DataFrames.

    Message structure:
    {
        'method': '<api_endpoint>',
        'id': '<unique_call_id>',
        'sub': <boolean>,
        'status': '<success|failed>',
        'params': {
            'uuid': '<stream_uuid>',
            'replace': <boolean>,
            'from_ts': '<timestamp>',
            'to_ts': '<timestamp>'
        },
        'data': <bytes_or_dataframe>,
        'authentication': {...},
        'stream_info': {...}
    }
    """

    def __init__(self, message: dict):
        """Initialize Message with dictionary data."""
        self.message = message

    @classmethod
    def create(
        cls,
        method: str,
        uuid: str = "",
        data: Any = None,
        is_subscription: bool = False,
        status: str = "success",
        from_ts: str = "",
        to_ts: str = "",
        replace: bool = False,
        authentication: Optional[dict] = None,
        stream_info: Optional[dict] = None,
    ) -> "Message":
        """Create a new Message with specified parameters."""
        return cls({
            "method": method,
            "id": str(time.time()),
            "sub": is_subscription,
            "status": status,
            "params": {
                "uuid": uuid,
                "replace": replace,
                "from_ts": from_ts,
                "to_ts": to_ts,
            },
            "data": data,
            "authentication": authentication,
            "stream_info": stream_info,
        })

    @classmethod
    def from_dataframe(
        cls,
        df: "pd.DataFrame",
        stream_id: str,
        method: str = "stream/data/insert",
        **kwargs,
    ) -> "Message":
        """Create Message from a pandas DataFrame."""
        if not HAS_PANDAS:
            raise ImportError("pandas is required for DataFrame support")

        return cls.create(
            method=method,
            uuid=stream_id,
            data=df,
            **kwargs,
        )

    def to_dict(self, is_response: bool = False) -> dict:
        """Convert Message to dictionary."""
        if is_response:
            return {
                "status": self.status,
                "message": self.sender_message,
                "id": self.id,
                "params": {"uuid": self.uuid},
                "data": self.data,
                "authentication": self.authentication,
                "stream_info": self.stream_info,
            }
        return {
            "method": self.method,
            "id": self.id,
            "sub": self.is_subscription,
            "status": self.status,
            "params": {
                "uuid": self.uuid,
                "replace": self.replace,
                "from_ts": self.from_timestamp,
                "to_ts": self.to_timestamp,
            },
            "data": self.data,
            "authentication": self.authentication,
            "stream_info": self.stream_info,
        }

    def to_json(self) -> str:
        """Convert Message to JSON string."""
        msg_dict = self.to_dict()
        # Handle DataFrame - convert to list for JSON
        if HAS_PANDAS and isinstance(msg_dict.get("data"), pd.DataFrame):
            msg_dict["data"] = msg_dict["data"].to_dict(orient="records")
        return json.dumps(msg_dict)

    def to_bytes(self, is_response: bool = False) -> bytes:
        """
        Convert Message to PyArrow IPC bytes for transmission.

        This is the format used by DataManager WebSocket protocol.
        """
        if not HAS_PYARROW:
            # Fallback to JSON
            return self.to_json().encode("utf-8")

        message_dict = self.to_dict(is_response)

        # Serialize DataFrame if present
        if HAS_PANDAS and isinstance(message_dict.get("data"), pd.DataFrame):
            message_dict["data"] = self._serialize_dataframe(message_dict["data"])

        # Convert to PyArrow table
        table = pa.Table.from_pydict({k: [v] for k, v in message_dict.items()})

        # Serialize to IPC format
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write(table)

        return sink.getvalue().to_pybytes()

    @classmethod
    def from_bytes(cls, byte_data: bytes) -> "Message":
        """
        Create Message from PyArrow IPC bytes.

        This deserializes the DataManager WebSocket protocol format.
        """
        if not HAS_PYARROW:
            # Fallback from JSON
            try:
                data = json.loads(byte_data.decode("utf-8"))
                return cls(data)
            except json.JSONDecodeError:
                raise ValueError("Cannot deserialize: pyarrow not available and data is not JSON")

        try:
            reader = pa.ipc.open_stream(pa.BufferReader(byte_data))
            table = reader.read_all()

            message_dict = {}
            for k, v in table.to_pydict().items():
                value = v[0] if v else None

                # Handle DataFrame deserialization
                if k == "data" and isinstance(value, bytes):
                    try:
                        message_dict[k] = cls._deserialize_dataframe(value)
                    except Exception:
                        message_dict[k] = value
                elif hasattr(value, "as_py"):
                    message_dict[k] = value.as_py()
                else:
                    message_dict[k] = value

            return cls(message_dict)

        except Exception as e:
            # Try JSON fallback
            try:
                data = json.loads(byte_data.decode("utf-8"))
                return cls(data)
            except json.JSONDecodeError:
                raise ValueError(f"Failed to deserialize message: {e}")

    @staticmethod
    def _serialize_dataframe(df: "pd.DataFrame") -> Optional[bytes]:
        """Serialize DataFrame to PyArrow IPC bytes."""
        if df is None:
            return None

        if not HAS_PYARROW or not HAS_PANDAS:
            raise ImportError("pandas and pyarrow are required for DataFrame serialization")

        try:
            sink = pa.BufferOutputStream()
            table = pa.Table.from_pandas(df)
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write(table)
            return sink.getvalue().to_pybytes()
        except Exception as e:
            raise ValueError(f"Failed to serialize DataFrame: {e}")

    @staticmethod
    def _deserialize_dataframe(data: bytes) -> Optional["pd.DataFrame"]:
        """Deserialize DataFrame from PyArrow IPC bytes."""
        if data is None:
            return None

        if not HAS_PYARROW or not HAS_PANDAS:
            raise ImportError("pandas and pyarrow are required for DataFrame deserialization")

        try:
            reader = pa.ipc.open_stream(pa.BufferReader(data))
            table = reader.read_all()
            return table.to_pandas()
        except Exception as e:
            raise ValueError(f"Failed to deserialize DataFrame: {e}")

    def get_dataframe(self) -> Optional["pd.DataFrame"]:
        """Get data as DataFrame if available."""
        data = self.data
        if HAS_PANDAS and isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, bytes):
            return self._deserialize_dataframe(data)
        if isinstance(data, (list, dict)):
            return pd.DataFrame(data) if HAS_PANDAS else None
        return None

    # Properties for accessing message fields
    @property
    def method(self) -> str:
        return self.message.get("method", "")

    @property
    def id(self) -> str:
        return self.message.get("id", "")

    @property
    def is_subscription(self) -> bool:
        return self.message.get("sub", False)

    @property
    def status(self) -> str:
        return self.message.get("status", "")

    @property
    def uuid(self) -> str:
        params = self.message.get("params", {})
        return params.get("uuid", "") if isinstance(params, dict) else ""

    @property
    def replace(self) -> bool:
        params = self.message.get("params", {})
        return params.get("replace", False) if isinstance(params, dict) else False

    @property
    def from_timestamp(self) -> str:
        params = self.message.get("params", {})
        return params.get("from_ts", "") if isinstance(params, dict) else ""

    @property
    def to_timestamp(self) -> str:
        params = self.message.get("params", {})
        return params.get("to_ts", "") if isinstance(params, dict) else ""

    @property
    def data(self) -> Any:
        return self.message.get("data")

    @property
    def authentication(self) -> Optional[dict]:
        return self.message.get("authentication")

    @property
    def stream_info(self) -> Optional[dict]:
        return self.message.get("stream_info")

    @property
    def sender_message(self) -> str:
        return self.message.get("message", "")


class SecurityPolicy:
    """
    Security policy for DataManager connections.

    Matches satorilib/datamanager/helper/datastruct.py SecurityPolicy.
    """

    def __init__(
        self,
        local_authentication: bool = True,
        remote_authentication: bool = True,
        local_encryption: bool = False,
        remote_encryption: bool = True,
    ):
        self.local_authentication = local_authentication
        self.remote_authentication = remote_authentication
        self.local_encryption = local_encryption
        self.remote_encryption = remote_encryption

    @classmethod
    def local(cls) -> "SecurityPolicy":
        """Security policy for local connections (Neuron <-> Engine)."""
        return cls(
            local_authentication=True,
            remote_authentication=True,
            local_encryption=False,
            remote_encryption=False,
        )

    @classmethod
    def peer(cls) -> "SecurityPolicy":
        """Security policy for peer connections (remote)."""
        return cls(
            local_authentication=True,
            remote_authentication=True,
            local_encryption=True,
            remote_encryption=True,
        )


class ChallengeResponse:
    """
    Challenge-response authentication helper.

    Implements the DataManager authentication protocol:
    1. Client sends pubkey + address
    2. Server responds with challenge + signature
    3. Client verifies server signature
    4. Client responds with signed challenge
    5. Server verifies, establishes shared secret
    """

    def __init__(self, identity=None):
        """
        Initialize with optional identity for signing.

        Args:
            identity: EvrmoreIdentity or similar with sign/verify methods
        """
        self.identity = identity
        self._challenge = None
        self._peer_pubkey = None

    def generate_challenge(self) -> str:
        """Generate a random challenge string."""
        self._challenge = hashlib.sha256(os.urandom(32)).hexdigest()
        return self._challenge

    def create_init_message(self) -> dict:
        """Create initial authentication message."""
        if not self.identity:
            raise ValueError("Identity required for authentication")

        return {
            "pubkey": self.identity.pubkey,
            "address": self.identity.address,
        }

    def create_challenge_response(self, challenge: str) -> dict:
        """Sign a challenge and create response."""
        if not self.identity:
            raise ValueError("Identity required for signing")

        signature = self.identity.sign(challenge)
        return {
            "challenge": challenge,
            "signature": signature,
            "pubkey": self.identity.pubkey,
        }

    def verify_challenge_response(self, response: dict, expected_challenge: str) -> bool:
        """Verify a challenge response."""
        if response.get("challenge") != expected_challenge:
            return False

        # Verify signature (implementation depends on identity type)
        if self.identity:
            try:
                return self.identity.verify(
                    response.get("pubkey", ""),
                    expected_challenge,
                    response.get("signature", ""),
                )
            except Exception:
                return False

        return True

    def derive_shared_secret(self, peer_pubkey: str) -> Optional[bytes]:
        """Derive ECDH shared secret with peer."""
        if not self.identity:
            return None

        self._peer_pubkey = peer_pubkey

        try:
            # Uses identity's ECDH method
            return self.identity.ecdh(peer_pubkey)
        except Exception as e:
            logger.warning(f"Failed to derive shared secret: {e}")
            return None


class DataManagerBridge:
    """
    Bridge between satorip2p and DataManager protocol.

    Handles:
    - Message serialization/deserialization
    - Stream subscription routing
    - DataFrame transmission over P2P
    """

    def __init__(self, peers: "Peers"):
        """
        Initialize DataManagerBridge.

        Args:
            peers: Peers instance for P2P communication
        """
        self.peers = peers
        self._subscriptions: Dict[str, Any] = {}  # stream_id -> callback
        self._pending_requests: Dict[str, Any] = {}  # request_id -> response event

    async def subscribe(
        self,
        stream_id: str,
        callback: Any,
    ) -> bool:
        """
        Subscribe to stream data via P2P.

        Args:
            stream_id: Stream UUID to subscribe to
            callback: Function called with (stream_id, dataframe) on new data

        Returns:
            True if subscription successful
        """
        def on_message(sid: str, data: Any):
            # Deserialize message
            if isinstance(data, bytes):
                msg = Message.from_bytes(data)
            elif isinstance(data, dict):
                msg = Message(data)
            else:
                return

            # Get DataFrame and call user callback
            df = msg.get_dataframe()
            if df is not None and callback:
                callback(sid, df)

        self._subscriptions[stream_id] = callback
        await self.peers.subscribe_async(stream_id, on_message)
        return True

    async def unsubscribe(self, stream_id: str) -> None:
        """Unsubscribe from stream."""
        self._subscriptions.pop(stream_id, None)
        await self.peers.unsubscribe_async(stream_id)

    async def publish_dataframe(
        self,
        stream_id: str,
        df: "pd.DataFrame",
        replace: bool = False,
    ) -> bool:
        """
        Publish DataFrame to stream.

        Args:
            stream_id: Stream UUID to publish to
            df: DataFrame to publish
            replace: If True, replace existing data

        Returns:
            True if published successfully
        """
        if not HAS_PANDAS:
            raise ImportError("pandas required for DataFrame publishing")

        msg = Message.from_dataframe(
            df=df,
            stream_id=stream_id,
            method="stream/data/insert",
            replace=replace,
        )

        # Publish via P2P
        await self.peers.publish(stream_id, msg.to_bytes())
        return True

    async def get_stream_data(
        self,
        stream_id: str,
        from_ts: str = "",
        to_ts: str = "",
    ) -> Optional["pd.DataFrame"]:
        """
        Request stream data from peers.

        Args:
            stream_id: Stream UUID to get data from
            from_ts: Start timestamp (optional)
            to_ts: End timestamp (optional)

        Returns:
            DataFrame with stream data, or None if not available
        """
        msg = Message.create(
            method="stream/data/get/range",
            uuid=stream_id,
            from_ts=from_ts,
            to_ts=to_ts,
        )

        # Find publishers for this stream
        publishers = await self.peers.discover_publishers(stream_id)
        if not publishers:
            logger.warning(f"No publishers found for stream {stream_id}")
            return None

        # Request from first available publisher
        for peer_id in publishers:
            try:
                response = await self.peers.request(peer_id, msg.to_bytes(), timeout=30)
                if response:
                    resp_msg = Message.from_bytes(response)
                    return resp_msg.get_dataframe()
            except Exception as e:
                logger.debug(f"Failed to get data from {peer_id}: {e}")
                continue

        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get bridge statistics."""
        return {
            "subscriptions": len(self._subscriptions),
            "pending_requests": len(self._pending_requests),
        }


# DataManager API endpoint constants (for reference)
class DataManagerAPI:
    """DataManager API endpoint constants."""

    # Authentication
    INIT_AUTH = "client/initiate/auth"

    # Stream operations
    SUBSCRIBE = "stream/subscribe"
    DATA_GET = "stream/data/get"
    DATA_INSERT = "stream/data/insert"
    DATA_DELETE = "stream/data/delete"
    DATA_GET_RANGE = "stream/data/get/range"
    OBSERVATION_GET_AT = "stream/observation/get/at"

    # PubSub mapping
    PUBSUB_SET = "pubsub/set"
    PUBSUB_GET = "pubsub/get"

    # Stream management
    SUBSCRIPTIONS_LIST = "streams/subscriptions/list"
    STREAM_ADD = "stream/add"
    STREAM_INACTIVE = "stream/inactive"
