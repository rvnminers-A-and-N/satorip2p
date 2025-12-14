"""
satorip2p/metrics.py

Prometheus metrics collection for satorip2p.

Provides metrics for monitoring P2P network health, connectivity,
message throughput, and latency.
"""

import time
import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Any
from dataclasses import dataclass, field

if TYPE_CHECKING:
    from .peers import Peers

logger = logging.getLogger("satorip2p.metrics")


@dataclass
class MetricValue:
    """Single metric value with labels."""
    name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    metric_type: str = "gauge"  # gauge, counter, histogram
    help_text: str = ""


class MetricsCollector:
    """
    Prometheus metrics collector for satorip2p.

    Collects and exposes metrics in Prometheus format for monitoring
    P2P network health, connectivity, and performance.

    Usage:
        from satorip2p import Peers
        from satorip2p.metrics import MetricsCollector

        peers = Peers(identity=identity)
        metrics = MetricsCollector(peers)

        await peers.start()

        # Get metrics in Prometheus format
        prometheus_output = metrics.collect()
    """

    # Metric definitions
    METRICS = {
        "satorip2p_connected_peers": {
            "type": "gauge",
            "help": "Number of currently connected peers",
        },
        "satorip2p_known_peers": {
            "type": "gauge",
            "help": "Number of known peers in the network",
        },
        "satorip2p_subscriptions_total": {
            "type": "gauge",
            "help": "Number of active stream subscriptions",
        },
        "satorip2p_publications_total": {
            "type": "gauge",
            "help": "Number of active stream publications",
        },
        "satorip2p_messages_sent_total": {
            "type": "counter",
            "help": "Total number of messages sent",
        },
        "satorip2p_messages_received_total": {
            "type": "counter",
            "help": "Total number of messages received",
        },
        "satorip2p_bytes_sent_total": {
            "type": "counter",
            "help": "Total bytes sent",
        },
        "satorip2p_bytes_received_total": {
            "type": "counter",
            "help": "Total bytes received",
        },
        "satorip2p_ping_latency_seconds": {
            "type": "histogram",
            "help": "Ping latency in seconds",
            "buckets": [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
        },
        "satorip2p_nat_type": {
            "type": "gauge",
            "help": "NAT type (0=unknown, 1=public, 2=private)",
        },
        "satorip2p_relay_enabled": {
            "type": "gauge",
            "help": "Whether relay is enabled (1=yes, 0=no)",
        },
        "satorip2p_uptime_seconds": {
            "type": "counter",
            "help": "Node uptime in seconds",
        },
        "satorip2p_dht_routing_table_size": {
            "type": "gauge",
            "help": "Number of peers in DHT routing table",
        },
        "satorip2p_pubsub_topics": {
            "type": "gauge",
            "help": "Number of subscribed pubsub topics",
        },
        "satorip2p_info": {
            "type": "gauge",
            "help": "Node information (peer_id, version as labels)",
        },
    }

    def __init__(self, peers: "Peers"):
        """
        Initialize metrics collector.

        Args:
            peers: Peers instance to collect metrics from
        """
        self.peers = peers
        self._start_time = time.time()

        # Counters (persist across collections)
        self._messages_sent = 0
        self._messages_received = 0
        self._bytes_sent = 0
        self._bytes_received = 0

        # Histogram buckets for latency
        self._latency_buckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
        self._latency_counts = {b: 0 for b in self._latency_buckets}
        self._latency_counts[float('inf')] = 0
        self._latency_sum = 0.0
        self._latency_count = 0

    def record_message_sent(self, byte_size: int = 0) -> None:
        """Record a sent message."""
        self._messages_sent += 1
        self._bytes_sent += byte_size

    def record_message_received(self, byte_size: int = 0) -> None:
        """Record a received message."""
        self._messages_received += 1
        self._bytes_received += byte_size

    def record_ping_latency(self, latency_seconds: float) -> None:
        """Record a ping latency measurement."""
        self._latency_sum += latency_seconds
        self._latency_count += 1

        # Update bucket counts
        for bucket in self._latency_buckets:
            if latency_seconds <= bucket:
                self._latency_counts[bucket] += 1
        self._latency_counts[float('inf')] += 1

    def _get_nat_type_value(self) -> int:
        """Convert NAT type to numeric value."""
        nat_type = self.peers.nat_type
        if nat_type == "PUBLIC":
            return 1
        elif nat_type == "PRIVATE":
            return 2
        return 0  # UNKNOWN

    def collect(self) -> str:
        """
        Collect all metrics and return in Prometheus format.

        Returns:
            Prometheus-formatted metrics string
        """
        lines = []

        # Helper to add metric
        def add_metric(name: str, value: float, labels: Dict[str, str] = None):
            metric_def = self.METRICS.get(name, {})
            help_text = metric_def.get("help", "")
            metric_type = metric_def.get("type", "gauge")

            # Add HELP and TYPE only once per metric
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {metric_type}")

            if labels:
                label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
                lines.append(f"{name}{{{label_str}}} {value}")
            else:
                lines.append(f"{name} {value}")

        # Collect current values
        try:
            # Connected peers
            add_metric("satorip2p_connected_peers", self.peers.connected_peers)

            # Known peers
            known_peers = len(self.peers._peer_info) if hasattr(self.peers, '_peer_info') else 0
            add_metric("satorip2p_known_peers", known_peers)

            # Subscriptions
            subs = len(self.peers.get_my_subscriptions())
            add_metric("satorip2p_subscriptions_total", subs)

            # Publications
            pubs = len(self.peers.get_my_publications())
            add_metric("satorip2p_publications_total", pubs)

            # Message counters
            add_metric("satorip2p_messages_sent_total", self._messages_sent)
            add_metric("satorip2p_messages_received_total", self._messages_received)
            add_metric("satorip2p_bytes_sent_total", self._bytes_sent)
            add_metric("satorip2p_bytes_received_total", self._bytes_received)

            # NAT type
            add_metric("satorip2p_nat_type", self._get_nat_type_value())

            # Relay enabled
            relay_enabled = 1 if self.peers.enable_relay else 0
            add_metric("satorip2p_relay_enabled", relay_enabled)

            # Uptime
            uptime = time.time() - self._start_time
            add_metric("satorip2p_uptime_seconds", uptime)

            # DHT routing table size
            if hasattr(self.peers, '_dht') and self.peers._dht:
                try:
                    dht_size = self.peers._dht.get_routing_table_size()
                    add_metric("satorip2p_dht_routing_table_size", dht_size)
                except Exception:
                    add_metric("satorip2p_dht_routing_table_size", 0)
            else:
                add_metric("satorip2p_dht_routing_table_size", 0)

            # Pubsub topics
            if hasattr(self.peers, '_topic_subscriptions'):
                topic_count = len(self.peers._topic_subscriptions)
                add_metric("satorip2p_pubsub_topics", topic_count)
            else:
                add_metric("satorip2p_pubsub_topics", 0)

            # Node info (with labels)
            peer_id = self.peers.peer_id or "unknown"
            lines.append(f"# HELP satorip2p_info Node information")
            lines.append(f"# TYPE satorip2p_info gauge")
            lines.append(f'satorip2p_info{{peer_id="{peer_id[:20]}...",version="0.1.0"}} 1')

            # Ping latency histogram
            if self._latency_count > 0:
                lines.append(f"# HELP satorip2p_ping_latency_seconds Ping latency in seconds")
                lines.append(f"# TYPE satorip2p_ping_latency_seconds histogram")

                cumulative = 0
                for bucket in self._latency_buckets:
                    cumulative += self._latency_counts[bucket]
                    lines.append(f'satorip2p_ping_latency_seconds_bucket{{le="{bucket}"}} {cumulative}')

                cumulative += self._latency_counts[float('inf')]
                lines.append(f'satorip2p_ping_latency_seconds_bucket{{le="+Inf"}} {cumulative}')
                lines.append(f"satorip2p_ping_latency_seconds_sum {self._latency_sum}")
                lines.append(f"satorip2p_ping_latency_seconds_count {self._latency_count}")

        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            lines.append(f"# Error collecting metrics: {e}")

        return "\n".join(lines) + "\n"

    def get_stats(self) -> Dict[str, Any]:
        """
        Get metrics as a dictionary (for JSON API).

        Returns:
            Dictionary of metric values
        """
        try:
            return {
                "connected_peers": self.peers.connected_peers,
                "known_peers": len(self.peers._peer_info) if hasattr(self.peers, '_peer_info') else 0,
                "subscriptions": len(self.peers.get_my_subscriptions()),
                "publications": len(self.peers.get_my_publications()),
                "messages_sent": self._messages_sent,
                "messages_received": self._messages_received,
                "bytes_sent": self._bytes_sent,
                "bytes_received": self._bytes_received,
                "nat_type": self.peers.nat_type,
                "relay_enabled": self.peers.enable_relay,
                "uptime_seconds": time.time() - self._start_time,
                "peer_id": self.peers.peer_id,
                "is_connected": self.peers.is_connected,
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {"error": str(e)}

    def reset_counters(self) -> None:
        """Reset all counters (useful for testing)."""
        self._messages_sent = 0
        self._messages_received = 0
        self._bytes_sent = 0
        self._bytes_received = 0
        self._latency_counts = {b: 0 for b in self._latency_buckets}
        self._latency_counts[float('inf')] = 0
        self._latency_sum = 0.0
        self._latency_count = 0
