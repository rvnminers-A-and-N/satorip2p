"""
satorip2p/nat/docker.py

Docker environment detection and NAT handling.
Works without privileged mode.
"""

import os
import socket
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("satorip2p.nat.docker")


@dataclass
class DockerInfo:
    """Docker environment information."""
    in_container: bool
    network_mode: str  # "host", "bridge", "overlay", "none", "unknown"
    container_ip: Optional[str] = None
    gateway_ip: Optional[str] = None
    host_ip: Optional[str] = None

    @property
    def is_bridge_mode(self) -> bool:
        """Check if running in bridge networking mode."""
        return self.network_mode == "bridge"

    @property
    def is_host_mode(self) -> bool:
        """Check if running in host networking mode."""
        return self.network_mode == "host"

    @property
    def needs_relay(self) -> bool:
        """Check if relay is likely needed (double NAT)."""
        return self.in_container and self.is_bridge_mode


def detect_docker_environment() -> DockerInfo:
    """
    Detect Docker networking configuration.

    Works without privileged mode by checking:
    - /proc/1/cgroup for docker signature
    - /.dockerenv file existence
    - Network interface configuration

    Returns:
        DockerInfo with detected configuration
    """
    in_container = _detect_container()

    if not in_container:
        return DockerInfo(
            in_container=False,
            network_mode="none",
            container_ip=None,
            gateway_ip=None,
            host_ip=None
        )

    # Detect network configuration
    container_ip = _get_container_ip()
    gateway_ip = _get_gateway_ip()
    network_mode = _detect_network_mode()

    # In bridge mode, gateway is usually the Docker host
    host_ip = gateway_ip if network_mode == "bridge" else None

    logger.info(
        f"Docker detected: mode={network_mode}, "
        f"container_ip={container_ip}, gateway={gateway_ip}"
    )

    return DockerInfo(
        in_container=True,
        network_mode=network_mode,
        container_ip=container_ip,
        gateway_ip=gateway_ip,
        host_ip=host_ip
    )


def _detect_container() -> bool:
    """Check if running inside a container."""
    # Method 1: Check for .dockerenv file
    if os.path.exists("/.dockerenv"):
        return True

    # Method 2: Check cgroup for docker/kubepods
    try:
        with open("/proc/1/cgroup", "r") as f:
            cgroup = f.read()
            if "docker" in cgroup or "kubepods" in cgroup or "containerd" in cgroup:
                return True
    except (FileNotFoundError, PermissionError):
        pass

    # Method 3: Check for container environment variables
    container_env_vars = [
        "KUBERNETES_SERVICE_HOST",
        "DOCKER_CONTAINER",
        "container",
    ]
    for var in container_env_vars:
        if os.environ.get(var):
            return True

    return False


def _get_container_ip() -> Optional[str]:
    """Get the container's IP address."""
    try:
        # Get hostname and resolve it
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname)
    except socket.error:
        pass

    # Fallback: try to get IP from network interfaces
    # Uses socket trick to query OS routing table (no actual traffic sent)
    # Try Cloudflare first, then Google as fallback
    for target_ip in ("1.1.1.1", "8.8.8.8"):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((target_ip, 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except socket.error:
            continue
    return None


def _get_gateway_ip() -> Optional[str]:
    """Get the default gateway IP (usually Docker host in bridge mode)."""
    # Method 1: Parse /proc/net/route
    try:
        with open("/proc/net/route") as f:
            for line in f:
                fields = line.strip().split()
                if len(fields) >= 3 and fields[1] == "00000000":  # Default route
                    gateway_hex = fields[2]
                    # Convert hex to IP (little-endian)
                    gateway_bytes = bytes.fromhex(gateway_hex)
                    gateway_ip = ".".join(str(b) for b in reversed(gateway_bytes))
                    return gateway_ip
    except (FileNotFoundError, PermissionError, ValueError):
        pass

    # Method 2: Try netifaces if available
    try:
        import netifaces
        gateways = netifaces.gateways()
        if "default" in gateways and netifaces.AF_INET in gateways["default"]:
            return gateways["default"][netifaces.AF_INET][0]
    except ImportError:
        pass

    return None


def _detect_network_mode() -> str:
    """Detect Docker network mode."""
    # In host mode, we see all host interfaces
    # In bridge mode, we typically only see eth0, lo

    try:
        # Count network interfaces
        interfaces = os.listdir("/sys/class/net")
        interface_count = len(interfaces)

        # Host mode typically has many interfaces (docker0, veth*, etc.)
        # Bridge mode typically has just eth0 and lo
        if interface_count > 4:
            return "host"

        # Check if we can see docker0 interface (only visible in host mode)
        if "docker0" in interfaces:
            return "host"

        # Check for veth interfaces (host mode indicator)
        if any(iface.startswith("veth") for iface in interfaces):
            return "host"

        return "bridge"

    except (FileNotFoundError, PermissionError):
        return "unknown"


def get_public_ip_from_container() -> Optional[str]:
    """
    Attempt to determine the public IP when running in a container.

    This is useful for nodes that want to advertise their
    reachable address to the network.
    """
    docker_info = detect_docker_environment()

    if not docker_info.in_container:
        return None

    if docker_info.is_host_mode:
        # In host mode, our IP is the host's IP
        return docker_info.container_ip

    # In bridge mode, we need external discovery
    # This would typically be done via AutoNAT in libp2p
    return None
