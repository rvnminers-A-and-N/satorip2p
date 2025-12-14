"""
satorip2p/nat/

NAT traversal utilities - UPnP and Docker detection.
"""

from .upnp import UPnPManager
from .docker import detect_docker_environment, DockerInfo

__all__ = ["UPnPManager", "detect_docker_environment", "DockerInfo"]
