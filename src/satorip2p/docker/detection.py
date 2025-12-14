"""
satorip2p/docker/detection.py

Re-export Docker detection utilities for convenience.
"""

# Re-export from nat.docker for convenience
from satorip2p.nat.docker import (
    detect_docker_environment,
    DockerInfo,
    get_public_ip_from_container,
)

__all__ = [
    'detect_docker_environment',
    'DockerInfo',
    'get_public_ip_from_container',
]
