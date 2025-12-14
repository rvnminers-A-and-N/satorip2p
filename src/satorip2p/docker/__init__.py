"""
satorip2p/docker/__init__.py

Docker-specific utilities and test nodes for satorip2p.
These are used for testing P2P functionality in Docker bridge mode.
"""

from .detection import detect_docker_environment, DockerInfo

__all__ = ['detect_docker_environment', 'DockerInfo']
