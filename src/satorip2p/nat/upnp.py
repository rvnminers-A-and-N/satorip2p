"""
satorip2p/nat/upnp.py

UPnP port mapping for NAT traversal.
Uses miniupnpc library for port forwarding.
"""

import logging
from typing import Optional, Tuple
import asyncio

logger = logging.getLogger("satorip2p.nat.upnp")


class UPnPManager:
    """
    Manages UPnP port mappings for P2P connectivity.

    Attempts to open ports on the router using UPnP IGD protocol.
    Falls back gracefully if UPnP is not available.

    Usage:
        upnp = UPnPManager()
        success = await upnp.map_port(24600)
        if success:
            print(f"Mapped port, external: {upnp.external_ip}:{upnp.external_port}")

        # Cleanup on shutdown
        await upnp.unmap_port(24600)
    """

    def __init__(self, discovery_timeout: int = 5):
        """
        Initialize UPnP manager.

        Args:
            discovery_timeout: Seconds to wait for UPnP discovery
        """
        self.discovery_timeout = discovery_timeout
        self._upnp = None
        self._mapped_ports: dict[int, int] = {}  # internal -> external
        self._external_ip: Optional[str] = None
        self._available: Optional[bool] = None

    @property
    def is_available(self) -> bool:
        """Check if UPnP is available on this network."""
        return self._available is True

    @property
    def external_ip(self) -> Optional[str]:
        """Get external IP address discovered via UPnP."""
        return self._external_ip

    def get_external_port(self, internal_port: int) -> Optional[int]:
        """Get the external port mapped to an internal port."""
        return self._mapped_ports.get(internal_port)

    async def discover(self) -> bool:
        """
        Discover UPnP devices on the network.

        Returns:
            True if UPnP IGD device found
        """
        if self._available is not None:
            return self._available

        try:
            import miniupnpc

            # Run discovery in thread pool (blocking operation)
            loop = asyncio.get_event_loop()
            self._upnp = miniupnpc.UPnP()
            self._upnp.discoverdelay = self.discovery_timeout * 1000  # milliseconds

            # Discover devices
            devices = await loop.run_in_executor(None, self._upnp.discover)

            if devices == 0:
                logger.info("No UPnP devices found")
                self._available = False
                return False

            # Select IGD (Internet Gateway Device)
            await loop.run_in_executor(None, self._upnp.selectigd)

            # Get external IP
            self._external_ip = await loop.run_in_executor(
                None, self._upnp.externalipaddress
            )

            logger.info(f"UPnP IGD found, external IP: {self._external_ip}")
            self._available = True
            return True

        except ImportError:
            logger.warning("miniupnpc not installed. UPnP disabled.")
            self._available = False
            return False
        except Exception as e:
            logger.warning(f"UPnP discovery failed: {e}")
            self._available = False
            return False

    async def map_port(
        self,
        internal_port: int,
        external_port: Optional[int] = None,
        protocol: str = "TCP",
        description: str = "Satori P2P"
    ) -> bool:
        """
        Map an external port to an internal port.

        Args:
            internal_port: Local port to map
            external_port: Desired external port (uses same as internal if None)
            protocol: "TCP" or "UDP"
            description: Description for the mapping

        Returns:
            True if mapping succeeded
        """
        if not await self.discover():
            return False

        external_port = external_port or internal_port

        try:
            loop = asyncio.get_event_loop()

            # Try to add port mapping
            result = await loop.run_in_executor(
                None,
                lambda: self._upnp.addportmapping(
                    external_port,
                    protocol,
                    self._upnp.lanaddr,
                    internal_port,
                    description,
                    ""  # Remote host (empty = any)
                )
            )

            if result:
                self._mapped_ports[internal_port] = external_port
                logger.info(
                    f"UPnP mapped {protocol} {self._external_ip}:{external_port} "
                    f"-> {self._upnp.lanaddr}:{internal_port}"
                )
                return True
            else:
                logger.warning(f"UPnP port mapping returned false")
                return False

        except Exception as e:
            logger.warning(f"UPnP port mapping failed: {e}")
            return False

    async def map_port_range(
        self,
        start_port: int,
        end_port: int,
        protocol: str = "TCP",
        description: str = "Satori P2P"
    ) -> list[Tuple[int, int]]:
        """
        Map a range of ports.

        Args:
            start_port: First port in range
            end_port: Last port in range
            protocol: "TCP" or "UDP"
            description: Description for mappings

        Returns:
            List of (internal_port, external_port) tuples that were mapped
        """
        mapped = []
        for port in range(start_port, end_port + 1):
            if await self.map_port(port, port, protocol, description):
                mapped.append((port, port))
        return mapped

    async def unmap_port(
        self,
        internal_port: int,
        protocol: str = "TCP"
    ) -> bool:
        """
        Remove a port mapping.

        Args:
            internal_port: Local port to unmap
            protocol: "TCP" or "UDP"

        Returns:
            True if unmapping succeeded
        """
        if not self._available or self._upnp is None:
            return False

        external_port = self._mapped_ports.get(internal_port, internal_port)

        try:
            loop = asyncio.get_event_loop()

            result = await loop.run_in_executor(
                None,
                lambda: self._upnp.deleteportmapping(external_port, protocol)
            )

            if result:
                self._mapped_ports.pop(internal_port, None)
                logger.info(f"UPnP unmapped {protocol} port {external_port}")
                return True
            else:
                logger.warning(f"UPnP port unmapping returned false")
                return False

        except Exception as e:
            logger.warning(f"UPnP port unmapping failed: {e}")
            return False

    async def unmap_all(self) -> None:
        """Remove all port mappings created by this manager."""
        ports_to_unmap = list(self._mapped_ports.keys())

        for port in ports_to_unmap:
            await self.unmap_port(port, "TCP")
            await self.unmap_port(port, "UDP")

    async def get_status(self) -> dict:
        """
        Get current UPnP status and mappings.

        Returns:
            Dictionary with status information
        """
        return {
            "available": self._available,
            "external_ip": self._external_ip,
            "mapped_ports": dict(self._mapped_ports),
            "lan_address": self._upnp.lanaddr if self._upnp else None,
        }

    async def __aenter__(self):
        """Async context manager entry."""
        await self.discover()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup mappings."""
        await self.unmap_all()
