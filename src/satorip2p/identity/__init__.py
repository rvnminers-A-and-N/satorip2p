"""
satorip2p/identity/

Identity management - bridges Evrmore wallet identity to libp2p peer identity.
"""

from .evrmore_bridge import EvrmoreIdentityBridge

__all__ = ["EvrmoreIdentityBridge"]
