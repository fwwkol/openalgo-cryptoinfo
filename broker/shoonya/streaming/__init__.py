"""
Shoonya WebSocket streaming module
"""

# Remove this line that causes the circular import:
# from .shoonya_adapter import ShoonyaWebSocketAdapter

from .shoonya_mapping import ShoonyaExchangeMapper, ShoonyaCapabilityRegistry
from .shoonya_websocket import ShoonyaWebSocket

__all__ = [
    # Remove 'ShoonyaWebSocketAdapter' from here
    # 'ShoonyaWebSocketAdapter',
    'ShoonyaExchangeMapper',
    'ShoonyaCapabilityRegistry',
    'ShoonyaWebSocket'
]
