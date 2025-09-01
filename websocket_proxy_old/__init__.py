"""WebSocket Proxy for shared memory market data distribution.

This module provides a high-performance WebSocket proxy that reads market data
from a shared memory ring buffer and distributes it to connected WebSocket clients.
"""

import logging

from .websocket_proxy_shm import WebSocketProxy
from .market_data import MarketDataMessage
from .ring_buffer import LockFreeRingBuffer
from .config import config

# Set up logger
logger = logging.getLogger(__name__)

__all__ = [
    'WebSocketProxy',
    'MarketDataMessage',
    'LockFreeRingBuffer',
    'config'
]
