"""WebSocket Proxy for shared memory market data distribution."""

import logging
from .websocket_proxy_shm import WebSocketProxy
from .market_data import MarketDataMessage
from .binary_market_data import BinaryMarketData
from .optimized_ring_buffer import OptimizedRingBuffer, OptimizedRingBufferPool
from .symbol_partitioner import SymbolPartitioner
from .parallel_websocket_processor import ParallelWebSocketProcessor, ProcessorStats, ProcessorConfig
from .massive_subscription_manager import MassiveSubscriptionManager, SubscriptionConfig, ConnectionStats
from .backpressure_aware_consumer import BackpressureAwareConsumer, BackpressureConfig, ConsumerStats
from .performance_monitor import PerformanceMonitor, PerformanceThresholds, AlertConfig, PerformanceMetrics
from .config import config
from .broker_factory import register_adapter, create_broker_adapter

# Set up logger
logger = logging.getLogger(__name__)

# Register other SHM adapters as they become available
try:
    from broker.flattrade.streaming.flattrade_adapter_shm import FlattradeSHMWebSocketAdapter
    register_adapter("flattrade", FlattradeSHMWebSocketAdapter)
    logger.debug("Flattrade SHM adapter registered")
except ImportError as e:
    logger.debug(f"Flattrade SHM adapter not available: {e}")

try:
    from broker.angel.streaming.angel_adapter_shm import AngelSHMWebSocketAdapter
    register_adapter("angel", AngelSHMWebSocketAdapter)
    logger.debug("Angel SHM adapter registered")
except ImportError as e:
    logger.debug(f"Angel SHM adapter not available: {e}")

try:
    from broker.shoonya.streaming.shoonya_adapter_shm import ShoonyaSHMWebSocketAdapter
    register_adapter("shoonya", ShoonyaSHMWebSocketAdapter)
    logger.debug("Shoonya SHM adapter registered")
except ImportError as e:
    logger.debug(f"Shoonya SHM adapter not available: {e}")

try:
    from broker.kotak.streaming.kotak_adapter_shm import KotakSHMWebSocketAdapter
    register_adapter("kotak", KotakSHMWebSocketAdapter)
    logger.debug("Kotak SHM adapter registered")
except ImportError as e:
    logger.debug(f"Kotak SHM adapter not available: {e}")

try:
    from broker.upstox.streaming.upstox_adapter_shm import UpstoxSHMWebSocketAdapter
    register_adapter("upstox", UpstoxSHMWebSocketAdapter)
    logger.debug("Upstox SHM adapter registered")
except ImportError as e:
    logger.debug(f"Upstox SHM adapter not available: {e}")

# Add more SHM adapters here as they are created
# try:
#     from broker.zerodha.streaming.zerodha_adapter_shm import ZerodhaSHMWebSocketAdapter
#     register_adapter("zerodha", ZerodhaSHMWebSocketAdapter)
# except ImportError:
#     pass

__all__ = [
    'WebSocketProxy',
    'MarketDataMessage',
    'BinaryMarketData',
    'OptimizedRingBuffer',
    'OptimizedRingBufferPool',
    'SymbolPartitioner',
    'ParallelWebSocketProcessor',
    'ProcessorStats',
    'ProcessorConfig',
    'MassiveSubscriptionManager',
    'SubscriptionConfig',
    'ConnectionStats',
    'BackpressureAwareConsumer',
    'BackpressureConfig',
    'ConsumerStats',
    'PerformanceMonitor',
    'PerformanceThresholds',
    'AlertConfig',
    'PerformanceMetrics',
    'config',
    'register_adapter',
    'create_broker_adapter'
]
