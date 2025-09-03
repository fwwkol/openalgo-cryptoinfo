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

# Register SHM-based adapters dynamically (lazy loading to avoid circular imports)
def _register_flattrade_adapter():
    """Lazy registration of Flattrade adapter to avoid circular imports."""
    try:
        from broker.flattrade.streaming.flattrade_adapter_shm import FlattradeSHMWebSocketAdapter
        register_adapter("flattrade", FlattradeSHMWebSocketAdapter)
        logger.debug("Flattrade SHM adapter registered")
        return True
    except ImportError as e:
        logger.debug(f"Flattrade SHM adapter not available: {e}")
        return False

# Register the adapter on first use
_flattrade_registered = False

# Register other SHM adapters as they become available
try:
    from broker.angel.streaming.angel_adapter_shm import AngelSHMWebSocketAdapter
    register_adapter("angel", AngelSHMWebSocketAdapter)
    logger.debug("Angel SHM adapter registered")
except ImportError as e:
    logger.debug(f"Angel SHM adapter not available: {e}")

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
