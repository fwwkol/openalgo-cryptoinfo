"""
Base class for broker WebSocket adapters using Shared Memory
"""

import os
import time
from abc import ABC, abstractmethod
from utils.logging import get_logger
from .optimized_ring_buffer import OptimizedRingBuffer

logger = get_logger(__name__)

class BaseBrokerWebSocketAdapter(ABC):
    """
    Base class for all broker-specific WebSocket adapters that implements
    common functionality and defines the interface for broker-specific implementations.
    Uses shared memory for market data publishing.
    """
    
    def __init__(self):
        self.logger = get_logger("broker_adapter")
        
        try:
            # Initialize instance variables
            self.subscriptions = {}
            self.connected = False
            
            # Initialize shared memory ring buffer
            shm_buffer_name = os.getenv('SHM_BUFFER_NAME', 'ws_proxy_buffer')
            try:
                self.ring_buffer = OptimizedRingBuffer(name=shm_buffer_name, create=False)
            except Exception as e:
                self.logger.error(f"Failed to initialize ring buffer: {e}")
                self.ring_buffer = None
                
        except Exception as e:
            self.logger.error(f"Error in adapter initialization: {e}")
            raise
    
    @abstractmethod
    def initialize(self, broker_name, user_id, auth_data=None):
        """Initialize connection with broker WebSocket API"""
        pass
    
    @abstractmethod
    def subscribe(self, symbol, exchange, mode=2, depth_level=5):
        """Subscribe to market data"""
        pass
    
    @abstractmethod
    def unsubscribe(self, symbol, exchange, mode=2):
        """Unsubscribe from market data"""
        pass
    
    @abstractmethod
    def connect(self):
        """Establish connection to the broker's WebSocket"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Disconnect from the broker's WebSocket"""
        pass
    
    def publish_market_data(self, symbol, data):
        """
        Publish market data to shared-memory ring buffer
        Args:
            symbol: Trading symbol
            data: Market data dictionary
        """
        try:
            if getattr(self, 'ring_buffer', None) is not None:
                # Convert data to BinaryMarketData format for optimized publishing
                from .binary_market_data import BinaryMarketData
                from .market_data import MarketDataMessage
                
                # Create MarketDataMessage from data
                msg = MarketDataMessage(
                    symbol=symbol,
                    timestamp=data.get('timestamp', time.time()),
                    price=float(data.get('ltp', 0) or data.get('price', 0)),
                    volume=int(data.get('volume', 0)),
                    message_type=1  # LTP type
                )
                
                # Convert to binary format
                binary_msg = BinaryMarketData.from_market_data_message(msg, data.get('exchange', 'NSE'))
                
                # Publish to ring buffer
                published = self.ring_buffer.publish_single(binary_msg)
                return published
            else:
                return False
        except Exception as e:
            self.logger.exception(f"Error publishing market data: {e}")
            return False
    
    def cleanup(self):
        """Clean up adapter resources."""
        try:
            if hasattr(self, 'ring_buffer') and self.ring_buffer:
                self.ring_buffer.close()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def __del__(self):
        """Destructor to ensure resources are properly cleaned up"""
        try:
            self.cleanup()
        except Exception:
            pass
    
    def _create_success_response(self, message, **kwargs):
        """Create a standard success response"""
        response = {'status': 'success', 'message': message}
        response.update(kwargs)
        return response
    
    def _create_error_response(self, code, message):
        """Create a standard error response"""
        return {'status': 'error', 'code': code, 'message': message}
