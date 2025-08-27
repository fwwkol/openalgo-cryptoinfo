import json
import os
from abc import ABC, abstractmethod
from utils.logging import get_logger
from .shm_publisher import SharedMemoryPublisher

# Initialize logger
logger = get_logger(__name__)

class BaseBrokerWebSocketAdapter(ABC):
    """
    Base class for all broker-specific WebSocket adapters that implements
    common functionality and defines the interface for broker-specific implementations.
    Uses shared memory for market data publishing (no ZeroMQ).
    """
    
    def __init__(self):
        self.logger = get_logger("broker_adapter")
        self.logger.info("BaseBrokerWebSocketAdapter initializing (SHM-only mode)")
        
        try:
            # Initialize instance variables
            self.subscriptions = {}
            self.connected = False
            
            # Initialize shared memory publisher for ring buffer flow
            shm_buffer_name = os.getenv('SHM_BUFFER_NAME', 'ws_proxy_buffer')
            try:
                self.shm_publisher = SharedMemoryPublisher(buffer_name=shm_buffer_name)
                self.logger.info(f"SharedMemoryPublisher initialized with buffer '{shm_buffer_name}'")
            except Exception as e:
                self.logger.error(f"Failed to initialize SharedMemoryPublisher: {e}")
                self.shm_publisher = None
            
            self.logger.info("BaseBrokerWebSocketAdapter initialized successfully")
        except Exception as e:
            self.logger.error(f"Error in BaseBrokerWebSocketAdapter init: {e}")
            raise
        
    @abstractmethod
    def initialize(self, broker_name, user_id, auth_data=None):
        """
        Initialize connection with broker WebSocket API
        
        Args:
            broker_name: The name of the broker (e.g., 'angel', 'zerodha')
            user_id: The user's ID or client code
            auth_data: Dict containing authentication data, if not provided will fetch from DB
        """
        pass
        
    @abstractmethod
    def subscribe(self, symbol, exchange, mode=2, depth_level=5):
        """
        Subscribe to market data with the specified mode and depth level
        
        Args:
            symbol: Trading symbol (e.g., 'RELIANCE')
            exchange: Exchange code (e.g., 'NSE', 'BSE')
            mode: Subscription mode - 1:LTP, 2:Quote, 4:Depth
            depth_level: Market depth level (5, 20, or 30 depending on broker support)
            
        Returns:
            dict: Response with status and capability information
        """
        pass
        
    @abstractmethod
    def unsubscribe(self, symbol, exchange, mode=2):
        """
        Unsubscribe from market data
        
        Args:
            symbol: Trading symbol
            exchange: Exchange code
            mode: Subscription mode
            
        Returns:
            dict: Response with status
        """
        pass
        
    @abstractmethod
    def connect(self):
        """
        Establish connection to the broker's WebSocket
        """
        pass
        
    @abstractmethod
    def disconnect(self):
        """
        Disconnect from the broker's WebSocket
        """
        pass
        
    def cleanup(self):
        """
        Clean up adapter resources.
        """
        try:
            if hasattr(self, 'shm_publisher') and self.shm_publisher:
                self.shm_publisher.cleanup()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
            
    def __del__(self):
        """
        Destructor to ensure resources are properly cleaned up
        """
        try:
            self.cleanup()
        except Exception:
            pass
    
    def publish_market_data(self, topic, data):
        """
        Publish market data to shared-memory ring buffer
        
        Args:
            topic: Topic string for subscriber filtering (e.g., 'NSE_RELIANCE_LTP')
            data: Market data dictionary
        """
        try:
            # Prefer SHM publisher when available
            if getattr(self, 'shm_publisher', None) is not None:
                # Extract symbol from topic pattern "EXCHANGE_SYMBOL_MODE" if possible
                symbol = None
                try:
                    parts = str(topic).split('_')
                    if len(parts) >= 2:
                        symbol = parts[1]
                except Exception:
                    symbol = None

                if not symbol:
                    symbol = data.get('symbol') if isinstance(data, dict) else None
                if not symbol:
                    # Fallback to whole topic as symbol identifier
                    symbol = str(topic)

                published = self.shm_publisher.publish_market_data(symbol, data if isinstance(data, dict) else {})
                if not published:
                    self.logger.debug("SHM publish returned False (buffer full or not ready)")
                return

            # If SHM publisher isn't available, log a debug message
            self.logger.debug("No SHM publisher available to publish market data")
        except Exception as e:
            self.logger.exception(f"Error publishing market data: {e}")
    
    def _create_success_response(self, message, **kwargs):
        """
        Create a standard success response
        """
        response = {
            'status': 'success',
            'message': message
        }
        response.update(kwargs)
        return response
    
    def _create_error_response(self, code, message):
        """
        Create a standard error response
        """
        return {
            'status': 'error',
            'code': code,
            'message': message
        }
