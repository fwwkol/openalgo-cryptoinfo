"""
AngelOne SHM WebSocket Adapter for OpenAlgo - Updated with OHLC Support
Publishes market data directly to the Shared Memory ring buffer with preserved OHLC data.
"""
import threading
import json
import logging
import time
from typing import Dict, Any, Optional, List
import os

from broker.angel.streaming.smartWebSocketV2 import SmartWebSocketV2
from database.auth_db import get_auth_token, get_feed_token
from database.token_db import get_token
from websocket_proxy.optimized_ring_buffer import OptimizedRingBuffer
from websocket_proxy.mapping import SymbolMapper
from websocket_proxy.binary_market_data import BinaryMarketData
from websocket_proxy.market_data import MarketDataMessage
from .angel_mapping import AngelExchangeMapper, AngelCapabilityRegistry


class Config:
    MODE_LTP = 1
    MODE_QUOTE = 2
    MODE_DEPTH = 3


class MarketDataCache:
    def __init__(self):
        self._cache = {}
        self._initialized_tokens = set()
        self._lock = threading.Lock()
        self.logger = logging.getLogger("market_cache")

    def update(self, token: str, data: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            cached_data = self._cache.get(token, {})
            merged = cached_data.copy()
            merged.update({k: v for k, v in data.items() if v not in [None, '', '-']})
            # preserve missing fields
            for k, v in cached_data.items():
                if k not in merged:
                    merged[k] = v
            self._cache[token] = merged
            return merged.copy()


def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == '' or value == '-':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    if value is None or value == '' or value == '-':
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


class AngelSHMWebSocketAdapter:
    """AngelOne adapter that publishes to SHM ring buffer with full OHLC support."""

    def __init__(self, shm_buffer_name: str = None):
        self.logger = logging.getLogger("angel_adapter_shm")
        self.user_id: Optional[str] = None
        self.broker_name = "angel"
        self.ws_client: Optional[SmartWebSocketV2] = None
        self.market_cache = MarketDataCache()
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        self.token_to_symbol: Dict[str, tuple] = {}
        self.running = False
        self.connected = False
        self.lock = threading.Lock()
        self.reconnect_attempts = 0
        self.reconnect_delay = 5  # Initial delay in seconds
        self.max_reconnect_delay = 60  # Maximum delay in seconds
        self.max_reconnect_attempts = 10

        # SHM ring buffer
        buffer_name = shm_buffer_name or os.getenv('SHM_BUFFER_NAME', 'ws_proxy_buffer')
        try:
            self.ring_buffer = OptimizedRingBuffer(name=buffer_name, create=False)
            self.logger.info(f"OptimizedRingBuffer initialized with buffer '{buffer_name}'")
        except Exception as e:
            self.logger.error(f"Failed to initialize OptimizedRingBuffer: {e}")
            self.ring_buffer = None

    def initialize(self, broker_name: str, user_id: str, auth_data: Optional[Dict[str, str]] = None) -> None:
        """
        Initialize connection with Angel WebSocket API
        
        Args:
            broker_name: Name of the broker (always 'angel' in this case)
            user_id: Client ID/user ID
            auth_data: If provided, use these credentials instead of fetching from DB
        
        Raises:
            ValueError: If required authentication tokens are not found
        """
        self.user_id = user_id
        self.broker_name = broker_name
        
        # Get tokens from database if not provided
        if not auth_data:
            # Fetch authentication tokens from database
            auth_token = get_auth_token(user_id)
            feed_token = get_feed_token(user_id)
            
            if not auth_token or not feed_token:
                self.logger.error(f"No authentication tokens found for user {user_id}")
                raise ValueError(f"No authentication tokens found for user {user_id}")
                
            # Use API key from environment or secure location
            api_key = os.getenv('BROKER_API_KEY', '')
        else:
            # Use provided tokens
            auth_token = auth_data.get('auth_token')
            feed_token = auth_data.get('feed_token')
            api_key = auth_data.get('api_key')
            
            if not auth_token or not feed_token or not api_key:
                self.logger.error("Missing required authentication data")
                raise ValueError("Missing required authentication data")
        
        # Create SmartWebSocketV2 instance
        self.ws_client = SmartWebSocketV2(
            auth_token=auth_token, 
            api_key=api_key, 
            client_code=user_id,  # client_code is the user_id
            feed_token=feed_token,
            max_retry_attempt=5
        )
        
        # Set callbacks
        self.ws_client.on_open = self._on_open
        self.ws_client.on_data = self._on_data
        self.ws_client.on_error = self._on_error
        self.ws_client.on_close = self._on_close
        self.ws_client.on_message = self._on_message
        
        self.running = True

    def connect(self) -> None:
        """Establish connection to Angel WebSocket"""
        if not self.ws_client:
            self.logger.error("WebSocket client not initialized. Call initialize() first.")
            return
            
        self.logger.info("Connecting to Angel WebSocket...")
        threading.Thread(target=self._connect_with_retry, daemon=True).start()

    def _connect_with_retry(self) -> None:
        """Connect to Angel WebSocket with retry logic"""
        while self.running and self.reconnect_attempts < self.max_reconnect_attempts:
            try:
                self.logger.info(f"Connecting to Angel WebSocket (attempt {self.reconnect_attempts + 1})")
                self.ws_client.connect()
                self.connected = True
                self.reconnect_attempts = 0  # Reset attempts on successful connection
                self.logger.info("Connected to Angel WebSocket successfully")
                break
                
            except Exception as e:
                self.reconnect_attempts += 1
                delay = min(self.reconnect_delay * (2 ** self.reconnect_attempts), self.max_reconnect_delay)
                self.logger.error(f"Connection failed: {e}. Retrying in {delay} seconds...")
                if self.running:
                    time.sleep(delay)
        
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.error("Max reconnection attempts reached. Giving up.")

    def disconnect(self) -> None:
        """Disconnect from Angel WebSocket"""
        self.running = False
        if self.ws_client:
            self.ws_client.close_connection()
        self.connected = False
        self.logger.info("Disconnected from Angel WebSocket (SHM mode)")

    def subscribe(self, symbol: str, exchange: str, mode: int = Config.MODE_QUOTE, depth_level: int = 5) -> Dict[str, Any]:
        """
        Subscribe to market data with Angel-specific implementation
        
        Args:
            symbol: Trading symbol (e.g., 'RELIANCE')
            exchange: Exchange code (e.g., 'NSE', 'BSE', 'NFO')
            mode: Subscription mode - 1:LTP, 2:Quote, 3:Snap Quote (Depth)
            depth_level: Market depth level (5, 20, 30)
            
        Returns:
            Dict: Response with status and error message if applicable
        """
        # Validate parameters
        if not (symbol and exchange and mode in [Config.MODE_LTP, Config.MODE_QUOTE, Config.MODE_DEPTH]):
            return {"status": "error", "code": "INVALID_PARAMS", "message": "Invalid subscription parameters"}
                                              
        # If depth mode, check if supported depth level
        if mode == Config.MODE_DEPTH and depth_level not in [5]:
            return {"status": "error", "code": "INVALID_DEPTH", 
                   "message": f"Invalid depth level {depth_level}. Must be 5"}
        
        # Map symbol to token using symbol mapper
        token_info = SymbolMapper.get_token_from_symbol(symbol, exchange)
        if not token_info:
            return {"status": "error", "code": "SYMBOL_NOT_FOUND", 
                   "message": f"Symbol {symbol} not found for exchange {exchange}"}
            
        token = token_info['token']
        brexchange = token_info['brexchange']
        
        # Check if the requested depth level is supported for this exchange
        is_fallback = False
        actual_depth = depth_level
        
        if mode == Config.MODE_DEPTH:  # Snap Quote mode (includes depth data)
            if not AngelCapabilityRegistry.is_depth_level_supported(exchange, depth_level):
                # If requested depth is not supported, use the highest available
                actual_depth = AngelCapabilityRegistry.get_fallback_depth_level(
                    exchange, depth_level
                )
                is_fallback = True
                
                self.logger.info(
                    f"Depth level {depth_level} not supported for {exchange}, "
                    f"using {actual_depth} instead"
                )
        
        # Create token list for Angel API
        token_list = [{
            "exchangeType": AngelExchangeMapper.get_exchange_type(brexchange),
            "tokens": [token]
        }]
        
        # Generate unique correlation ID that includes mode to prevent overwriting
        correlation_id = f"{symbol}_{exchange}_{mode}"
        
        subscription = {
            'symbol': symbol,
            'exchange': exchange,
            'brexchange': brexchange,
            'token': token,
            'mode': mode,
            'depth_level': depth_level,
            'actual_depth': actual_depth,
            'token_list': token_list,
            'is_fallback': is_fallback,
            'correlation_id': correlation_id
        }
        
        # Store subscription for reconnection
        with self.lock:
            self.subscriptions[correlation_id] = subscription
            self.token_to_symbol[token] = (symbol, exchange)
        
        # Subscribe if connected
        if self.connected and self.ws_client:
            try:
                self.ws_client.subscribe(correlation_id, mode, token_list)
                self.logger.debug(f"[BROKER<-APP] subscribe: {symbol}.{exchange} mode={mode}")
            except Exception as e:
                self.logger.error(f"Error subscribing to {symbol}.{exchange}: {e}")
                return {"status": "error", "code": "SUBSCRIPTION_ERROR", "message": str(e)}
        
        # Return success with capability info
        message = f"Subscribed to {symbol}.{exchange}"
        if is_fallback:
            message = f"Using depth level {actual_depth} instead of requested {depth_level}"
            
        return {
            "status": "success", 
            "message": message,
            "symbol": symbol,
            "exchange": exchange,
            "mode": mode,
            "requested_depth": depth_level,
            "actual_depth": actual_depth,
            "is_fallback": is_fallback
        }

    def unsubscribe(self, symbol: str, exchange: str, mode: int = Config.MODE_QUOTE) -> Dict[str, Any]:
        """
        Unsubscribe from market data with enhanced error handling and state validation.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange code
            mode: Subscription mode
            
        Returns:
            Dict: Response with status
        """
        if not symbol or not exchange:
            self.logger.warning(f"Invalid unsubscribe parameters: symbol='{symbol}', exchange='{exchange}'")
            return {"status": "error", "code": "INVALID_PARAMS", "message": "Symbol and exchange are required"}
        
        if mode not in [Config.MODE_LTP, Config.MODE_QUOTE, Config.MODE_DEPTH]:
            self.logger.warning(f"Invalid mode {mode} for unsubscribe {symbol}.{exchange}")
            return {"status": "error", "code": "INVALID_MODE", "message": f"Invalid mode: {mode}"}
        
        # Map symbol to token
        token_info = SymbolMapper.get_token_from_symbol(symbol, exchange)
        if not token_info:
            return {"status": "error", "code": "SYMBOL_NOT_FOUND", 
                   "message": f"Symbol {symbol} not found for exchange {exchange}"}
            
        token = token_info['token']
        brexchange = token_info['brexchange']
        
        # Create token list for Angel API
        token_list = [{
            "exchangeType": AngelExchangeMapper.get_exchange_type(brexchange),
            "tokens": [token]
        }]
        
        # Generate correlation ID
        correlation_id = f"{symbol}_{exchange}_{mode}"
        
        with self.lock:
            if correlation_id not in self.subscriptions:
                self.logger.warning(f"Unsubscribe requested for {symbol}.{exchange} mode={mode} but not found in subscriptions.")
                self._cleanup_orphaned_subscription(symbol, exchange, mode)
                return {
                    "status": "success", 
                    "message": f"Already unsubscribed from {symbol}.{exchange}", 
                    "symbol": symbol, 
                    "exchange": exchange, 
                    "mode": mode
                }
            
            subscription = self.subscriptions[correlation_id]
            
            # Unsubscribe if connected
            try:
                if self.connected and self.ws_client:
                    self.logger.debug(f"[BROKER<-APP] unsubscribe: {symbol}.{exchange} mode={mode}")
                    self.ws_client.unsubscribe(correlation_id, mode, token_list)
                    self.logger.debug(f"WebSocket unsubscribe successful for {symbol}.{exchange} mode={mode}")
                else:
                    self.logger.warning(f"WebSocket client not available for unsubscribe: {symbol}.{exchange}")
            except Exception as e:
                self.logger.warning(f"Error during websocket unsubscribe for {symbol}.{exchange}: {e}, continuing with cleanup")
            
            # Remove from subscriptions
            del self.subscriptions[correlation_id]
            
            # Clean up token mapping if no other subscriptions use this token
            if not any(sub.get('token') == token for sub in self.subscriptions.values()):
                self.token_to_symbol.pop(token, None)
                self.logger.debug(f"Cleaned up token mapping for {token}")
        
        self.logger.info(f"Successfully unsubscribed from {symbol}.{exchange} mode={mode}")
        return {
            "status": "success", 
            "message": f"Unsubscribed from {symbol}.{exchange}",
            "symbol": symbol, 
            "exchange": exchange, 
            "mode": mode
        }

    def _cleanup_orphaned_subscription(self, symbol: str, exchange: str, mode: int) -> None:
        """Clean up any orphaned websocket subscriptions."""
        try:
            token_info = SymbolMapper.get_token_from_symbol(symbol, exchange)
            if not token_info:
                self.logger.debug(f"Cannot cleanup orphaned subscription - token not found for {symbol}.{exchange}")
                return
            
            token = token_info['token']
            brexchange = token_info['brexchange']
            
            token_list = [{
                "exchangeType": AngelExchangeMapper.get_exchange_type(brexchange),
                "tokens": [token]
            }]
            
            correlation_id = f"{symbol}_{exchange}_{mode}"
            
            if self.connected and self.ws_client:
                self.logger.debug(f"Attempting to cleanup orphaned subscription for {symbol}.{exchange}")
                self.ws_client.unsubscribe(correlation_id, mode, token_list)
                
        except Exception as e:
            self.logger.debug(f"Error during orphaned subscription cleanup for {symbol}.{exchange}: {e}")

    def _on_open(self, wsapp) -> None:
        """Callback when connection is established"""
        self.logger.info("Connected to Angel WebSocket")
        self.connected = True
        
        # Resubscribe to existing subscriptions if reconnecting
        self._resubscribe_all()

    def _resubscribe_all(self):
        """Resubscribe to all existing subscriptions after reconnection"""
        with self.lock:
            for correlation_id, sub in self.subscriptions.items():
                try:
                    self.ws_client.subscribe(correlation_id, sub["mode"], sub["token_list"])
                    self.logger.info(f"Resubscribed to {sub['symbol']}.{sub['exchange']}")
                except Exception as e:
                    self.logger.error(f"Error resubscribing to {sub['symbol']}.{sub['exchange']}: {e}")

    def _on_error(self, wsapp, error) -> None:
        """Callback for WebSocket errors"""
        self.logger.error(f"Angel WebSocket error: {error}")

    def _on_close(self, wsapp) -> None:
        """Callback when connection is closed"""
        self.logger.info("Angel WebSocket connection closed")
        self.connected = False
        
        # Attempt to reconnect if we're still running
        if self.running:
            self._schedule_reconnection()

    def _schedule_reconnection(self):
        """Schedule reconnection with exponential backoff"""
        delay = min(self.reconnect_delay * (2 ** self.reconnect_attempts), self.max_reconnect_delay)
        self.reconnect_attempts += 1
        threading.Timer(delay, self._attempt_reconnection).start()

    def _attempt_reconnection(self):
        """Attempt to reconnect to Angel WebSocket"""
        try:
            if self.reconnect_attempts <= self.max_reconnect_attempts:
                self.logger.info(f"Attempting reconnection (attempt {self.reconnect_attempts})")
                threading.Thread(target=self._connect_with_retry, daemon=True).start()
        except Exception as e:
            self.logger.error(f"Reconnection error: {e}")

    def _on_message(self, wsapp, message) -> None:
        """Callback for text messages from the WebSocket"""
        self.logger.debug(f"[BROKER->APP] text message: {message}")

    def _on_data(self, wsapp, message) -> None:
        """Callback for market data from the WebSocket"""
        try:
            self.logger.debug(f"[BROKER->APP] raw data: Type: {type(message)}")
            
            # Check if we're getting binary data as per Angel's documentation
            if isinstance(message, (bytes, bytearray)):
                self.logger.debug(f"Received binary data of length: {len(message)}")
                # Parse binary data according to Angel's format if needed
                return
                
            # The message should be a dictionary for market data
            if not isinstance(message, dict):
                self.logger.warning(f"Received message is not a dictionary: {type(message)}")
                return
                
            # Extract token and exchange_type from message
            token = message.get('token')
            exchange_type = message.get('exchange_type')
            
            if not token:
                self.logger.warning("Received message without token")
                return
                
            self.logger.debug(f"Processing message with token: {token}, exchange_type: {exchange_type}")
            
            # Find the subscription that matches this token
            subscription = None
            with self.lock:
                for sub in self.subscriptions.values():
                    if (sub['token'] == token and 
                        AngelExchangeMapper.get_exchange_type(sub['brexchange']) == exchange_type):
                        subscription = sub
                        break
            
            if not subscription:
                self.logger.warning(f"Received data for unsubscribed token: {token}")
                return
            
            # Process the message
            self._process_market_message(message, subscription)
            
        except Exception as e:
            self.logger.error(f"Error processing market data: {e}", exc_info=True)

    def _process_market_message(self, data: Dict[str, Any], subscription: Dict[str, Any]) -> None:
        """Process market message and publish to SHM"""
        symbol = subscription['symbol']
        exchange = subscription['exchange']
        mode = subscription['mode']
        token = subscription['token']
        
        # Update cache and get merged data
        if token:
            data = self.market_cache.update(token, data)
        
        # Get actual mode from message if available, otherwise use subscription mode
        actual_msg_mode = data.get('subscription_mode', mode)
        
        # Normalize the data
        normalized = self._normalize_market_data(data, actual_msg_mode)
        normalized.update({
            'symbol': symbol,
            'exchange': exchange,
            'timestamp': int(time.time() * 1000)
        })
        
        # Create topic
        mode_str = {Config.MODE_LTP: 'LTP', Config.MODE_QUOTE: 'QUOTE', Config.MODE_DEPTH: 'DEPTH'}[mode]
        topic = f"{exchange}_{symbol}_{mode_str}"
        
        self.logger.debug(f"[APP->SHM] topic={topic} symbol={symbol} payload={json.dumps(normalized)[:200]}...")
        
        # Publish to SHM
        self._publish_market_data(topic, normalized)

    def _normalize_market_data(self, message: Dict[str, Any], mode: int) -> Dict[str, Any]:
        """
        Normalize broker-specific data format to a common format
        
        Args:
            message: The raw message from the broker
            mode: Subscription mode
            
        Returns:
            Dict: Normalized market data
        """
        # Angel sends prices in paise (1/100 of a rupee) so we need to divide by 100
        
        if mode == Config.MODE_LTP:  # LTP mode
            return {
                'mode': Config.MODE_LTP,
                'ltp': safe_float(message.get('last_traded_price', 0)) / 100,
                'ltt': safe_int(message.get('exchange_timestamp', 0)),
                'angel_timestamp': safe_int(message.get('exchange_timestamp', 0))
            }
            
        elif mode == Config.MODE_QUOTE:  # Quote mode
            return {
                'mode': Config.MODE_QUOTE,
                'ltp': safe_float(message.get('last_traded_price', 0)) / 100,
                'volume': safe_int(message.get('volume_trade_for_the_day', 0)),
                'open': safe_float(message.get('open_price_of_the_day', 0)) / 100,
                'high': safe_float(message.get('high_price_of_the_day', 0)) / 100,
                'low': safe_float(message.get('low_price_of_the_day', 0)) / 100,
                'close': safe_float(message.get('closed_price', 0)) / 100,
                'last_quantity': safe_int(message.get('last_traded_quantity', 0)),
                'average_price': safe_float(message.get('average_traded_price', 0)) / 100,
                'total_buy_quantity': safe_int(message.get('total_buy_quantity', 0)),
                'total_sell_quantity': safe_int(message.get('total_sell_quantity', 0)),
                'ltt': safe_int(message.get('exchange_timestamp', 0)),
                'angel_timestamp': safe_int(message.get('exchange_timestamp', 0))
            }
            
        elif mode == Config.MODE_DEPTH:  # Snap Quote mode (includes depth data)
            result = {
                'mode': Config.MODE_DEPTH,
                'ltp': safe_float(message.get('last_traded_price', 0)) / 100,
                'volume': safe_int(message.get('volume_trade_for_the_day', 0)),
                'open': safe_float(message.get('open_price', 0)) / 100,
                'high': safe_float(message.get('high_price', 0)) / 100,
                'low': safe_float(message.get('low_price', 0)) / 100,
                'close': safe_float(message.get('close_price', 0)) / 100,
                'oi': safe_int(message.get('open_interest', 0)),
                'upper_circuit': safe_float(message.get('upper_circuit_limit', 0)) / 100,
                'lower_circuit': safe_float(message.get('lower_circuit_limit', 0)) / 100,
                'ltt': safe_int(message.get('exchange_timestamp', 0)),
                'angel_timestamp': safe_int(message.get('exchange_timestamp', 0))
            }
            
            # Add depth data if available
            result['depth'] = {
                'buy': self._extract_depth_data(message, is_buy=True),
                'sell': self._extract_depth_data(message, is_buy=False)
            }
            result['depth_level'] = 5
            
            return result
        else:
            return {}

    def _extract_depth_data(self, message: Dict[str, Any], is_buy: bool) -> List[Dict[str, Any]]:
        """
        Extract depth data from Angel's message format
        
        Args:
            message: The raw message containing depth data
            is_buy: Whether to extract buy or sell side
            
        Returns:
            List: List of depth levels with price, quantity, and orders
        """
        depth = []
        side_label = 'Buy' if is_buy else 'Sell'
        
        # Log the raw message structure to help debug
        self.logger.debug(f"Extracting {side_label} depth data from message: {list(message.keys())}")
        
        # Check for different possible depth data formats that Angel might send
        best_5_key = 'best_5_buy_data' if is_buy else 'best_5_sell_data'
        depth_20_key = 'depth_20_buy_data' if is_buy else 'depth_20_sell_data'
        best_five_key = 'best_five_buy_market_data' if is_buy else 'best_five_sell_market_data'
        
        depth_data = None
        
        # Try different data format keys
        if best_5_key in message and isinstance(message[best_5_key], list):
            depth_data = message[best_5_key]
            self.logger.debug(f"Found {side_label} depth data using {best_5_key}: {len(depth_data)} levels")
        elif depth_20_key in message and isinstance(message[depth_20_key], list):
            depth_data = message[depth_20_key]
            self.logger.debug(f"Found {side_label} depth data using {depth_20_key}: {len(depth_data)} levels")
        elif best_five_key in message and isinstance(message[best_five_key], list):
            depth_data = message[best_five_key]
            self.logger.debug(f"Found {side_label} depth data using {best_five_key}: {len(depth_data)} levels")
        
        # Process depth data
        if depth_data:
            for level in depth_data:
                if isinstance(level, dict):
                    price = level.get('price', 0)
                    # Ensure price is properly scaled (divide by 100)
                    if price > 0:
                        price = price / 100
                    
                    depth.append({
                        'price': price,
                        'quantity': level.get('quantity', 0),
                        'orders': level.get('no of orders', 0)
                    })
        
        # If no depth data found, return empty levels as fallback
        if not depth:
            self.logger.warning(f"No {side_label} depth data found in message. Available keys: {list(message.keys())}")
            for i in range(5):  # Default to 5 empty levels
                depth.append({
                    'price': 0.0,
                    'quantity': 0,
                    'orders': 0
                })
        else:
            # Log the depth data being returned for debugging
            self.logger.debug(f"{side_label} depth data found: {len(depth)} levels")
            if depth and depth[0]['price'] > 0:
                self.logger.debug(f"{side_label} depth first level: Price={depth[0]['price']}, Qty={depth[0]['quantity']}")
            
        return depth

    def _publish_market_data(self, topic: str, data: Dict[str, Any]) -> None:
        """
        Enhanced publish method that preserves OHLC data using the new binary format.
        """
        if self.ring_buffer is None:
            self.logger.debug("Ring buffer not available; drop message")
            return
        
        try:
            symbol = data.get('symbol', '')
            exchange = data.get('exchange', 'NSE')
            mode = data.get('mode', 1)
            
            # For depth data (mode 3), preserve full depth structure by using dict format
            if mode == Config.MODE_DEPTH:
                ltp_value = data.get('ltp', 0) or data.get('price', 0)
                price_float = float(ltp_value) if ltp_value else 0.0
                
                self.logger.debug(f"Price conversion - ltp_value: {ltp_value}, price_float: {price_float}")
                
                enriched_data = data.copy()
                enriched_data.update({
                    'price': price_float,
                    'ltp': price_float,
                })
                
                self.logger.debug(f"Depth data preserved - symbol: {symbol}, depth levels: {len(data.get('depth', {}).get('buy', []))}")
                
                published = self.ring_buffer.publish_single(enriched_data)
                if not published:
                    self.logger.debug(f"[APP->SHM] publish failed for symbol={symbol} (buffer full)")
                else:
                    self.logger.debug(f"[APP->SHM] published depth data for symbol={symbol}")
            else:
                # For LTP/Quote modes, use the new enhanced binary format that preserves OHLC
                self.logger.debug(f"Publishing OHLC data for {symbol}: O={data.get('open', 0)}, H={data.get('high', 0)}, L={data.get('low', 0)}, C={data.get('close', 0)}")
                
                # Create binary message using the new from_normalized_data method
                binary_msg = BinaryMarketData.from_normalized_data(data, exchange)
                
                self.logger.debug(f"Binary message created - price: {binary_msg.get_price_float()}, symbol: {binary_msg.get_symbol_string()}, open: {binary_msg.get_open_price_float()}")
                
                published = self.ring_buffer.publish_single(binary_msg)
                if not published:
                    self.logger.debug(f"[APP->SHM] publish failed for symbol={symbol} (buffer full)")
                else:
                    self.logger.debug(f"[APP->SHM] published symbol={symbol}")
                
        except Exception as e:
            self.logger.error(f"Error publishing market data for {topic}: {e}")
            self.logger.debug(f"Failed data: {data}")

    def _create_error_response(self, code: str, message: str) -> Dict[str, Any]:
        """Create standardized error response"""
        return {
            "status": "error",
            "code": code,
            "message": message
        }

    def _create_success_response(self, message: str, **kwargs) -> Dict[str, Any]:
        """Create standardized success response"""
        response = {
            "status": "success",
            "message": message
        }
        response.update(kwargs)
        return response