"""
Fyers SHM WebSocket Adapter for OpenAlgo - Enhanced Version
Publishes market data directly to the Shared Memory ring buffer with preserved OHLC data.
Handles WebSocket streaming for all exchanges: NSE, NFO, BSE, BFO, MCX
Uses HSM binary protocol for real-time data
"""

import logging
import time
import threading
from typing import Dict, List, Any, Optional, Callable
from collections import defaultdict
import os
import json

from database.auth_db import get_auth_token
from websocket_proxy.optimized_ring_buffer import OptimizedRingBuffer
from websocket_proxy.mapping import SymbolMapper
from websocket_proxy.binary_market_data import BinaryMarketData
from websocket_proxy.market_data import MarketDataMessage
from .fyers_hsm_websocket import FyersHSMWebSocket
from .fyers_token_converter import FyersTokenConverter
from .fyers_mapping import FyersDataMapper


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


class FyersSHMWebSocketAdapter:
    """
    Fyers SHM WebSocket adapter for OpenAlgo streaming service
    Publishes market data directly to the Shared Memory ring buffer
    Follows OpenAlgo adapter pattern similar to Angel, Zerodha etc.
    """
    
    def __init__(self, shm_buffer_name: str = None):
        """
        Initialize Fyers SHM adapter
        
        Args:
            shm_buffer_name: Name of the shared memory buffer
        """
        self.logger = logging.getLogger("fyers_adapter_shm")
        self.user_id: Optional[str] = None
        self.userid: Optional[str] = None  # For backward compatibility
        self.broker_name = "fyers"
        self.access_token: Optional[str] = None
        self.ws_client: Optional[FyersHSMWebSocket] = None
        self.market_cache = MarketDataCache()
        
        # Initialize components
        self.token_converter = None
        self.data_mapper = FyersDataMapper()
        
        # Subscription tracking
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        self.active_subscriptions = {}  # symbol -> subscription_info
        self.subscription_callbacks = {}  # data_type -> callback
        self.symbol_to_hsm = {}  # symbol -> hsm_token mapping
        self.hsm_to_symbol = {}  # hsm_token -> symbol mapping (reverse lookup)
        self.token_to_symbol: Dict[str, tuple] = {}
        self.ws_subscription_refs: Dict[str, Dict[str, int]] = {}
        
        # Connection state
        self.connected = False
        self.connecting = False
        self.running = False
        self.reconnect_attempts = 0
        
        # Threading
        self.lock = threading.Lock()
        
        # Deduplication tracking
        self.last_data = {}  # symbol -> {ltp, timestamp} for deduplication
        
        # SHM ring buffer
        buffer_name = shm_buffer_name or os.getenv('SHM_BUFFER_NAME', 'ws_proxy_buffer')
        try:
            self.ring_buffer = OptimizedRingBuffer(name=buffer_name, create=False)
            self.logger.info(f"OptimizedRingBuffer initialized with buffer '{buffer_name}'")
        except Exception as e:
            self.logger.error(f"Failed to initialize OptimizedRingBuffer: {e}")
            self.ring_buffer = None
        
        self.logger.info("Fyers SHM adapter initialized")

    def initialize(self, broker_name: str, user_id: str, auth_data: Optional[Dict[str, str]] = None) -> None:
        """Initialize the adapter with broker credentials"""
        self.user_id = user_id
        self.userid = user_id  # For backward compatibility
        self.broker_name = broker_name
        
        # Get access token from database
        self.access_token = get_auth_token(user_id)
        if not self.access_token:
            raise ValueError(f"Missing Fyers access token for user {user_id}")
        
        # Initialize token converter
        self.token_converter = FyersTokenConverter(self.access_token)
        
        self.running = True
        self.logger.info(f"Fyers adapter initialized for user: {user_id}")
    
    def connect(self) -> None:
        """
        Connect to Fyers HSM WebSocket
        
        Raises:
            ConnectionError if connection fails
        """
        if not self.access_token:
            self.logger.error("Access token not available. Call initialize() first.")
            raise ConnectionError("Access token not available. Call initialize() first.")
            
        if self.connected:
            self.logger.warning("Already connected to Fyers WebSocket")
            return
        
        if self.connecting:
            self.logger.warning("Connection already in progress")
            return
        
        try:
            self.connecting = True
            self.logger.info("Connecting to Fyers HSM WebSocket...")
            
            # Initialize WebSocket client
            self.ws_client = FyersHSMWebSocket(
                access_token=self.access_token,
                log_path=""
            )
            
            # Set callbacks
            self.ws_client.set_callbacks(
                on_message=self._on_message,
                on_error=self._on_error,
                on_open=self._on_open,
                on_close=self._on_close
            )
            
            # Connect
            self.ws_client.connect()
            
            # Wait for authentication
            timeout = 15
            start_time = time.time()
            while not self.ws_client.is_connected() and time.time() - start_time < timeout:
                time.sleep(0.1)
            
            if self.ws_client.is_connected():
                self.connected = True
                self.reconnect_attempts = 0
                self.logger.info("Connected to Fyers HSM WebSocket successfully")
            else:
                self.logger.error("Failed to authenticate with Fyers HSM WebSocket")
                raise ConnectionError("Failed to connect to Fyers WebSocket")
                
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            raise ConnectionError(f"Failed to connect to Fyers WebSocket: {e}")
        finally:
            self.connecting = False
    
    def disconnect(self, clear_mappings=True):
        """
        Disconnect from Fyers WebSocket
        
        Args:
            clear_mappings: If True, clear all mappings. If False, preserve them for reconnection.
        """
        try:
            self.running = False
            self.connected = False
            if self.ws_client:
                self.ws_client.disconnect()
                self.ws_client = None
            
            # Only clear mappings if requested (for complete disconnect)
            if clear_mappings:
                self.subscriptions.clear()
                self.active_subscriptions.clear()
                self.symbol_to_hsm.clear()
                self.hsm_to_symbol.clear()
                self.subscription_callbacks.clear()
                self.token_to_symbol.clear()
                self.ws_subscription_refs.clear()
                self.last_data.clear()
                self.logger.info("Disconnected from Fyers WebSocket (cleared all mappings)")
            else:
                # Keep mappings but clear active subscriptions for reconnection
                self.subscriptions.clear()
                self.active_subscriptions.clear()
                self.subscription_callbacks.clear()
                self.ws_subscription_refs.clear()
                self.last_data.clear()
                self.logger.info(f"Disconnected from Fyers WebSocket (preserved {len(self.hsm_to_symbol)} mappings)")
                
        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")

    def subscribe(self, symbol: str, exchange: str, mode: int = Config.MODE_QUOTE, depth_level: int = 5) -> Dict[str, Any]:
        """
        Subscribe to a single symbol (SHM pattern compatibility)
        
        Args:
            symbol: Symbol name
            exchange: Exchange name
            mode: Subscription mode (LTP, QUOTE, DEPTH)
            depth_level: Depth level for depth subscriptions
            
        Returns:
            Dict with subscription status
        """
        if not (symbol and exchange and mode in [Config.MODE_LTP, Config.MODE_QUOTE, Config.MODE_DEPTH]):
            return {"status": "error", "code": "INVALID_PARAMS", "message": "Invalid subscription parameters"}

        # Convert single symbol to list format for internal processing
        symbols = [{"symbol": symbol, "exchange": exchange}]
        
        # Determine data type based on mode
        if mode == Config.MODE_DEPTH:
            data_type = "DepthUpdate"
        else:
            data_type = "SymbolUpdate"
        
        # Use internal subscribe_symbols method
        success = self.subscribe_symbols(symbols, data_type, self._shm_callback)
        
        if success:
            # Store subscription info for SHM pattern
            correlation_id = f"{symbol}_{exchange}_{mode}"
            subscription = {
                'symbol': symbol,
                'exchange': exchange,
                'mode': mode,
                'depth_level': depth_level,
                'data_type': data_type,
            }
            
            with self.lock:
                self.subscriptions[correlation_id] = subscription
            
            return {
                "status": "success", 
                "message": f"Subscribed to {symbol}.{exchange}", 
                "symbol": symbol, 
                "exchange": exchange, 
                "mode": mode
            }
        else:
            return {
                "status": "error", 
                "code": "SUBSCRIPTION_FAILED", 
                "message": f"Failed to subscribe to {symbol}.{exchange}"
            }

    def unsubscribe(self, symbol: str, exchange: str, mode: int = Config.MODE_QUOTE) -> Dict[str, Any]:
        """
        Unsubscribe from symbol (SHM pattern compatibility)
        
        Args:
            symbol: Symbol name
            exchange: Exchange name
            mode: Subscription mode
            
        Returns:
            Dict with unsubscription status
        """
        if not symbol or not exchange:
            self.logger.warning(f"Invalid unsubscribe parameters: symbol='{symbol}', exchange='{exchange}'")
            return {"status": "error", "code": "INVALID_PARAMS", "message": "Symbol and exchange are required"}
        
        if mode not in [Config.MODE_LTP, Config.MODE_QUOTE, Config.MODE_DEPTH]:
            self.logger.warning(f"Invalid mode {mode} for unsubscribe {symbol}.{exchange}")
            return {"status": "error", "code": "INVALID_MODE", "message": f"Invalid mode: {mode}"}
        
        correlation_id = f"{symbol}_{exchange}_{mode}"
        
        with self.lock:
            if correlation_id not in self.subscriptions:
                self.logger.warning(f"Unsubscribe requested for {symbol}.{exchange} mode={mode} but not found in subscriptions.")
                return {
                    "status": "success", 
                    "message": f"Already unsubscribed from {symbol}.{exchange}", 
                    "symbol": symbol, 
                    "exchange": exchange, 
                    "mode": mode
                }
            
            # Remove from subscriptions
            del self.subscriptions[correlation_id]
            
            # Remove from active subscriptions and other mappings
            full_symbol = f"{exchange}:{symbol}"
            self.active_subscriptions.pop(full_symbol, None)
            
            # Clean up callback
            data_type = "DepthUpdate" if mode == Config.MODE_DEPTH else "SymbolUpdate"
            callback_key = f"{data_type}_{full_symbol}"
            self.subscription_callbacks.pop(callback_key, None)
        
        self.logger.info(f"Successfully unsubscribed from {symbol}.{exchange} mode={mode}")
        return {
            "status": "success", 
            "message": f"Unsubscribed from {symbol}.{exchange}", 
            "symbol": symbol, 
            "exchange": exchange, 
            "mode": mode
        }
    
    def subscribe_symbols(self, symbols: List[Dict[str, str]], data_type: str, callback: Callable):
        """
        Subscribe to symbols for market data (Enhanced Original Fyers method)
        
        Args:
            symbols: List of symbol dicts with 'exchange' and 'symbol' keys
            data_type: Type of data ("SymbolUpdate", "DepthUpdate")
            callback: Callback function to receive data
        """
        if not self.connected:
            self.logger.error("Not connected to Fyers WebSocket")
            return False
        
        try:
            with self.lock:
                self.logger.info(f"\n" + "="*60)
                self.logger.info(f"SUBSCRIBING TO {len(symbols)} SYMBOLS")
                self.logger.info(f"Data type: {data_type}")
                self.logger.info(f"Symbols to subscribe: {symbols}")
                self.logger.info("="*60)
                
                # Store callback per symbol to prevent data mixing
                for symbol_info in symbols:
                    exchange = symbol_info.get("exchange", "NSE")
                    symbol = symbol_info.get("symbol", "")
                    if symbol:
                        full_symbol = f"{exchange}:{symbol}"
                        callback_key = f"{data_type}_{full_symbol}"
                        self.subscription_callbacks[callback_key] = callback
                        self.logger.info(f"Stored callback for {callback_key}")
                
                # Store subscription info for tracking
                valid_symbols = []
                for symbol_info in symbols:
                    exchange = symbol_info.get("exchange", "NSE")
                    symbol = symbol_info.get("symbol", "")
                    
                    if not symbol:
                        continue
                    
                    valid_symbols.append({"exchange": exchange, "symbol": symbol})
                    
                    # Store subscription info
                    full_symbol = f"{exchange}:{symbol}"
                    self.active_subscriptions[full_symbol] = {
                        "exchange": exchange,
                        "symbol": symbol,
                        "data_type": data_type,
                        "subscribed_at": time.time()
                    }
                
                if not valid_symbols:
                    self.logger.warning("No valid symbols to subscribe")
                    return False
                
                self.logger.info(f"Converting {len(valid_symbols)} OpenAlgo symbols to HSM format using database lookup...")
                
                # Convert OpenAlgo symbols directly to HSM tokens using database lookup
                hsm_tokens, token_mappings, invalid_symbols = self.token_converter.convert_openalgo_symbols_to_hsm(
                    valid_symbols, data_type
                )
                
                if invalid_symbols:
                    self.logger.warning(f"Invalid symbols: {invalid_symbols}")
                
                if not hsm_tokens:
                    self.logger.error("No valid HSM tokens generated")
                    return False
                
                # CRITICAL FIX: Ensure proper HSM token mapping
                # The tokens are generated in the same order as valid_symbols
                self.logger.info(f"\nCreating HSM mappings for {len(hsm_tokens)} tokens...")
                self.logger.info(f"HSM Tokens: {hsm_tokens}")
                self.logger.info(f"Valid Symbols: {[f'{s['exchange']}:{s['symbol']}' for s in valid_symbols]}")
                
                # Primary mapping strategy: Map by order (most reliable)
                for i, hsm_token in enumerate(hsm_tokens):
                    if i < len(valid_symbols):
                        symbol_info = valid_symbols[i]
                        full_symbol = f"{symbol_info['exchange']}:{symbol_info['symbol']}"
                        
                        # Store bidirectional mappings
                        self.symbol_to_hsm[full_symbol] = hsm_token
                        self.hsm_to_symbol[hsm_token] = full_symbol
                        # Also store for token_to_symbol mapping
                        self.token_to_symbol[hsm_token] = (symbol_info['symbol'], symbol_info['exchange'])
                        
                        # Get brsymbol for logging
                        brsymbol = token_mappings.get(hsm_token, "N/A")
                        self.logger.info(f"✅ Mapped #{i+1}: {full_symbol} <-> {hsm_token}")
                        self.logger.debug(f"   Brsymbol: {brsymbol}")
                
                # Verify all active subscriptions have mappings
                unmapped_subs = []
                for full_symbol in self.active_subscriptions:
                    if full_symbol not in self.symbol_to_hsm:
                        unmapped_subs.append(full_symbol)
                        self.logger.warning(f"⚠️ Unmapped subscription: {full_symbol}")
                
                # If there are unmapped subscriptions and unused tokens, map them
                if unmapped_subs:
                    unused_tokens = [t for t in hsm_tokens if t not in self.hsm_to_symbol]
                    if unused_tokens:
                        self.logger.info(f"Attempting to map {len(unmapped_subs)} unmapped subscriptions...")
                        for i, full_symbol in enumerate(unmapped_subs):
                            if i < len(unused_tokens):
                                hsm_token = unused_tokens[i]
                                self.symbol_to_hsm[full_symbol] = hsm_token
                                self.hsm_to_symbol[hsm_token] = full_symbol
                                # Extract symbol and exchange for token_to_symbol
                                parts = full_symbol.split(':', 1)
                                if len(parts) == 2:
                                    self.token_to_symbol[hsm_token] = (parts[1], parts[0])
                                self.logger.info(f"✅ Recovery mapped: {full_symbol} <-> {hsm_token}")
                
                # Final verification
                self.logger.info(f"\n📊 Mapping Summary:")
                self.logger.info(f"   Active subscriptions: {len(self.active_subscriptions)}")
                self.logger.info(f"   HSM tokens generated: {len(hsm_tokens)}")
                self.logger.info(f"   Mappings created: {len(self.hsm_to_symbol)}")
                self.logger.info(f"   Forward mappings (symbol->hsm): {self.symbol_to_hsm}")
                self.logger.info(f"   Reverse mappings (hsm->symbol): {self.hsm_to_symbol}")
                
                self.logger.info(f"\nSubscribing to {len(hsm_tokens)} HSM tokens...")
                for token in hsm_tokens:
                    self.logger.info(f"  ➡️ {token}")
                
                # Subscribe to HSM WebSocket with all tokens at once
                self.ws_client.subscribe_symbols(hsm_tokens, token_mappings)
                
                self.logger.info(f"\n✅ Successfully sent subscription for {len(hsm_tokens)} HSM tokens")
                self.logger.info(f"Expected data for {len(self.active_subscriptions)} symbols")
                self.logger.info("="*60 + "\n")
                return True
                
        except Exception as e:
            self.logger.error(f"Subscription error: {e}")
            return False
    
    def subscribe_ltp(self, symbols: List[Dict[str, str]], callback: Callable):
        """Subscribe to LTP data (Original Fyers method)"""
        return self.subscribe_symbols(symbols, "SymbolUpdate", callback)
    
    def subscribe_quote(self, symbols: List[Dict[str, str]], callback: Callable):
        """Subscribe to Quote data (Original Fyers method)"""
        return self.subscribe_symbols(symbols, "SymbolUpdate", callback)
    
    def subscribe_depth(self, symbols: List[Dict[str, str]], callback: Callable):
        """Subscribe to Depth data (Original Fyers method)"""
        return self.subscribe_symbols(symbols, "DepthUpdate", callback)
    
    def unsubscribe_symbols(self, symbols: List[Dict[str, str]]):
        """
        Unsubscribe from symbols (Original Fyers method)
        Note: HSM protocol doesn't support individual unsubscription easily
        """
        self.logger.warning("HSM protocol doesn't support selective unsubscription")
        self.logger.info("To unsubscribe, disconnect and reconnect with new symbol list")

    def _shm_callback(self, data: Dict[str, Any]):
        """Internal callback for SHM publishing"""
        # This callback is used when subscribing through the SHM pattern
        # It publishes data to the SHM buffer
        self._publish_to_shm(data)
    
    def _on_open(self):
        """Handle WebSocket connection open"""
        self.logger.info("Fyers WebSocket connection opened")
        self.connected = True
        self._resubscribe_all()
    
    def _on_close(self):
        """Handle WebSocket connection close"""
        self.connected = False
        self.logger.info("Fyers WebSocket connection closed")
        if self.running:
            self._schedule_reconnection()
    
    def _on_error(self, error):
        """Handle WebSocket error"""
        self.logger.error(f"Fyers WebSocket error: {error}")

    def _resubscribe_all(self):
        """Resubscribe to all symbols after reconnection"""
        with self.lock:
            if not self.active_subscriptions:
                return
                
            self.logger.info(f"Resubscribing to {len(self.active_subscriptions)} symbols...")
            
            # Group symbols by data type
            symbol_updates = []
            depth_updates = []
            
            for full_symbol, sub_info in self.active_subscriptions.items():
                symbol_dict = {
                    "exchange": sub_info["exchange"],
                    "symbol": sub_info["symbol"]
                }
                
                if sub_info["data_type"] == "DepthUpdate":
                    depth_updates.append(symbol_dict)
                else:
                    symbol_updates.append(symbol_dict)
            
            # Resubscribe
            if symbol_updates:
                self.subscribe_symbols(symbol_updates, "SymbolUpdate", self._shm_callback)
            if depth_updates:
                self.subscribe_symbols(depth_updates, "DepthUpdate", self._shm_callback)

    def _schedule_reconnection(self):
        """Schedule reconnection attempt"""
        delay = min(5 * (2 ** self.reconnect_attempts), 60)
        self.reconnect_attempts += 1
        self.logger.info(f"Scheduling reconnection in {delay} seconds (attempt {self.reconnect_attempts})")
        threading.Timer(delay, self._attempt_reconnection).start()

    def _attempt_reconnection(self):
        """Attempt to reconnect to WebSocket"""
        try:
            if self.connected:
                return
            self.connect()
            self.logger.info("Reconnected successfully")
        except Exception as e:
            self.logger.error(f"Reconnection error: {e}")
    
    def _on_message(self, fyers_data: Dict[str, Any]):
        """
        Handle incoming market data from Fyers (Enhanced for SHM)
        
        Args:
            fyers_data: Raw data from Fyers HSM WebSocket
        """
        try:
            if not fyers_data:
                return
            
            # Find matching subscription using enhanced logic
            matched_subscription = self._find_matching_subscription_enhanced(fyers_data)
            if not matched_subscription:
                return
            
            # Determine data type and get appropriate callback
            fyers_type = fyers_data.get("type", "sf")
            update_type = fyers_data.get("update_type", "snapshot")
            
            if fyers_type == "dp":
                openalgo_data_type = "Depth"
            else:
                openalgo_data_type = "Quote"
            
            # Map to OpenAlgo format
            mapped_data = self.data_mapper.map_fyers_data(fyers_data, openalgo_data_type)
            if not mapped_data:
                return
            
            # Override symbol and exchange with subscription details
            mapped_data["symbol"] = matched_subscription['symbol']
            mapped_data["exchange"] = matched_subscription['exchange']
            mapped_data["update_type"] = update_type
            mapped_data["timestamp"] = int(time.time() * 1000)
            
            # Determine mode based on data type
            if openalgo_data_type == "Depth":
                mapped_data["mode"] = Config.MODE_DEPTH
            else:
                mapped_data["mode"] = Config.MODE_QUOTE
            
            # Deduplication check
            if self._is_duplicate_data(mapped_data):
                return
            
            # Update cache
            hsm_token = fyers_data.get('hsm_token')
            if hsm_token:
                self.market_cache.update(hsm_token, mapped_data)
            
            # Find appropriate callback
            full_symbol = f"{matched_subscription['exchange']}:{matched_subscription['symbol']}"
            data_type = matched_subscription.get('data_type', 'SymbolUpdate')
            callback_key = f"{data_type}_{full_symbol}"
            callback = self.subscription_callbacks.get(callback_key)
            
            if callback:
                # Call the original callback (for backward compatibility)
                callback(mapped_data)
            
            # Always publish to SHM for new pattern
            self._publish_to_shm(mapped_data)
            
            # Debug logging
            if openalgo_data_type == "Depth":
                depth = mapped_data.get('depth', {})
                buy_levels = depth.get('buy', [])
                sell_levels = depth.get('sell', [])
                bid1 = buy_levels[0]['price'] if buy_levels else 'N/A'
                ask1 = sell_levels[0]['price'] if sell_levels else 'N/A'
                self.logger.debug(f"🎉 {full_symbol} depth: Bid={bid1}, Ask={ask1}")
            else:
                self.logger.debug(f"🎉 {full_symbol} data: LTP={mapped_data.get('ltp', 0)}")
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            self.logger.debug(f"Raw data: {fyers_data}")

    def _find_matching_subscription_enhanced(self, fyers_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enhanced subscription matching using improved logic from new Fyers adapter"""
        # Try to match using HSM token first (most reliable)
        hsm_token = fyers_data.get('hsm_token')
        if hsm_token:
            # Use bidirectional mapping for fast lookup
            if hsm_token in self.hsm_to_symbol:
                full_symbol = self.hsm_to_symbol[hsm_token]
                if full_symbol in self.active_subscriptions:
                    matched_subscription = self.active_subscriptions[full_symbol]
                    self.logger.debug(f"✅ Matched by HSM token: {hsm_token} -> {full_symbol}")
                    return matched_subscription
            else:
                # Log missing mapping for debugging
                self.logger.debug(f"HSM token {hsm_token} not in mappings")
                self.logger.debug(f"Current HSM->Symbol mappings: {self.hsm_to_symbol}")
                # Try fallback matching
                for full_symbol, sub_info in self.active_subscriptions.items():
                    if full_symbol in self.symbol_to_hsm and self.symbol_to_hsm[full_symbol] == hsm_token:
                        matched_subscription = sub_info
                        # Update reverse mapping for future fast lookup
                        self.hsm_to_symbol[hsm_token] = full_symbol
                        self.logger.info(f"✅ Matched by HSM token (fallback): {hsm_token} -> {full_symbol}")
                        return matched_subscription

        # If no match by HSM token, try matching by original_symbol field
        if 'original_symbol' in fyers_data:
            original_symbol = fyers_data.get('original_symbol', '')
            # Try exact match
            if original_symbol in self.active_subscriptions:
                matched_subscription = self.active_subscriptions[original_symbol]
                self.logger.info(f"✅ Matched by original_symbol: {original_symbol}")
                return matched_subscription
            else:
                # Try to find a match in active subscriptions
                for full_symbol, sub_info in self.active_subscriptions.items():
                    # Check for NFO futures match
                    if sub_info['exchange'] == 'NFO' and 'NIFTY' in original_symbol and 'FUT' in original_symbol:
                        if 'NIFTY' in sub_info['symbol'] and 'FUT' in sub_info['symbol']:
                            matched_subscription = sub_info
                            self.logger.info(f"✅ Matched NFO future by pattern: {original_symbol} -> {full_symbol}")
                            # Update the mapping for future use
                            if hsm_token and hsm_token not in self.hsm_to_symbol:
                                self.hsm_to_symbol[hsm_token] = full_symbol
                                self.symbol_to_hsm[full_symbol] = hsm_token
                                self.token_to_symbol[hsm_token] = (sub_info['symbol'], sub_info['exchange'])
                            return matched_subscription

        # If no match by token, fall back to symbol matching from fyers data
        fyers_symbol = fyers_data.get('symbol', '')
        if fyers_symbol:
            # Try exact match first
            for full_symbol, sub_info in self.active_subscriptions.items():
                # Check various matching patterns
                if sub_info['symbol'] in fyers_symbol or fyers_symbol.endswith(sub_info['symbol']):
                    matched_subscription = sub_info
                    self.logger.info(f"✅ Matched by symbol name: {fyers_symbol} -> {full_symbol}")
                    # Update the mapping for future use
                    if hsm_token and hsm_token not in self.hsm_to_symbol:
                        self.hsm_to_symbol[hsm_token] = full_symbol
                        self.symbol_to_hsm[full_symbol] = hsm_token
                        self.token_to_symbol[hsm_token] = (sub_info['symbol'], sub_info['exchange'])
                    return matched_subscription
                # Special case for NFO futures
                elif sub_info['exchange'] == 'NFO' and 'FUT' in sub_info['symbol']:
                    # Extract core symbol from both
                    fyers_core = fyers_symbol.replace('-EQ', '').split('FUT')[0] if 'FUT' in fyers_symbol else ''
                    sub_core = sub_info['symbol'].split('FUT')[0] if 'FUT' in sub_info['symbol'] else ''
                    if fyers_core and sub_core and fyers_core in sub_core:
                        matched_subscription = sub_info
                        self.logger.info(f"✅ Matched NFO by core symbol: {fyers_symbol} -> {full_symbol}")
                        # Update the mapping for future use
                        if hsm_token and hsm_token not in self.hsm_to_symbol:
                            self.hsm_to_symbol[hsm_token] = full_symbol
                            self.symbol_to_hsm[full_symbol] = hsm_token
                            self.token_to_symbol[hsm_token] = (sub_info['symbol'], sub_info['exchange'])
                        return matched_subscription
            
            # If still no match and only one subscription, use it
            if len(self.active_subscriptions) == 1:
                for full_symbol, sub_info in self.active_subscriptions.items():
                    matched_subscription = sub_info
                    self.logger.info(f"✅ Single subscription match: {full_symbol}")
                    return matched_subscription

        # Final check - if still no match, log detailed debug info and return None
        self.logger.warning(f"❌ No HSM token match for data. HSM token: {hsm_token}")
        self.logger.debug(f"   HSM to Symbol mappings: {self.hsm_to_symbol}")
        self.logger.debug(f"   Symbol to HSM mappings: {self.symbol_to_hsm}")
        self.logger.debug(f"   Active subscriptions: {list(self.active_subscriptions.keys())}")
        self.logger.debug(f"   Fyers symbol: {fyers_data.get('symbol', 'N/A')}")
        self.logger.debug(f"   Original symbol: {fyers_data.get('original_symbol', 'N/A')}")
        return None

    def _is_duplicate_data(self, mapped_data: Dict[str, Any]) -> bool:
        """Check if data is duplicate"""
        symbol_key = f"{mapped_data.get('exchange')}:{mapped_data.get('symbol')}"
        current_ltp = mapped_data.get('ltp', 0)
        current_time = mapped_data.get('timestamp', 0)
        
        if symbol_key in self.last_data:
            last_ltp = self.last_data[symbol_key].get('ltp', 0)
            last_time = self.last_data[symbol_key].get('timestamp', 0)
            
            # Skip if same LTP within 100ms
            if (current_ltp == last_ltp and 
                abs(current_time - last_time) < 100):
                return True
        
        # Update last data
        self.last_data[symbol_key] = {
            'ltp': current_ltp,
            'timestamp': current_time
        }
        
        return False

    def _publish_to_shm(self, data: Dict[str, Any]) -> None:
        """
        Publish market data to SHM buffer
        
        Args:
            data: Normalized market data
        """
        if self.ring_buffer is None:
            self.logger.debug("Ring buffer not available; drop message")
            return
        
        try:
            symbol = data.get('symbol', '')
            exchange = data.get('exchange', 'NSE')
            mode = data.get('mode', Config.MODE_QUOTE)
            
            # Create topic
            mode_str = {Config.MODE_LTP: 'LTP', Config.MODE_QUOTE: 'QUOTE', Config.MODE_DEPTH: 'DEPTH'}[mode]
            topic = f"{exchange}_{symbol}_{mode_str}"
            
            self.logger.debug(f"[APP->SHM] topic={topic} symbol={symbol} payload={json.dumps(data)[:200]}...")
            
            # For depth data, preserve full depth structure
            if mode == Config.MODE_DEPTH:
                ltp_value = data.get('ltp', 0) or data.get('price', 0)
                price_float = float(ltp_value) if ltp_value else 0.0
                
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
                # For LTP/Quote modes, use binary format that preserves OHLC
                self.logger.debug(f"Publishing OHLC data for {symbol}: O={data.get('open', 0)}, H={data.get('high', 0)}, L={data.get('low', 0)}, C={data.get('close', 0)}")
                
                binary_msg = BinaryMarketData.from_normalized_data(data, exchange)
                
                self.logger.debug(f"Binary message created - price: {binary_msg.get_price_float()}, symbol: {binary_msg.get_symbol_string()}, open: {binary_msg.get_open_price_float()}")
                
                published = self.ring_buffer.publish_single(binary_msg)
                if not published:
                    self.logger.debug(f"[APP->SHM] publish failed for symbol={symbol} (buffer full)")
                else:
                    self.logger.debug(f"[APP->SHM] published symbol={symbol}")
                
        except Exception as e:
            self.logger.error(f"Error publishing market data for {symbol}: {e}")
            self.logger.debug(f"Failed data: {data}")
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get connection status information (Original Fyers method)
        
        Returns:
            Dict with connection status details
        """
        return {
            "connected": self.connected,
            "authenticated": self.ws_client.is_connected() if self.ws_client else False,
            "active_subscriptions": len(self.active_subscriptions),
            "websocket_url": FyersHSMWebSocket.HSM_URL,
            "protocol": "HSM Binary",
            "user_id": self.user_id or self.userid
        }
    
    def get_subscriptions(self) -> Dict[str, Any]:
        """
        Get current subscriptions (Original Fyers method)
        
        Returns:
            Dict with subscription details
        """
        return {
            "total_subscriptions": len(self.active_subscriptions),
            "subscriptions": dict(self.active_subscriptions),
            "hsm_mappings": dict(self.symbol_to_hsm)
        }
    
    def is_connected(self) -> bool:
        """Check if adapter is connected and ready (Original Fyers method)"""
        return self.connected and (self.ws_client.is_connected() if self.ws_client else False)


# Alias for backward compatibility
FyersAdapter = FyersSHMWebSocketAdapter