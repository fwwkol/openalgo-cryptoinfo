"""
Complete Optimized Upstox SHM WebSocket Adapter - Low Latency Version

Key optimizations:
1. Removed caching for reduced complexity
2. Increased ThreadPoolExecutor workers
3. Direct async message processing
4. Simplified data normalization
5. Batch processing capabilities
"""

import threading
import json
import logging
import time
import asyncio
from typing import Dict, Any, Optional, List
import os
from concurrent.futures import ThreadPoolExecutor
from database.auth_db import get_auth_token
from websocket_proxy.optimized_ring_buffer import OptimizedRingBuffer
from websocket_proxy.mapping import SymbolMapper
from websocket_proxy.binary_market_data import BinaryMarketData
from websocket_proxy.market_data import MarketDataMessage
from .upstox_mapping import UpstoxExchangeMapper
from .upstox_client import UpstoxWebSocketClient

class Config:
    # Upstox subscription modes
    MODE_LTP = 1
    MODE_QUOTE = 2  # Maps to 'full' mode
    MODE_DEPTH = 3  # Maps to 'full' mode with depth
    MODE_OPTION_GREEKS = 4
    MODE_FULL_D30 = 5
    
    # Mode mapping to Upstox strings
    MODE_MAPPING = {
        MODE_LTP: 'ltpc',
        MODE_QUOTE: 'full',
        MODE_DEPTH: 'full',
        MODE_OPTION_GREEKS: 'option_greeks',
        MODE_FULL_D30: 'full_d30'
    }

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

class UpstoxSHMWebSocketAdapter:
    """Optimized Upstox adapter with removed caching and improved performance."""
    
    def __init__(self, shm_buffer_name: str = None):
        self.logger = logging.getLogger("upstox_adapter_shm_optimized")
        self.user_id: Optional[str] = None
        self.broker_name = "upstox"
        self.ws_client: Optional[UpstoxWebSocketClient] = None
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        self.instrument_key_to_symbol: Dict[str, tuple] = {}
        self.running = False
        self.connected = False
        self.lock = threading.Lock()
        self.reconnect_attempts = 0
        
        # Event loop and thread management - INCREASED WORKERS
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.loop_thread: Optional[threading.Thread] = None
        self.executor = ThreadPoolExecutor(max_workers=8)  # Increased from 2 to 8
        
        # SHM ring buffer
        buffer_name = shm_buffer_name or os.getenv('SHM_BUFFER_NAME', 'ws_proxy_buffer')
        try:
            self.ring_buffer = OptimizedRingBuffer(name=buffer_name, create=False)
            self.logger.info(f"OptimizedRingBuffer initialized with buffer '{buffer_name}'")
        except Exception as e:
            self.logger.error(f"Failed to initialize OptimizedRingBuffer: {e}")
            self.ring_buffer = None

        # Performance monitoring
        self.message_count = 0
        self.last_perf_log = time.time()

    def initialize(self, broker_name: str, user_id: str, auth_data: Optional[Dict[str, str]] = None) -> None:
        """Initialize the adapter with broker credentials"""
        self.user_id = user_id
        self.broker_name = broker_name
        
        # Get auth token from database
        auth_token = get_auth_token(user_id)
        if not auth_token:
            raise ValueError(f"Missing Upstox auth token for user {user_id}")
        
        # Initialize WebSocket client
        self.ws_client = UpstoxWebSocketClient(auth_token=auth_token)
        
        # Setup callbacks - DIRECT ASYNC PROCESSING
        self.ws_client.callbacks['on_connect'] = self._on_connect_direct
        self.ws_client.callbacks['on_message'] = self._on_message_direct  # Direct processing
        self.ws_client.callbacks['on_error'] = self._on_error_direct
        self.ws_client.callbacks['on_close'] = self._on_close_direct
        
        self.running = True

    def connect(self) -> None:
        """Connect to Upstox WebSocket - synchronous interface"""
        if not self.ws_client:
            self.logger.error("WebSocket client not initialized. Call initialize() first.")
            return
        
        self.logger.info("Connecting to Upstox WebSocket...")
        
        # Start event loop in separate thread
        self._start_event_loop()
        
        # Connect to WebSocket in event loop
        future = asyncio.run_coroutine_threadsafe(self.ws_client.connect(), self.loop)
        
        try:
            success = future.result(timeout=15.0)  # 15 second timeout
            if success:
                self.connected = True
                self.reconnect_attempts = 0
                self.logger.info("Connected to Upstox WebSocket successfully")
            else:
                raise ConnectionError("Failed to connect to Upstox WebSocket")
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            raise ConnectionError(f"Failed to connect to Upstox WebSocket: {e}")

    def disconnect(self) -> None:
        """Disconnect from Upstox WebSocket - synchronous interface"""
        self.running = False
        
        if self.ws_client and self.loop:
            future = asyncio.run_coroutine_threadsafe(self.ws_client.disconnect(), self.loop)
            try:
                future.result(timeout=5.0)
            except Exception as e:
                self.logger.error(f"Error during disconnect: {e}")
        
        self._stop_event_loop()
        self.connected = False
        self.logger.info("Disconnected from Upstox WebSocket (SHM mode)")

    def _start_event_loop(self) -> None:
        """Start asyncio event loop in separate thread"""
        def run_loop():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            try:
                self.loop.run_forever()
            finally:
                self.loop.close()
        
        self.loop_thread = threading.Thread(target=run_loop, daemon=True)
        self.loop_thread.start()
        
        # Wait for loop to start
        timeout = 5.0
        start_time = time.time()
        while self.loop is None and (time.time() - start_time) < timeout:
            time.sleep(0.01)
        
        if self.loop is None:
            raise RuntimeError("Failed to start event loop")

    def _stop_event_loop(self) -> None:
        """Stop the event loop thread"""
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
        
        if self.loop_thread and self.loop_thread.is_alive():
            self.loop_thread.join(timeout=5.0)

    def subscribe(self, symbol: str, exchange: str, mode: int = Config.MODE_QUOTE, depth_level: int = 5) -> Dict[str, Any]:
        """Subscribe to market data for a symbol"""
        if not (symbol and exchange and mode in [Config.MODE_LTP, Config.MODE_QUOTE, Config.MODE_DEPTH, Config.MODE_OPTION_GREEKS, Config.MODE_FULL_D30]):
            return {"status": "error", "code": "INVALID_PARAMS", "message": "Invalid subscription parameters"}
        
        # Get token info from symbol mapper
        token_info = SymbolMapper.get_token_from_symbol(symbol, exchange)
        if not token_info:
            return {"status": "error", "code": "SYMBOL_NOT_FOUND", "message": f"Symbol {symbol} not found"}
        
        token = token_info['token']
        brexchange = token_info['brexchange']
        upstox_exchange = UpstoxExchangeMapper.get_exchange_type(brexchange)
        
        # Extract symbol ID
        symbol_id = token.split('|')[-1] if '|' in token else token
        instrument_key = f"{upstox_exchange}|{symbol_id}"
        
        subscription = {
            'symbol': symbol,
            'exchange': exchange,
            'mode': mode,
            'depth_level': depth_level,
            'token': token,
            'instrument_key': instrument_key,
            'upstox_mode': Config.MODE_MAPPING[mode]
        }
        
        correlation_id = f"{symbol}_{exchange}_{mode}"
        
        with self.lock:
            self.subscriptions[correlation_id] = subscription
            self.instrument_key_to_symbol[instrument_key] = (symbol, exchange)
        
        if self.connected and self.loop:
            asyncio.run_coroutine_threadsafe(
                self._websocket_subscribe(subscription), 
                self.loop
            )
        
        return {
            "status": "success", 
            "message": f"Subscribed to {symbol}.{exchange}", 
            "symbol": symbol, 
            "exchange": exchange, 
            "mode": mode
        }

    def unsubscribe(self, symbol: str, exchange: str, mode: int = Config.MODE_QUOTE) -> Dict[str, Any]:
        """Unsubscribe from symbol with enhanced error handling and state validation."""
        if not symbol or not exchange:
            self.logger.warning(f"Invalid unsubscribe parameters: symbol='{symbol}', exchange='{exchange}'")
            return {"status": "error", "code": "INVALID_PARAMS", "message": "Symbol and exchange are required"}
        
        if mode not in [Config.MODE_LTP, Config.MODE_QUOTE, Config.MODE_DEPTH, Config.MODE_OPTION_GREEKS, Config.MODE_FULL_D30]:
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
            
            subscription = self.subscriptions[correlation_id]
            instrument_key = subscription.get('instrument_key')
            
            # Remove this specific subscription
            del self.subscriptions[correlation_id]
            
            # Check if there are still other subscriptions for the same instrument key
            remaining_subscriptions = [
                sub for sub in self.subscriptions.values() 
                if sub.get('instrument_key') == instrument_key
            ]
            
            try:
                # Only unsubscribe from WebSocket if no more subscriptions for this instrument
                if not remaining_subscriptions:
                    self.logger.debug(f"No more subscriptions for {instrument_key}, unsubscribing from WebSocket")
                    if self.connected and self.loop:
                        asyncio.run_coroutine_threadsafe(
                            self._websocket_unsubscribe(subscription), 
                            self.loop
                        )
                    
                    # Remove instrument key mapping only if no more subscriptions
                    self.instrument_key_to_symbol.pop(instrument_key, None)
                    self.logger.debug(f"Cleaned up instrument key mapping for {instrument_key}")
                else:
                    self.logger.debug(f"Keeping instrument key {instrument_key} - {len(remaining_subscriptions)} subscriptions remain")
                
            except Exception as e:
                self.logger.warning(f"Error during websocket unsubscribe for {symbol}.{exchange}: {e}")
        
        self.logger.info(f"Successfully unsubscribed from {symbol}.{exchange} mode={mode}")
        return {
            "status": "success",
            "message": f"Unsubscribed from {symbol}.{exchange}",
            "symbol": symbol,
            "exchange": exchange,
            "mode": mode
        }

    # DIRECT ASYNC CALLBACKS - NO THREAD POOL QUEUING
    async def _on_connect_direct(self) -> None:
        """Direct async connect handler - no queuing"""
        self.logger.info("Connected to Upstox WebSocket")
        self.connected = True
        await self._resubscribe_all()

    async def _on_error_direct(self, error_message: str) -> None:
        """Direct async error handler"""
        self.logger.error(f"Upstox WebSocket error: {error_message}")

    async def _on_close_direct(self) -> None:
        """Direct async close handler"""
        self.logger.info("Upstox WebSocket closed")
        self.connected = False
        if self.running:
            await self._schedule_reconnection()

    async def _on_message_direct(self, data: Dict[str, Any]) -> None:
        """DIRECT async message processing - major performance improvement"""
        # Performance monitoring
        self.message_count += 1
        current_time = time.time()
        if current_time - self.last_perf_log > 10:
            msg_per_sec = self.message_count / (current_time - self.last_perf_log)
            self.logger.info(f"Processing {msg_per_sec:.1f} messages/sec")
            self.message_count = 0
            self.last_perf_log = current_time

        try:
            msg_type = data.get('type')
            if msg_type == 'market_info':
                self.logger.info(f"Market info received: {data}")
                return

            # ✅ Check for feeds directly instead of message type
            feeds = data.get('feeds')
            if feeds:
                current_ts = data.get('currentTs')
                # Add debug logging to confirm processing
                self.logger.debug(f"Processing {len(feeds)} feeds")
                await self._process_batch_feeds(feeds, current_ts)
            else:
                # Log unhandled message types for debugging
                self.logger.debug(f"Unhandled message: {data}")
                
        except Exception as e:
            self.logger.error(f"Message processing error: {e}")


    async def _process_batch_feeds(self, feeds: Dict[str, Any], current_ts: str) -> None:
        """Process multiple feeds efficiently in batch"""
        batch_operations = []
        
        for instrument_key, feed_data in feeds.items():
            if instrument_key in self.instrument_key_to_symbol:
                symbol, exchange = self.instrument_key_to_symbol[instrument_key]
                matching_subscriptions = self._find_matching_subscriptions(instrument_key)
                
                for subscription in matching_subscriptions:
                    # Prepare batch operation
                    batch_operations.append({
                        'feed_data': feed_data,
                        'subscription': subscription,
                        'symbol': symbol,
                        'exchange': exchange,
                        'current_ts': current_ts,
                        'instrument_key': instrument_key
                    })
        
        # Process batch operations
        for op in batch_operations:
            await self._process_single_feed_async(
                op['feed_data'], op['subscription'], 
                op['symbol'], op['exchange'], 
                op['current_ts']
            )

    async def _process_single_feed_async(self, feed_data: Dict[str, Any], subscription: Dict[str, Any], 
                                       symbol: str, exchange: str, current_ts: str) -> None:
        """Process single feed asynchronously - NO CACHING"""
        mode = subscription['mode']
        
        # DIRECT NORMALIZATION - NO CACHE LAYER
        normalized = self._normalize_market_data_direct(feed_data, mode)
        normalized.update({
            'symbol': symbol,
            'exchange': exchange,
            'timestamp': int(time.time() * 1000) if not current_ts else int(current_ts)
        })
        
        # Create topic
        mode_name = {
            Config.MODE_LTP: 'LTP',
            Config.MODE_QUOTE: 'QUOTE', 
            Config.MODE_DEPTH: 'DEPTH',
            Config.MODE_OPTION_GREEKS: 'GREEKS',
            Config.MODE_FULL_D30: 'FULL_D30'
        }[mode]
        
        topic = f"{exchange}_{symbol}_{mode_name}"
        
        # Direct publish - no additional queueing
        self._publish_market_data_direct(topic, normalized)

    def _normalize_market_data_direct(self, data: Dict[str, Any], mode: int) -> Dict[str, Any]:
        """SIMPLIFIED normalization - direct processing without caching"""
        # Check if this is fullFeed data
        full_feed = data.get('fullFeed', {})
        if full_feed:
            # ✅ Handle both marketFF (equities) and indexFF (indices)
            market_ff = full_feed.get('marketFF') or full_feed.get('indexFF', {})
            ltpc = market_ff.get('ltpc', {})

            # Base LTP data
            base_data = {
                'mode': mode,
                'ltp': safe_float(ltpc.get('ltp')),
                'close_price': safe_float(ltpc.get('cp')),
                'last_trade_time': ltpc.get('ltt'),
                'last_trade_quantity': safe_int(ltpc.get('ltq')),
            }

            if mode == Config.MODE_LTP:
                return base_data

            elif mode in [Config.MODE_QUOTE, Config.MODE_DEPTH]:
                # Extract OHLC data efficiently
                market_ohlc = market_ff.get('marketOHLC', {})
                ohlc_data = market_ohlc.get('ohlc', [])

                # Find daily OHLC quickly
                daily_ohlc = {}
                for ohlc in ohlc_data:
                    if ohlc.get('interval') == '1d':
                        daily_ohlc = ohlc
                        break

                # Enhanced data with OHLC
                enhanced_data = base_data.copy()
                enhanced_data.update({
                    'open': safe_float(daily_ohlc.get('open')),
                    'high': safe_float(daily_ohlc.get('high')),
                    'low': safe_float(daily_ohlc.get('low')),
                    'close': safe_float(daily_ohlc.get('close')),
                    'volume': safe_int(daily_ohlc.get('vol')),
                    'average_price': safe_float(market_ff.get('atp')),
                    'volume_traded_today': safe_int(market_ff.get('vtt')),
                    'open_interest': safe_int(market_ff.get('oi')),
                    'implied_volatility': safe_float(market_ff.get('iv')),
                    'total_buy_quantity': safe_int(market_ff.get('tbq')),
                    'total_sell_quantity': safe_int(market_ff.get('tsq')),
                })

                # Add option Greeks if available
                option_greeks = market_ff.get('optionGreeks', {})
                if option_greeks:
                    enhanced_data.update({
                        'delta': safe_float(option_greeks.get('delta')),
                        'theta': safe_float(option_greeks.get('theta')),
                        'gamma': safe_float(option_greeks.get('gamma')),
                        'vega': safe_float(option_greeks.get('vega')),
                        'rho': safe_float(option_greeks.get('rho')),
                    })

                # Add market depth for DEPTH mode
                if mode == Config.MODE_DEPTH:
                    market_level = market_ff.get('marketLevel', {})
                    bid_ask_quotes = market_level.get('bidAskQuote', [])
                    
                    depth_data = {
                        'buy': [{'price': safe_float(q.get('bidP')), 'quantity': safe_int(q.get('bidQ')), 'orders': 1} for q in bid_ask_quotes],
                        'sell': [{'price': safe_float(q.get('askP')), 'quantity': safe_int(q.get('askQ')), 'orders': 1} for q in bid_ask_quotes]
                    }
                    
                    enhanced_data['depth'] = depth_data
                    enhanced_data['depth_level'] = len(bid_ask_quotes)

                return enhanced_data

        else:
            # Handle direct LTPC data
            ltpc = data.get('ltpc', {})
            return {
                'mode': mode,
                'ltp': safe_float(ltpc.get('ltp')),
                'close_price': safe_float(ltpc.get('cp')),
                'last_trade_time': ltpc.get('ltt'),
                'last_trade_quantity': safe_int(ltpc.get('ltq')),
            }


    def _publish_market_data_direct(self, topic: str, data: Dict[str, Any]) -> None:
        """Direct publish without additional processing overhead"""
        if self.ring_buffer is None:
            return
        
        try:
            mode = data.get('mode', 1)
            
            if mode == Config.MODE_DEPTH:
                # Preserve full depth structure
                enriched_data = data.copy()
                ltp_value = data.get('ltp', 0) or data.get('price', 0)
                enriched_data['price'] = float(ltp_value) if ltp_value else 0.0
                
                self.ring_buffer.publish_single(enriched_data)
            else:
                # Use binary format for LTP/Quote modes
                binary_msg = BinaryMarketData.from_normalized_data(data, data.get('exchange', 'NSE'))
                self.ring_buffer.publish_single(binary_msg)
                    
        except Exception as e:
            self.logger.error(f"Error publishing market data for {topic}: {e}")

    async def _websocket_subscribe(self, subscription: Dict) -> None:
        """Subscribe via WebSocket"""
        instrument_key = subscription['instrument_key']
        upstox_mode = subscription['upstox_mode']
        
        if self.ws_client and self.connected:
            success = await self.ws_client.subscribe([instrument_key], upstox_mode)
            if success:
                self.logger.debug(f"Successfully subscribed to {instrument_key}")
            else:
                self.logger.error(f"Failed to subscribe to {instrument_key}")

    async def _websocket_unsubscribe(self, subscription: Dict) -> bool:
        """Unsubscribe from websocket"""
        instrument_key = subscription.get('instrument_key')
        
        if not instrument_key:
            self.logger.warning(f"Invalid subscription data for websocket unsubscribe: {subscription}")
            return False
        
        self.logger.debug(f"[BROKER<-APP] unsubscribe: instrument_key={instrument_key}")
        
        try:
            if self.ws_client and self.connected:
                success = await self.ws_client.unsubscribe([instrument_key])
                if not success:
                    self.logger.warning(f"Failed to send unsubscribe for {instrument_key}")
                    return False
                return True
            else:
                self.logger.warning(f"WebSocket client not available for unsubscribe: {instrument_key}")
                return False
        except Exception as e:
            self.logger.error(f"Error in websocket unsubscribe for {instrument_key}: {e}")
            return False

    async def _resubscribe_all(self):
        """Resubscribe to all instruments after connection"""
        with self.lock:
            # Group subscriptions by instrument key
            instrument_subscriptions = {}
            
            for sub in self.subscriptions.values():
                instrument_key = sub['instrument_key']
                if instrument_key not in instrument_subscriptions:
                    instrument_subscriptions[instrument_key] = set()
                instrument_subscriptions[instrument_key].add(sub['upstox_mode'])
            
            # Subscribe for each instrument
            for instrument_key, modes in instrument_subscriptions.items():
                # Choose most comprehensive mode
                if 'full' in modes or 'full_d30' in modes:
                    subscribe_mode = 'full' if 'full' in modes else 'full_d30'
                elif 'option_greeks' in modes:
                    subscribe_mode = 'option_greeks'
                else:
                    subscribe_mode = 'ltpc'
                
                success = await self.ws_client.subscribe([instrument_key], subscribe_mode)
                if success:
                    self.logger.info(f"Resubscribed to {instrument_key} in {subscribe_mode} mode")
                else:
                    self.logger.error(f"Failed to resubscribe to {instrument_key}")

    async def _schedule_reconnection(self):
        """Schedule reconnection with exponential backoff"""
        delay = min(5 * (2 ** self.reconnect_attempts), 60)
        self.reconnect_attempts += 1
        
        self.logger.info(f"Scheduling reconnection in {delay} seconds (attempt {self.reconnect_attempts})")
        await asyncio.sleep(delay)
        await self._attempt_reconnection()

    async def _attempt_reconnection(self):
        """Attempt to reconnect to WebSocket"""
        try:
            # Reinitialize WebSocket client
            auth_token = get_auth_token(self.user_id)
            if auth_token:
                self.ws_client = UpstoxWebSocketClient(auth_token=auth_token)
                
                # Setup callbacks
                self.ws_client.callbacks['on_connect'] = self._on_connect_direct
                self.ws_client.callbacks['on_message'] = self._on_message_direct
                self.ws_client.callbacks['on_error'] = self._on_error_direct
                self.ws_client.callbacks['on_close'] = self._on_close_direct
                
                success = await self.ws_client.connect()
                if success:
                    self.connected = True
                    self.reconnect_attempts = 0
                    self.logger.info("Reconnected successfully")
                else:
                    self.logger.error("Reconnection failed")
            else:
                self.logger.error("No auth token available for reconnection")
                
        except Exception as e:
            self.logger.error(f"Reconnection error: {e}")

    def _find_matching_subscriptions(self, instrument_key: str) -> List[Dict]:
        """Find subscriptions matching the instrument key - optimized with no lock for read"""
        return [s for s in self.subscriptions.values() if s['instrument_key'] == instrument_key]

    def get_subscriptions(self) -> Dict[str, Any]:
        """Get current subscriptions status"""
        with self.lock:
            return {
                "count": len(self.subscriptions),
                "subscriptions": list(self.subscriptions.keys()),
                "connected": self.connected,
                "running": self.running
            }

    def is_connected(self) -> bool:
        """Check if adapter is connected"""
        return self.connected

    def is_running(self) -> bool:
        """Check if adapter is running"""
        return self.running