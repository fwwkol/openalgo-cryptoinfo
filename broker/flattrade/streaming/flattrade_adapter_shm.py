"""
Flattrade SHM WebSocket Adapter for OpenAlgo
Publishes market data directly to the Shared Memory ring buffer (no ZeroMQ).
"""
import threading
import json
import logging
import time
from typing import Dict, Any, Optional, List
import os

from database.auth_db import get_auth_token
from websocket_proxy.shm_publisher import SharedMemoryPublisher
from websocket_proxy.mapping import SymbolMapper
from .flattrade_mapping import FlattradeExchangeMapper
from .flattrade_websocket import FlattradeWebSocket


class Config:
    MODE_LTP = 1
    MODE_QUOTE = 2
    MODE_DEPTH = 3

    MSG_AUTH = 'ck'
    MSG_TOUCHLINE_FULL = 'tf'
    MSG_TOUCHLINE_PARTIAL = 'tk'
    MSG_DEPTH_FULL = 'df'
    MSG_DEPTH_PARTIAL = 'dk'


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


class FlattradeSHMWebSocketAdapter:
    """Flattrade adapter that publishes to SHM ring buffer only."""

    def __init__(self, shm_buffer_name: str = None):
        self.logger = logging.getLogger("flattrade_websocket_shm")
        self.user_id: Optional[str] = None
        self.broker_name = "flattrade"
        self.ws_client: Optional[FlattradeWebSocket] = None
        self.market_cache = MarketDataCache()
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        self.token_to_symbol: Dict[str, tuple] = {}
        self.ws_subscription_refs: Dict[str, Dict[str, int]] = {}
        self.running = False
        self.connected = False
        self.lock = threading.Lock()
        self.reconnect_attempts = 0

        # SHM publisher
        buffer_name = shm_buffer_name or os.getenv('SHM_BUFFER_NAME', 'ws_proxy_buffer')
        try:
            self.shm_publisher = SharedMemoryPublisher(buffer_name=buffer_name)
            self.logger.info(f"SharedMemoryPublisher initialized with buffer '{buffer_name}'")
        except Exception as e:
            self.logger.error(f"Failed to initialize SharedMemoryPublisher: {e}")
            self.shm_publisher = None

    def initialize(self, broker_name: str, user_id: str, auth_data: Optional[Dict[str, str]] = None) -> None:
        self.user_id = user_id
        self.broker_name = broker_name

        api_key = os.getenv('BROKER_API_KEY', '')
        if ':::' in api_key:
            self.actid = api_key.split(':::')[0]
        else:
            self.actid = user_id

        self.susertoken = get_auth_token(user_id)
        if not self.actid or not self.susertoken:
            raise ValueError(f"Missing Flattrade credentials for user {user_id}")

        self.ws_client = FlattradeWebSocket(
            user_id=self.actid,
            actid=self.actid,
            susertoken=self.susertoken,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open,
        )
        self.running = True

    def connect(self) -> None:
        if not self.ws_client:
            self.logger.error("WebSocket client not initialized. Call initialize() first.")
            return
        self.logger.info("Connecting to Flattrade WebSocket...")
        if self.ws_client.connect():
            self.connected = True
            self.reconnect_attempts = 0
            self.logger.info("Connected to Flattrade WebSocket successfully")
        else:
            raise ConnectionError("Failed to connect to Flattrade WebSocket")

    def disconnect(self) -> None:
        self.running = False
        if self.ws_client:
            self.ws_client.stop()
        # No ZMQ to clean
        self.connected = False
        self.logger.info("Disconnected from Flattrade WebSocket (SHM mode)")

    def subscribe(self, symbol: str, exchange: str, mode: int = Config.MODE_QUOTE, depth_level: int = 5) -> Dict[str, Any]:
        if not (symbol and exchange and mode in [Config.MODE_LTP, Config.MODE_QUOTE, Config.MODE_DEPTH]):
            return {"status": "error", "code": "INVALID_PARAMS", "message": "Invalid subscription parameters"}

        token_info = SymbolMapper.get_token_from_symbol(symbol, exchange)
        if not token_info:
            return {"status": "error", "code": "SYMBOL_NOT_FOUND", "message": f"Symbol {symbol} not found"}

        token = token_info['token']
        brexchange = token_info['brexchange']
        flattrade_exchange = FlattradeExchangeMapper.to_flattrade_exchange(brexchange)
        scrip = f"{flattrade_exchange}|{token}"

        subscription = {
            'symbol': symbol,
            'exchange': exchange,
            'mode': mode,
            'depth_level': depth_level,
            'token': token,
            'scrip': scrip,
        }
        correlation_id = f"{symbol}_{exchange}_{mode}"

        with self.lock:
            self.subscriptions[correlation_id] = subscription
            self.token_to_symbol[token] = (symbol, exchange)

        if self.connected:
            self._websocket_subscribe(subscription)

        return {"status": "success", "message": f"Subscribed to {symbol}.{exchange}", "symbol": symbol, "exchange": exchange, "mode": mode}

    def unsubscribe(self, symbol: str, exchange: str, mode: int = Config.MODE_QUOTE) -> Dict[str, Any]:
        """
        Unsubscribe from symbol with enhanced error handling and state validation.
        
        Args:
            symbol: Symbol to unsubscribe from
            exchange: Exchange for the symbol
            mode: Subscription mode (LTP, QUOTE, DEPTH)
            
        Returns:
            Dict containing status and message
        """
        # Validate input parameters
        if not symbol or not exchange:
            self.logger.warning(f"Invalid unsubscribe parameters: symbol='{symbol}', exchange='{exchange}'")
            return {"status": "error", "code": "INVALID_PARAMS", "message": "Symbol and exchange are required"}
        
        if mode not in [Config.MODE_LTP, Config.MODE_QUOTE, Config.MODE_DEPTH]:
            self.logger.warning(f"Invalid mode {mode} for unsubscribe {symbol}.{exchange}")
            return {"status": "error", "code": "INVALID_MODE", "message": f"Invalid mode: {mode}"}
        
        correlation_id = f"{symbol}_{exchange}_{mode}"
        
        with self.lock:
            # Check if subscription exists
            if correlation_id not in self.subscriptions:
                # This is a recoverable state mismatch - log as warning, not error
                self.logger.warning(f"Unsubscribe requested for {symbol}.{exchange} mode={mode} but not found in subscriptions. "
                                  f"Available subscriptions: {list(self.subscriptions.keys())}")
                
                # Still attempt to clean up any orphaned websocket subscriptions
                self._cleanup_orphaned_subscription(symbol, exchange, mode)
                
                return {
                    "status": "success", 
                    "message": f"Already unsubscribed from {symbol}.{exchange}", 
                    "symbol": symbol, 
                    "exchange": exchange, 
                    "mode": mode
                }
            
            subscription = self.subscriptions[correlation_id]
            
            # Attempt websocket unsubscribe with error handling
            try:
                self.logger.debug(f"Attempting websocket unsubscribe for {symbol}.{exchange} mode={mode}")
                unsubscribe_success = self._websocket_unsubscribe(subscription)
                if not unsubscribe_success:
                    self.logger.warning(f"WebSocket unsubscribe failed for {symbol}.{exchange} mode={mode}, but continuing with cleanup")
                else:
                    self.logger.debug(f"WebSocket unsubscribe successful for {symbol}.{exchange} mode={mode}")
            except Exception as e:
                self.logger.warning(f"Error during websocket unsubscribe for {symbol}.{exchange}: {e}, continuing with cleanup")
            
            # Always clean up local subscription state regardless of websocket result
            del self.subscriptions[correlation_id]
            
            # Clean up token mapping if no other subscriptions exist for this token
            token = subscription.get('token')
            if token and not any(sub.get('token') == token for sub in self.subscriptions.values()):
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

    def _websocket_subscribe(self, subscription: Dict) -> None:
        scrip = subscription['scrip']
        mode = subscription['mode']
        self.logger.debug(f"[BROKER<-APP] subscribe: scrip={scrip} mode={mode}")
        if scrip not in self.ws_subscription_refs:
            self.ws_subscription_refs[scrip] = {'touchline_count': 0, 'depth_count': 0}
        if mode in [Config.MODE_LTP, Config.MODE_QUOTE]:
            if self.ws_subscription_refs[scrip]['touchline_count'] == 0:
                self.logger.debug(f"[BROKER<-APP] subscribe_touchline send for {scrip}")
                self.ws_client.subscribe_touchline(scrip)
            self.ws_subscription_refs[scrip]['touchline_count'] += 1
        elif mode == Config.MODE_DEPTH:
            if self.ws_subscription_refs[scrip]['depth_count'] == 0:
                self.logger.debug(f"[BROKER<-APP] subscribe_depth send for {scrip}")
                self.ws_client.subscribe_depth(scrip)
            self.ws_subscription_refs[scrip]['depth_count'] += 1

    def _websocket_unsubscribe(self, subscription: Dict) -> bool:
        """
        Unsubscribe from websocket with enhanced error handling and reference counting.
        
        Args:
            subscription: Subscription dictionary containing scrip and mode
            
        Returns:
            bool: True if unsubscribe was successful, False otherwise
        """
        scrip = subscription.get('scrip')
        mode = subscription.get('mode')
        
        if not scrip or mode is None:
            self.logger.warning(f"Invalid subscription data for websocket unsubscribe: {subscription}")
            return False
        
        self.logger.debug(f"[BROKER<-APP] unsubscribe: scrip={scrip} mode={mode}")
        
        # Check if we have reference tracking for this scrip
        if scrip not in self.ws_subscription_refs:
            self.logger.warning(f"No websocket subscription references found for {scrip}")
            return True  # Consider this successful since it's already unsubscribed
        
        success = True
        
        try:
            if mode in [Config.MODE_LTP, Config.MODE_QUOTE]:
                # Handle touchline unsubscription
                current_count = self.ws_subscription_refs[scrip]['touchline_count']
                if current_count <= 0:
                    self.logger.warning(f"Touchline count already 0 for {scrip}, skipping unsubscribe")
                    return True
                
                self.ws_subscription_refs[scrip]['touchline_count'] -= 1
                
                if self.ws_subscription_refs[scrip]['touchline_count'] <= 0:
                    self.logger.debug(f"[BROKER<-APP] unsubscribe_touchline send for {scrip}")
                    if self.ws_client and self.connected:
                        success = self.ws_client.unsubscribe_touchline(scrip)
                        if not success:
                            self.logger.warning(f"Failed to send touchline unsubscribe for {scrip}")
                    else:
                        self.logger.warning(f"WebSocket client not available for touchline unsubscribe: {scrip}")
                    
                    # Reset count to 0 regardless of websocket result
                    self.ws_subscription_refs[scrip]['touchline_count'] = 0
                    
            elif mode == Config.MODE_DEPTH:
                # Handle depth unsubscription
                current_count = self.ws_subscription_refs[scrip]['depth_count']
                if current_count <= 0:
                    self.logger.warning(f"Depth count already 0 for {scrip}, skipping unsubscribe")
                    return True
                
                self.ws_subscription_refs[scrip]['depth_count'] -= 1
                
                if self.ws_subscription_refs[scrip]['depth_count'] <= 0:
                    self.logger.debug(f"[BROKER<-APP] unsubscribe_depth send for {scrip}")
                    if self.ws_client and self.connected:
                        success = self.ws_client.unsubscribe_depth(scrip)
                        if not success:
                            self.logger.warning(f"Failed to send depth unsubscribe for {scrip}")
                    else:
                        self.logger.warning(f"WebSocket client not available for depth unsubscribe: {scrip}")
                    
                    # Reset count to 0 regardless of websocket result
                    self.ws_subscription_refs[scrip]['depth_count'] = 0
            
            # Clean up scrip reference if both counts are 0
            if (self.ws_subscription_refs[scrip]['touchline_count'] <= 0 and 
                self.ws_subscription_refs[scrip]['depth_count'] <= 0):
                del self.ws_subscription_refs[scrip]
                self.logger.debug(f"Cleaned up websocket subscription references for {scrip}")
                
        except Exception as e:
            self.logger.error(f"Error in websocket unsubscribe for {scrip}: {e}")
            success = False
        
        return success

    def _cleanup_orphaned_subscription(self, symbol: str, exchange: str, mode: int) -> None:
        """
        Clean up any orphaned websocket subscriptions that might exist without local tracking.
        
        Args:
            symbol: Symbol to clean up
            exchange: Exchange for the symbol
            mode: Subscription mode
        """
        try:
            # Try to get token info to construct scrip
            from websocket_proxy.mapping import SymbolMapper
            from .flattrade_mapping import FlattradeExchangeMapper
            
            token_info = SymbolMapper.get_token_from_symbol(symbol, exchange)
            if not token_info:
                self.logger.debug(f"Cannot cleanup orphaned subscription - token not found for {symbol}.{exchange}")
                return
            
            token = token_info['token']
            brexchange = token_info['brexchange']
            flattrade_exchange = FlattradeExchangeMapper.to_flattrade_exchange(brexchange)
            scrip = f"{flattrade_exchange}|{token}"
            
            # Create a temporary subscription object for cleanup
            temp_subscription = {
                'scrip': scrip,
                'mode': mode,
                'symbol': symbol,
                'exchange': exchange
            }
            
            self.logger.debug(f"Attempting to cleanup orphaned subscription for {scrip}")
            self._websocket_unsubscribe(temp_subscription)
            
        except Exception as e:
            self.logger.debug(f"Error during orphaned subscription cleanup for {symbol}.{exchange}: {e}")

    def get_subscription_stats(self) -> Dict[str, Any]:
        """
        Get current subscription statistics for debugging and monitoring.
        
        Returns:
            Dict containing subscription statistics
        """
        with self.lock:
            stats = {
                'total_subscriptions': len(self.subscriptions),
                'subscription_details': {},
                'websocket_refs': dict(self.ws_subscription_refs),
                'token_mappings': len(self.token_to_symbol),
                'connected': self.connected,
                'running': self.running
            }
            
            # Group subscriptions by symbol for easier debugging
            for correlation_id, sub in self.subscriptions.items():
                symbol = sub.get('symbol', 'unknown')
                if symbol not in stats['subscription_details']:
                    stats['subscription_details'][symbol] = []
                stats['subscription_details'][symbol].append({
                    'exchange': sub.get('exchange'),
                    'mode': sub.get('mode'),
                    'correlation_id': correlation_id,
                    'scrip': sub.get('scrip')
                })
            
            return stats

    def validate_subscription_state(self) -> Dict[str, Any]:
        """
        Validate consistency between different subscription tracking mechanisms.
        
        Returns:
            Dict containing validation results and any inconsistencies found
        """
        with self.lock:
            issues = []
            
            # Check for orphaned websocket references
            for scrip, refs in self.ws_subscription_refs.items():
                has_local_subs = any(sub.get('scrip') == scrip for sub in self.subscriptions.values())
                if not has_local_subs and (refs['touchline_count'] > 0 or refs['depth_count'] > 0):
                    issues.append(f"Orphaned websocket reference for {scrip}: {refs}")
            
            # Check for missing websocket references
            for correlation_id, sub in self.subscriptions.items():
                scrip = sub.get('scrip')
                if scrip and scrip not in self.ws_subscription_refs:
                    issues.append(f"Missing websocket reference for subscription {correlation_id} (scrip: {scrip})")
            
            return {
                'is_valid': len(issues) == 0,
                'issues': issues,
                'total_subscriptions': len(self.subscriptions),
                'total_ws_refs': len(self.ws_subscription_refs)
            }

    def _cleanup_orphaned_subscription(self, symbol: str, exchange: str, mode: int) -> None:
        """
        Clean up any orphaned websocket subscriptions that might exist without local tracking.
        
        Args:
            symbol: Symbol to clean up
            exchange: Exchange for the symbol
            mode: Subscription mode
        """
        try:
            # Try to get token info to construct scrip
            from websocket_proxy.mapping import SymbolMapper
            from .flattrade_mapping import FlattradeExchangeMapper
            
            token_info = SymbolMapper.get_token_from_symbol(symbol, exchange)
            if not token_info:
                self.logger.debug(f"Cannot clean up orphaned subscription - token not found for {symbol}.{exchange}")
                return
            
            token = token_info['token']
            brexchange = token_info['brexchange']
            flattrade_exchange = FlattradeExchangeMapper.to_flattrade_exchange(brexchange)
            scrip = f"{flattrade_exchange}|{token}"
            
            # Check if there are any websocket references for this scrip
            if scrip in self.ws_subscription_refs:
                self.logger.info(f"Cleaning up orphaned websocket subscription for {scrip}")
                
                # Force cleanup based on mode
                if mode in [Config.MODE_LTP, Config.MODE_QUOTE] and self.ws_subscription_refs[scrip]['touchline_count'] > 0:
                    if self.ws_client and self.connected:
                        self.ws_client.unsubscribe_touchline(scrip)
                    self.ws_subscription_refs[scrip]['touchline_count'] = 0
                
                if mode == Config.MODE_DEPTH and self.ws_subscription_refs[scrip]['depth_count'] > 0:
                    if self.ws_client and self.connected:
                        self.ws_client.unsubscribe_depth(scrip)
                    self.ws_subscription_refs[scrip]['depth_count'] = 0
                
                # Remove scrip reference if both counts are 0
                if (self.ws_subscription_refs[scrip]['touchline_count'] <= 0 and 
                    self.ws_subscription_refs[scrip]['depth_count'] <= 0):
                    del self.ws_subscription_refs[scrip]
                    
            # Clean up token mapping if it exists
            if token in self.token_to_symbol:
                del self.token_to_symbol[token]
                self.logger.debug(f"Cleaned up orphaned token mapping for {token}")
                
        except Exception as e:
            self.logger.warning(f"Error cleaning up orphaned subscription for {symbol}.{exchange}: {e}")

    def _on_open(self, ws):
        self.logger.info("Connected to Flattrade WebSocket")
        self.connected = True
        self._resubscribe_all()

    def _on_error(self, ws, error):
        self.logger.error(f"Flattrade WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        self.logger.info(f"Flattrade WebSocket closed: {code} - {msg}")
        self.connected = False
        if self.running:
            self._schedule_reconnection()

    def _resubscribe_all(self):
        with self.lock:
            self.ws_subscription_refs = {}
            touchline_scrips = set()
            depth_scrips = set()
            for sub in self.subscriptions.values():
                scrip = sub['scrip']
                mode = sub['mode']
                if scrip not in self.ws_subscription_refs:
                    self.ws_subscription_refs[scrip] = {'touchline_count': 0, 'depth_count': 0}
                if mode in [Config.MODE_LTP, Config.MODE_QUOTE]:
                    touchline_scrips.add(scrip)
                    self.ws_subscription_refs[scrip]['touchline_count'] += 1
                elif mode == Config.MODE_DEPTH:
                    depth_scrips.add(scrip)
                    self.ws_subscription_refs[scrip]['depth_count'] += 1
            if touchline_scrips:
                self.ws_client.subscribe_touchline('#'.join(touchline_scrips))
            if depth_scrips:
                self.ws_client.subscribe_depth('#'.join(depth_scrips))

    def _schedule_reconnection(self):
        delay = min(5 * (2 ** self.reconnect_attempts), 60)
        self.reconnect_attempts += 1
        threading.Timer(delay, self._attempt_reconnection).start()

    def _attempt_reconnection(self):
        try:
            self.ws_client = FlattradeWebSocket(
                user_id=self.actid,
                actid=self.actid,
                susertoken=self.susertoken,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open,
            )
            if self.ws_client.connect():
                self.connected = True
                self.reconnect_attempts = 0
                self.logger.info("Reconnected successfully")
        except Exception as e:
            self.logger.error(f"Reconnection error: {e}")

    def _on_message(self, ws, message):
        # Raw inbound from broker
        self.logger.debug(f"[BROKER->APP] raw: {message}")
        try:
            data = json.loads(message)
            msg_type = data.get('t')
            if msg_type == Config.MSG_AUTH:
                self.logger.info(f"Authentication response: {data}")
                return
            if msg_type in (Config.MSG_TOUCHLINE_FULL, Config.MSG_TOUCHLINE_PARTIAL, Config.MSG_DEPTH_FULL, Config.MSG_DEPTH_PARTIAL):
                self._process_market_message(data)
        except Exception as e:
            self.logger.error(f"Message processing error: {e}")

    def _process_market_message(self, data: Dict[str, Any]) -> None:
        msg_type = data.get('t')
        token = data.get('tk')
        if not (msg_type and token and token in self.token_to_symbol):
            return
        symbol, exchange = self.token_to_symbol.get(token, (None, None))
        if not symbol:
            return
        for sub in self._find_matching_subscriptions(token):
            if self._should_process_message(msg_type, sub['mode']):
                self._process_subscription_message(data, sub, symbol, exchange)

    def _find_matching_subscriptions(self, token: str) -> List[Dict]:
        with self.lock:
            return [s for s in self.subscriptions.values() if s['token'] == token]

    def _should_process_message(self, msg_type: str, mode: int) -> bool:
        touchline = {Config.MSG_TOUCHLINE_FULL, Config.MSG_TOUCHLINE_PARTIAL}
        depth = {Config.MSG_DEPTH_FULL, Config.MSG_DEPTH_PARTIAL}
        if mode in [Config.MODE_LTP, Config.MODE_QUOTE]:
            return msg_type in touchline
        if mode == Config.MODE_DEPTH:
            return msg_type in depth
        return False

    def _process_subscription_message(self, data: Dict[str, Any], subscription: Dict[str, Any], symbol: str, exchange: str) -> None:
        mode = subscription['mode']
        msg_type = data.get('t')
        token = data.get('tk')
        if token:
            data = self.market_cache.update(token, data)
        normalized = self._normalize_market_data(data, msg_type, mode)
        normalized.update({'symbol': symbol, 'exchange': exchange, 'timestamp': int(time.time() * 1000)})
        topic = f"{exchange}_{symbol}_{ {Config.MODE_LTP: 'LTP', Config.MODE_QUOTE: 'QUOTE', Config.MODE_DEPTH: 'DEPTH'}[mode] }"
        self.logger.debug(f"[APP->SHM] topic={topic} symbol={symbol} payload={json.dumps(normalized)[:200]}...")
        self._publish_market_data(topic, normalized)

    def _normalize_market_data(self, data: Dict[str, Any], msg_type: str, mode: int) -> Dict[str, Any]:
        if mode == Config.MODE_LTP:
            return {'mode': Config.MODE_LTP, 'ltp': safe_float(data.get('lp')), 'flattrade_timestamp': safe_int(data.get('ft'))}
        if mode == Config.MODE_QUOTE:
            return {
                'mode': Config.MODE_QUOTE,
                'ltp': safe_float(data.get('lp')),
                'volume': safe_int(data.get('v')),
                'open': safe_float(data.get('o')),
                'high': safe_float(data.get('h')),
                'low': safe_float(data.get('l')),
                'close': safe_float(data.get('c')),
                'average_price': safe_float(data.get('ap')),
                'percent_change': safe_float(data.get('pc')),
                'last_quantity': safe_int(data.get('ltq')),
                'last_trade_time': data.get('ltt'),
                'flattrade_timestamp': safe_int(data.get('ft')),
            }
        # DEPTH
        result = {
            'mode': Config.MODE_DEPTH,
            'ltp': safe_float(data.get('lp')),
            'volume': safe_int(data.get('v')),
            'open': safe_float(data.get('o')),
            'high': safe_float(data.get('h')),
            'low': safe_float(data.get('l')),
            'close': safe_float(data.get('c')),
            'average_price': safe_float(data.get('ap')),
            'percent_change': safe_float(data.get('pc')),
            'last_quantity': safe_int(data.get('ltq')),
            'last_trade_time': data.get('ltt'),
            'total_buy_quantity': safe_int(data.get('tbq')),
            'total_sell_quantity': safe_int(data.get('tsq')),
            'flattrade_timestamp': safe_int(data.get('ft')),
        }
        result['depth'] = {
            'buy': [
                {'price': safe_float(data.get('bp1')), 'quantity': safe_int(data.get('bq1')), 'orders': safe_int(data.get('bo1'))},
                {'price': safe_float(data.get('bp2')), 'quantity': safe_int(data.get('bq2')), 'orders': safe_int(data.get('bo2'))},
                {'price': safe_float(data.get('bp3')), 'quantity': safe_int(data.get('bq3')), 'orders': safe_int(data.get('bo3'))},
                {'price': safe_float(data.get('bp4')), 'quantity': safe_int(data.get('bq4')), 'orders': safe_int(data.get('bo4'))},
                {'price': safe_float(data.get('bp5')), 'quantity': safe_int(data.get('bq5')), 'orders': safe_int(data.get('bo5'))},
            ],
            'sell': [
                {'price': safe_float(data.get('sp1')), 'quantity': safe_int(data.get('sq1')), 'orders': safe_int(data.get('so1'))},
                {'price': safe_float(data.get('sp2')), 'quantity': safe_int(data.get('sq2')), 'orders': safe_int(data.get('so2'))},
                {'price': safe_float(data.get('sp3')), 'quantity': safe_int(data.get('sq3')), 'orders': safe_int(data.get('so3'))},
                {'price': safe_float(data.get('sp4')), 'quantity': safe_int(data.get('sq4')), 'orders': safe_int(data.get('so4'))},
                {'price': safe_float(data.get('sp5')), 'quantity': safe_int(data.get('sq5')), 'orders': safe_int(data.get('so5'))},
            ],
        }
        result['depth_level'] = 5
        return result

    def _publish_market_data(self, topic: str, data: Dict[str, Any]) -> None:
        if self.shm_publisher is None:
            self.logger.debug("SHM publisher not available; drop message")
            return
        # Extract symbol from topic pattern "EXCHANGE_SYMBOL_MODE"
        parts = str(topic).split('_')
        symbol = data.get('symbol') or (parts[1] if len(parts) >= 2 else topic)
        ok = self.shm_publisher.publish_market_data(symbol, data)
        if not ok:
            self.logger.debug(f"[APP->SHM] publish failed for symbol={symbol}")
        else:
            self.logger.debug(f"[APP->SHM] published symbol={symbol}")
