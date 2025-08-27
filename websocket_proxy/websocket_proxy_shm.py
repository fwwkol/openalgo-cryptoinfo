import asyncio
import json
import logging
import os
import signal
import sys
import threading
import time
from collections import defaultdict, deque
from typing import Dict, Set, Optional, List, Callable, Any, Deque, Awaitable, Tuple
import websockets
from websockets.exceptions import ConnectionClosed

from .ring_buffer import LockFreeRingBuffer
from .market_data import MarketDataMessage
from .config import config
from broker.flattrade.streaming.flattrade_adapter_shm import FlattradeSHMWebSocketAdapter
try:
    # Optional: database auth for API key verification and token lookup
    from database.auth_db import verify_api_key, get_broker_name, get_auth_token
except Exception:  # In environments without DB, skip auth backend
    verify_api_key = None
    get_broker_name = None
    get_auth_token = None

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ws-proxy")

# Configure websockets logger
websockets_logger = logging.getLogger('websockets')
websockets_logger.setLevel(logging.WARNING)

class WebSocketProxy:
    def __init__(self):
        """Initialize WebSocket proxy with configuration from environment."""
        # Initialize shared memory ring buffer
        self.ring_buffer = LockFreeRingBuffer(
            name=config.SHM_BUFFER_NAME,
            size=config.SHM_BUFFER_SIZE
        )
        
        # Client state
        self.clients: Dict[websockets.WebSocketServerProtocol, Dict[str, Any]] = {}
        # Map (symbol, mode) -> set of websockets
        self.subscriptions: Dict[Tuple[str, int], Set[websockets.WebSocketServerProtocol]] = defaultdict(set)
        # Map client -> set of (symbol, mode)
        self.client_subscriptions: Dict[websockets.WebSocketServerProtocol, Set[Tuple[str, int]]] = defaultdict(set)
        # Store per-(symbol, mode) metadata from subscribe requests (e.g., exchange)
        self.symbol_meta: Dict[Tuple[str, int], Dict[str, Any]] = {}
        # Cache last sent payload per (symbol, mode) to replay on new subscription
        self.last_values: Dict[Tuple[str, int], str] = {}
        
        # Broker adapter management (integrated from AdapterManager)
        self._adapter_lock = threading.RLock()
        # Enhanced subscription metadata tracking
        self._adapter_subscriptions: Dict[str, Dict[str, Any]] = {}  # symbol -> metadata
        self._flattrade: Optional[FlattradeSHMWebSocketAdapter] = None
        
        # Config for broker adapters
        self.enable_flattrade = os.getenv("ENABLE_FLATTRADE", "true").lower() in ("1", "true", "yes")
        self.flattrade_user = os.getenv("FLATTRADE_USER_ID", "root")
        
        # Statistics
        self.metrics = {
            'messages_received': 0,
            'messages_sent': 0,
            'connection_count': 0,
            'active_connections': 0,
            'errors': 0,
            'batch_processed': 0,
            'batch_errors': 0
        }
        
        # Message queue for batching (store JSON strings)
        self.message_queue: List[Tuple[str, Set[websockets.WebSocketServerProtocol]]] = []
        self.batch_processor_task: Optional[asyncio.Task] = None
        self.shutdown_event = asyncio.Event()
        self.market_data_task: Optional[asyncio.Task] = None
        self.maintenance_task: Optional[asyncio.Task] = None
    
    async def _handle_connection(self, websocket):
        """Handle a new WebSocket connection (websockets v12: single connection arg)."""
        # Check max connections
        if len(self.clients) >= config.WS_MAX_CONNECTIONS:
            await self._send_error(websocket, "maximum_connections", 
                                f"Maximum number of connections reached ({config.WS_MAX_CONNECTIONS})")
            await websocket.close()
            return
        
        # Register client
        client_id = f"client-{id(websocket)}"
        self.clients[websocket] = {
            'id': client_id,
            'connected_at': time.time(),
            'subscribed_symbols': set(),
            'last_active': time.time(),
            'message_count': 0,
            'authenticated': (not config.AUTH_REQUIRED),
            'user_id': None,
            'broker': None,
        }
        
        self.metrics['connection_count'] += 1
        self.metrics['active_connections'] = len(self.clients)
        
        logger.info(f"Client connected: {client_id} (total: {len(self.clients)})")
        
        try:
            # Send welcome message
            await self._send_success(websocket, "connected", {
                "client_id": client_id,
                "max_subscriptions": 10000,  # High limit since we're using sets
                "max_message_size": 65536,   # 64KB
                "server_time": int(time.time()),
                "batch_supported": True,
                "auth_required": bool(config.AUTH_REQUIRED),
            })
            
            # Process incoming messages
            async for message in websocket:
                try:
                    self.clients[websocket]['last_active'] = time.time()
                    await self._handle_message(websocket, message)
                except Exception as e:
                    self.metrics['errors'] += 1
                    logger.error(f"Error handling message from {client_id}: {e}", exc_info=True)
                    try:
                        await self._send_error(websocket, "processing_error", str(e))
                    except:
                        pass  # Client might be disconnected already
                    
        except websockets.exceptions.ConnectionClosed:
            logger.debug(f"Client disconnected: {client_id}")
        except Exception as e:
            self.metrics['errors'] += 1
            logger.error(f"Connection error with {client_id}: {e}", exc_info=True)
        finally:
            # Clean up
            await self._close_connection(websocket)
            await self._cleanup_client(client_id)
    
    async def _handle_message(self, websocket: websockets.WebSocketServerProtocol, message: str):
        """Handle incoming WebSocket message."""
        try:
            client = self.clients.get(websocket)
            if not client:
                return
            
            # Debug log raw inbound message
            logger.debug(f"[WS<-CLIENT] {client['id']} msg: {message}")
            
            data = json.loads(message)
            action = data.get('action')
            symbol = data.get('symbol')
            exchange = data.get('exchange')
            mode = data.get('mode')
            
            if action == 'ping':
                await self._send_success(websocket, 'pong')
            elif action in ('authenticate', 'auth'):
                await self._authenticate(websocket, data)
            elif action == 'subscribe' and symbol:
                await self._subscribe(websocket, symbol, exchange=exchange, mode=mode)
            elif action == 'unsubscribe' and symbol:
                await self._unsubscribe(websocket, symbol, mode=mode)
            elif action == 'list_subscriptions':
                await self._list_subscriptions(websocket)
                
        except json.JSONDecodeError:
            await self._send_error(websocket, 'invalid_json', 'Invalid JSON received')
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)
            await self._send_error(websocket, 'processing_error', str(e))
    
    async def _send_success(self, websocket: websockets.WebSocketServerProtocol, event: str, data: Optional[Dict[str, Any]] = None):
        """Send a standardized success envelope to the client."""
        payload = {
            "type": "success",
            "event": event,
            "data": data or {},
            "ts": int(time.time())
        }
        await self._send_message(websocket, payload)
    
    async def _send_error(self, websocket: websockets.WebSocketServerProtocol, code: str, message: str, data: Optional[Dict[str, Any]] = None):
        """Send a standardized error envelope to the client and update metrics."""
        self.metrics['errors'] += 1
        payload = {
            "type": "error",
            "code": code,
            "message": message,
            "data": data or {},
            "ts": int(time.time())
        }
        await self._send_message(websocket, payload)
    
    async def _list_subscriptions(self, websocket: websockets.WebSocketServerProtocol):
        """Send the list of symbols the client is subscribed to."""
        client = self.clients.get(websocket)
        subs = sorted(list({s for (s, _m) in self.client_subscriptions.get(websocket, set())})) if client else []
        await self._send_success(websocket, 'subscriptions', {"symbols": subs})
    
    async def _cleanup_client(self, client_id: str):
        """Final logging/cleanup after a client disconnects."""
        logger.info(f"Client cleanup completed: {client_id}")
    
    async def _subscribe(self, websocket: websockets.WebSocketServerProtocol, symbol: str, exchange: Optional[str] = None, mode: Optional[int] = None):
        """Subscribe client to symbol updates."""
        client = self.clients.get(websocket)
        if not client:
            return
        if config.AUTH_REQUIRED and not client.get('authenticated'):
            logger.debug(f"[AUTH] subscribe blocked for unauthenticated {client['id']}")
            await self._send_error(websocket, 'not_authenticated', 'Authenticate first')
            return
            # normalize mode
        if mode is None:
            mode = 1
        key = (symbol, int(mode))

        if key not in self.client_subscriptions[websocket]:
            self.client_subscriptions[websocket].add(key)
            self.subscriptions[key].add(websocket)

            # Save/refresh (symbol, mode) metadata
            meta = self.symbol_meta.get(key, {})
            if exchange:
                meta['exchange'] = exchange
            if 'exchange' not in meta:
                meta['exchange'] = 'NSE'
            self.symbol_meta[key] = meta

            logger.info(f"Client {client['id']} subscribed to {symbol} (mode={mode})")
            await self._send_success(websocket, 'subscribed', {"symbol": symbol})

            # Trigger upstream subscription (refcounted per symbol)
            try:
                self._broker_subscribe(symbol=symbol, exchange=meta['exchange'], mode=mode, depth=5)
                logger.info(f"Upstream subscribe requested for {meta['exchange']}:{symbol} mode={mode}")
            except Exception as e:
                logger.error(f"Broker subscribe error for {symbol}: {e}", exc_info=True)
                # cleanup local
                try:
                    self.client_subscriptions[websocket].discard(key)
                    self.subscriptions[key].discard(websocket)
                    if not self.subscriptions[key]:
                        del self.subscriptions[key]
                    await self._send_error(websocket, 'subscription_failed', f'Failed to subscribe to {symbol}: {str(e)}')
                    return
                except Exception as cleanup_error:
                    logger.error(f"Error during subscription cleanup for {symbol}: {cleanup_error}")

            # Immediately send last known value for this (symbol, mode)
            last = self.last_values.get(key)
            if last:
                try:
                    await self._send_message(websocket, last)
                except Exception:
                    logger.debug(f"Failed to send last value for {symbol}, mode={mode} to {client['id']}")
    
    async def _unsubscribe(self, websocket: websockets.WebSocketServerProtocol, symbol: str, mode: Optional[int] = None):
        """Unsubscribe client from symbol updates."""
        client = self.clients.get(websocket)
        if not client:
            return
        if config.AUTH_REQUIRED and not client.get('authenticated'):
            logger.debug(f"[AUTH] unsubscribe blocked for unauthenticated {client['id']}")
            await self._send_error(websocket, 'not_authenticated', 'Authenticate first')
            return
            # normalize handling: if mode provided, remove only that (symbol, mode). else remove all modes for symbol.
        keys_to_remove: List[Tuple[str, int]] = []
        if mode is not None:
            keys_to_remove = [(symbol, int(mode))] if (symbol, int(mode)) in self.client_subscriptions.get(websocket, set()) else []
        else:
            keys_to_remove = [k for k in self.client_subscriptions.get(websocket, set()) if k[0] == symbol]

        if not keys_to_remove:
            logger.warning(f"Client {client['id']} attempted to unsubscribe from {symbol} but was not subscribed")
            await self._send_error(websocket, 'not_subscribed', f'Not subscribed to {symbol}')
        else:
            for key in keys_to_remove:
                _symbol, _mode = key
                self.client_subscriptions[websocket].discard(key)
                self.subscriptions[key].discard(websocket)
                if not self.subscriptions[key]:
                    try:
                        del self.subscriptions[key]
                    except KeyError:
                        pass
                # Upstream refcount decrement per removal
                try:
                    self._broker_unsubscribe(_symbol)
                    logger.info(f"Upstream unsubscribe requested for {_symbol}")
                except Exception as e:
                    logger.error(f"Broker unsubscribe error for {_symbol}: {e}", exc_info=True)

            logger.info(f"Client {client['id']} unsubscribed from {symbol}")
            await self._send_success(websocket, 'unsubscribed', {"symbol": symbol})
    
    async def _batch_processor(self) -> None:
        """Process messages in batches for better throughput."""
        logger.debug("Starting batch processor")
        
        while not self.shutdown_event.is_set():
            try:
                if not self.message_queue:
                    await asyncio.sleep(0.001)  # Small sleep to prevent busy waiting
                    continue
                
                # Process messages in batches
                current_batch = self.message_queue[:config.BATCH_SIZE]
                del self.message_queue[:len(current_batch)]
                
                if not current_batch:
                    continue
                
                # Group messages by client to send multiple messages in one go
                client_messages: Dict[websockets.WebSocketServerProtocol, List[bytes]] = {}
                
                for message, clients in current_batch:
                    for client in clients:
                        if client not in client_messages:
                            client_messages[client] = []
                        client_messages[client].append(message)
                
                # Send batched messages
                send_tasks = []
                for client, messages in client_messages.items():
                    if len(messages) == 1:
                        send_tasks.append(self._send_message(client, messages[0]))
                    else:
                        # Send as a batch array of JSON strings
                        send_tasks.append(self._send_message(client, json.dumps(messages)))
                
                # Wait for all sends to complete
                results = await asyncio.gather(*send_tasks, return_exceptions=True)
                
                # Update metrics
                success_count = sum(1 for r in results if r is True)
                self.metrics['messages_sent'] += success_count
                self.metrics['batch_processed'] += 1
                
                if success_count < len(results):
                    self.metrics['batch_errors'] += 1
                    # Log details about failed sends for debugging
                    failed_count = len(results) - success_count
                    logger.warning(f"Batch send partially failed: {success_count}/{len(results)} successful, {failed_count} failed")
                    
                    # Log specific errors (but limit to avoid spam)
                    error_count = 0
                    for i, result in enumerate(results):
                        if result is not True and error_count < 3:  # Limit error logging
                            logger.debug(f"Batch send error {i+1}: {result}")
                            error_count += 1
                
            except Exception as e:
                self.metrics['errors'] += 1
                logger.error(f"Error in batch processor: {e}", exc_info=True)
                await asyncio.sleep(1)  # Prevent tight loop on errors
    
    async def _process_market_data(self):
        """Process messages from the ring buffer and queue them for batch processing."""
        while not self.shutdown_event.is_set():
            try:
                # Process messages in small batches to balance latency and throughput
                for _ in range(10):  # Process up to 10 messages per iteration
                    message = self.ring_buffer.consume()
                    if message is None:
                        break
                    
                    # Update metrics
                    self.metrics['messages_received'] += 1
                    
                    # Get subscribers for this (symbol, mode)
                    mode = int(getattr(message, 'message_type', 1))
                    subscribers = self.subscriptions.get((message.symbol, mode), set())
                    if not subscribers:
                        logger.debug(f"[SHM->WS] No subscribers for {message.symbol}; dropping")
                        continue
                    
                    # Build legacy envelope expected by clients (type=market_data)
                    # Prefer exchange from exact (symbol, mode) meta; fallback to any mode meta for symbol
                    exchange = self.symbol_meta.get((message.symbol, mode), {}).get('exchange', None)
                    if exchange is None:
                        try:
                            exchange = next((m.get('exchange') for (s, m_mode), m in self.symbol_meta.items() if s == message.symbol and 'exchange' in m), None)
                        except Exception:
                            exchange = None
                    if exchange is None:
                        exchange = 'NSE'
                    payload = {
                        'type': 'market_data',
                        'exchange': exchange,
                        'symbol': message.symbol,
                        'mode': mode,
                        'data': {
                            'ltp': message.price,
                            'timestamp': message.timestamp,
                            'volume': message.volume,
                            'bid': message.bid_price,
                            'ask': message.ask_price,
                            'bid_qty': message.bid_quantity,
                            'ask_qty': message.ask_quantity,
                            'open': message.open_price,
                            'high': message.high_price,
                            'low': message.low_price,
                            'close': message.close_price,
                            'avg_price': message.average_price,
                            'percent_change': message.percent_change,
                            'last_qty': message.last_quantity,
                        }
                    }

                    # Add totals and depth for depth mode or when depth values exist
                    try:
                        depth_buy = [
                            {'price': float(message.bid_price_1 or 0), 'quantity': int(message.bid_qty_1 or 0), 'orders': int(message.bid_orders_1 or 0)},
                            {'price': float(message.bid_price_2 or 0), 'quantity': int(message.bid_qty_2 or 0), 'orders': int(message.bid_orders_2 or 0)},
                            {'price': float(message.bid_price_3 or 0), 'quantity': int(message.bid_qty_3 or 0), 'orders': int(message.bid_orders_3 or 0)},
                            {'price': float(message.bid_price_4 or 0), 'quantity': int(message.bid_qty_4 or 0), 'orders': int(message.bid_orders_4 or 0)},
                            {'price': float(message.bid_price_5 or 0), 'quantity': int(message.bid_qty_5 or 0), 'orders': int(message.bid_orders_5 or 0)},
                        ]
                        depth_sell = [
                            {'price': float(message.ask_price_1 or 0), 'quantity': int(message.ask_qty_1 or 0), 'orders': int(message.ask_orders_1 or 0)},
                            {'price': float(message.ask_price_2 or 0), 'quantity': int(message.ask_qty_2 or 0), 'orders': int(message.ask_orders_2 or 0)},
                            {'price': float(message.ask_price_3 or 0), 'quantity': int(message.ask_qty_3 or 0), 'orders': int(message.ask_orders_3 or 0)},
                            {'price': float(message.ask_price_4 or 0), 'quantity': int(message.ask_qty_4 or 0), 'orders': int(message.ask_orders_4 or 0)},
                            {'price': float(message.ask_price_5 or 0), 'quantity': int(message.ask_qty_5 or 0), 'orders': int(message.ask_orders_5 or 0)},
                        ]

                        has_depth = any(
                            (lvl.get('price') or lvl.get('quantity') or lvl.get('orders'))
                            for lvl in (depth_buy + depth_sell)
                        )

                        if mode == 3 or has_depth:
                            payload['data']['total_buy_quantity'] = int(message.total_buy_quantity or 0)
                            payload['data']['total_sell_quantity'] = int(message.total_sell_quantity or 0)
                            payload['data']['depth'] = {
                                'buy': depth_buy,
                                'sell': depth_sell,
                            }
                    except Exception as depth_err:
                        logger.debug(f"Depth build error for {message.symbol}: {depth_err}")
                    message_json = json.dumps(payload)
                    # Update last value cache for replay on new subscriptions for this (symbol, mode)
                    self.last_values[(message.symbol, mode)] = message_json
                    logger.debug(f"[SHM->WS] symbol={message.symbol} mode={mode} subs={len(subscribers)} payload={message_json[:200]}...")
                    self.message_queue.append((message_json, subscribers))
                    
                    # Apply backpressure if queue is getting too large
                    if len(self.message_queue) > config.WS_MAX_QUEUE * 2:
                        await asyncio.sleep(0.001)  # Small delay to slow down producer
                        break
                
                # Small sleep to prevent busy waiting when no messages
                await asyncio.sleep(0.001)
                
            except asyncio.CancelledError:
                logger.info("Market data processing cancelled")
                break
            except Exception as e:
                self.metrics['errors'] += 1
                logger.error(f"Error processing market data: {e}", exc_info=True)
                await asyncio.sleep(1)  # Avoid tight loop on errors
    
    async def _send_message(self, client: websockets.WebSocketServerProtocol, message: Any) -> bool:
        """Safely send a message to a WebSocket client with error handling."""
        try:
            # Find client id for logging
            cid = self.clients.get(client, {}).get('id', 'unknown')
            preview = message if isinstance(message, str) else json.dumps(message)
            logger.debug(f"[WS->CLIENT] {cid} sending: {preview[:200]}...")
            if isinstance(message, (str, bytes)):
                await client.send(message)
            else:
                await client.send(json.dumps(message))
            return True
        except (ConnectionClosed, RuntimeError) as e:
            logger.debug(f"Client disconnected while sending: {e}")
            await self._close_connection(client)
            return False
        except Exception as e:
            self.metrics['errors'] += 1
            logger.error(f"Error sending message: {e}", exc_info=True)
            return False
    
    async def _close_connection(self, websocket: websockets.WebSocketServerProtocol):
        """Clean up resources for a client connection with enhanced error handling."""
        if websocket not in self.clients:
            logger.debug("Attempted to close connection for unknown client")
            return
            
        client = self.clients[websocket]
        client_id = client['id']
        subscribed_keys = list(self.client_subscriptions.get(websocket, set()))
        logger.debug(f"Closing connection for {client_id} with {len(subscribed_keys)} subscriptions")

        # Remove all subscriptions with proper cleanup (per (symbol, mode))
        for key in subscribed_keys:
            try:
                symbol, mode = key
                # Remove this websocket from the (symbol, mode) subscriber set
                self.subscriptions[key].discard(websocket)

                # If no more subscribers for this (symbol, mode), delete entry and upstream ref-- once
                if not self.subscriptions[key]:
                    try:
                        del self.subscriptions[key]
                    except KeyError:
                        pass
                    # Clean up associated (symbol, mode) metadata and last value
                    self.symbol_meta.pop(key, None)
                    self.last_values.pop(key, None)
                    # Decrement adapter refcount for this symbol once per key removal
                    try:
                        self._broker_unsubscribe(symbol)
                        logger.debug(f"Upstream ref-- for {symbol} due to closing {client_id} (mode={mode})")
                    except Exception as e:
                        logger.warning(f"Error during upstream unsubscribe for {symbol}: {e}")
            except Exception as e:
                logger.warning(f"Error cleaning up subscription for {key} during connection close: {e}")

        # Remove mapping of this client to its subscriptions
        try:
            self.client_subscriptions.pop(websocket, None)
        except Exception:
            pass
        
        # Remove client from tracking
        try:
            del self.clients[websocket]
            self.metrics['active_connections'] = len(self.clients)
            logger.debug(f"Removed client {client_id} from tracking")
        except KeyError:
            logger.warning(f"Client {client_id} already removed from tracking")
        
        # Close the WebSocket connection with improved compatibility
        try:
            # Check various ways the websocket might indicate if it's closed
            is_closed = False
            
            # Try different attributes that might indicate closed state
            if hasattr(websocket, 'closed'):
                is_closed = websocket.closed
            elif hasattr(websocket, 'close_code'):
                # If close_code exists and is not None, connection is likely closed
                is_closed = websocket.close_code is not None
            elif hasattr(websocket, 'state'):
                # Check if state indicates closed (websockets v10+)
                is_closed = str(websocket.state).lower() in ['closed', 'closing']
            else:
                # Fallback: assume we need to close if we can't determine state
                is_closed = False
            
            if not is_closed:
                await websocket.close()
                logger.debug(f"Closed WebSocket connection for {client_id}")
            else:
                logger.debug(f"WebSocket connection for {client_id} already closed")
                
        except Exception as e:
            # Don't log this as an error since the connection might already be closed
            logger.debug(f"Could not close WebSocket for {client_id}: {e}")
        
        logger.info(f"Client disconnected: {client_id} (total: {len(self.clients)})")
        
        # Clean up message queue: keep all messages, just remove this websocket from recipient sets
        self.message_queue = [
            (msg, (clients - {websocket}))
            for msg, clients in self.message_queue
        ]

    async def _authenticate(self, websocket: websockets.WebSocketServerProtocol, data: Dict[str, Any]):
        """Authenticate client via API key if required."""
        client = self.clients.get(websocket)
        if not client:
            return
        if not config.AUTH_REQUIRED:
            client['authenticated'] = True
            # Send auth response in the format clients expect: top-level type 'auth' and status 'success'
            await self._send_message(websocket, {
                'type': 'auth',
                'status': 'success',
                'message': 'Auth not required'
            })
            return
        api_key = data.get('api_key')
        if not api_key:
            await self._send_error(websocket, 'authentication_error', 'API key is required')
            return
        if verify_api_key is None:
            await self._send_error(websocket, 'authentication_error', 'Auth backend unavailable')
            return
        try:
            user_id = verify_api_key(api_key)
        except Exception as e:
            logger.error(f"[AUTH] verify_api_key error: {e}")
            await self._send_error(websocket, 'authentication_error', 'Verification failed')
            return
        if not user_id:
            await self._send_error(websocket, 'authentication_error', 'Invalid API key')
            return
        broker = None
        if get_broker_name is not None:
            try:
                broker = get_broker_name(api_key)
            except Exception as e:
                logger.debug(f"[AUTH] get_broker_name error: {e}")
        client['authenticated'] = True
        client['user_id'] = user_id
        client['broker'] = broker
        logger.info(f"[AUTH] {client['id']} authenticated user_id={user_id} broker={broker}")
        # Send auth response in the format clients expect: top-level type 'auth' and status 'success'
        await self._send_message(websocket, {
            'type': 'auth',
            'status': 'success',
            'message': 'Authentication successful',
            'user_id': user_id,
            'broker': broker,
        })
    
    def _start_broker_adapters(self):
        """Start broker adapters with comprehensive error handling."""
        with self._adapter_lock:
            logger.info("Starting broker adapters")
            
            if self.enable_flattrade:
                try:
                    logger.debug("Initializing Flattrade adapter")
                    self._flattrade = FlattradeSHMWebSocketAdapter(shm_buffer_name=config.SHM_BUFFER_NAME)
                    
                    # Pre-check credentials to avoid noisy traceback when not configured
                    if get_auth_token is not None:
                        try:
                            susertoken = get_auth_token(self.flattrade_user)
                        except Exception as cred_err:
                            susertoken = None
                            logger.debug(f"Credential lookup error for user {self.flattrade_user}: {cred_err}")
                        if not susertoken:
                            logger.warning(f"Flattrade credentials not found for user '{self.flattrade_user}'. Skipping Flattrade adapter startup.")
                            # Ensure adapter is not kept
                            self._flattrade = None
                            # Continue without raising; other adapters (if any) can start
                            return
                    
                    logger.debug(f"Initializing Flattrade adapter for user: {self.flattrade_user}")
                    self._flattrade.initialize(broker_name="flattrade", user_id=self.flattrade_user)
                    
                    logger.debug("Connecting Flattrade adapter")
                    self._flattrade.connect()
                    
                    logger.info("Flattrade adapter started successfully (SHM mode)")
                    
                    # Perform state reconciliation after successful connection
                    try:
                        logger.debug("Performing post-connection state reconciliation")
                        reconcile_result = self.reconcile_subscription_state(auto_fix=True)
                        if reconcile_result['reconciliation_needed']:
                            logger.info(f"Post-connection reconciliation completed: "
                                      f"{len(reconcile_result['actions_taken'])} actions taken")
                    except Exception as reconcile_error:
                        logger.warning(f"Error during post-connection reconciliation: {reconcile_error}")
                    
                except Exception as e:
                    logger.error(f"Failed to start Flattrade adapter: {e}", exc_info=True)
                    # Clean up partial initialization
                    if self._flattrade:
                        try:
                            self._flattrade.disconnect()
                        except Exception as cleanup_error:
                            logger.warning(f"Error during Flattrade adapter cleanup: {cleanup_error}")
                        finally:
                            self._flattrade = None
                    
                    # Continue without Flattrade adapter
                    logger.warning("Continuing without Flattrade adapter due to initialization failure")
            else:
                logger.info("Flattrade adapter disabled via ENABLE_FLATTRADE=false")
    
    def _stop_broker_adapters(self):
        """Stop broker adapters with comprehensive error handling and cleanup."""
        with self._adapter_lock:
            logger.info("Stopping broker adapters")
            
            # Stop Flattrade adapter
            if self._flattrade:
                try:
                    logger.debug("Disconnecting Flattrade adapter")
                    self._flattrade.disconnect()
                    logger.info("Flattrade adapter stopped successfully")
                except Exception as e:
                    logger.error(f"Error stopping Flattrade adapter: {e}", exc_info=True)
                finally:
                    self._flattrade = None
            
            # Clear all subscription state
            try:
                subscription_count = len(self._adapter_subscriptions)
                self._adapter_subscriptions.clear()
                logger.debug(f"Cleared {subscription_count} adapter subscriptions")
            except Exception as e:
                logger.warning(f"Error clearing adapter subscriptions: {e}")
            
            logger.info("Broker adapters stopped and cleaned up")
    
    def _broker_subscribe(self, symbol: str, exchange: str = "NSE", mode: int = 1, depth: int = 5):
        """Reference-counted subscribe to broker adapter with enhanced metadata tracking."""
        key = symbol.upper()
        with self._adapter_lock:
            # Start adapters if not already started
            if not self._flattrade and self.enable_flattrade:
                self._start_broker_adapters()
            
            # Get or create subscription metadata
            if key not in self._adapter_subscriptions:
                self._adapter_subscriptions[key] = {
                    'count': 0,
                    'exchange': exchange,
                    'mode': mode,
                    'depth': depth,
                    'correlation_id': f"{symbol}_{exchange}_{mode}",
                    'created_at': time.time(),
                    'last_updated': time.time()
                }
            
            # Update metadata and increment count
            metadata = self._adapter_subscriptions[key]
            metadata['count'] += 1
            metadata['last_updated'] = time.time()
            
            # Validate consistency - warn if parameters don't match existing subscription
            if (metadata['exchange'] != exchange or 
                metadata['mode'] != mode or 
                metadata['depth'] != depth):
                logger.warning(f"Subscription parameter mismatch for {key}: "
                             f"existing=({metadata['exchange']}, {metadata['mode']}, {metadata['depth']}) "
                             f"requested=({exchange}, {mode}, {depth}).")

                # If a higher mode is requested (e.g., upgrade LTP->DEPTH), try to upgrade upstream
                try:
                    requested_mode = int(mode) if mode is not None else metadata['mode']
                    requested_depth = int(depth) if depth is not None else metadata['depth']
                except Exception:
                    requested_mode = metadata['mode']
                    requested_depth = metadata['depth']

                if requested_mode > metadata['mode']:
                    try:
                        if self._flattrade:
                            res = self._flattrade.subscribe(
                                symbol=key,
                                exchange=metadata['exchange'],
                                mode=requested_mode,
                                depth_level=requested_depth
                            )
                            if res and res.get("status") == "success":
                                old_mode, old_depth = metadata['mode'], metadata['depth']
                                metadata['mode'] = requested_mode
                                metadata['depth'] = requested_depth
                                metadata['last_updated'] = time.time()
                                # Update correlation id to reflect new mode
                                old_correlation_id = metadata['correlation_id']
                                metadata['correlation_id'] = f"{symbol}_{metadata['exchange']}_{metadata['mode']}"
                                if old_correlation_id != metadata['correlation_id']:
                                    logger.debug(f"Updated correlation_id for {key}: {old_correlation_id} -> {metadata['correlation_id']}")
                                logger.info(f"Upgraded upstream subscription for {metadata['exchange']}:{key} "
                                            f"mode {old_mode}->{metadata['mode']} depth {old_depth}->{metadata['depth']}")
                            else:
                                logger.warning(f"Failed to upgrade subscription for {key} to mode={requested_mode}: {res}")
                        else:
                            logger.warning(f"Cannot upgrade subscription for {key}: flattrade adapter not available")
                    except Exception as upgrade_err:
                        logger.warning(f"Error upgrading subscription for {key} to mode={requested_mode}: {upgrade_err}")
                
                # Update correlation_id if parameters changed
                old_correlation_id = metadata['correlation_id']
                metadata['correlation_id'] = f"{symbol}_{metadata['exchange']}_{metadata['mode']}"
                if old_correlation_id != metadata['correlation_id']:
                    logger.debug(f"Updated correlation_id for {key}: {old_correlation_id} -> {metadata['correlation_id']}")
            
            if metadata['count'] == 1:
                # First subscriber -> ask adapter to subscribe upstream
                if self._flattrade:
                    res = self._flattrade.subscribe(
                        symbol=key, 
                        exchange=metadata['exchange'], 
                        mode=metadata['mode'], 
                        depth_level=metadata['depth']
                    )
                    if res.get("status") != "success":
                        logger.error(f"Flattrade subscribe failed for {metadata['exchange']}:{key}: {res}")
                        # Rollback on failure
                        metadata['count'] -= 1
                        if metadata['count'] <= 0:
                            del self._adapter_subscriptions[key]
                        return
                logger.info(f"Upstream subscribed: {metadata['exchange']}:{key} mode={metadata['mode']}")
            else:
                logger.debug(f"Ref++ for {key}: {metadata['count']}")
    
    def _broker_unsubscribe(self, symbol: str):
        """Reference-counted unsubscribe from broker adapter with enhanced error handling and logging."""
        if not symbol:
            logger.error("Cannot unsubscribe: symbol is empty or None")
            return
            
        key = symbol.upper()
        logger.debug(f"Processing broker unsubscribe for symbol: {key}")
        
        with self._adapter_lock:
            if key not in self._adapter_subscriptions:
                # This is a recoverable state mismatch - log detailed information for debugging
                logger.warning(f"Unsubscribe requested for {key} but no subscription metadata found. "
                             f"Available subscriptions: {list(self._adapter_subscriptions.keys())}")
                
                # Attempt to clean up any orphaned state in the adapter
                self._cleanup_orphaned_adapter_subscription(key)
                return
            
            metadata = self._adapter_subscriptions[key]
            original_count = metadata['count']
            metadata['count'] -= 1
            metadata['last_updated'] = time.time()
            
            logger.debug(f"Subscription ref count for {key}: {original_count} -> {metadata['count']}")
            
            if metadata['count'] <= 0:
                # Last subscriber left -> unsubscribe upstream
                logger.info(f"Last subscriber for {key}, initiating upstream unsubscribe")
                
                upstream_success = False
                try:
                    if self._flattrade:
                        logger.debug(f"Calling Flattrade unsubscribe for {key} "
                                   f"(exchange={metadata['exchange']}, mode={metadata['mode']})")
                        
                        res = self._flattrade.unsubscribe(
                            symbol=key, 
                            exchange=metadata['exchange'], 
                            mode=metadata['mode']
                        )
                        
                        if res and res.get("status") == "success":
                            logger.info(f"Flattrade unsubscribe successful for {key}")
                            upstream_success = True
                        elif res and res.get("status") == "error":
                            # Handle specific error codes gracefully
                            error_code = res.get("code", "UNKNOWN")
                            error_message = res.get("message", "No message")
                            
                            if error_code == "NOT_SUBSCRIBED":
                                logger.warning(f"Flattrade reports {key} not subscribed (state mismatch recovered): {error_message}")
                                upstream_success = True  # Treat as success since it's already unsubscribed
                            else:
                                logger.warning(f"Flattrade unsubscribe failed for {key} [{error_code}]: {error_message}")
                        elif res:
                            logger.warning(f"Unexpected Flattrade unsubscribe response for {key}: {res}")
                        else:
                            logger.warning(f"No response from Flattrade unsubscribe for {key}")
                    else:
                        logger.warning(f"Flattrade adapter not available for unsubscribe: {key}")
                        
                except Exception as e:
                    logger.error(f"Exception during upstream unsubscribe for {key}: {e}", exc_info=True)
                finally:
                    # Always clean up metadata regardless of upstream result
                    try:
                        del self._adapter_subscriptions[key]
                        logger.info(f"Cleaned up subscription metadata for {key} "
                                  f"(upstream_success={upstream_success})")
                    except KeyError:
                        logger.warning(f"Subscription metadata for {key} already removed")
                        
                    logger.info(f"Upstream unsubscribed: {key}")
            else:
                logger.debug(f"Ref-- for {key}: {metadata['count']} (still has subscribers)")
    
    def _cleanup_orphaned_adapter_subscription(self, symbol: str):
        """
        Attempt to clean up any orphaned subscription state in the adapter.
        
        Args:
            symbol: Symbol to clean up
        """
        try:
            if self._flattrade:
                logger.debug(f"Attempting to clean up orphaned adapter subscription for {symbol}")
                
                # Try common exchange/mode combinations that might be orphaned
                common_combinations = [
                    ("NSE", 1),  # NSE LTP
                    ("NSE", 2),  # NSE QUOTE  
                    ("BSE", 1),  # BSE LTP
                    ("BSE", 2),  # BSE QUOTE
                ]
                
                for exchange, mode in common_combinations:
                    try:
                        res = self._flattrade.unsubscribe(symbol=symbol, exchange=exchange, mode=mode)
                        if res and res.get("status") == "success":
                            logger.info(f"Cleaned up orphaned subscription: {symbol}.{exchange} mode={mode}")
                        elif res and res.get("code") == "NOT_SUBSCRIBED":
                            logger.debug(f"No orphaned subscription found for {symbol}.{exchange} mode={mode}")
                    except Exception as e:
                        logger.debug(f"Error checking orphaned subscription {symbol}.{exchange}: {e}")
                        
        except Exception as e:
            logger.warning(f"Error during orphaned subscription cleanup for {symbol}: {e}")
    
    def get_subscription_debug_info(self) -> Dict[str, Any]:
        """
        Get detailed subscription state information for debugging.
        
        Returns:
            Dict containing comprehensive subscription state information
        """
        with self._adapter_lock:
            debug_info = {
                'timestamp': time.time(),
                'total_clients': len(self.clients),
                'total_subscriptions': len(self.subscriptions),
                'total_adapter_subscriptions': len(self._adapter_subscriptions),
                'flattrade_connected': self._flattrade is not None and getattr(self._flattrade, 'connected', False),
                'client_details': [],
                'subscription_details': {},
                'adapter_subscription_details': {},
                'metrics': self.metrics.copy()
            }
            
            # Client details
            for ws, client in self.clients.items():
                keys = list(self.client_subscriptions.get(ws, set()))
                debug_info['client_details'].append({
                    'id': client['id'],
                    'connected_at': client['connected_at'],
                    'subscriptions': [{'symbol': s, 'mode': m} for (s, m) in keys],
                    'subscription_count': len(keys),
                    'authenticated': client.get('authenticated', False)
                })
            
            # Subscription details
            for key, subscribers in self.subscriptions.items():
                debug_info['subscription_details'][key] = {
                    'subscriber_count': len(subscribers),
                    'metadata': self.symbol_meta.get(key, {}),
                    'has_last_value': key in self.last_values
                }
            
            # Adapter subscription details
            for symbol, metadata in self._adapter_subscriptions.items():
                debug_info['adapter_subscription_details'][symbol] = metadata.copy()
            
            return debug_info
    
    def log_subscription_state_summary(self):
        """Log a summary of current subscription state for debugging."""
        try:
            debug_info = self.get_subscription_debug_info()
            
            logger.info(f"=== Subscription State Summary ===")
            logger.info(f"Clients: {debug_info['total_clients']}")
            logger.info(f"Local subscriptions: {debug_info['total_subscriptions']}")
            logger.info(f"Adapter subscriptions: {debug_info['total_adapter_subscriptions']}")
            logger.info(f"Flattrade connected: {debug_info['flattrade_connected']}")
            
            if debug_info['subscription_details']:
                logger.info(f"Active symbols: {', '.join(debug_info['subscription_details'].keys())}")
            
            logger.info(f"Messages sent: {debug_info['metrics']['messages_sent']}")
            logger.info(f"Errors: {debug_info['metrics']['errors']}")
            logger.info(f"=== End Summary ===")
            
        except Exception as e:
            logger.warning(f"Error logging subscription state summary: {e}")
    
    def _validate_subscription_state(self) -> Dict[str, Any]:
        """
        Comprehensive validation of subscription state consistency across all components.
        
        Returns:
            Dict containing detailed validation results and any issues found
        """
        with self._adapter_lock:
            issues = []
            warnings = []
            
            # 1. Check adapter subscription consistency
            for symbol, metadata in list(self._adapter_subscriptions.items()):
                # Check for invalid counts
                if metadata['count'] <= 0:
                    issues.append(f"Adapter subscription {symbol} has non-positive count: {metadata['count']}")
                
                # Check for stale subscriptions (older than 2 hours with no updates)
                age = time.time() - metadata.get('last_updated', metadata.get('created_at', 0))
                if age > 7200:  # 2 hours
                    warnings.append(f"Adapter subscription {symbol} is stale (last updated {age:.0f}s ago)")
                
                # Check for missing required metadata fields
                required_fields = ['count', 'exchange', 'mode', 'correlation_id', 'created_at']
                for field in required_fields:
                    if field not in metadata:
                        issues.append(f"Adapter subscription {symbol} missing required field: {field}")
            
            # 2. Check local subscription consistency (keys are (symbol, mode))
            local_symbol_counts_by_symbol: Dict[str, int] = {}
            for key, subscribers in self.subscriptions.items():
                symbol, mode = key
                local_symbol_counts_by_symbol[symbol] = local_symbol_counts_by_symbol.get(symbol, 0) + len(subscribers)
                
                # Check for empty subscriber sets (should be cleaned up)
                if len(subscribers) == 0:
                    issues.append(f"Local subscription {key} has empty subscriber set")
                
                # Check if (symbol, mode) metadata exists
                if key not in self.symbol_meta:
                    warnings.append(f"Local subscription {key} missing metadata")
            
            # 3. Cross-validate local vs adapter subscriptions
            adapter_symbols = set(self._adapter_subscriptions.keys())  # base symbol strings
            local_symbols = set(local_symbol_counts_by_symbol.keys())   # base symbol strings
            
            # Symbols in adapter but not in local subscriptions
            orphaned_adapter = adapter_symbols - local_symbols
            for symbol in orphaned_adapter:
                issues.append(f"Orphaned adapter subscription: {symbol} (no local subscribers)")
            
            # Symbols in local but not in adapter subscriptions
            missing_adapter = local_symbols - adapter_symbols
            for symbol in missing_adapter:
                issues.append(f"Missing adapter subscription: {symbol} (has local subscribers)")
            
            # 4. Validate Flattrade adapter state if available
            flattrade_issues = []
            if self._flattrade:
                try:
                    # Get adapter's internal subscription state
                    adapter_subscriptions = getattr(self._flattrade, 'subscriptions', {})
                    adapter_ws_refs = getattr(self._flattrade, 'ws_subscription_refs', {})
                    
                    # Check for mismatches between our tracking and adapter's tracking
                    for symbol in adapter_symbols:
                        metadata = self._adapter_subscriptions[symbol]
                        correlation_id = metadata.get('correlation_id')
                        
                        if correlation_id and correlation_id not in adapter_subscriptions:
                            flattrade_issues.append(f"Adapter missing subscription for {symbol} (correlation_id: {correlation_id})")
                    
                    # Check for orphaned adapter subscriptions
                    for correlation_id in adapter_subscriptions:
                        # Extract symbol from correlation_id (format: symbol_exchange_mode)
                        parts = correlation_id.split('_')
                        if len(parts) >= 1:
                            adapter_symbol = parts[0].upper()
                            if adapter_symbol not in adapter_symbols:
                                flattrade_issues.append(f"Orphaned Flattrade subscription: {correlation_id}")
                
                except Exception as e:
                    warnings.append(f"Could not validate Flattrade adapter state: {e}")
            
            # 5. Check client subscription consistency
            client_issues = []
            total_client_subscriptions = 0
            for ws, client in self.clients.items():
                keys = self.client_subscriptions.get(ws, set())
                total_client_subscriptions += len(keys)
                # Check if client's subscriptions match our tracking
                for key in keys:
                    if key not in self.subscriptions or ws not in self.subscriptions.get(key, set()):
                        client_issues.append(f"Client {client.get('id', 'unknown')} subscription to {key} not tracked properly")
            
            return {
                'is_valid': len(issues) == 0,
                'has_warnings': len(warnings) > 0,
                'issues': issues,
                'warnings': warnings,
                'flattrade_issues': flattrade_issues,
                'client_issues': client_issues,
                'summary': {
                    'total_adapter_subscriptions': len(self._adapter_subscriptions),
                    'total_local_subscriptions': len(self.subscriptions),
                    'total_clients': len(self.clients),
                    'total_client_subscriptions': total_client_subscriptions,
                    'orphaned_adapter_count': len(orphaned_adapter),
                    'missing_adapter_count': len(missing_adapter),
                    'flattrade_connected': self._flattrade is not None and getattr(self._flattrade, 'connected', False)
                },
                'subscription_details': {
                    symbol: {
                        'adapter_count': meta['count'],
                        'local_count': local_symbol_counts.get(symbol, 0),
                        'exchange': meta['exchange'],
                        'mode': meta['mode'],
                        'age_seconds': time.time() - meta.get('created_at', 0),
                        'correlation_id': meta.get('correlation_id')
                    }
                    for symbol, meta in self._adapter_subscriptions.items()
                }
            }
    
    def _cleanup_stale_subscriptions(self, max_age_seconds: int = 7200) -> int:
        """
        Clean up stale or invalid subscription entries.
        
        Args:
            max_age_seconds: Maximum age for subscriptions before considering them stale
        
        Returns:
            Number of subscriptions cleaned up
        """
        cleaned_count = 0
        current_time = time.time()
        
        with self._adapter_lock:
            # Remove subscriptions with zero or negative counts
            invalid_count_symbols = [
                symbol for symbol, metadata in self._adapter_subscriptions.items()
                if metadata['count'] <= 0
            ]
            
            # Remove stale subscriptions
            stale_symbols = [
                symbol for symbol, metadata in self._adapter_subscriptions.items()
                if (current_time - metadata.get('last_updated', metadata.get('created_at', 0))) > max_age_seconds
            ]
            
            # Combine all symbols to remove
            to_remove = set(invalid_count_symbols + stale_symbols)
            
            for symbol in to_remove:
                try:
                    metadata = self._adapter_subscriptions[symbol]
                    logger.warning(f"Cleaning up stale/invalid subscription: {symbol} "
                                 f"(count={metadata['count']}, age={current_time - metadata.get('last_updated', 0):.0f}s)")
                    
                    # Attempt upstream cleanup
                    if self._flattrade:
                        try:
                            self._flattrade.unsubscribe(
                                symbol=symbol,
                                exchange=metadata.get('exchange', 'NSE'),
                                mode=metadata.get('mode', 1)
                            )
                        except Exception as e:
                            logger.debug(f"Error during upstream cleanup for stale subscription {symbol}: {e}")
                    
                    # Remove from tracking
                    del self._adapter_subscriptions[symbol]
                    cleaned_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error cleaning up subscription {symbol}: {e}")
            
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} stale/invalid subscriptions")
        
        return cleaned_count
        
        return cleaned_count
    
    def get_subscription_stats(self) -> Dict[str, Any]:
        """
        Get detailed subscription statistics for monitoring and debugging.
        
        Returns:
            Dict containing subscription statistics
        """
        with self._adapter_lock:
            total_refs = sum(meta['count'] for meta in self._adapter_subscriptions.values())
            
            return {
                'total_symbols': len(self._adapter_subscriptions),
                'total_references': total_refs,
                'average_refs_per_symbol': total_refs / len(self._adapter_subscriptions) if self._adapter_subscriptions else 0,
                'subscriptions_by_exchange': {},
                'subscriptions_by_mode': {},
                'oldest_subscription_age': 0,
                'newest_subscription_age': 0
            }
    
    async def _periodic_subscription_maintenance(self):
        """
        Periodically validate and clean up subscription state.
        Runs every 5 minutes to maintain subscription health.
        """
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(300)  # 5 minutes
                
                if self.shutdown_event.is_set():
                    break
                
                # Validate subscription state
                validation_result = self._validate_subscription_state()
                if not validation_result['is_valid']:
                    logger.warning(f"Subscription state validation found issues: {validation_result['issues']}")
                
                # Clean up stale subscriptions
                cleaned = self._cleanup_stale_subscriptions()
                if cleaned > 0:
                    logger.info(f"Periodic maintenance cleaned up {cleaned} stale subscriptions")
                
                # Log subscription statistics for monitoring
                stats = self.get_subscription_stats()
                logger.debug(f"Subscription stats: {stats['total_symbols']} symbols, "
                           f"{stats['total_references']} total references")
                
            except asyncio.CancelledError:
                logger.info("Subscription maintenance task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in subscription maintenance: {e}", exc_info=True)
                await asyncio.sleep(60)  # Wait 1 minute before retrying on error

    def reconcile_subscription_state(self, auto_fix: bool = False) -> Dict[str, Any]:
        """
        Reconcile subscription state between proxy and adapter components.
        
        Args:
            auto_fix: If True, automatically fix detected issues
        
        Returns:
            Dict containing reconciliation results
        """
        logger.info("Starting subscription state reconciliation")
        
        # First, validate current state
        validation_result = self._validate_subscription_state()
        
        if validation_result['is_valid'] and not validation_result['has_warnings']:
            logger.info("Subscription state is consistent, no reconciliation needed")
            return {
                'reconciliation_needed': False,
                'validation_result': validation_result,
                'actions_taken': []
            }
        
        actions_taken = []
        
        if auto_fix:
            logger.info("Auto-fix enabled, attempting to resolve issues")
            
            with self._adapter_lock:
                # 1. Clean up stale subscriptions
                cleaned_count = self._cleanup_stale_subscriptions()
                if cleaned_count > 0:
                    actions_taken.append(f"Cleaned up {cleaned_count} stale subscriptions")
                
                # 2. Fix orphaned adapter subscriptions (adapter has subscription but no local subscribers)
                adapter_symbols = set(self._adapter_subscriptions.keys())
                local_symbols = set(self.subscriptions.keys())
                orphaned_adapter = adapter_symbols - local_symbols
                
                for symbol in orphaned_adapter:
                    try:
                        logger.info(f"Removing orphaned adapter subscription: {symbol}")
                        metadata = self._adapter_subscriptions[symbol]
                        
                        # Attempt upstream cleanup
                        if self._flattrade:
                            self._flattrade.unsubscribe(
                                symbol=symbol,
                                exchange=metadata.get('exchange', 'NSE'),
                                mode=metadata.get('mode', 1)
                            )
                        
                        del self._adapter_subscriptions[symbol]
                        actions_taken.append(f"Removed orphaned adapter subscription: {symbol}")
                        
                    except Exception as e:
                        logger.warning(f"Error removing orphaned subscription {symbol}: {e}")
                
                # 3. Add missing adapter subscriptions (local has subscribers but no adapter subscription)
                missing_adapter = local_symbols - adapter_symbols
                
                for symbol in missing_adapter:
                    try:
                        logger.info(f"Adding missing adapter subscription: {symbol}")
                        # Choose any available (symbol, mode) meta for this symbol
                        chosen_key = next((k for k in self.symbol_meta.keys() if isinstance(k, tuple) and k[0] == symbol), None)
                        meta = self.symbol_meta.get(chosen_key, {}) if chosen_key else {}
                        exchange = meta.get('exchange', 'NSE')
                        mode = meta.get('mode', 1) if meta.get('mode') is not None else 1
                        # Count actual local subscribers across all modes for this symbol
                        subscriber_count = sum(len(subs) for (s, m), subs in self.subscriptions.items() if s == symbol)
                        
                        if subscriber_count > 0:
                            # Create adapter subscription
                            self._broker_subscribe(symbol=symbol, exchange=exchange, mode=mode, depth=5)
                            actions_taken.append(f"Added missing adapter subscription: {symbol} (count={subscriber_count})")
                        
                    except Exception as e:
                        logger.warning(f"Error adding missing adapter subscription for {symbol}: {e}")
        
        # Re-validate after fixes
        final_validation = self._validate_subscription_state()
        
        result = {
            'reconciliation_needed': True,
            'auto_fix_enabled': auto_fix,
            'initial_validation': validation_result,
            'final_validation': final_validation,
            'actions_taken': actions_taken,
            'issues_resolved': len(validation_result['issues']) - len(final_validation['issues']),
            'warnings_resolved': len(validation_result['warnings']) - len(final_validation['warnings'])
        }
        
        logger.info(f"Reconciliation complete. Issues resolved: {result['issues_resolved']}, "
                   f"Warnings resolved: {result['warnings_resolved']}, "
                   f"Actions taken: {len(actions_taken)}")
        
        return result

    def schedule_periodic_validation(self, interval_seconds: int = 300):
        """
        Schedule periodic subscription state validation.
        
        Args:
            interval_seconds: Interval between validation checks (default: 5 minutes)
        """
        async def validation_task():
            while not self.shutdown_event.is_set():
                try:
                    await asyncio.sleep(interval_seconds)
                    
                    if self.shutdown_event.is_set():
                        break
                    
                    logger.debug("Running periodic subscription state validation")
                    validation_result = self._validate_subscription_state()
                    
                    if not validation_result['is_valid']:
                        logger.warning(f"Subscription state validation failed: {len(validation_result['issues'])} issues found")
                        
                        # Auto-fix critical issues
                        if len(validation_result['issues']) > 0:
                            logger.info("Attempting automatic reconciliation")
                            reconcile_result = self.reconcile_subscription_state(auto_fix=True)
                            
                            if reconcile_result['issues_resolved'] > 0:
                                logger.info(f"Auto-reconciliation resolved {reconcile_result['issues_resolved']} issues")
                    
                    elif validation_result['has_warnings']:
                        logger.debug(f"Subscription state has {len(validation_result['warnings'])} warnings")
                    
                    # Periodic cleanup of stale subscriptions
                    cleaned = self._cleanup_stale_subscriptions()
                    if cleaned > 0:
                        logger.info(f"Periodic cleanup removed {cleaned} stale subscriptions")
                
                except asyncio.CancelledError:
                    logger.debug("Periodic validation task cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in periodic validation: {e}", exc_info=True)
        
        # Start the validation task
        asyncio.create_task(validation_task())
        logger.info(f"Scheduled periodic subscription validation every {interval_seconds} seconds")

    def manual_reconciliation(self) -> Dict[str, Any]:
        """
        Manually trigger subscription state reconciliation with detailed reporting.
        
        Returns:
            Dict containing detailed reconciliation results
        """
        logger.info("Manual subscription state reconciliation triggered")
        
        try:
            # Perform comprehensive validation
            validation_result = self._validate_subscription_state()
            
            # Log current state
            logger.info("=== Manual Reconciliation Report ===")
            logger.info(f"Total issues found: {len(validation_result['issues'])}")
            logger.info(f"Total warnings found: {len(validation_result['warnings'])}")
            
            if validation_result['issues']:
                logger.warning("Issues found:")
                for issue in validation_result['issues']:
                    logger.warning(f"  - {issue}")
            
            if validation_result['warnings']:
                logger.info("Warnings found:")
                for warning in validation_result['warnings']:
                    logger.info(f"  - {warning}")
            
            # Perform reconciliation with auto-fix
            reconcile_result = self.reconcile_subscription_state(auto_fix=True)
            
            # Log reconciliation results
            if reconcile_result['actions_taken']:
                logger.info("Actions taken during reconciliation:")
                for action in reconcile_result['actions_taken']:
                    logger.info(f"  - {action}")
            else:
                logger.info("No actions needed during reconciliation")
            
            # Log final state
            final_validation = reconcile_result['final_validation']
            logger.info(f"Final state - Issues: {len(final_validation['issues'])}, "
                       f"Warnings: {len(final_validation['warnings'])}")
            
            logger.info("=== End Manual Reconciliation Report ===")
            
            return reconcile_result
            
        except Exception as e:
            logger.error(f"Error during manual reconciliation: {e}", exc_info=True)
            return {
                'error': str(e),
                'reconciliation_needed': True,
                'actions_taken': [],
                'issues_resolved': 0
            }
    
    async def start(self):
        """Start the WebSocket proxy server."""
        # Start broker adapters
        self._start_broker_adapters()
        
        # Start the batch processor
        self.batch_processor_task = asyncio.create_task(self._batch_processor())
        
        # Start the market data consumer
        self.market_data_task = asyncio.create_task(self._process_market_data())
        
        # Schedule periodic subscription state validation
        self.schedule_periodic_validation(interval_seconds=300)  # Every 5 minutes
        
        # Start the subscription maintenance task
        self.maintenance_task = asyncio.create_task(self._periodic_subscription_maintenance())
        
        # Start the WebSocket server
        server = await websockets.serve(
            self._handle_connection,
            host=config.WS_HOST,
            port=config.WS_PORT,
            max_size=2**24,  # 16MB max message size
            max_queue=config.WS_MAX_QUEUE,
            ping_interval=config.WS_PING_INTERVAL,
            ping_timeout=config.WS_PING_TIMEOUT,
            close_timeout=5,
            compression=None
        )
        
        logger.info(f"WebSocket proxy started on ws://{config.WS_HOST}:{config.WS_PORT}")
        logger.info(f"Shared memory buffer: {config.SHM_BUFFER_NAME} ({config.SHM_BUFFER_SIZE} bytes)")
        logger.info(f"Max messages: {config.MAX_MESSAGES}, Message size: {config.MESSAGE_SIZE} bytes")
        
        # Handle graceful shutdown safely: only register asyncio signal handlers in the main thread
        loop = asyncio.get_running_loop()
        if threading.current_thread() is threading.main_thread():
            try:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
                logger.info("Asyncio signal handlers registered in main thread")
            except (NotImplementedError, RuntimeError, ValueError) as e:
                # Platforms like Windows or non-compatible event loops may not support this
                logger.debug(f"Signal handlers not available: {e}; relying on shutdown_event")
        else:
            logger.debug("Skipping asyncio signal handlers (not in main thread); relying on shutdown_event")
        
        try:
            await self.shutdown_event.wait()
        except asyncio.CancelledError:
            logger.info("Shutdown signal received")
        finally:
            logger.info("Shutting down gracefully...")
            await self.shutdown()
            server.close()
            await server.wait_closed()

    async def shutdown(self):
        """Gracefully shut down background tasks and close all client connections."""
        # Signal shutdown
        if not self.shutdown_event.is_set():
            self.shutdown_event.set()

        # Stop broker adapters
        self._stop_broker_adapters()

        # Cancel background tasks
        tasks = []
        if self.batch_processor_task and not self.batch_processor_task.done():
            self.batch_processor_task.cancel()
            tasks.append(self.batch_processor_task)
        if self.market_data_task and not self.market_data_task.done():
            self.market_data_task.cancel()
            tasks.append(self.market_data_task)
        if self.maintenance_task and not self.maintenance_task.done():
            self.maintenance_task.cancel()
            tasks.append(self.maintenance_task)

        # Close client connections
        close_tasks = []
        for ws in list(self.clients.keys()):
            close_tasks.append(self._close_connection(ws))
        if close_tasks:
            try:
                await asyncio.gather(*close_tasks, return_exceptions=True)
            except Exception:
                pass

        # Await task cancellations
        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                pass

async def main():
    # Example usage
    proxy = SharedMemoryWebSocketProxy(port=8765)
    server = await proxy.start_server()
    
    try:
        # Keep the server running
        await asyncio.Future()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        proxy.stop()
        server.close()
        await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
