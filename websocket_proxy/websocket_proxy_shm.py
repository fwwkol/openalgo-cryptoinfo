import asyncio
import json
import logging
import os
import threading
import time
from collections import defaultdict
from typing import Dict, Set, Optional, List, Any, Tuple

import websockets
from websockets.exceptions import ConnectionClosed

from .optimized_ring_buffer import OptimizedRingBuffer
from .market_data import MarketDataMessage
from .config import config

from utils.logging import get_logger

logger = get_logger(__name__)

# Import database functions - handle gracefully if not available
try:
    from database.auth_db import verify_api_key, get_broker_name, get_auth_token
    logger.debug("Database auth available")
except ImportError as e:
    logger.debug(f"Database auth not available: {e}")
    verify_api_key = None
    get_broker_name = None
    get_auth_token = None

# Import broker factory for dynamic broker creation
try:
    from .broker_factory import create_broker_adapter
    logger.debug("Broker factory available")
except ImportError as e:
    logger.debug(f"Broker factory not available: {e}")
    create_broker_adapter = None

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ws-proxy-shm")

class WebSocketProxy:
    """High-performance WebSocket proxy using shared memory for market data distribution"""
    
    def __init__(self):
        """Initialize WebSocket proxy with shared memory ring buffer"""
        # Initialize shared memory ring buffer
        self.ring_buffer = OptimizedRingBuffer(
            name=config.SHM_BUFFER_NAME,
            size=config.SHM_BUFFER_SIZE,
            create=True
        )
        
        # Client management (following ZeroMQ pattern)
        self.clients: Dict[websockets.WebSocketServerProtocol, Dict[str, Any]] = {}
        self.subscriptions: Dict[str, Set[websockets.WebSocketServerProtocol]] = defaultdict(set)  # topic -> clients
        self.client_subscriptions: Dict[websockets.WebSocketServerProtocol, Set[str]] = defaultdict(set)  # client -> topics
        self.symbol_meta: Dict[str, Dict[str, Any]] = {}  # topic -> metadata
        self.last_values: Dict[str, str] = {}  # topic -> last message
        
        # Broker adapter management (same pattern as ZeroMQ server)
        self.broker_adapters = {}  # Maps user_id to broker adapter
        self.user_mapping = {}     # Maps client_id to user_id  
        self.user_broker_mapping = {}  # Maps user_id to broker_name
        
        # Performance metrics
        self.metrics = {
            'messages_received': 0,
            'messages_sent': 0,
            'connection_count': 0,
            'active_connections': 0,
            'errors': 0,
            'batch_processed': 0
        }
        
        # Background tasks
        self.shutdown_event = asyncio.Event()
        self.market_data_task: Optional[asyncio.Task] = None
        self.batch_processor_task: Optional[asyncio.Task] = None
        self.message_queue: List[Tuple[str, Set[websockets.WebSocketServerProtocol]]] = []
    
    async def start(self):
        """Start the WebSocket proxy server"""
        logger.info("Starting WebSocket Proxy (Shared Memory)")
        logger.info(f"Configuration: {config.to_dict()}")
        
        # Validate configuration
        if error := config.validate():
            logger.error(f"Invalid configuration: {error}")
            raise ValueError(f"Invalid configuration: {error}")
        
        try:
            # Start background tasks
            self.market_data_task = asyncio.create_task(self._process_market_data())
            self.batch_processor_task = asyncio.create_task(self._batch_processor())
            
            # Start WebSocket server
            self.server = await websockets.serve(
                self._handle_connection,
                config.WS_HOST,
                config.WS_PORT,
                ping_interval=config.WS_PING_INTERVAL,
                ping_timeout=config.WS_PING_TIMEOUT
            )
            
            logger.info(f"WebSocket server started on {config.WS_HOST}:{config.WS_PORT}")
            
            # Wait for shutdown
            await self.shutdown_event.wait()
            
        except Exception as e:
            logger.error(f"Error starting WebSocket proxy: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Shutdown the WebSocket proxy"""
        logger.info("Shutting down WebSocket proxy...")
        
        # Signal shutdown
        self.shutdown_event.set()
        
        # Cancel background tasks
        for task in [self.market_data_task, self.batch_processor_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Close WebSocket server
        if hasattr(self, 'server'):
            self.server.close()
            await self.server.wait_closed()
        
        # Close all client connections
        close_tasks = []
        for websocket in list(self.clients.keys()):
            close_tasks.append(self._close_connection(websocket))
        
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        # Disconnect all broker adapters (same as ZeroMQ version)
        for user_id, adapter in self.broker_adapters.items():
            try:
                adapter.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting adapter for user {user_id}: {e}")
        
        # Close shared memory
        if self.ring_buffer:
            self.ring_buffer.close()
        
        logger.info("WebSocket proxy shutdown complete")
    
    async def _handle_connection(self, websocket):
        """Handle WebSocket connection following ZeroMQ proxy pattern"""
        if len(self.clients) >= config.WS_MAX_CONNECTIONS:
            await self._send_error(websocket, "maximum_connections",
                                 f"Maximum connections reached ({config.WS_MAX_CONNECTIONS})")
            await websocket.close()
            return
        
        client_id = f"client-{id(websocket)}"
        self.clients[websocket] = {
            'id': client_id,
            'connected_at': time.time(),
            'last_active': time.time(),
            'authenticated': not config.AUTH_REQUIRED,
            'user_id': None,
            'broker': None
        }
        
        self.metrics['connection_count'] += 1
        self.metrics['active_connections'] = len(self.clients)
        logger.info(f"Client connected: {client_id} (total: {len(self.clients)})")
        
        try:
            await self._send_success(websocket, "connected", {
                "client_id": client_id,
                "server_time": int(time.time()),
                "auth_required": config.AUTH_REQUIRED
            })
            
            async for message in websocket:
                try:
                    self.clients[websocket]['last_active'] = time.time()
                    await self._handle_message(websocket, message)
                except Exception as e:
                    self.metrics['errors'] += 1
                    logger.error(f"Error handling message from {client_id}: {e}")
                    await self._send_error(websocket, "processing_error", str(e))
        
        except ConnectionClosed:
            logger.debug(f"Client disconnected: {client_id}")
        except Exception as e:
            logger.error(f"Connection error with {client_id}: {e}")
        finally:
            await self._close_connection(websocket)
    
    async def _handle_message(self, websocket: websockets.WebSocketServerProtocol, message: str):
        """Handle incoming WebSocket messages following ZeroMQ pattern"""
        try:
            client = self.clients.get(websocket)
            if not client:
                return
            
            data = json.loads(message)
            action = data.get('action')
            
            if action == 'ping':
                await self._send_success(websocket, 'pong')
            elif action in ('authenticate', 'auth'):
                await self._authenticate(websocket, data)
            elif action == 'subscribe':
                await self._subscribe(websocket, data)
            elif action == 'unsubscribe':
                await self._unsubscribe(websocket, data)
            elif action == 'list_subscriptions':
                await self._list_subscriptions(websocket)
            else:
                await self._send_error(websocket, 'invalid_action', f'Unknown action: {action}')
        
        except json.JSONDecodeError:
            await self._send_error(websocket, 'invalid_json', 'Invalid JSON received')
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            await self._send_error(websocket, 'processing_error', str(e))
    
    async def _authenticate(self, websocket: websockets.WebSocketServerProtocol, data: dict):
        """Authenticate client using same pattern as ZeroMQ server"""
        client = self.clients.get(websocket)
        if not client:
            return
        
        # Handle no auth required case
        if not config.AUTH_REQUIRED:
            client['authenticated'] = True
            await self._send_message(websocket, {
                'type': 'auth',
                'status': 'success',
                'message': 'Authentication not required',
                'user_id': 'anonymous',
                'broker': 'none'
            })
            logger.info(f"Client {client['id']} authenticated (auth not required)")
            return
        
        # Verify API key (same as ZeroMQ version)
        api_key = data.get('api_key')
        if not api_key:
            await self._send_message(websocket, {
                'type': 'auth',
                'status': 'error',
                'message': 'API key required'
            })
            return
        
        if not verify_api_key:
            await self._send_message(websocket, {
                'type': 'auth',
                'status': 'error',
                'message': 'Authentication backend not available'
            })
            return
        
        try:
            # Verify API key and get user info (same as ZeroMQ)
            user_id = verify_api_key(api_key)
            if not user_id:
                await self._send_message(websocket, {
                    'type': 'auth',
                    'status': 'error',
                    'message': 'Invalid API key'
                })
                return
            
            # Get broker name (same as ZeroMQ)
            broker_name = None
            if get_broker_name:
                try:
                    broker_name = get_broker_name(api_key)
                except Exception as e:
                    logger.debug(f"Could not get broker name: {e}")
            
            if not broker_name:
                await self._send_message(websocket, {
                    'type': 'auth',
                    'status': 'error',
                    'message': 'Could not determine broker for API key'
                })
                return
            
            # Store mappings (same as ZeroMQ pattern)
            client_id = id(websocket)
            self.user_mapping[client_id] = user_id
            self.user_broker_mapping[user_id] = broker_name
            
            # Create or reuse broker adapter (same as ZeroMQ version)
            if user_id not in self.broker_adapters:
                try:
                    adapter = self._create_broker_adapter(broker_name, user_id)
                    if adapter:
                        self.broker_adapters[user_id] = adapter
                        logger.info(f"Created {broker_name} adapter for user {user_id}")
                    else:
                        logger.warning(f"Could not create {broker_name} adapter for user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to create broker adapter: {e}")
            
            client['authenticated'] = True
            client['user_id'] = user_id
            client['broker'] = broker_name
            
            await self._send_message(websocket, {
                'type': 'auth',
                'status': 'success',
                'message': 'Authentication successful',
                'user_id': user_id,
                'broker': broker_name
            })
            logger.info(f"Client {client['id']} authenticated: user_id={user_id}, broker={broker_name}")
            
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            await self._send_message(websocket, {
                'type': 'auth',
                'status': 'error',
                'message': 'Authentication failed'
            })
    
    def _create_broker_adapter(self, broker_name: str, user_id: str):
        """Create broker adapter following ZeroMQ server pattern"""
        if not create_broker_adapter:
            logger.warning("Broker factory not available")
            return None
        
        try:
            # Create adapter using the factory
            adapter = create_broker_adapter(broker_name)
            if not adapter:
                logger.warning(f"No adapter available for broker: {broker_name}")
                return None
            
            logger.info(f"Created {broker_name} adapter: {type(adapter).__name__}")
            
            # Initialize adapter
            try:
                result = adapter.initialize(broker_name, user_id)
                if result and not result.get('success', True):
                    logger.error(f"Failed to initialize {broker_name} adapter: {result.get('error')}")
                    return None
            except Exception as e:
                logger.error(f"Error initializing {broker_name} adapter: {e}")
                return None
            
            # Connect to broker
            try:
                result = adapter.connect()
                if result and not result.get('success', True):
                    logger.error(f"Failed to connect {broker_name} adapter: {result.get('error')}")
                    return None
            except Exception as e:
                logger.error(f"Error connecting {broker_name} adapter: {e}")
                return None
            
            logger.info(f"Successfully created and connected {broker_name} adapter")
            return adapter
            
        except Exception as e:
            logger.error(f"Error creating {broker_name} adapter: {e}")
            return None
    
    async def _subscribe(self, websocket: websockets.WebSocketServerProtocol, data: dict):
        """Subscribe client to symbol updates following ZeroMQ pattern exactly"""
        client = self.clients.get(websocket)
        if not client:
            return

        if config.AUTH_REQUIRED and not client.get('authenticated'):
            await self._send_error(websocket, 'not_authenticated', 'Authenticate first')
            return

        symbol = data.get('symbol')
        exchange = data.get('exchange', 'NSE')
        mode = data.get('mode', 1)

        if not symbol:
            await self._send_error(websocket, 'missing_symbol', 'Symbol is required')
            return

        # Create ZeroMQ-style topic key (NSE_TCS_LTP format)
        mode_str = {1: 'LTP', 2: 'QUOTE', 3: 'DEPTH'}.get(mode, 'LTP')
        topic_key = f"{exchange}_{symbol}_{mode_str}"
        
        # Add to local subscriptions using topic as key
        if topic_key not in self.client_subscriptions[websocket]:
            self.client_subscriptions[websocket].add(topic_key)
            self.subscriptions[topic_key].add(websocket)
            logger.debug(f"Added subscription - client: {client['id']}, topic: {topic_key}")

        # Store metadata for this topic
        if topic_key not in self.symbol_meta:
            self.symbol_meta[topic_key] = {
                'exchange': exchange, 
                'symbol': symbol, 
                'mode': mode,
                'mode_str': mode_str
            }

        # Subscribe via broker adapter (same as ZeroMQ pattern)
        client_id = id(websocket)
        user_id = self.user_mapping.get(client_id)
        if user_id and user_id in self.broker_adapters:
            try:
                adapter = self.broker_adapters[user_id]
                result = adapter.subscribe(symbol, exchange, mode)
                logger.info(f"Broker subscription for {exchange}:{symbol} mode={mode}: {result}")
            except Exception as e:
                logger.error(f"Broker subscribe error for {symbol}: {e}")

        logger.info(f"Client {client['id']} subscribed to topic: {topic_key}")
        await self._send_success(websocket, 'subscribed', {
            "symbol": symbol, 
            "exchange": exchange,
            "mode": mode,
            "topic": topic_key
        })

    
    async def _unsubscribe(self, websocket: websockets.WebSocketServerProtocol, data: dict):
        """Unsubscribe client from symbol updates following ZeroMQ pattern"""
        client = self.clients.get(websocket)
        if not client:
            return

        if config.AUTH_REQUIRED and not client.get('authenticated'):
            await self._send_error(websocket, 'not_authenticated', 'Authenticate first')
            return

        symbol = data.get('symbol')
        exchange = data.get('exchange', 'NSE')
        mode = data.get('mode', 1)

        if not symbol:
            await self._send_error(websocket, 'missing_symbol', 'Symbol is required')
            return

        # Create ZeroMQ-style topic key
        mode_str = {1: 'LTP', 2: 'QUOTE', 3: 'DEPTH'}.get(mode, 'LTP')
        topic_key = f"{exchange}_{symbol}_{mode_str}"

        # Remove from local subscriptions
        if topic_key in self.client_subscriptions[websocket]:
            self.client_subscriptions[websocket].discard(topic_key)
            self.subscriptions[topic_key].discard(websocket)
            logger.debug(f"Removed subscription - client: {client['id']}, topic: {topic_key}")

            # If no more clients for this topic, clean up
            if not self.subscriptions[topic_key]:
                del self.subscriptions[topic_key]
                
                # Get metadata before potentially deleting it
                meta = self.symbol_meta.get(topic_key, {})
                symbol = meta.get('symbol', symbol)
                exchange = meta.get('exchange', exchange)
                mode = meta.get('mode', mode)
                
                # Unsubscribe from broker if we have the necessary info
                client_id = id(websocket)
                user_id = self.user_mapping.get(client_id)
                if user_id and user_id in self.broker_adapters:
                    try:
                        adapter = self.broker_adapters[user_id]
                        result = adapter.unsubscribe(symbol, exchange, mode)
                        logger.info(f"Broker unsubscription for {topic_key}: {result}")
                    except Exception as e:
                        logger.error(f"Broker unsubscribe error for {topic_key}: {e}")
                
                # Clean up metadata if no more subscriptions
                if topic_key in self.symbol_meta:
                    del self.symbol_meta[topic_key]
                if topic_key in self.last_values:
                    del self.last_values[topic_key]

        logger.info(f"Client {client['id']} unsubscribed from topic: {topic_key}")
        await self._send_success(websocket, 'unsubscribed', {
            "symbol": symbol, 
            "exchange": exchange,
            "mode": mode,
            "topic": topic_key
        })

    
    async def _list_subscriptions(self, websocket: websockets.WebSocketServerProtocol):
        """List client's subscriptions with topic information"""
        client = self.clients.get(websocket)
        if not client:
            return
        
        # Get all topics this client is subscribed to
        topics = list(self.client_subscriptions.get(websocket, set()))
        
        # Extract subscription details for each topic
        subscriptions = []
        for topic in topics:
            meta = self.symbol_meta.get(topic, {})
            subscriptions.append({
                'topic': topic,
                'symbol': meta.get('symbol', topic.split('_')[1] if len(topic.split('_')) > 1 else 'unknown'),
                'exchange': meta.get('exchange', 'NSE'),
                'mode': meta.get('mode', 1),
                'mode_str': meta.get('mode_str', 'LTP')
            })
        
        await self._send_success(websocket, 'subscriptions', {
            'count': len(subscriptions),
            'subscriptions': subscriptions
        })
    
    async def _process_market_data(self):
        """
        Enhanced process messages from the ring buffer with better OHLC data handling.
        """
        logger.debug("Starting market data processor")
        while not self.shutdown_event.is_set():
            try:
                message = self.ring_buffer.consume_single()
                if message is None:
                    await asyncio.sleep(0.001)
                    continue

                try:
                    logger.debug(f"Raw message type: {type(message)}")
                    
                    if hasattr(message, 'to_normalized_data'):
                        # Enhanced: Use the new to_normalized_data method to preserve OHLC
                        data = message.to_normalized_data()
                        symbol = data['symbol']
                        exchange = data['exchange']
                        mode = data['mode']
                        
                        logger.debug(f"BinaryMarketData converted with OHLC - symbol: {symbol}, exchange: {exchange}, mode: {mode}, "
                                f"price: {data['ltp']}, open: {data['open']}, high: {data['high']}, low: {data['low']}, close: {data['close']}")
                        
                    elif hasattr(message, 'to_market_data_message'):
                        # Fallback for legacy binary format
                        market_msg = message.to_market_data_message()
                        
                        symbol = message.get_symbol_string()
                        mode = message.message_type
                        
                        exchange_map = {1: 'NSE', 2: 'BSE', 3: 'MCX', 4: 'NFO'}
                        exchange = exchange_map.get(message.exchange_id, 'NSE')
                        
                        data = {
                            'symbol': symbol,
                            'exchange': exchange,
                            'price': message.get_price_float(),
                            'ltp': message.get_price_float(),
                            'volume': message.volume,
                            'timestamp': message.get_timestamp_seconds() * 1000,
                            'mode': mode,
                            # Default OHLC to 0 for legacy binary format
                            'open': 0,
                            'high': 0,
                            'low': 0,
                            'close': 0
                        }
                        
                        logger.debug(f"Legacy BinaryMarketData converted - symbol: {symbol}, exchange: {exchange}, mode: {mode}, price: {data['ltp']}")
                        
                    elif isinstance(message, dict):
                        # Handle case where message is already a dict (depth data or legacy)
                        data = message
                        symbol = data.get('symbol', '')
                        exchange = data.get('exchange', 'NSE')
                        mode = data.get('mode', 1)
                        logger.debug(f"Dict message - symbol: {symbol}, exchange: {exchange}, mode: {mode}")
                    else:
                        logger.warning(f"Unknown message type: {type(message)}")
                        continue
                    
                    # Create topic key using the actual exchange from the message
                    mode_str = self._get_mode_string(mode)
                    topic_key = f"{exchange}_{symbol}_{mode_str}"
                    
                    # Get clients subscribed to this topic
                    clients = self.subscriptions.get(topic_key, set())
                    
                    if not clients:
                        logger.debug(f"No clients subscribed to topic: {topic_key}")
                        continue

                    # Format the message for clients
                    formatted_msg = self._format_for_client(data, mode)
                    
                    logger.debug(f"Formatted message for clients: {formatted_msg}")
                    
                    # Add to batch queue for processing
                    self.message_queue.append((formatted_msg, clients))
                    self.metrics['messages_received'] += 1
                    
                    # Store last value for new subscribers
                    self.last_values[topic_key] = formatted_msg
                    
                    logger.debug(f"Queued message for {len(clients)} clients on topic: {topic_key}")
                    
                except Exception as e:
                    logger.error(f"Error formatting message: {e}")
                    logger.error(f"Message data: {data if 'data' in locals() else 'N/A'}")
                    continue

            except Exception as e:
                logger.error(f"Error in market data processor: {e}")
                await asyncio.sleep(1)

    def _get_mode_string(self, mode: int) -> str:
        """Convert mode integer to string like ZeroMQ implementation"""
        return {1: 'LTP', 2: 'QUOTE', 3: 'DEPTH'}.get(mode, 'UNKNOWN')

    def _format_for_client(self, data: dict, mode: int) -> dict:
        """
        Enhanced format message data for client compatibility - now preserves OHLC data from binary format.
        """
        symbol = data.get('symbol', '')
        exchange = data.get('exchange', 'NSE')
        timestamp = data.get('timestamp', int(time.time() * 1000))
        time_str = data.get('time', time.strftime('%H:%M:%S'))
        
        # Base result with common fields
        result = {
            'type': 'market_data',
            'exchange': exchange,
            'symbol': symbol,
            'mode': mode,
            'timestamp': timestamp,
            'time': time_str,
            'ltp': float(data.get('price', 0) or data.get('ltp', 0)),
            'volume': float(data.get('volume', 0)),
            'data': {}
        }
        
        # Add mode-specific data with enhanced OHLC support
        if mode == 1:  # LTP mode
            result['data'] = {
                'ltp': float(data.get('price', 0) or data.get('ltp', 0)),
                'last_traded_quantity': float(data.get('last_quantity', 0)),
                'average_traded_price': float(data.get('average_price', 0)),
                'volume_traded': float(data.get('volume', 0)),
                'total_buy_quantity': float(data.get('total_buy_quantity', 0)),
                'total_sell_quantity': float(data.get('total_sell_quantity', 0)),
                # Enhanced OHLC support from binary format
                'open': float(data.get('open', 0)),
                'high': float(data.get('high', 0)),
                'low': float(data.get('low', 0)),
                'close': float(data.get('close', 0)),
                'net_change': float(data.get('change', 0)),
                'percent_change': float(data.get('change_percent', 0))
            }
        elif mode == 2:  # QUOTE mode
            result['data'] = {
                'ltp': float(data.get('price', 0) or data.get('ltp', 0)),
                'last_traded_quantity': float(data.get('last_quantity', 0)),
                'average_traded_price': float(data.get('average_price', 0)),
                'volume_traded': float(data.get('volume', 0)),
                'total_buy_quantity': float(data.get('total_buy_quantity', 0)),
                'total_sell_quantity': float(data.get('total_sell_quantity', 0)),
                # Enhanced OHLC support - now properly extracted from binary data
                'open': float(data.get('open', 0)),
                'high': float(data.get('high', 0)),
                'low': float(data.get('low', 0)),
                'close': float(data.get('close', 0)),
                'net_change': float(data.get('change', 0)),
                'percent_change': float(data.get('change_percent', data.get('percent_change', 0))),
                # Bid/Ask data
                'best_bid': float(data.get('bid_price', data.get('best_bid', 0))),
                'best_ask': float(data.get('ask_price', data.get('best_ask', 0))),
                'best_bid_quantity': float(data.get('bid_quantity', data.get('bid_qty', 0))),
                'best_ask_quantity': float(data.get('ask_quantity', data.get('ask_qty', 0))),
                'upper_circuit': float(data.get('upper_circuit', 0)),
                'lower_circuit': float(data.get('lower_circuit', 0)),
                'oi': float(data.get('open_interest', 0)),
                'prev_close': float(data.get('prev_close', 0))
            }
        elif mode == 3:  # DEPTH mode
            depth_data = data.get('depth', {})
            bids = depth_data.get('buy', depth_data.get('bids', []))
            asks = depth_data.get('sell', depth_data.get('asks', []))
            
            result['data'] = {
                'ltp': float(data.get('price', 0) or data.get('ltp', 0)),
                'volume_traded': float(data.get('volume', 0)),
                # OHLC data for depth mode
                'open': float(data.get('open', 0)),
                'high': float(data.get('high', 0)),
                'low': float(data.get('low', 0)),
                'close': float(data.get('close', 0)),
                'depth': {
                    'bids': bids,
                    'asks': asks
                }
            }
        else:
            # For unknown modes, include all available data
            result['data'] = data
        
        return result

    
    async def _batch_processor(self):
        """Process message queue in batches with improved error handling and logging"""
        logger.debug("Starting batch processor")
        while not self.shutdown_event.is_set():
            try:
                if not self.message_queue:
                    await asyncio.sleep(0.001)
                    continue
                
                # Process batch
                batch_size = min(config.BATCH_SIZE, len(self.message_queue))
                batch = self.message_queue[:batch_size]
                del self.message_queue[:batch_size]
                
                if not batch:
                    continue
                
                # Log batch processing
                logger.debug(f"Processing batch of {len(batch)} messages")
                
                # Send messages with error handling per client
                success_count = 0
                error_count = 0
                
                for message_data, clients in batch:
                    if not isinstance(message_data, (dict, str)):
                        logger.warning(f"Skipping invalid message format: {type(message_data)}")
                        continue
                        
                    # Convert message to JSON string if it's a dict
                    try:
                        message_json = json.dumps(message_data) if isinstance(message_data, dict) else message_data
                    except (TypeError, ValueError) as e:
                        logger.error(f"Failed to serialize message: {e}, data: {message_data}")
                        continue
                    
                    # Process each client in the batch
                    for client in list(clients):  # Create a copy to avoid modification during iteration
                        if client not in self.clients:
                            logger.debug("Skipping disconnected client")
                            continue
                            
                        try:
                            # Send the message and track success/failure
                            success = await self._send_message(client, message_json)
                            if success:
                                success_count += 1
                            else:
                                error_count += 1
                                logger.warning(f"Failed to send message to client {id(client)}")
                                
                        except (websockets.exceptions.ConnectionClosed, ConnectionResetError) as e:
                            logger.debug(f"Client connection closed: {e}")
                            await self._close_connection(client)
                            error_count += 1
                            
                        except Exception as e:
                            logger.error(f"Error sending message to client {id(client)}: {e}", exc_info=True)
                            error_count += 1
                
                # Update metrics
                self.metrics['messages_sent'] += success_count
                if error_count > 0:
                    self.metrics['errors'] += error_count
                self.metrics['batch_processed'] += 1
                
                # Log batch completion
                if success_count > 0 or error_count > 0:
                    logger.debug(f"Batch processed: {success_count} messages sent, {error_count} errors")
                
            except asyncio.CancelledError:
                logger.info("Batch processor received cancellation signal")
                raise
                
            except Exception as e:
                logger.error(f"Unexpected error in batch processor: {e}", exc_info=True)
                await asyncio.sleep(1)  # Prevent tight loop on errors
    
    async def _close_connection(self, websocket):
        """Close WebSocket connection and cleanup following ZeroMQ pattern"""
        try:
            client = self.clients.get(websocket)
            if not client:
                return
            
            client_id = id(websocket)
            user_id = self.user_mapping.get(client_id)
            
            # Clean up all subscriptions for this client
            topics_to_unsubscribe = list(self.client_subscriptions.get(websocket, set()))
            for topic_key in topics_to_unsubscribe:
                # Remove from client's subscriptions
                self.client_subscriptions[websocket].discard(topic_key)
                
                # Remove client from topic's subscribers
                if topic_key in self.subscriptions:
                    self.subscriptions[topic_key].discard(websocket)
                    
                    # If no more subscribers, clean up the topic
                    if not self.subscriptions[topic_key]:
                        del self.subscriptions[topic_key]
                        
                        # Unsubscribe from broker if we have the necessary info
                        if user_id and user_id in self.broker_adapters and topic_key in self.symbol_meta:
                            try:
                                meta = self.symbol_meta[topic_key]
                                adapter = self.broker_adapters[user_id]
                                adapter.unsubscribe(
                                    meta['symbol'], 
                                    meta['exchange'], 
                                    meta['mode']
                                )
                                logger.info(f"Unsubscribed from broker for topic: {topic_key}")
                            except Exception as e:
                                logger.error(f"Error unsubscribing from broker for topic {topic_key}: {e}")
                        
                        # Clean up metadata if no more subscribers
                        if topic_key in self.symbol_meta:
                            del self.symbol_meta[topic_key]
                        if topic_key in self.last_values:
                            del self.last_values[topic_key]
            
            # Remove client from client_subscriptions
            if websocket in self.client_subscriptions:
                del self.client_subscriptions[websocket]
            
            # Clean up user mapping (same as ZeroMQ)
            user_id = self.user_mapping.get(client_id)
            if user_id:
                # Check if this was the last client for this user
                is_last_client = all(
                    other_user_id != user_id 
                    for other_client_id, other_user_id in self.user_mapping.items()
                    if other_client_id != client_id
                )
                
                # If last client, handle broker adapter cleanup
                if is_last_client and user_id in self.broker_adapters:
                    try:
                        adapter = self.broker_adapters[user_id]
                        broker_name = self.user_broker_mapping.get(user_id, 'unknown')
                        
                        # For Flattrade and Shoonya, keep connection alive but unsubscribe all
                        if broker_name in ['flattrade', 'shoonya'] and hasattr(adapter, 'unsubscribe_all'):
                            logger.info(f"{broker_name} adapter for user {user_id}: last client disconnected. Unsubscribing all symbols.")
                            adapter.unsubscribe_all()
                        else:
                            # For all other brokers, disconnect completely
                            logger.info(f"Last client for user {user_id} disconnected. Disconnecting {broker_name} adapter.")
                            if hasattr(adapter, 'disconnect'):
                                adapter.disconnect()
                            del self.broker_adapters[user_id]
                        
                        # Clean up user broker mapping
                        if user_id in self.user_broker_mapping:
                            del self.user_broker_mapping[user_id]
                            
                    except Exception as e:
                        logger.error(f"Error managing broker adapter for user {user_id}: {e}")
                
                # Clean up user mapping
                del self.user_mapping[client_id]
            
            # Remove from clients
            del self.clients[websocket]
            self.metrics['active_connections'] = len(self.clients)
            logger.info(f"Client cleaned up: {client['id']}")
            
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
    
    async def _send_message(self, websocket, data):
        """Send message to WebSocket client"""
        try:
            if isinstance(data, dict):
                message = json.dumps(data)
            else:
                message = data
            
            await websocket.send(message)
            return True
        except Exception as e:
            logger.debug(f"Failed to send message: {e}")
            return False
    
    async def _send_success(self, websocket, event: str, data: dict = None):
        """Send success response"""
        payload = {
            "type": "success",
            "event": event,
            "data": data or {},
            "ts": int(time.time())
        }
        await self._send_message(websocket, payload)
    
    async def _send_error(self, websocket, code: str, message: str):
        """Send error response"""
        payload = {
            "type": "error",
            "code": code,
            "message": message,
            "ts": int(time.time())
        }
        await self._send_message(websocket, payload)