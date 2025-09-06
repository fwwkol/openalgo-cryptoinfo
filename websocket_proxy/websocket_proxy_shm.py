import asyncio
import json
import logging
import os
import threading
import time
from collections import defaultdict, deque
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

class HighPerformanceWebSocketProxy:
    """Ultra-high-performance WebSocket proxy optimized for nanosecond-level market data"""

    def __init__(self):
        """Initialize WebSocket proxy with optimized architecture"""
        # Initialize shared memory ring buffer
        self.ring_buffer = OptimizedRingBuffer(
            name=config.SHM_BUFFER_NAME,
            size=config.SHM_BUFFER_SIZE,
            create=True
        )

        # Client management with performance optimizations
        self.clients: Dict[websockets.WebSocketServerProtocol, Dict[str, Any]] = {}
        self.subscriptions: Dict[str, Set[websockets.WebSocketServerProtocol]] = defaultdict(set)
        self.client_subscriptions: Dict[websockets.WebSocketServerProtocol, Set[str]] = defaultdict(set)
        self.symbol_meta: Dict[str, Dict[str, Any]] = {}
        
        # CRITICAL OPTIMIZATION: Remove centralized last_values storage
        # This was causing memory bottlenecks and serialization overhead
        
        # Broker adapter management
        self.broker_adapters = {}
        self.user_mapping = {}
        self.user_broker_mapping = {}

        # Performance metrics with more granular tracking
        self.metrics = {
            'messages_received': 0,
            'messages_sent': 0,
            'connection_count': 0,
            'active_connections': 0,
            'errors': 0,
            'processing_latency_ns': deque(maxlen=1000),  # Track latencies
            'queue_depth': 0,
            'dropped_messages': 0
        }

        # CRITICAL OPTIMIZATION: Direct delivery without queueing for real-time data
        self.direct_delivery_mode = getattr(config, 'DIRECT_DELIVERY_MODE', True)
        self.max_queue_size = getattr(config, 'MAX_QUEUE_SIZE', 100)
        self.batch_size = getattr(config, 'RING_BUFFER_BATCH_SIZE', 50)
        self.poll_interval = getattr(config, 'PROCESSING_POLL_INTERVAL_MS', 0.1) / 1000
        
        # Background tasks
        self.shutdown_event = asyncio.Event()
        self.market_data_task: Optional[asyncio.Task] = None
        
        # OPTIMIZATION: Use asyncio.Queue for better performance than list
        self.message_queue = asyncio.Queue(maxsize=self.max_queue_size)
        self.batch_processor_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the WebSocket proxy server with optimized startup"""
        logger.info("Starting High-Performance WebSocket Proxy (Shared Memory)")
        logger.info(f"Configuration: {config.to_dict()}")

        # Validate configuration
        if error := config.validate():
            logger.error(f"Invalid configuration: {error}")
            raise ValueError(f"Invalid configuration: {error}")

        try:
            # OPTIMIZATION: Start market data processor first to begin consuming immediately
            self.market_data_task = asyncio.create_task(self._process_market_data_optimized())
            
            # OPTIMIZATION: Start batch processor only if direct delivery is disabled
            if not self.direct_delivery_mode:
                self.batch_processor_task = asyncio.create_task(self._batch_processor_optimized())

            # Start WebSocket server with optimized settings
            self.server = await websockets.serve(
                self._handle_connection,
                config.WS_HOST,
                config.WS_PORT,
                ping_interval=config.WS_PING_INTERVAL,
                ping_timeout=config.WS_PING_TIMEOUT,
                # OPTIMIZATION: Increase max message size and disable compression for speed
                max_size=getattr(config, 'WS_MAX_MESSAGE_SIZE', 65536),  # 64KB max message size
                compression=getattr(config, 'WS_COMPRESSION', None)  # Disable compression for lowest latency
            )

            logger.info(f"High-performance WebSocket server started on {config.WS_HOST}:{config.WS_PORT}")
            logger.info(f"Direct delivery mode: {self.direct_delivery_mode}")
            logger.info(f"Batch size: {self.batch_size}, Poll interval: {self.poll_interval*1000}ms")

            # Wait for shutdown
            await self.shutdown_event.wait()

        except Exception as e:
            logger.error(f"Error starting WebSocket proxy: {e}")
            raise
        finally:
            await self.shutdown()

    async def _process_market_data_optimized(self):
        """
        CRITICAL OPTIMIZATION: Ultra-fast market data processing with direct delivery
        Eliminates queueing bottlenecks and minimizes processing overhead
        """
        logger.debug("Starting optimized market data processor with direct delivery")
        
        # Pre-allocate message format template to avoid repeated dict creation
        message_template = {
            'type': 'market_data',
            'topic': '',
            'symbol': '',
            'exchange': '',
            'mode': 0,
            'mode_str': '',
            'data': {},
            'timestamp': 0
        }
        
        # Mode mapping for faster lookup
        mode_map = {1: 'LTP', 2: 'QUOTE', 3: 'DEPTH'}
        
        while not self.shutdown_event.is_set():
            try:
                # OPTIMIZATION: Batch consume multiple messages at once
                messages = self.ring_buffer.consume_batch(max_messages=self.batch_size)
                
                if not messages:
                    # OPTIMIZATION: Reduced sleep time for faster polling
                    await asyncio.sleep(self.poll_interval)
                    continue

                start_time = time.perf_counter_ns()
                
                # OPTIMIZATION: Process messages in batch with pre-allocated structures
                processed_count = 0
                for message in messages:
                    try:
                        # Fast message parsing with minimal overhead
                        symbol, exchange, mode, data = self._parse_message_fast(message)
                        
                        if not symbol:
                            continue

                        # OPTIMIZATION: Direct topic key construction without string formatting
                        mode_str = mode_map.get(mode, 'LTP')
                        topic_key = f"{exchange}_{symbol}_{mode_str}"

                        # OPTIMIZATION: Direct client lookup without copy()
                        subscribed_clients = self.subscriptions.get(topic_key)
                        
                        if not subscribed_clients:
                            continue

                        # OPTIMIZATION: Reuse message template instead of creating new dict
                        message_template['topic'] = topic_key
                        message_template['symbol'] = symbol
                        message_template['exchange'] = exchange
                        message_template['mode'] = mode
                        message_template['mode_str'] = mode_str
                        message_template['data'] = data
                        message_template['timestamp'] = int(time.time() * 1000)

                        # CRITICAL OPTIMIZATION: Direct delivery to avoid queueing delays
                        if self.direct_delivery_mode:
                            await self._deliver_message_direct(message_template, subscribed_clients)
                        else:
                            # Fallback to queue only if direct delivery is disabled
                            try:
                                await self.message_queue.put((message_template.copy(), subscribed_clients.copy()))
                            except asyncio.QueueFull:
                                # OPTIMIZATION: Drop messages instead of blocking on full queue
                                self.metrics['dropped_messages'] += 1
                                logger.warning(f"Message queue full, dropping message for {topic_key}")

                        processed_count += 1
                        self.metrics['messages_received'] += 1

                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        self.metrics['errors'] += 1
                        continue

                # Track processing latency
                end_time = time.perf_counter_ns()
                latency_ns = end_time - start_time
                self.metrics['processing_latency_ns'].append(latency_ns)
                
                # Log performance every 1000 messages
                if self.metrics['messages_received'] % 1000 == 0:
                    avg_latency_us = sum(self.metrics['processing_latency_ns']) / len(self.metrics['processing_latency_ns']) / 1000
                    logger.debug(f"Processed {processed_count} messages, avg latency: {avg_latency_us:.2f}μs")

            except asyncio.CancelledError:
                logger.info("Market data processor received cancellation signal")
                raise
            except Exception as e:
                logger.error(f"Unexpected error in market data processor: {e}", exc_info=True)
                await asyncio.sleep(self.poll_interval)

    def _parse_message_fast(self, message) -> Tuple[str, str, int, dict]:
        """
        OPTIMIZATION: Fast message parsing with minimal overhead
        Returns (symbol, exchange, mode, data) tuple
        """
        try:
            # Handle BinaryMarketData format (optimized)
            if hasattr(message, 'to_normalized_data'):
                data = message.to_normalized_data()
                # FIX: Use the exchange from the normalized data directly
                return data['symbol'], data['exchange'], data['mode'], data
            
            elif hasattr(message, 'to_market_data_message'):
                # Optimized legacy binary format handling
                symbol = message.get_symbol_string()
                mode = message.message_type
                
                # FIX: Enhanced exchange mapping for index symbols
                exchange_map = {
                    0: 'NSE',
                    1: 'NSE', 
                    2: 'BSE', 
                    3: 'MCX', 
                    4: 'NFO',
                    # Add special handling for index symbols
                    5: 'NSE_INDEX',
                    6: 'BSE_INDEX'
                }
                
                # Check if this is an index symbol by token or symbol pattern
                if symbol in ['NIFTY', 'BANKNIFTY', 'SENSEX', 'BANKEX'] or 'INDEX' in symbol:
                    if message.exchange_id <= 1:  # NSE-based index
                        exchange = 'NSE_INDEX'
                    elif message.exchange_id == 2:  # BSE-based index  
                        exchange = 'BSE_INDEX'
                    else:
                        exchange = exchange_map.get(message.exchange_id, 'NSE')
                else:
                    exchange = exchange_map.get(message.exchange_id, 'NSE')
                
                # Fast exchange mapping with bounds checking
                exchange_map = ['', 'NSE', 'BSE', 'MCX', 'NFO']
                exchange = exchange_map[min(max(message.exchange_id, 0), 4)]
                
                # Pre-build data dict with essential fields only
                data = {
                    'symbol': symbol,
                    'exchange': exchange,
                    'ltp': message.get_price_float(),
                    'volume': message.volume,
                    'timestamp': message.get_timestamp_seconds() * 1000,
                    'mode': mode,
                    'open': message.get_open_price_float(),
                    'high': message.get_high_price_float(),
                    'low': message.get_low_price_float(),
                    'close': message.get_close_price_float(),
                    'bid_price': message.get_bid_price_float(),
                    'ask_price': message.get_ask_price_float()
                }
                
                return symbol, exchange, mode, data
                
            elif isinstance(message, dict):
                # Fast dict handling
                symbol = message.get('symbol', '')
                exchange = message.get('exchange', 'NSE')
                mode = message.get('mode', 1)
                return symbol, exchange, mode, message
                
            elif hasattr(message, 'to_dict'):
                # MarketDataMessage format
                data = message.to_dict()
                symbol = data.get('symbol', '')
                exchange = data.get('exchange', 'NSE')
                mode = data.get('type', 1)
                return symbol, exchange, mode, data
                
            else:
                logger.warning(f"Unknown message format: {type(message)}")
                return '', 'NSE', 1, {}
                
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            return '', 'NSE', 1, {}

    async def _deliver_message_direct(self, message_data: dict, clients: Set[websockets.WebSocketServerProtocol]):
        """
        CRITICAL OPTIMIZATION: Direct message delivery without queueing
        Uses asyncio.gather for concurrent delivery to all clients
        """
        if not clients:
            return

        # OPTIMIZATION: Pre-serialize message once for all clients
        try:
            message_json = json.dumps(message_data)
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize message: {e}")
            return

        # OPTIMIZATION: Create send tasks for concurrent delivery
        send_tasks = []
        valid_clients = []
        
        for client in clients:
            if client in self.clients:  # Quick validity check
                valid_clients.append(client)
                send_tasks.append(self._send_message_fast(client, message_json))

        if not send_tasks:
            return

        # CRITICAL: Use asyncio.gather for concurrent delivery to all clients
        try:
            results = await asyncio.gather(*send_tasks, return_exceptions=True)
            
            # Track metrics and handle failures
            success_count = 0
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    # Handle client disconnect
                    if isinstance(result, (websockets.exceptions.ConnectionClosed, ConnectionResetError)):
                        asyncio.create_task(self._close_connection(valid_clients[i]))
                    else:
                        logger.error(f"Error sending to client: {result}")
                        self.metrics['errors'] += 1
                else:
                    success_count += 1

            self.metrics['messages_sent'] += success_count
            
        except Exception as e:
            logger.error(f"Error in direct delivery: {e}")
            self.metrics['errors'] += 1

    async def _send_message_fast(self, websocket: websockets.WebSocketServerProtocol, message_json: str):
        """
        OPTIMIZATION: Ultra-fast message sending with minimal error handling overhead
        """
        try:
            await websocket.send(message_json)
            return True
        except (websockets.exceptions.ConnectionClosed, ConnectionResetError):
            # Let the caller handle connection cleanup
            raise
        except Exception as e:
            # Log and re-raise for caller to handle
            logger.debug(f"Send error: {e}")
            raise

    async def _batch_processor_optimized(self):
        """
        OPTIMIZATION: High-performance batch processor for fallback mode only
        Only used when direct_delivery_mode is False
        """
        logger.debug("Starting optimized batch processor (fallback mode)")
        
        while not self.shutdown_event.is_set():
            try:
                # OPTIMIZATION: Use asyncio.Queue with timeout for better performance
                try:
                    message_data, clients = await asyncio.wait_for(
                        self.message_queue.get(), 
                        timeout=self.poll_interval
                    )
                except asyncio.TimeoutError:
                    continue

                # OPTIMIZATION: Pre-serialize once
                try:
                    message_json = json.dumps(message_data) if isinstance(message_data, dict) else message_data
                except (TypeError, ValueError) as e:
                    logger.error(f"Failed to serialize message: {e}")
                    self.message_queue.task_done()
                    continue

                # OPTIMIZATION: Concurrent delivery to all clients in batch
                send_tasks = []
                valid_clients = []
                
                for client in clients:
                    if client in self.clients:
                        valid_clients.append(client)
                        send_tasks.append(self._send_message_fast(client, message_json))

                if send_tasks:
                    results = await asyncio.gather(*send_tasks, return_exceptions=True)
                    
                    # Process results efficiently
                    success_count = 0
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            if isinstance(result, (websockets.exceptions.ConnectionClosed, ConnectionResetError)):
                                asyncio.create_task(self._close_connection(valid_clients[i]))
                            else:
                                self.metrics['errors'] += 1
                        else:
                            success_count += 1

                    self.metrics['messages_sent'] += success_count

                # Mark queue task as done
                self.message_queue.task_done()

            except asyncio.CancelledError:
                logger.info("Batch processor received cancellation signal")
                raise
            except Exception as e:
                logger.error(f"Unexpected error in batch processor: {e}", exc_info=True)

    async def _handle_connection(self, websocket):
        """Handle WebSocket connection with optimized setup"""
        if len(self.clients) >= config.WS_MAX_CONNECTIONS:
            await self._send_error(websocket, "maximum_connections",
                                 f"Maximum connections reached ({config.WS_MAX_CONNECTIONS})")
            await websocket.close()
            return

        client_id = f"client-{id(websocket)}"
        
        # OPTIMIZATION: Minimal client state initialization
        self.clients[websocket] = {
            'id': client_id,
            'connected_at': time.time(),
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
                "auth_required": config.AUTH_REQUIRED,
                "direct_delivery": self.direct_delivery_mode
            })

            async for message in websocket:
                try:
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
        """Handle incoming WebSocket messages with optimized parsing"""
        try:
            client = self.clients.get(websocket)
            if not client:
                return

            data = json.loads(message)
            action = data.get('action')

            # OPTIMIZATION: Use dict dispatch for faster action handling
            action_handlers = {
                'ping': self._handle_ping,
                'authenticate': self._authenticate,
                'auth': self._authenticate,
                'subscribe': self._subscribe_optimized,
                'unsubscribe': self._unsubscribe_optimized,
                'list_subscriptions': self._list_subscriptions,
                'get_performance': self._get_performance_metrics
            }

            handler = action_handlers.get(action)
            if handler:
                await handler(websocket, data)
            else:
                await self._send_error(websocket, 'invalid_action', f'Unknown action: {action}')

        except json.JSONDecodeError:
            await self._send_error(websocket, 'invalid_json', 'Invalid JSON received')
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            await self._send_error(websocket, 'processing_error', str(e))

    async def _handle_ping(self, websocket: websockets.WebSocketServerProtocol, data: dict):
        """Handle ping with timestamp for latency measurement"""
        client_timestamp = data.get('timestamp', 0)
        server_timestamp = int(time.time() * 1000)
        
        await self._send_success(websocket, 'pong', {
            'client_timestamp': client_timestamp,
            'server_timestamp': server_timestamp,
            'latency_ms': server_timestamp - client_timestamp if client_timestamp else 0
        })

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

    async def _subscribe_optimized(self, websocket: websockets.WebSocketServerProtocol, data: dict):
        """OPTIMIZATION: High-speed subscription with minimal overhead"""
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

        # OPTIMIZATION: Fast topic key generation
        mode_str = {1: 'LTP', 2: 'QUOTE', 3: 'DEPTH'}.get(mode, 'LTP')
        topic_key = f"{exchange}_{symbol}_{mode_str}"

        # OPTIMIZATION: Direct set operations without checking membership first
        self.client_subscriptions[websocket].add(topic_key)
        self.subscriptions[topic_key].add(websocket)

        # OPTIMIZATION: Minimal metadata storage
        if topic_key not in self.symbol_meta:
            self.symbol_meta[topic_key] = {
                'exchange': exchange,
                'symbol': symbol,
                'mode': mode,
                'mode_str': mode_str
            }

        # Broker subscription (unchanged)
        client_id = id(websocket)
        user_id = self.user_mapping.get(client_id)
        if user_id and user_id in self.broker_adapters:
            try:
                adapter = self.broker_adapters[user_id]
                result = adapter.subscribe(symbol, exchange, mode)
                logger.debug(f"Broker subscription for {exchange}:{symbol} mode={mode}: {result}")
            except Exception as e:
                logger.error(f"Broker subscribe error for {symbol}: {e}")

        logger.info(f"Client {client['id']} subscribed to topic: {topic_key}")

        await self._send_success(websocket, 'subscribed', {
            "symbol": symbol,
            "exchange": exchange,
            "mode": mode,
            "topic": topic_key,
            "direct_delivery": self.direct_delivery_mode
        })

    async def _unsubscribe_optimized(self, websocket: websockets.WebSocketServerProtocol, data: dict):
        """OPTIMIZATION: High-speed unsubscription"""
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

        # OPTIMIZATION: Fast topic key generation
        mode_str = {1: 'LTP', 2: 'QUOTE', 3: 'DEPTH'}.get(mode, 'LTP')
        topic_key = f"{exchange}_{symbol}_{mode_str}"

        # OPTIMIZATION: Direct set operations
        self.client_subscriptions[websocket].discard(topic_key)
        self.subscriptions[topic_key].discard(websocket)

        # Clean up empty topic subscriptions
        if not self.subscriptions[topic_key]:
            del self.subscriptions[topic_key]
            # Clean up metadata
            if topic_key in self.symbol_meta:
                del self.symbol_meta[topic_key]

        # Broker unsubscription (unchanged)
        client_id = id(websocket)
        user_id = self.user_mapping.get(client_id)
        if user_id and user_id in self.broker_adapters:
            try:
                adapter = self.broker_adapters[user_id]
                result = adapter.unsubscribe(symbol, exchange, mode)
                logger.debug(f"Broker unsubscription for {topic_key}: {result}")
            except Exception as e:
                logger.error(f"Broker unsubscribe error for {topic_key}: {e}")

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

    async def _get_performance_metrics(self, websocket: websockets.WebSocketServerProtocol, data: dict):
        """Get real-time performance metrics"""
        avg_latency_ns = 0
        if self.metrics['processing_latency_ns']:
            avg_latency_ns = sum(self.metrics['processing_latency_ns']) / len(self.metrics['processing_latency_ns'])

        metrics = {
            'messages_received': self.metrics['messages_received'],
            'messages_sent': self.metrics['messages_sent'],
            'active_connections': self.metrics['active_connections'],
            'errors': self.metrics['errors'],
            'dropped_messages': self.metrics['dropped_messages'],
            'avg_latency_ns': avg_latency_ns,
            'avg_latency_us': avg_latency_ns / 1000,
            'queue_depth': self.message_queue.qsize() if not self.direct_delivery_mode else 0,
            'total_subscriptions': sum(len(subs) for subs in self.subscriptions.values()),
            'direct_delivery_mode': self.direct_delivery_mode,
            'batch_size': self.batch_size,
            'poll_interval_ms': self.poll_interval * 1000
        }

        await self._send_success(websocket, 'performance_metrics', metrics)

    async def _close_connection(self, websocket):
        """OPTIMIZATION: Fast connection cleanup with minimal broker impact"""
        try:
            client = self.clients.get(websocket)
            if not client:
                return

            client_id = id(websocket)
            user_id = self.user_mapping.get(client_id)

            # OPTIMIZATION: Batch unsubscribe from broker to reduce API calls
            if user_id and user_id in self.broker_adapters:
                topics_to_unsubscribe = list(self.client_subscriptions.get(websocket, set()))
                
                if topics_to_unsubscribe:
                    try:
                        adapter = self.broker_adapters[user_id]
                        
                        # OPTIMIZATION: Group unsubscriptions by symbol for batch processing
                        unsubscribe_batch = []
                        for topic_key in topics_to_unsubscribe:
                            meta = self.symbol_meta.get(topic_key, {})
                            if meta:
                                unsubscribe_batch.append((meta['symbol'], meta['exchange'], meta['mode']))
                        
                        # OPTIMIZATION: Use batch unsubscribe if available
                        if hasattr(adapter, 'unsubscribe_batch') and unsubscribe_batch:
                            adapter.unsubscribe_batch(unsubscribe_batch)
                            logger.info(f"Batch unsubscribed {len(unsubscribe_batch)} symbols for user {user_id}")
                        else:
                            # Fallback to individual unsubscribes
                            for symbol, exchange, mode in unsubscribe_batch:
                                adapter.unsubscribe(symbol, exchange, mode)
                            
                    except Exception as e:
                        logger.error(f"Error batch unsubscribing for user {user_id}: {e}")

            # OPTIMIZATION: Fast cleanup of client subscriptions
            topics_to_cleanup = list(self.client_subscriptions.get(websocket, set()))
            
            for topic_key in topics_to_cleanup:
                # Remove client from topic subscribers
                if topic_key in self.subscriptions:
                    self.subscriptions[topic_key].discard(websocket)
                    
                    # Clean up empty topics
                    if not self.subscriptions[topic_key]:
                        del self.subscriptions[topic_key]
                        if topic_key in self.symbol_meta:
                            del self.symbol_meta[topic_key]

            # Remove client subscriptions
            if websocket in self.client_subscriptions:
                del self.client_subscriptions[websocket]

            # Clean up user mapping and broker management
            if user_id:
                # Check if this was the last client for this user
                is_last_client = all(
                    other_user_id != user_id
                    for other_client_id, other_user_id in self.user_mapping.items()
                    if other_client_id != client_id
                )

                # Handle broker adapter cleanup for last client
                if is_last_client and user_id in self.broker_adapters:
                    try:
                        adapter = self.broker_adapters[user_id]
                        broker_name = self.user_broker_mapping.get(user_id, 'unknown')

                        # Keep connection alive for Flattrade and Shoonya
                        if broker_name in ['flattrade', 'shoonya'] and hasattr(adapter, 'unsubscribe_all'):
                            logger.info(f"{broker_name} adapter for user {user_id}: last client disconnected. Unsubscribing all symbols.")
                            adapter.unsubscribe_all()
                        else:
                            # Disconnect other brokers
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

    async def shutdown(self):
        """OPTIMIZATION: Fast shutdown process"""
        logger.info("Shutting down high-performance WebSocket proxy...")

        # Signal shutdown
        self.shutdown_event.set()

        # Cancel background tasks
        tasks_to_cancel = [self.market_data_task]
        if not self.direct_delivery_mode:
            tasks_to_cancel.append(self.batch_processor_task)
            
        for task in tasks_to_cancel:
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

        # OPTIMIZATION: Concurrent client disconnection
        close_tasks = []
        for websocket in list(self.clients.keys()):
            close_tasks.append(self._close_connection(websocket))

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

        # Disconnect all broker adapters
        for user_id, adapter in self.broker_adapters.items():
            try:
                adapter.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting adapter for user {user_id}: {e}")

        # Close shared memory
        if self.ring_buffer:
            self.ring_buffer.close()

        logger.info("High-performance WebSocket proxy shutdown complete")

    async def _send_message(self, websocket, data):
        """Send message to WebSocket client with optimization"""
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


# OPTIMIZATION: Use the optimized class as default
WebSocketProxy = HighPerformanceWebSocketProxy


# Entry point for running the proxy
async def main():
    """Main entry point for high-performance WebSocket proxy"""
    proxy = HighPerformanceWebSocketProxy()
    await proxy.start()


if __name__ == "__main__":
    # OPTIMIZATION: Set optimal asyncio event loop policy for performance
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())