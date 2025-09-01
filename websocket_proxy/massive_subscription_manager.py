"""
Massive Subscription Manager for handling thousands of symbols efficiently.
Manages multiple connections with intelligent batching and rate limiting.
"""

import time
import threading
import logging
from typing import List, Dict, Optional, Callable, Any, Set
from dataclasses import dataclass, field
from collections import defaultdict
import asyncio
from concurrent.futures import ThreadPoolExecutor

from .symbol_partitioner import SymbolPartitioner
from .optimized_ring_buffer import OptimizedRingBufferPool

logger = logging.getLogger(__name__)


@dataclass
class ConnectionStats:
    """Statistics for a single connection."""
    connection_id: int
    symbols_subscribed: int = 0
    symbols_failed: int = 0
    messages_received: int = 0
    last_message_time: float = 0.0
    connection_status: str = "disconnected"
    error_count: int = 0
    start_time: float = field(default_factory=time.time)
    rate_limit_hits: int = 0
    
    def get_success_rate(self) -> float:
        """Get subscription success rate."""
        total = self.symbols_subscribed + self.symbols_failed
        return (self.symbols_subscribed / total) if total > 0 else 0.0
    
    def get_uptime(self) -> float:
        """Get connection uptime in seconds."""
        return time.time() - self.start_time


@dataclass
class SubscriptionConfig:
    """Configuration for subscription management."""
    max_symbols_per_connection: int = 1000
    batch_size: int = 50
    batch_delay: float = 0.01  # Delay between batches
    connection_timeout: float = 30.0
    retry_attempts: int = 3
    retry_delay: float = 1.0
    rate_limit_delay: float = 0.1
    health_check_interval: float = 60.0
    broker_name: str = "flattrade"
    default_mode: int = 1  # LTP mode for minimal bandwidth


class MassiveSubscriptionManager:
    """
    Manages massive symbol subscriptions across multiple connections.
    Handles rate limiting, connection management, and fault tolerance.
    """
    
    def __init__(self, config: Optional[SubscriptionConfig] = None):
        """
        Initialize massive subscription manager.
        
        Args:
            config: Subscription configuration (uses defaults if None)
        """
        self.config = config or SubscriptionConfig()
        
        # Connection management
        self.connections: List[Any] = []  # FlattradeSHMWebSocketAdapter instances
        self.connection_stats: Dict[int, ConnectionStats] = {}
        self.symbol_to_connection: Dict[str, int] = {}  # symbol_key -> connection_id
        
        # Subscription tracking
        self.subscribed_symbols: Set[str] = set()
        self.failed_symbols: Set[str] = set()
        self.pending_symbols: Set[str] = set()
        
        # Threading and async management
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="SubManager")
        self.health_check_thread: Optional[threading.Thread] = None
        self.running = False
        
        # Statistics and monitoring
        self.stats_lock = threading.Lock()
        self.total_subscription_time = 0.0
        self.start_time = time.time()
        
        # Buffer pool for data distribution
        self.buffer_pool: Optional[OptimizedRingBufferPool] = None
        
        logger.info(f"Initialized MassiveSubscriptionManager with config: {self.config}")
    
    def initialize_buffer_pool(self, num_buffers: int = None, buffer_size: int = None):
        """
        Initialize buffer pool for data distribution.
        
        Args:
            num_buffers: Number of buffers (defaults to number of connections)
            buffer_size: Size of each buffer in bytes
        """
        if num_buffers is None:
            num_buffers = max(4, len(self.connections))
        
        if buffer_size is None:
            buffer_size = 2 * 1024 * 1024  # 2MB default
        
        self.buffer_pool = OptimizedRingBufferPool(
            num_buffers=num_buffers,
            buffer_size=buffer_size
        )
        
        logger.info(f"Initialized buffer pool with {num_buffers} buffers")
    
    def subscribe_bulk(self, symbols: List[Dict], auth_data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Handle thousands of symbols efficiently with intelligent batching.
        
        Args:
            symbols: List of symbol dictionaries with 'symbol' and 'exchange' keys
            auth_data: Authentication data for broker connections
            
        Returns:
            Dict: Subscription results and statistics
        """
        if not symbols:
            logger.warning("No symbols provided for bulk subscription")
            return {"success": False, "error": "No symbols provided"}
        
        logger.info(f"Starting bulk subscription for {len(symbols)} symbols")
        start_time = time.time()
        
        try:
            # Validate symbols
            validated_symbols = self._validate_symbols(symbols)
            if not validated_symbols:
                return {"success": False, "error": "No valid symbols found"}
            
            # Group symbols by connection capacity
            symbol_groups = self._group_symbols(validated_symbols)
            
            # Initialize buffer pool if not already done
            if self.buffer_pool is None:
                self.initialize_buffer_pool(num_buffers=len(symbol_groups))
            
            # Create connections as needed
            self._ensure_connections(len(symbol_groups), auth_data)
            
            # Start health monitoring
            self._start_health_monitoring()
            
            # Subscribe to symbols in parallel
            subscription_results = self._subscribe_parallel(symbol_groups)
            
            # Calculate final statistics
            end_time = time.time()
            self.total_subscription_time = end_time - start_time
            
            results = {
                "success": True,
                "total_symbols": len(validated_symbols),
                "subscribed": len(self.subscribed_symbols),
                "failed": len(self.failed_symbols),
                "connections_used": len(self.connections),
                "subscription_time": self.total_subscription_time,
                "symbols_per_second": len(validated_symbols) / self.total_subscription_time,
                "connection_stats": {i: stats.__dict__ for i, stats in self.connection_stats.items()},
                "detailed_results": subscription_results
            }
            
            logger.info(f"Bulk subscription completed: {results['subscribed']}/{results['total_symbols']} "
                       f"symbols in {results['subscription_time']:.2f}s")
            
            return results
            
        except Exception as e:
            logger.error(f"Bulk subscription failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _validate_symbols(self, symbols: List[Dict]) -> List[Dict]:
        """
        Validate and deduplicate symbols.
        
        Args:
            symbols: List of symbol dictionaries
            
        Returns:
            List of validated symbols
        """
        validated = []
        seen_symbols = set()
        
        for i, symbol_info in enumerate(symbols):
            try:
                if not isinstance(symbol_info, dict):
                    logger.warning(f"Symbol at index {i} is not a dictionary")
                    continue
                
                if 'symbol' not in symbol_info or 'exchange' not in symbol_info:
                    logger.warning(f"Symbol at index {i} missing required keys")
                    continue
                
                symbol = symbol_info['symbol'].strip()
                exchange = symbol_info['exchange'].strip()
                
                if not symbol or not exchange:
                    logger.warning(f"Symbol at index {i} has empty values")
                    continue
                
                # Create unique key
                symbol_key = f"{exchange}:{symbol}"
                
                if symbol_key in seen_symbols:
                    logger.debug(f"Duplicate symbol skipped: {symbol_key}")
                    continue
                
                seen_symbols.add(symbol_key)
                validated.append({
                    'symbol': symbol,
                    'exchange': exchange,
                    'mode': symbol_info.get('mode', self.config.default_mode),
                    'symbol_key': symbol_key
                })
                
            except Exception as e:
                logger.warning(f"Error validating symbol at index {i}: {e}")
                continue
        
        logger.info(f"Validated {len(validated)} symbols from {len(symbols)} input symbols")
        return validated
    
    def _group_symbols(self, symbols: List[Dict]) -> List[List[Dict]]:
        """
        Group symbols into connection-sized chunks with load balancing.
        
        Args:
            symbols: List of validated symbols
            
        Returns:
            List of symbol groups
        """
        # Use symbol partitioner for better distribution
        partitioner = SymbolPartitioner(
            num_partitions=max(1, len(symbols) // self.config.max_symbols_per_connection + 1)
        )
        
        partitions = partitioner.distribute_symbols(symbols)
        
        # Convert partitions to groups, ensuring no group exceeds max size
        groups = []
        for partition_symbols in partitions.values():
            # Split large partitions into smaller groups
            for i in range(0, len(partition_symbols), self.config.max_symbols_per_connection):
                group = partition_symbols[i:i + self.config.max_symbols_per_connection]
                groups.append(group)
        
        logger.info(f"Grouped {len(symbols)} symbols into {len(groups)} connection groups")
        return groups
    
    def _ensure_connections(self, num_groups: int, auth_data: Optional[Dict]):
        """
        Ensure we have enough connections for all symbol groups.
        
        Args:
            num_groups: Number of symbol groups
            auth_data: Authentication data
        """
        while len(self.connections) < num_groups:
            connection_id = len(self.connections)
            
            try:
                # Import here to avoid circular imports
                from broker.flattrade.streaming.flattrade_adapter_shm import FlattradeSHMWebSocketAdapter
                
                buffer_name = f"bulk_buffer_{connection_id}"
                adapter = FlattradeSHMWebSocketAdapter(buffer_name)
                adapter.initialize(
                    self.config.broker_name, 
                    f"bulk_user_{connection_id}", 
                    auth_data
                )
                
                # Connect with timeout
                if adapter.connect():
                    self.connections.append(adapter)
                    self.connection_stats[connection_id] = ConnectionStats(connection_id=connection_id)
                    self.connection_stats[connection_id].connection_status = "connected"
                    
                    logger.info(f"Created connection {connection_id}")
                else:
                    raise Exception("Failed to connect to broker")
                    
            except Exception as e:
                logger.error(f"Failed to create connection {connection_id}: {e}")
                # Create placeholder for failed connection
                self.connections.append(None)
                self.connection_stats[connection_id] = ConnectionStats(connection_id=connection_id)
                self.connection_stats[connection_id].connection_status = "failed"
                self.connection_stats[connection_id].error_count += 1
    
    def _subscribe_parallel(self, symbol_groups: List[List[Dict]]) -> List[Dict]:
        """
        Subscribe to symbol groups in parallel.
        
        Args:
            symbol_groups: List of symbol groups
            
        Returns:
            List of subscription results per group
        """
        futures = []
        
        # Submit subscription tasks to thread pool
        for group_id, symbol_group in enumerate(symbol_groups):
            if group_id < len(self.connections) and self.connections[group_id] is not None:
                future = self.executor.submit(
                    self._subscribe_group,
                    group_id,
                    symbol_group
                )
                futures.append((group_id, future))
        
        # Collect results
        results = []
        for group_id, future in futures:
            try:
                result = future.result(timeout=self.config.connection_timeout)
                results.append(result)
            except Exception as e:
                logger.error(f"Group {group_id} subscription failed: {e}")
                results.append({
                    "group_id": group_id,
                    "success": False,
                    "error": str(e),
                    "subscribed": 0,
                    "failed": len(symbol_groups[group_id]) if group_id < len(symbol_groups) else 0
                })
        
        return results
    
    def _subscribe_group(self, group_id: int, symbols: List[Dict]) -> Dict[str, Any]:
        """
        Subscribe to a group of symbols on a specific connection.
        
        Args:
            group_id: Connection group ID
            symbols: List of symbols to subscribe
            
        Returns:
            Dict: Subscription results for this group
        """
        connection = self.connections[group_id]
        stats = self.connection_stats[group_id]
        
        if connection is None:
            logger.error(f"No connection available for group {group_id}")
            return {
                "group_id": group_id,
                "success": False,
                "error": "No connection available",
                "subscribed": 0,
                "failed": len(symbols)
            }
        
        logger.info(f"Subscribing group {group_id} with {len(symbols)} symbols")
        
        subscribed_count = 0
        failed_count = 0
        
        # Subscribe in batches to avoid rate limits
        for i in range(0, len(symbols), self.config.batch_size):
            batch = symbols[i:i + self.config.batch_size]
            
            for symbol_info in batch:
                try:
                    success = connection.subscribe(
                        symbol_info['symbol'],
                        symbol_info['exchange'],
                        mode=symbol_info.get('mode', self.config.default_mode)
                    )
                    
                    if success:
                        subscribed_count += 1
                        with self.stats_lock:
                            self.subscribed_symbols.add(symbol_info['symbol_key'])
                            self.symbol_to_connection[symbol_info['symbol_key']] = group_id
                    else:
                        failed_count += 1
                        with self.stats_lock:
                            self.failed_symbols.add(symbol_info['symbol_key'])
                        
                        stats.symbols_failed += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to subscribe to {symbol_info}: {e}")
                    failed_count += 1
                    stats.error_count += 1
                    
                    with self.stats_lock:
                        self.failed_symbols.add(symbol_info['symbol_key'])
            
            # Rate limiting delay between batches
            if i + self.config.batch_size < len(symbols):
                time.sleep(self.config.batch_delay)
        
        # Update connection stats
        stats.symbols_subscribed += subscribed_count
        stats.symbols_failed += failed_count
        
        result = {
            "group_id": group_id,
            "success": True,
            "subscribed": subscribed_count,
            "failed": failed_count,
            "total": len(symbols),
            "success_rate": subscribed_count / len(symbols) if symbols else 0.0
        }
        
        logger.info(f"Group {group_id} completed: {subscribed_count}/{len(symbols)} symbols subscribed")
        return result
    
    def unsubscribe_bulk(self, symbols: List[Dict]) -> Dict[str, Any]:
        """
        Unsubscribe from multiple symbols.
        
        Args:
            symbols: List of symbols to unsubscribe
            
        Returns:
            Dict: Unsubscription results
        """
        logger.info(f"Starting bulk unsubscription for {len(symbols)} symbols")
        
        unsubscribed_count = 0
        failed_count = 0
        
        for symbol_info in symbols:
            try:
                symbol_key = f"{symbol_info['exchange']}:{symbol_info['symbol']}"
                
                if symbol_key in self.symbol_to_connection:
                    connection_id = self.symbol_to_connection[symbol_key]
                    connection = self.connections[connection_id]
                    
                    if connection is not None:
                        success = connection.unsubscribe(
                            symbol_info['symbol'],
                            symbol_info['exchange'],
                            mode=symbol_info.get('mode', self.config.default_mode)
                        )
                        
                        if success:
                            unsubscribed_count += 1
                            with self.stats_lock:
                                self.subscribed_symbols.discard(symbol_key)
                                del self.symbol_to_connection[symbol_key]
                        else:
                            failed_count += 1
                    else:
                        failed_count += 1
                else:
                    logger.warning(f"Symbol {symbol_key} not found in subscriptions")
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to unsubscribe from {symbol_info}: {e}")
                failed_count += 1
        
        return {
            "success": True,
            "unsubscribed": unsubscribed_count,
            "failed": failed_count,
            "total": len(symbols)
        }
    
    def get_subscription_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive subscription statistics.
        
        Returns:
            Dict: Subscription statistics
        """
        with self.stats_lock:
            total_subscribed = len(self.subscribed_symbols)
            total_failed = len(self.failed_symbols)
            total_pending = len(self.pending_symbols)
        
        # Calculate connection statistics
        active_connections = sum(1 for stats in self.connection_stats.values() 
                               if stats.connection_status == "connected")
        
        total_messages = sum(stats.messages_received for stats in self.connection_stats.values())
        
        return {
            "total_subscribed": total_subscribed,
            "total_failed": total_failed,
            "total_pending": total_pending,
            "active_connections": active_connections,
            "total_connections": len(self.connections),
            "total_messages_received": total_messages,
            "uptime": time.time() - self.start_time,
            "subscription_time": self.total_subscription_time,
            "connection_details": {
                i: {
                    "symbols_subscribed": stats.symbols_subscribed,
                    "symbols_failed": stats.symbols_failed,
                    "messages_received": stats.messages_received,
                    "connection_status": stats.connection_status,
                    "error_count": stats.error_count,
                    "success_rate": stats.get_success_rate(),
                    "uptime": stats.get_uptime()
                }
                for i, stats in self.connection_stats.items()
            }
        }
    
    def _start_health_monitoring(self):
        """Start health monitoring thread."""
        if self.health_check_thread is None or not self.health_check_thread.is_alive():
            self.running = True
            self.health_check_thread = threading.Thread(
                target=self._health_monitor_loop,
                daemon=True,
                name="SubscriptionHealthMonitor"
            )
            self.health_check_thread.start()
            logger.info("Started health monitoring")
    
    def _health_monitor_loop(self):
        """Health monitoring loop."""
        while self.running:
            try:
                # Check connection health
                for connection_id, connection in enumerate(self.connections):
                    if connection is not None:
                        stats = self.connection_stats[connection_id]
                        
                        # Update connection status based on adapter state
                        if hasattr(connection, 'is_connected') and connection.is_connected():
                            stats.connection_status = "connected"
                        else:
                            stats.connection_status = "disconnected"
                            stats.error_count += 1
                
                # Sleep until next check
                time.sleep(self.config.health_check_interval)
                
            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")
                time.sleep(5.0)
    
    def shutdown(self):
        """Shutdown all connections and cleanup resources."""
        logger.info("Shutting down MassiveSubscriptionManager...")
        
        self.running = False
        
        # Stop health monitoring
        if self.health_check_thread and self.health_check_thread.is_alive():
            self.health_check_thread.join(timeout=5.0)
        
        # Disconnect all connections
        for i, connection in enumerate(self.connections):
            if connection is not None:
                try:
                    connection.disconnect()
                    logger.debug(f"Disconnected connection {i}")
                except Exception as e:
                    logger.warning(f"Error disconnecting connection {i}: {e}")
        
        # Cleanup buffer pool
        if self.buffer_pool:
            try:
                self.buffer_pool.cleanup()
            except Exception as e:
                logger.warning(f"Error cleaning up buffer pool: {e}")
        
        # Shutdown executor
        self.executor.shutdown(wait=True, timeout=10.0)
        
        logger.info("MassiveSubscriptionManager shutdown complete")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.shutdown()


def test_massive_subscription_manager():
    """Test the massive subscription manager."""
    print("Testing Massive Subscription Manager...")
    
    # Create test symbols
    symbols = []
    for i in range(500):
        symbols.append({
            "symbol": f"STOCK{i:04d}",
            "exchange": "NSE" if i % 2 == 0 else "BSE",
            "mode": 1
        })
    
    # Test configuration
    config = SubscriptionConfig(
        max_symbols_per_connection=100,
        batch_size=25,
        batch_delay=0.005
    )
    
    # Test manager
    with MassiveSubscriptionManager(config) as manager:
        print(f"Created manager with config: {config}")
        
        # Note: This would normally connect to real broker
        # For demo, we'll just show the setup
        print(f"Would subscribe to {len(symbols)} symbols")
        
        # Show symbol grouping
        validated_symbols = manager._validate_symbols(symbols)
        groups = manager._group_symbols(validated_symbols)
        
        print(f"Validated {len(validated_symbols)} symbols")
        print(f"Grouped into {len(groups)} connection groups")
        
        for i, group in enumerate(groups[:3]):  # Show first 3 groups
            print(f"Group {i}: {len(group)} symbols")
        
        # In real usage:
        # results = manager.subscribe_bulk(symbols, auth_data={"token": "abc123"})
        print("(In production, would perform actual broker subscriptions)")


if __name__ == "__main__":
    test_massive_subscription_manager()