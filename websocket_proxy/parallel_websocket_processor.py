"""
Parallel WebSocket Processor for handling massive symbol subscriptions.
Distributes symbols across multiple processes to eliminate bottlenecks.
"""

import multiprocessing as mp
import threading
import time
import logging
import signal
import os
import queue
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Optional, Callable, Any, Tuple
from dataclasses import dataclass, field

from .symbol_partitioner import SymbolPartitioner
from .optimized_ring_buffer import OptimizedRingBufferPool
from .binary_market_data import BinaryMarketData

logger = logging.getLogger(__name__)


@dataclass
class ProcessorStats:
    """Statistics for a processor instance."""
    partition_id: int
    symbols_count: int
    messages_processed: int = 0
    messages_per_second: float = 0.0
    last_message_time: float = 0.0
    connection_status: str = "disconnected"
    error_count: int = 0
    start_time: float = field(default_factory=time.time)
    buffer_utilization: float = 0.0
    
    def update_throughput(self, message_count: int):
        """Update throughput statistics."""
        current_time = time.time()
        if self.last_message_time > 0:
            time_diff = current_time - self.last_message_time
            if time_diff > 0:
                self.messages_per_second = message_count / time_diff
        
        self.messages_processed += message_count
        self.last_message_time = current_time
    
    def get_uptime(self) -> float:
        """Get processor uptime in seconds."""
        return time.time() - self.start_time


@dataclass
class ProcessorConfig:
    """Configuration for a processor instance."""
    partition_id: int
    symbols: List[Dict]
    buffer_name: str
    broker_name: str = "flattrade"
    user_id: str = "default_user"
    max_symbols_per_connection: int = 1000
    reconnect_delay: float = 5.0
    heartbeat_interval: float = 30.0
    batch_size: int = 100
    auth_data: Optional[Dict] = None 


class ParallelWebSocketProcessor:
    """
    Manages multiple WebSocket processors for handling massive symbol subscriptions.
    Distributes load across processes to prevent bottlenecks and ensure scalability.
    """
    
    def __init__(self, num_processes: int = 4, max_symbols_per_process: int = 2000):
        """
        Initialize parallel WebSocket processor.
        
        Args:
            num_processes: Number of parallel processes to spawn
            max_symbols_per_process: Maximum symbols per process
        """
        self.num_processes = num_processes
        self.max_symbols_per_process = max_symbols_per_process
        
        # Core components
        self.symbol_partitioner = SymbolPartitioner(num_processes)
        self.ring_buffer_pool = OptimizedRingBufferPool(num_processes * 2)  # Extra buffers for load balancing
        
        # Process management
        self.executor: Optional[ProcessPoolExecutor] = None
        self.process_futures: Dict[int, Any] = {}
        self.processor_stats: Dict[int, ProcessorStats] = {}
        self.shutdown_event = mp.Event()
        
        # Monitoring
        self.stats_lock = threading.Lock()
        self.monitor_thread: Optional[threading.Thread] = None
        self.running = False
        
        logger.info(f"Initialized ParallelWebSocketProcessor with {num_processes} processes")
    
    def start_processors(self, all_symbols: List[Dict], broker_name: str = "flattrade", 
                        auth_data: Optional[Dict] = None) -> bool:
        """
        Start all processors with symbol distribution.
        
        Args:
            all_symbols: List of all symbols to process
            broker_name: Broker adapter name
            auth_data: Authentication data for broker
            
        Returns:
            bool: True if all processors started successfully
        """
        try:
            # Validate input
            if not all_symbols:
                logger.error("No symbols provided")
                return False
            
            if len(all_symbols) > self.num_processes * self.max_symbols_per_process:
                logger.warning(f"Symbol count ({len(all_symbols)}) exceeds recommended capacity")
            
            # Partition symbols across processes
            partitions = self.symbol_partitioner.distribute_symbols(all_symbols)
            
            if not partitions:
                logger.error("Symbol partitioning failed")
                return False
            
            # Start process executor
            self.executor = ProcessPoolExecutor(
                max_workers=self.num_processes,
                mp_context=mp.get_context('spawn')  # Use spawn for better isolation
            )
            
            # Start each processor
            for partition_id, symbols in partitions.items():
                config = ProcessorConfig(
                    partition_id=partition_id,
                    symbols=symbols,
                    buffer_name=f"market_data_{partition_id}",
                    broker_name=broker_name,
                    user_id=f"user_{partition_id}",
                    auth_data=auth_data
                )
                
                # Submit processor task
                future = self.executor.submit(
                    _run_processor_worker,
                    config,
                    self.shutdown_event
                )
                
                self.process_futures[partition_id] = future
                
                # Initialize stats
                self.processor_stats[partition_id] = ProcessorStats(
                    partition_id=partition_id,
                    symbols_count=len(symbols)
                )
                
                logger.info(f"Started processor {partition_id} with {len(symbols)} symbols")
            
            self.running = True
            
            # Start monitoring thread
            self._start_monitoring()
            
            logger.info(f"All {len(partitions)} processors started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start processors: {e}")
            self.shutdown()
            return False
    
    def shutdown(self):
        """Shutdown all processors gracefully."""
        logger.info("Shutting down parallel processors...")
        
        self.running = False
        self.shutdown_event.set()
        
        # Stop monitoring
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)
        
        # Shutdown executor
        if self.executor:
            try:
                # Wait for processes to finish gracefully
                self.executor.shutdown(wait=True, timeout=10.0)
            except Exception as e:
                logger.warning(f"Error during executor shutdown: {e}")
        
        # Cleanup ring buffer pool
        try:
            self.ring_buffer_pool.cleanup()
        except Exception as e:
            logger.warning(f"Error cleaning up ring buffer pool: {e}")
        
        logger.info("Parallel processors shutdown complete")
    
    def get_overall_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics for all processors.
        
        Returns:
            Dict: Overall statistics
        """
        with self.stats_lock:
            total_symbols = sum(stats.symbols_count for stats in self.processor_stats.values())
            total_messages = sum(stats.messages_processed for stats in self.processor_stats.values())
            total_errors = sum(stats.error_count for stats in self.processor_stats.values())
            
            # Calculate average throughput
            active_processors = [s for s in self.processor_stats.values() 
                               if s.connection_status == "connected"]
            avg_throughput = (sum(s.messages_per_second for s in active_processors) / 
                            len(active_processors)) if active_processors else 0
            
            # Get buffer pool stats
            pool_stats = self.ring_buffer_pool.get_pool_stats()
            
            return {
                "total_processors": len(self.processor_stats),
                "active_processors": len(active_processors),
                "total_symbols": total_symbols,
                "total_messages_processed": total_messages,
                "total_errors": total_errors,
                "average_throughput": avg_throughput,
                "buffer_pool_utilization": pool_stats.get("overall_utilization", 0),
                "uptime": time.time() - min(s.start_time for s in self.processor_stats.values()) if self.processor_stats else 0,
                "processor_details": {
                    pid: {
                        "symbols_count": stats.symbols_count,
                        "messages_processed": stats.messages_processed,
                        "messages_per_second": stats.messages_per_second,
                        "connection_status": stats.connection_status,
                        "error_count": stats.error_count,
                        "uptime": stats.get_uptime(),
                        "buffer_utilization": stats.buffer_utilization
                    }
                    for pid, stats in self.processor_stats.items()
                }
            }
    
    def get_partition_balance(self) -> Dict[str, float]:
        """Get symbol distribution balance across partitions."""
        return self.symbol_partitioner.get_partition_balance()
    
    def restart_processor(self, partition_id: int) -> bool:
        """
        Restart a specific processor.
        
        Args:
            partition_id: ID of processor to restart
            
        Returns:
            bool: True if restart successful
        """
        if partition_id not in self.process_futures:
            logger.error(f"Processor {partition_id} not found")
            return False
        
        try:
            # Cancel existing future
            future = self.process_futures[partition_id]
            future.cancel()
            
            # Get original config (would need to store this)
            # For now, log that restart is needed
            logger.warning(f"Processor {partition_id} restart requested - manual intervention required")
            
            return False  # Not implemented yet
            
        except Exception as e:
            logger.error(f"Failed to restart processor {partition_id}: {e}")
            return False
    
    def _start_monitoring(self):
        """Start monitoring thread for processor health."""
        self.monitor_thread = threading.Thread(
            target=self._monitor_processors,
            daemon=True,
            name="ProcessorMonitor"
        )
        self.monitor_thread.start()
    
    def _monitor_processors(self):
        """Monitor processor health and update statistics."""
        logger.info("Started processor monitoring")
        
        while self.running:
            try:
                # Check processor futures
                for partition_id, future in self.process_futures.items():
                    if future.done():
                        # Process completed (possibly with error)
                        try:
                            result = future.result(timeout=0.1)
                            logger.info(f"Processor {partition_id} completed: {result}")
                        except Exception as e:
                            logger.error(f"Processor {partition_id} failed: {e}")
                            
                            # Update stats
                            if partition_id in self.processor_stats:
                                self.processor_stats[partition_id].connection_status = "failed"
                                self.processor_stats[partition_id].error_count += 1
                
                # Update buffer utilization stats
                pool_stats = self.ring_buffer_pool.get_pool_stats()
                for partition_id in self.processor_stats:
                    buffer_name = f"buffer_{partition_id}"
                    if buffer_name in pool_stats.get("buffers", {}):
                        utilization = pool_stats["buffers"][buffer_name]["utilization"]
                        self.processor_stats[partition_id].buffer_utilization = utilization
                
                # Sleep before next check
                time.sleep(5.0)
                
            except Exception as e:
                logger.error(f"Error in processor monitoring: {e}")
                time.sleep(1.0)
        
        logger.info("Processor monitoring stopped")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.shutdown()


def _run_processor_worker(config: ProcessorConfig, shutdown_event: mp.Event) -> Dict[str, Any]:
    """
    Worker function that runs in separate process.
    
    Args:
        config: Processor configuration
        shutdown_event: Event to signal shutdown
        
    Returns:
        Dict: Final statistics
    """
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Processor {config.partition_id} received signal {signum}")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info(f"Starting processor worker {config.partition_id} with {len(config.symbols)} symbols")
    
    stats = {
        "partition_id": config.partition_id,
        "symbols_processed": 0,
        "messages_processed": 0,
        "errors": 0,
        "start_time": time.time()
    }
    
    try:
        # Import broker adapter (done here to avoid import issues in multiprocessing)
        from broker.flattrade.streaming.flattrade_adapter_shm import FlattradeSHMWebSocketAdapter
        
        # Initialize adapter
        adapter = FlattradeSHMWebSocketAdapter(config.buffer_name)
        adapter.initialize(config.broker_name, config.user_id, config.auth_data)
        
        # Connect to broker
        if not adapter.connect():
            raise Exception("Failed to connect to broker")
        
        logger.info(f"Processor {config.partition_id} connected to broker")
        
        # Subscribe to symbols in batches
        batch_size = config.batch_size
        for i in range(0, len(config.symbols), batch_size):
            if shutdown_event.is_set():
                break
                
            batch = config.symbols[i:i + batch_size]
            
            for symbol_info in batch:
                try:
                    adapter.subscribe(
                        symbol_info['symbol'],
                        symbol_info['exchange'],
                        mode=1  # LTP mode for minimal bandwidth
                    )
                    stats["symbols_processed"] += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to subscribe to {symbol_info}: {e}")
                    stats["errors"] += 1
            
            # Small delay between batches to avoid overwhelming broker
            time.sleep(0.01)
        
        logger.info(f"Processor {config.partition_id} subscribed to {stats['symbols_processed']} symbols")
        
        # Main processing loop
        message_count = 0
        last_stats_time = time.time()
        
        while not shutdown_event.is_set():
            try:
                # Process messages (this would be handled by the adapter automatically)
                # For now, just simulate processing
                time.sleep(0.1)
                message_count += 1
                
                # Update stats periodically
                current_time = time.time()
                if current_time - last_stats_time >= 10.0:  # Every 10 seconds
                    stats["messages_processed"] = message_count
                    last_stats_time = current_time
                    
                    logger.debug(f"Processor {config.partition_id}: {message_count} messages processed")
                
            except Exception as e:
                logger.error(f"Error in processor {config.partition_id} main loop: {e}")
                stats["errors"] += 1
                time.sleep(1.0)
        
        # Cleanup
        adapter.disconnect()
        
        stats["end_time"] = time.time()
        stats["duration"] = stats["end_time"] - stats["start_time"]
        
        logger.info(f"Processor {config.partition_id} completed successfully")
        return stats
        
    except Exception as e:
        logger.error(f"Processor {config.partition_id} failed: {e}")
        stats["error"] = str(e)
        stats["end_time"] = time.time()
        return stats


def test_parallel_processor():
    """Test the parallel WebSocket processor."""
    print("Testing Parallel WebSocket Processor...")
    
    # Create test symbols
    test_symbols = []
    for i in range(100):
        test_symbols.append({
            "symbol": f"STOCK{i:03d}",
            "exchange": "NSE" if i % 2 == 0 else "BSE"
        })
    
    # Test processor
    with ParallelWebSocketProcessor(num_processes=4) as processor:
        print(f"Created processor with {processor.num_processes} processes")
        
        # Test symbol distribution
        balance = processor.get_partition_balance()
        print(f"Initial balance: {balance}")
        
        # Start processors (would normally connect to real broker)
        success = processor.start_processors(
            test_symbols, 
            broker_name="flattrade",
            auth_data={"test": "data"}
        )
        
        if success:
            print("Processors started successfully")
            
            # Monitor for a short time
            for i in range(5):
                time.sleep(2)
                stats = processor.get_overall_stats()
                print(f"Stats update {i+1}: {stats['active_processors']} active, "
                      f"{stats['total_symbols']} symbols")
        else:
            print("Failed to start processors")


if __name__ == "__main__":
    test_parallel_processor()