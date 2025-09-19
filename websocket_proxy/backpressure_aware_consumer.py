"""
Backpressure Aware Consumer for high-throughput market data consumption.
Handles multiple ring buffers with intelligent backpressure management and flow control.
"""

import queue
import threading
import time
import logging
from typing import List, Dict, Optional, Callable, Any, Set
from dataclasses import dataclass, field
from collections import deque
import statistics

from .optimized_ring_buffer import OptimizedRingBuffer
from .binary_market_data import BinaryMarketData

logger = logging.getLogger(__name__)


@dataclass
class ConsumerStats:
    """Statistics for a consumer instance."""
    buffer_name: str
    messages_consumed: int = 0
    messages_dropped: int = 0
    queue_overflows: int = 0
    backpressure_events: int = 0
    avg_latency_ms: float = 0.0
    throughput_msg_per_sec: float = 0.0
    last_message_time: float = 0.0
    start_time: float = field(default_factory=time.time)
    
    # Performance tracking (reduced for production)
    latency_samples: deque = field(default_factory=lambda: deque(maxlen=100))
    throughput_samples: deque = field(default_factory=lambda: deque(maxlen=50))
    
    def update_latency(self, latency_ms: float):
        """Update latency statistics."""
        self.latency_samples.append(latency_ms)
        if self.latency_samples:
            self.avg_latency_ms = statistics.mean(self.latency_samples)
    
    def update_throughput(self, messages_count: int, time_window: float):
        """Update throughput statistics."""
        if time_window > 0:
            throughput = messages_count / time_window
            self.throughput_samples.append(throughput)
            if self.throughput_samples:
                self.throughput_msg_per_sec = statistics.mean(self.throughput_samples)
    
    def get_uptime(self) -> float:
        """Get consumer uptime in seconds."""
        return time.time() - self.start_time


@dataclass
class BackpressureConfig:
    """Configuration for backpressure management."""
    max_queue_size: int = 10000
    batch_size: int = 100
    empty_sleep_ms: float = 1.0
    empty_threshold: int = 1000
    backpressure_threshold: float = 0.8  # Queue utilization threshold
    drop_strategy: str = "oldest"  # "oldest", "newest", "random"
    adaptive_batching: bool = True
    max_batch_size: int = 500
    min_batch_size: int = 10
    latency_target_ms: float = 1.0
    throughput_window_sec: float = 1.0


class BackpressureAwareConsumer:
    """
    High-performance consumer with intelligent backpressure management.
    
    Features:
    - Multiple buffer consumption with load balancing
    - Adaptive batch sizing based on load
    - Intelligent queue management with overflow handling
    - Comprehensive statistics and monitoring
    - Configurable drop strategies for overload scenarios
    """
    
    def __init__(self, buffer_names: List[str], config: Optional[BackpressureConfig] = None):
        """
        Initialize backpressure aware consumer.
        
        Args:
            buffer_names: List of buffer names to consume from
            config: Backpressure configuration (uses defaults if None)
        """
        self.config = config or BackpressureConfig()
        self.buffer_names = buffer_names
        
        # Initialize buffers (attach to existing shared memory)
        self.buffers = []
        for name in buffer_names:
            try:
                buffer = OptimizedRingBuffer(name, create=False)
                self.buffers.append(buffer)
                logger.info(f"Attached to buffer: {name}")
            except Exception as e:
                logger.error(f"Failed to attach to buffer {name}: {e}")
                self.buffers.append(None)
        
        # Consumer queues with configurable size
        self.consumer_queues = [
            queue.Queue(maxsize=self.config.max_queue_size) 
            for _ in self.buffers
        ]
        
        # Threading and control
        self.running = False
        self.consumer_threads: List[threading.Thread] = []
        self.stats_lock = threading.Lock()
        
        # Statistics tracking
        self.consumer_stats: Dict[int, ConsumerStats] = {}
        for i, name in enumerate(buffer_names):
            self.consumer_stats[i] = ConsumerStats(buffer_name=name)
        
        # Callbacks and handlers
        self.message_handlers: Dict[int, Callable] = {}  # buffer_id -> handler
        self.global_message_handler: Optional[Callable] = None
        self.error_handler: Optional[Callable] = None
        
        # Adaptive batching state
        self.adaptive_batch_sizes = [self.config.batch_size] * len(self.buffers)
        self.last_adaptation_time = [time.time()] * len(self.buffers)
        
        logger.info(f"Initialized BackpressureAwareConsumer for {len(buffer_names)} buffers")
    
    def set_message_handler(self, buffer_id: int, handler: Callable[[BinaryMarketData], None]):
        """
        Set message handler for specific buffer.
        
        Args:
            buffer_id: Buffer index
            handler: Message handler function
        """
        self.message_handlers[buffer_id] = handler
    
    def set_global_message_handler(self, handler: Callable[[int, BinaryMarketData], None]):
        """
        Set global message handler for all buffers.
        
        Args:
            handler: Global handler function (receives buffer_id and message)
        """
        self.global_message_handler = handler
    
    def set_error_handler(self, handler: Callable[[Exception, int], None]):
        """
        Set error handler for consumer errors.
        
        Args:
            handler: Error handler function (receives exception and buffer_id)
        """
        self.error_handler = handler
    
    def start_consuming(self) -> List[threading.Thread]:
        """
        Start consumer threads for each buffer.
        
        Returns:
            List of consumer threads
        """
        if self.running:
            logger.warning("Consumer already running")
            return self.consumer_threads
        
        self.running = True
        self.consumer_threads = []
        
        # Start consumer thread for each buffer
        for i, buffer in enumerate(self.buffers):
            if buffer is not None:
                thread = threading.Thread(
                    target=self._consume_buffer,
                    args=(i, buffer, self.consumer_queues[i]),
                    daemon=True,
                    name=f"Consumer-{i}-{self.buffer_names[i]}"
                )
                thread.start()
                self.consumer_threads.append(thread)
                logger.info(f"Started consumer thread for buffer {i}: {self.buffer_names[i]}")
        
        logger.info(f"Started {len(self.consumer_threads)} consumer threads")
        return self.consumer_threads
    
    def stop_consuming(self):
        """Stop all consumer threads."""
        logger.info("Stopping consumer threads...")
        self.running = False
        
        # Wait for threads to finish
        for thread in self.consumer_threads:
            if thread.is_alive():
                thread.join(timeout=5.0)
        
        logger.info("All consumer threads stopped")
    
    def _consume_buffer(self, buffer_id: int, buffer: OptimizedRingBuffer, output_queue: queue.Queue):
        """
        Consume messages from a specific buffer with backpressure management.
        
        Args:
            buffer_id: Buffer identifier
            buffer: OptimizedRingBuffer instance
            output_queue: Output queue for processed messages
        """
        logger.info(f"Started consuming buffer {buffer_id}: {self.buffer_names[buffer_id]}")
        
        consecutive_empty = 0
        last_stats_time = time.time()
        messages_in_window = 0
        
        while self.running:
            try:
                # Adaptive batch sizing
                current_batch_size = self._get_adaptive_batch_size(buffer_id, buffer, output_queue)
                
                # Consume batch of messages
                start_time = time.perf_counter()
                messages = self._consume_batch(buffer, batch_size=current_batch_size)
                consume_time = time.perf_counter() - start_time
                
                if not messages:
                    consecutive_empty += 1
                    
                    # Adaptive sleeping when no data
                    if consecutive_empty > self.config.empty_threshold:
                        sleep_time = min(self.config.empty_sleep_ms / 1000.0, 0.01)
                        time.sleep(sleep_time)
                    
                    continue
                
                consecutive_empty = 0
                messages_in_window += len(messages)
                
                # Process messages with backpressure handling
                processed_count = self._process_messages_with_backpressure(
                    buffer_id, messages, output_queue
                )
                
                # Update statistics
                with self.stats_lock:
                    stats = self.consumer_stats[buffer_id]
                    stats.messages_consumed += processed_count
                    stats.last_message_time = time.time()
                    
                    # Update latency (consume time per message)
                    if processed_count > 0:
                        latency_ms = (consume_time / processed_count) * 1000
                        stats.update_latency(latency_ms)
                
                # Update throughput statistics periodically
                current_time = time.time()
                if current_time - last_stats_time >= self.config.throughput_window_sec:
                    time_window = current_time - last_stats_time
                    
                    with self.stats_lock:
                        stats = self.consumer_stats[buffer_id]
                        stats.update_throughput(messages_in_window, time_window)
                    
                    messages_in_window = 0
                    last_stats_time = current_time
                
            except Exception as e:
                logger.error(f"Error in consumer {buffer_id}: {e}")
                
                if self.error_handler:
                    try:
                        self.error_handler(e, buffer_id)
                    except Exception as handler_error:
                        logger.error(f"Error in error handler: {handler_error}")
                
                # Brief sleep on error to prevent tight loop
                time.sleep(0.1)
        
        logger.info(f"Consumer {buffer_id} stopped")
    
    def _consume_batch(self, buffer: OptimizedRingBuffer, batch_size: int) -> List[BinaryMarketData]:
        """
        Consume a batch of messages from buffer.
        
        Args:
            buffer: Buffer to consume from
            batch_size: Maximum number of messages to consume
            
        Returns:
            List of consumed messages
        """
        return buffer.consume_batch(max_messages=batch_size)
    
    def _process_messages_with_backpressure(self, buffer_id: int, messages: List[BinaryMarketData], 
                                          output_queue: queue.Queue) -> int:
        """
        Process messages with intelligent backpressure handling.
        
        Args:
            buffer_id: Buffer identifier
            messages: List of messages to process
            output_queue: Output queue
            
        Returns:
            Number of messages successfully processed
        """
        processed_count = 0
        
        for msg in messages:
            try:
                # Check queue utilization for backpressure
                queue_utilization = output_queue.qsize() / self.config.max_queue_size
                
                if queue_utilization >= self.config.backpressure_threshold:
                    # Handle backpressure
                    if self._handle_backpressure(buffer_id, msg, output_queue):
                        processed_count += 1
                    else:
                        # Message dropped due to backpressure
                        with self.stats_lock:
                            self.consumer_stats[buffer_id].messages_dropped += 1
                            self.consumer_stats[buffer_id].backpressure_events += 1
                else:
                    # Normal processing
                    try:
                        output_queue.put_nowait(msg)
                        processed_count += 1
                        
                        # Call message handlers
                        self._call_message_handlers(buffer_id, msg)
                        
                    except queue.Full:
                        # Queue became full between check and put
                        if self._handle_backpressure(buffer_id, msg, output_queue):
                            processed_count += 1
                        else:
                            with self.stats_lock:
                                self.consumer_stats[buffer_id].messages_dropped += 1
                
            except Exception as e:
                logger.warning(f"Error processing message in buffer {buffer_id}: {e}")
                continue
        
        return processed_count
    
    def _handle_backpressure(self, buffer_id: int, msg: BinaryMarketData, 
                           output_queue: queue.Queue) -> bool:
        """
        Handle backpressure situation with configurable drop strategy.
        
        Args:
            buffer_id: Buffer identifier
            msg: Message to process
            output_queue: Output queue
            
        Returns:
            True if message was processed, False if dropped
        """
        try:
            if self.config.drop_strategy == "oldest":
                # Drop oldest message and add new one
                try:
                    output_queue.get_nowait()  # Remove oldest
                    output_queue.put_nowait(msg)  # Add new
                    self._call_message_handlers(buffer_id, msg)
                    return True
                except queue.Empty:
                    # Queue became empty, just add the message
                    output_queue.put_nowait(msg)
                    self._call_message_handlers(buffer_id, msg)
                    return True
                    
            elif self.config.drop_strategy == "newest":
                # Drop the new message
                return False
                
            elif self.config.drop_strategy == "random":
                # Randomly drop either old or new message
                import random
                if random.random() < 0.5:
                    try:
                        output_queue.get_nowait()
                        output_queue.put_nowait(msg)
                        self._call_message_handlers(buffer_id, msg)
                        return True
                    except queue.Empty:
                        output_queue.put_nowait(msg)
                        self._call_message_handlers(buffer_id, msg)
                        return True
                else:
                    return False
            
        except Exception as e:
            logger.warning(f"Error in backpressure handling: {e}")
            return False
        
        return False
    
    def _call_message_handlers(self, buffer_id: int, msg: BinaryMarketData):
        """
        Call registered message handlers.
        
        Args:
            buffer_id: Buffer identifier
            msg: Message to handle
        """
        try:
            # Call buffer-specific handler
            if buffer_id in self.message_handlers:
                self.message_handlers[buffer_id](msg)
            
            # Call global handler
            if self.global_message_handler:
                self.global_message_handler(buffer_id, msg)
                
        except Exception as e:
            logger.warning(f"Error in message handler: {e}")
    
    def _get_adaptive_batch_size(self, buffer_id: int, buffer: OptimizedRingBuffer, 
                                output_queue: queue.Queue) -> int:
        """
        Calculate adaptive batch size based on current conditions.
        
        Args:
            buffer_id: Buffer identifier
            buffer: Buffer instance
            output_queue: Output queue
            
        Returns:
            Optimal batch size
        """
        if not self.config.adaptive_batching:
            return self.config.batch_size
        
        current_time = time.time()
        
        # Only adapt every second to avoid thrashing
        if current_time - self.last_adaptation_time[buffer_id] < 1.0:
            return self.adaptive_batch_sizes[buffer_id]
        
        self.last_adaptation_time[buffer_id] = current_time
        
        # Get current conditions
        buffer_utilization = buffer.get_utilization()
        queue_utilization = output_queue.qsize() / self.config.max_queue_size
        
        with self.stats_lock:
            stats = self.consumer_stats[buffer_id]
            current_latency = stats.avg_latency_ms
            current_throughput = stats.throughput_msg_per_sec
        
        current_batch_size = self.adaptive_batch_sizes[buffer_id]
        
        # Adaptation logic
        if buffer_utilization > 0.8:  # High buffer utilization
            # Increase batch size to consume faster
            new_batch_size = min(current_batch_size * 1.2, self.config.max_batch_size)
        elif queue_utilization > 0.8:  # High queue utilization
            # Decrease batch size to reduce memory pressure
            new_batch_size = max(current_batch_size * 0.8, self.config.min_batch_size)
        elif current_latency > self.config.latency_target_ms * 2:  # High latency
            # Decrease batch size for lower latency
            new_batch_size = max(current_batch_size * 0.9, self.config.min_batch_size)
        elif current_latency < self.config.latency_target_ms * 0.5:  # Low latency
            # Increase batch size for better throughput
            new_batch_size = min(current_batch_size * 1.1, self.config.max_batch_size)
        else:
            # Keep current batch size
            new_batch_size = current_batch_size
        
        self.adaptive_batch_sizes[buffer_id] = int(new_batch_size)
        return self.adaptive_batch_sizes[buffer_id]
    
    def get_consumer_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive consumer statistics.
        
        Returns:
            Dict: Consumer statistics
        """
        with self.stats_lock:
            stats_copy = {i: stats.__dict__.copy() for i, stats in self.consumer_stats.items()}
        
        # Remove deque objects for JSON serialization
        for buffer_stats in stats_copy.values():
            buffer_stats.pop('latency_samples', None)
            buffer_stats.pop('throughput_samples', None)
        
        # Calculate aggregate statistics
        total_consumed = sum(stats['messages_consumed'] for stats in stats_copy.values())
        total_dropped = sum(stats['messages_dropped'] for stats in stats_copy.values())
        avg_throughput = sum(stats['throughput_msg_per_sec'] for stats in stats_copy.values())
        
        return {
            "total_messages_consumed": total_consumed,
            "total_messages_dropped": total_dropped,
            "total_throughput_msg_per_sec": avg_throughput,
            "drop_rate": total_dropped / (total_consumed + total_dropped) if (total_consumed + total_dropped) > 0 else 0.0,
            "active_consumers": len([t for t in self.consumer_threads if t.is_alive()]),
            "buffer_stats": stats_copy,
            "adaptive_batch_sizes": self.adaptive_batch_sizes.copy(),
            "config": self.config.__dict__
        }
    
    def get_queue_stats(self) -> Dict[int, Dict[str, Any]]:
        """
        Get queue utilization statistics.
        
        Returns:
            Dict: Queue statistics per buffer
        """
        queue_stats = {}
        
        for i, q in enumerate(self.consumer_queues):
            queue_stats[i] = {
                "current_size": q.qsize(),
                "max_size": self.config.max_queue_size,
                "utilization": q.qsize() / self.config.max_queue_size,
                "is_full": q.full(),
                "is_empty": q.empty()
            }
        
        return queue_stats
    
    def reset_stats(self):
        """Reset all consumer statistics."""
        with self.stats_lock:
            for stats in self.consumer_stats.values():
                stats.messages_consumed = 0
                stats.messages_dropped = 0
                stats.queue_overflows = 0
                stats.backpressure_events = 0
                stats.latency_samples.clear()
                stats.throughput_samples.clear()
                stats.start_time = time.time()
        
        logger.info("Reset consumer statistics")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.stop_consuming()


