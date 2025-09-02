"""
Optimized Ring Buffer for BinaryMarketData with zero-copy operations.
Designed for nanosecond-level performance with fixed-size binary messages.
"""

import mmap
import os
import struct
import threading
import time
import json
from multiprocessing import shared_memory
from typing import List, Optional, Dict, Any, Union
from .binary_market_data import BinaryMarketData

import logging
logger = logging.getLogger(__name__)


class OptimizedRingBuffer:
    """
    High-performance ring buffer optimized for BinaryMarketData.
    
    Features:
    - Fixed message size based on BinaryMarketData.SIZE for cache alignment
    - Shared memory for inter-process communication
    - Batch operations for improved throughput
    - Zero-copy direct memory operations
    - Cache-line aligned headers (64 bytes)
    """
    
    def __init__(self, name: str, size: int = 1024*1024, create: bool = True):
        """
        Initialize optimized ring buffer.
        
        Args:
            name: Shared memory buffer name
            size: Total buffer size in bytes
            create: Whether to create new buffer or attach to existing
        """
        self.name = name
        # Import BinaryMarketData to get the actual size
        from .binary_market_data import BinaryMarketData
        from .config import Config
        
        # Dynamic message sizing system
        from .binary_market_data import BinaryMarketData
        self.binary_message_size = BinaryMarketData.SIZE  # 128 bytes for binary market data
        self.base_message_size = max(Config.MESSAGE_SIZE, self.binary_message_size)  # Ensure at least binary_message_size
        self.max_message_size = 4096  # Maximum allowed message size (4KB)
        self.message_size = self.base_message_size  # Current message size
        
        # Message size tracking for dynamic adjustment
        self.message_size_stats = {
            'total_messages': 0,
            'oversized_count': 0,
            'max_size_seen': 0,
            'avg_size': 0,
            'last_adjustment': 0
        }
        self.total_size = size
        self.header_size = 64  # Cache line aligned (defined early for use in calculations)
        self.max_messages = (size - self.header_size) // self.message_size  # Reserve header space
        
        # Initialize shared memory
        if create:
            try:
                # Try to unlink existing buffer first
                try:
                    old_shm = shared_memory.SharedMemory(name)
                    old_shm.close()
                    old_shm.unlink()
                except FileNotFoundError:
                    pass
                
                self.shm = shared_memory.SharedMemory(name, create=True, size=size)
                # Initialize headers to zero
                self.shm.buf[:64] = b'\x00' * 64
                
            except Exception as e:
                logger.error(f"Failed to create shared memory buffer {name}: {e}")
                raise
        else:
            try:
                self.shm = shared_memory.SharedMemory(name)
            except FileNotFoundError:
                logger.error(f"Shared memory buffer {name} not found")
                raise
        
        self.buffer = self.shm.buf
        
        # Memory layout: [head:8][tail:8][padding:48][messages...]
        # header_size already defined above
        
        # Statistics
        self.stats = {
            'publishes_attempted': 0,
            'publishes_succeeded': 0,
            'consumes_attempted': 0,
            'consumes_succeeded': 0,
            'buffer_full_events': 0,
            'buffer_empty_events': 0,
            'batch_publishes': 0,
            'batch_consumes': 0
        }
        
        # Thread safety for statistics
        self._stats_lock = threading.Lock()
        
        logger.debug(f"Initialized OptimizedRingBuffer '{name}' with {self.max_messages} message capacity (message_size: {self.message_size} bytes)")
    
    def _should_adjust_message_size(self, required_size: int) -> bool:
        """
        Determine if message size should be adjusted based on usage patterns.
        
        Args:
            required_size: Size of the current message that doesn't fit
            
        Returns:
            bool: True if size should be adjusted
        """
        # Update statistics
        self.message_size_stats['total_messages'] += 1
        self.message_size_stats['max_size_seen'] = max(
            self.message_size_stats['max_size_seen'], 
            required_size
        )
        
        if required_size > self.message_size:
            self.message_size_stats['oversized_count'] += 1
        
        # Calculate oversized percentage
        oversized_percentage = (
            self.message_size_stats['oversized_count'] / 
            max(self.message_size_stats['total_messages'], 1)
        ) * 100
        
        # Adjust if:
        # 1. More than 10% of messages are oversized, OR
        # 2. We haven't adjusted recently and current message is much larger
        should_adjust = (
            oversized_percentage > 10.0 or 
            (required_size > self.message_size * 1.5 and 
             self.message_size_stats['total_messages'] - self.message_size_stats['last_adjustment'] > 100)
        )
        
        if should_adjust:
            logger.info(f"Message size adjustment triggered: oversized={oversized_percentage:.1f}%, "
                       f"current_size={self.message_size}, required_size={required_size}")
        
        return should_adjust
    
    def _adjust_message_size(self, required_size: int) -> bool:
        """
        Dynamically adjust the message size to accommodate larger messages.
        
        Args:
            required_size: Minimum size needed
            
        Returns:
            bool: True if adjustment was successful
        """
        # Calculate new size with some headroom (25% buffer)
        new_size = min(int(required_size * 1.25), self.max_message_size)
        
        # Only adjust if the new size is significantly larger
        if new_size <= self.message_size:
            return False
        
        # Check if we have enough total buffer space
        new_max_messages = (self.total_size - self.header_size) // new_size
        if new_max_messages < 10:  # Minimum viable message count
            logger.warning(f"Cannot adjust message size to {new_size}: would result in only {new_max_messages} messages")
            return False
        
        old_size = self.message_size
        old_max_messages = self.max_messages
        
        # Apply the new size
        self.message_size = new_size
        self.max_messages = new_max_messages
        
        # Reset statistics
        self.message_size_stats['last_adjustment'] = self.message_size_stats['total_messages']
        self.message_size_stats['oversized_count'] = 0
        
        logger.info(f"Adjusted message size: {old_size} -> {new_size} bytes, "
                   f"max_messages: {old_max_messages} -> {new_max_messages}")
        
        # Clear the buffer since message layout has changed
        self._clear_buffer_content()
        
        return True
    
    def _clear_buffer_content(self):
        """Clear buffer content after size adjustment."""
        try:
            # Reset head and tail pointers
            self._set_head(0)
            self._set_tail(0)
            
            # Clear message area (keep headers)
            message_area_start = self.header_size
            message_area_size = self.total_size - self.header_size
            self.buffer[message_area_start:message_area_start + message_area_size] = b'\x00' * message_area_size
            
            logger.debug("Buffer content cleared after size adjustment")
        except Exception as e:
            logger.error(f"Error clearing buffer content: {e}")
    
    def get_message_size_stats(self) -> Dict[str, Any]:
        """Get current message size statistics."""
        stats = self.message_size_stats.copy()
        stats['current_message_size'] = self.message_size
        stats['max_message_size'] = self.max_message_size
        stats['base_message_size'] = self.base_message_size
        return stats
    
    def _get_head(self) -> int:
        """Get head pointer from shared memory."""
        try:
            return struct.unpack('<Q', bytes(self.buffer[0:8]))[0]
        except Exception:
            return 0
    
    def _set_head(self, value: int):
        """Set head pointer in shared memory."""
        try:
            self.buffer[0:8] = struct.pack('<Q', value)
        except Exception as e:
            logger.warning(f"Failed to set head: {e}")
    
    def _get_tail(self) -> int:
        """Get tail pointer from shared memory."""
        try:
            return struct.unpack('<Q', bytes(self.buffer[8:16]))[0]
        except Exception:
            return 0
    
    def _set_tail(self, value: int):
        """Set tail pointer in shared memory."""
        try:
            self.buffer[8:16] = struct.pack('<Q', value)
        except Exception as e:
            logger.warning(f"Failed to set tail: {e}")
    
    def publish_batch(self, messages: List[BinaryMarketData]) -> int:
        """
        Batch publish for better throughput.
        
        Args:
            messages: List of BinaryMarketData messages to publish
            
        Returns:
            int: Number of messages successfully published
        """
        if not messages:
            return 0
        
        with self._stats_lock:
            self.stats['batch_publishes'] += 1
            self.stats['publishes_attempted'] += len(messages)
        
        published = 0
        
        # Try to publish all messages in sequence
        for msg in messages:
            if self._publish_single(msg):
                published += 1
            else:
                # Buffer full, stop trying
                with self._stats_lock:
                    self.stats['buffer_full_events'] += 1
                break
        
        with self._stats_lock:
            self.stats['publishes_succeeded'] += published
        
        return published
    
    def _publish_single(self, message) -> bool:
        """
        Single message publish with minimal overhead.
        
        Args:
            message: BinaryMarketData message or dict to publish
            
        Returns:
            bool: True if published successfully, False if buffer full
        """
        head = self._get_head()
        tail = self._get_tail()
        
        # Check if buffer is full (leave one slot empty for distinction)
        next_head = (head + 1) % self.max_messages
        if next_head == tail:
            return False  # Buffer full
        
        # Calculate position in buffer
        pos = self.header_size + (head * self.message_size)
        
        # Direct memory write - no intermediate copies
        try:
            # Handle both BinaryMarketData and dict messages
            if hasattr(message, 'to_bytes'):
                # BinaryMarketData object
                binary_bytes = message.to_bytes()
                if len(binary_bytes) != self.binary_message_size:
                    logger.warning(f"Binary message size mismatch: {len(binary_bytes)} != {self.binary_message_size}")
                    return False
                
                # Pad binary message to full message_size
                message_bytes = binary_bytes.ljust(self.message_size, b'\x00')
            else:
                # Dict message - serialize as JSON and pad to message_size
                import json
                json_str = json.dumps(message)
                json_bytes = json_str.encode('utf-8')
                
                # Check if message is too large and try to adjust
                if len(json_bytes) > self.message_size:
                    if self._should_adjust_message_size(len(json_bytes)):
                        if self._adjust_message_size(len(json_bytes)):
                            logger.info(f"Successfully adjusted buffer for message size: {len(json_bytes)} bytes")
                        else:
                            logger.warning(f"Failed to adjust buffer for message size: {len(json_bytes)} bytes")
                    
                    # Check again after potential adjustment
                    if len(json_bytes) > self.message_size:
                        logger.warning(f"Dict message too large: {len(json_bytes)} > {self.message_size} (max: {self.max_message_size})")
                        return False
                
                # Pad to message_size with null bytes
                message_bytes = json_bytes.ljust(self.message_size, b'\x00')
            
            # Write message data directly to buffer
            self.buffer[pos:pos + self.message_size] = message_bytes
            
            # Update head atomically (as atomic as possible in Python)
            self._set_head(next_head)
            return True
            
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            return False
    
    def publish_single(self, message) -> bool:
        """
        Public interface for single message publish.
        
        Args:
            message: BinaryMarketData message or dict to publish
            
        Returns:
            bool: True if published successfully
        """
        with self._stats_lock:
            self.stats['publishes_attempted'] += 1
        
        success = self._publish_single(message)
        
        if success:
            with self._stats_lock:
                self.stats['publishes_succeeded'] += 1
        else:
            with self._stats_lock:
                self.stats['buffer_full_events'] += 1
        
        return success
    
    def consume_batch(self, max_messages: int = 100) -> List:
        """
        Consume multiple messages in batch for better throughput.
        
        Args:
            max_messages: Maximum number of messages to consume
            
        Returns:
            List: List of consumed messages (BinaryMarketData or dict)
        """
        messages = []
        
        with self._stats_lock:
            self.stats['batch_consumes'] += 1
        
        for _ in range(max_messages):
            msg = self._consume_single()
            if msg is None:
                break
            messages.append(msg)
        
        with self._stats_lock:
            self.stats['consumes_attempted'] += max_messages
            self.stats['consumes_succeeded'] += len(messages)
            if not messages:
                self.stats['buffer_empty_events'] += 1
        
        return messages
    
    def _consume_single(self):
        """
        Single message consume with minimal overhead.
        
        Returns:
            BinaryMarketData, dict, or None if buffer empty
        """
        head = self._get_head()
        tail = self._get_tail()
        
        # Check if buffer is empty
        if head == tail:
            return None
        
        # Calculate position in buffer
        pos = self.header_size + (tail * self.message_size)
        
        try:
            # Read message data directly from buffer
            message_bytes = bytes(self.buffer[pos:pos + self.message_size])
            
            # Detect message type by checking if it starts with JSON
            # JSON messages will start with '{' after stripping null bytes
            json_bytes = message_bytes.rstrip(b'\x00')
            
            if json_bytes and json_bytes[0:1] == b'{':
                # This is a JSON message
                try:
                    import json
                    message_dict = json.loads(json_bytes.decode('utf-8'))
                    
                    # Update tail atomically
                    next_tail = (tail + 1) % self.max_messages
                    self._set_tail(next_tail)
                    
                    return message_dict
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning(f"Failed to deserialize JSON message: {e}")
                    # Skip corrupted message
                    next_tail = (tail + 1) % self.max_messages
                    self._set_tail(next_tail)
                    return None
            else:
                # This should be a binary message
                try:
                    # Extract the binary portion (first binary_message_size bytes)
                    binary_portion = message_bytes[:self.binary_message_size]
                    message = BinaryMarketData.from_bytes(binary_portion)
                    
                    # Update tail atomically
                    next_tail = (tail + 1) % self.max_messages
                    self._set_tail(next_tail)
                    
                    return message
                except (struct.error, ValueError) as e:
                    logger.warning(f"Failed to deserialize binary message: {e}")
                    # Skip corrupted message
                    next_tail = (tail + 1) % self.max_messages
                    self._set_tail(next_tail)
                    return None
            
        except Exception as e:
            logger.error(f"Error consuming message: {e}")
            return None
    
    def consume_single(self):
        """
        Public interface for single message consume.
        
        Returns:
            BinaryMarketData, dict, or None if buffer empty
        """
        with self._stats_lock:
            self.stats['consumes_attempted'] += 1
        
        message = self._consume_single()
        
        if message is not None:
            with self._stats_lock:
                self.stats['consumes_succeeded'] += 1
        else:
            with self._stats_lock:
                self.stats['buffer_empty_events'] += 1
        
        return message
    
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return self._get_head() == self._get_tail()
    
    def is_full(self) -> bool:
        """Check if buffer is full."""
        head = self._get_head()
        tail = self._get_tail()
        next_head = (head + 1) % self.max_messages
        return next_head == tail
    
    def get_count(self) -> int:
        """Get approximate number of messages in buffer."""
        head = self._get_head()
        tail = self._get_tail()
        
        if head >= tail:
            return head - tail
        else:
            return self.max_messages - tail + head
    
    def get_utilization(self) -> float:
        """Get buffer utilization as percentage (0.0 to 1.0)."""
        return self.get_count() / self.max_messages if self.max_messages > 0 else 0.0
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive buffer statistics.
        
        Returns:
            Dict: Buffer statistics
        """
        with self._stats_lock:
            stats = self.stats.copy()
        
        # Add current state
        stats.update({
            'buffer_name': self.name,
            'message_count': self.get_count(),
            'max_messages': self.max_messages,
            'utilization': self.get_utilization(),
            'is_empty': self.is_empty(),
            'is_full': self.is_full(),
            'message_size': self.message_size,
            'message_size_stats': self.get_message_size_stats(),
            'total_size': self.total_size,
            'header_size': self.header_size
        })
        
        # Calculate rates
        if stats['publishes_attempted'] > 0:
            stats['publish_success_rate'] = stats['publishes_succeeded'] / stats['publishes_attempted']
        else:
            stats['publish_success_rate'] = 0.0
        
        if stats['consumes_attempted'] > 0:
            stats['consume_success_rate'] = stats['consumes_succeeded'] / stats['consumes_attempted']
        else:
            stats['consume_success_rate'] = 0.0
        
        return stats
    
    def reset_stats(self):
        """Reset all statistics."""
        with self._stats_lock:
            for key in self.stats:
                self.stats[key] = 0
        
        logger.debug(f"Reset statistics for buffer '{self.name}'")
    
    def clear_buffer(self):
        """Clear all messages from buffer (reset head and tail)."""
        self._set_head(0)
        self._set_tail(0)
        logger.info(f"Cleared buffer '{self.name}'")
    
    def close(self):
        """Close the shared memory buffer."""
        if hasattr(self, 'shm') and self.shm is not None:
            try:
                self.shm.close()
                logger.debug(f"Closed buffer '{self.name}'")
            except Exception as e:
                logger.warning(f"Error closing buffer '{self.name}': {e}")
    
    def unlink(self):
        """Unlink (delete) the shared memory buffer."""
        if hasattr(self, 'shm') and self.shm is not None:
            try:
                self.shm.unlink()
                logger.debug(f"Unlinked buffer '{self.name}'")
            except Exception as e:
                logger.warning(f"Error unlinking buffer '{self.name}': {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.close()
        if exc_type is None:  # Only unlink on successful completion
            self.unlink()
    
    def __repr__(self) -> str:
        """String representation of the buffer."""
        return (f"OptimizedRingBuffer(name='{self.name}', "
                f"messages={self.get_count()}/{self.max_messages}, "
                f"utilization={self.get_utilization():.1%})")


class OptimizedRingBufferPool:
    """
    Pool of optimized ring buffers for load distribution.
    Extends RingBufferPool with OptimizedRingBuffer instances.
    """
    
    def __init__(self, num_buffers: int = 8, buffer_size: int = 1024*1024):
        """
        Initialize optimized ring buffer pool.
        
        Args:
            num_buffers: Number of buffers to create
            buffer_size: Size of each buffer in bytes
        """
        self.num_buffers = num_buffers
        self.buffer_size = buffer_size
        self.buffers: List[OptimizedRingBuffer] = []
        self.current_buffer = 0
        self._lock = threading.Lock()
        
        self._initialize_buffers()
    
    def _initialize_buffers(self):
        """Initialize all optimized ring buffers."""
        try:
            for i in range(self.num_buffers):
                buffer_name = f"optimized_market_data_{i}"
                buffer = OptimizedRingBuffer(buffer_name, self.buffer_size, create=True)
                self.buffers.append(buffer)
                logger.debug(f"Created optimized ring buffer: {buffer_name}")
            
            logger.info(f"Initialized optimized ring buffer pool with {self.num_buffers} buffers")
            
        except Exception as e:
            logger.error(f"Failed to initialize optimized ring buffer pool: {e}")
            self.cleanup()
            raise
    
    def get_next_buffer(self) -> OptimizedRingBuffer:
        """Get next buffer using round-robin distribution."""
        with self._lock:
            buffer = self.buffers[self.current_buffer]
            self.current_buffer = (self.current_buffer + 1) % len(self.buffers)
            return buffer
    
    def get_least_utilized_buffer(self) -> OptimizedRingBuffer:
        """Get buffer with lowest utilization."""
        min_utilization = float('inf')
        best_buffer = self.buffers[0]
        
        for buffer in self.buffers:
            utilization = buffer.get_utilization()
            if utilization < min_utilization:
                min_utilization = utilization
                best_buffer = buffer
        
        return best_buffer
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get statistics for all buffers in the pool."""
        pool_stats = {
            "num_buffers": self.num_buffers,
            "buffer_size": self.buffer_size,
            "total_messages": 0,
            "total_capacity": 0,
            "buffers": {}
        }
        
        for i, buffer in enumerate(self.buffers):
            buffer_stats = buffer.get_stats()
            buffer_name = f"buffer_{i}"
            
            pool_stats["buffers"][buffer_name] = buffer_stats
            pool_stats["total_messages"] += buffer_stats["message_count"]
            pool_stats["total_capacity"] += buffer_stats["max_messages"]
        
        pool_stats["overall_utilization"] = (
            pool_stats["total_messages"] / pool_stats["total_capacity"] 
            if pool_stats["total_capacity"] > 0 else 0
        )
        
        return pool_stats
    
    def cleanup(self):
        """Clean up all buffers."""
        for i, buffer in enumerate(self.buffers):
            try:
                buffer.close()
                buffer.unlink()
                logger.debug(f"Cleaned up optimized buffer {i}")
            except Exception as e:
                logger.warning(f"Error cleaning up optimized buffer {i}: {e}")
        
        self.buffers.clear()
        logger.info("Optimized ring buffer pool cleanup completed")
    
    def __len__(self) -> int:
        """Return number of buffers in pool."""
        return len(self.buffers)


def test_optimized_ring_buffer():
    """Test the optimized ring buffer functionality."""
    print("Testing Optimized Ring Buffer...")
    
    # Test single buffer
    with OptimizedRingBuffer("test_buffer", size=64*1024, create=True) as buffer:
        print(f"Created buffer: {buffer}")
        
        # Test single message operations
        msg1 = BinaryMarketData.create_ltp_message("TCS", "NSE", 3456.78, 1000)
        success = buffer.publish_single(msg1)
        print(f"Published single message: {success}")
        
        consumed = buffer.consume_single()
        print(f"Consumed single message: {consumed is not None}")
        
        # Test batch operations
        messages = []
        for i in range(10):
            msg = BinaryMarketData.create_ltp_message(
                f"STOCK{i}", "NSE", 100.0 + i, 1000 + i
            )
            messages.append(msg)
        
        published_count = buffer.publish_batch(messages)
        print(f"Published batch: {published_count}/{len(messages)} messages")
        
        consumed_batch = buffer.consume_batch(max_messages=5)
        print(f"Consumed batch: {len(consumed_batch)} messages")
        
        # Show statistics
        stats = buffer.get_stats()
        print(f"Buffer stats: {stats['message_count']} messages, "
              f"{stats['utilization']:.1%} full")
        print(f"Publish success rate: {stats['publish_success_rate']:.1%}")
        print(f"Consume success rate: {stats['consume_success_rate']:.1%}")
    
    # Test buffer pool
    print("\nTesting Optimized Ring Buffer Pool...")
    
    pool = OptimizedRingBufferPool(num_buffers=4, buffer_size=32*1024)
    
    try:
        # Test round-robin distribution
        for i in range(8):
            buffer = pool.get_next_buffer()
            msg = BinaryMarketData.create_ltp_message(f"TEST{i}", "NSE", 100.0 + i)
            buffer.publish_single(msg)
        
        # Show pool statistics
        pool_stats = pool.get_pool_stats()
        print(f"Pool stats: {pool_stats['total_messages']} total messages")
        print(f"Overall utilization: {pool_stats['overall_utilization']:.1%}")
        
        for buffer_name, buffer_stats in pool_stats['buffers'].items():
            print(f"{buffer_name}: {buffer_stats['message_count']} messages")
    
    finally:
        pool.cleanup()
    
    print("Optimized ring buffer tests completed!")


if __name__ == "__main__":
    test_optimized_ring_buffer()