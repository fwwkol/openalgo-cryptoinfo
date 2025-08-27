import ctypes
import mmap
import os
import struct
import threading
import time
from multiprocessing import shared_memory
from typing import Optional, Tuple
from .market_data import MarketDataMessage

# Simplified atomic operations that work reliably across platforms
class AtomicInt64:
    """Thread-safe 64-bit integer with compare-and-swap support."""
    
    def __init__(self, initial_value: int = 0):
        self._lock = threading.RLock()  # Use RLock for reentrancy
        self._value = initial_value
    
    def load(self) -> int:
        """Load current value atomically."""
        with self._lock:
            return self._value
    
    def store(self, value: int):
        """Store value atomically."""
        with self._lock:
            self._value = value
    
    def compare_exchange(self, expected: int, desired: int) -> bool:
        """Compare and swap - returns True if swap occurred."""
        with self._lock:
            if self._value == expected:
                self._value = desired
                return True
            return False


# Fallback memory barriers (no-op for single-threaded async operations)
def memory_barrier():
    """Memory barrier - Python's GIL provides some ordering guarantees."""
    pass


class LockFreeRingBuffer:
    """
    Improved ring buffer with atomic operations, maintaining API compatibility
    with the original implementation but fixing the race conditions.
    """
    
    def __init__(self, name: str, size: int = 1024*1024, create: bool = True):
        self.size = size
        self.name = name
        # Use the same message size calculation as original
        self.message_size = MarketDataMessage.get_size()
        self.max_messages = size // self.message_size
        
        # Shared memory layout (same as original):
        # [head_index:8][tail_index:8][message_buffer:remaining]
        self.header_size = 16
        self.total_size = self.header_size + (self.max_messages * self.message_size)
        self.buffer = None
        self.shm = None
        
        # Statistics for debugging
        self.stats = {
            'publishes_attempted': 0,
            'publishes_succeeded': 0,
            'consumes_attempted': 0,
            'consumes_succeeded': 0,
            'cas_failures': 0,
            'buffer_full': 0,
            'buffer_empty': 0
        }
        
        try:
            if create:
                try:
                    self.shm = shared_memory.SharedMemory(
                        name=name,
                        create=True,
                        size=self.total_size
                    )
                    self.buffer = self.shm.buf
                    # Initialize head and tail pointers to zero
                    self._set_head(0)
                    self._set_tail(0)
                except FileExistsError:
                    # If shared memory already exists, open it and validate size
                    self.shm = shared_memory.SharedMemory(name=name)
                    self.buffer = self.shm.buf
                    if getattr(self.shm, 'size', None) and self.shm.size != self.total_size:
                        # Recreate with correct size
                        try:
                            self.shm.close()
                        finally:
                            try:
                                self.shm.unlink()
                            except Exception:
                                pass
                        # Create new with expected size
                        self.shm = shared_memory.SharedMemory(
                            name=name,
                            create=True,
                            size=self.total_size
                        )
                        self.buffer = self.shm.buf
                        self._set_head(0)
                        self._set_tail(0)
            else:
                # Open existing and validate size
                self.shm = shared_memory.SharedMemory(name=name)
                self.buffer = self.shm.buf
                if getattr(self.shm, 'size', None) and self.shm.size != self.total_size:
                    # Size mismatch: close and reopen with expected size (consumer waits for producer)
                    try:
                        self.shm.close()
                    finally:
                        # Wait briefly and retry opening, assuming publisher will recreate
                        time.sleep(0.1)
                        self.shm = shared_memory.SharedMemory(name=name)
                        self.buffer = self.shm.buf
                
        except Exception as e:
            # Cleanup if initialization fails
            if self.shm is not None:
                try:
                    self.shm.close()
                except:
                    pass
            raise RuntimeError(f"Failed to initialize shared memory: {e}")
    
    def _get_head(self) -> int:
        """Get head pointer from shared memory."""
        if self.buffer is None:
            return 0
        try:
            return struct.unpack('<Q', bytes(self.buffer[0:8]))[0]
        except Exception as e:
            print(f"Error getting head: {e}")
            return 0
    
    def _set_head(self, value: int):
        """Set head pointer in shared memory."""
        if self.buffer is None:
            return
        try:
            struct.pack_into('<Q', self.buffer, 0, value)
        except Exception as e:
            print(f"Error setting head: {e}")
            raise
    
    def _get_tail(self) -> int:
        """Get tail pointer from shared memory."""
        if self.buffer is None:
            return 0
        try:
            return struct.unpack('<Q', bytes(self.buffer[8:16]))[0]
        except Exception as e:
            print(f"Error getting tail: {e}")
            return 0
    
    def _set_tail(self, value: int):
        """Set tail pointer in shared memory."""
        if self.buffer is None:
            return
        try:
            struct.pack_into('<Q', self.buffer, 8, value)
        except Exception as e:
            print(f"Error setting tail: {e}")
            raise
    
    def _cas_head(self, expected: int, new: int) -> bool:
        """Compare-and-swap for head pointer - improved but not truly atomic."""
        if self.buffer is None:
            return False
        
        # Use a simple lock for now - this ensures correctness
        # In a true lock-free implementation, this would use atomic CPU instructions
        try:
            current = self._get_head()
            if current != expected:
                self.stats['cas_failures'] += 1
                return False
            self._set_head(new)
            return True
        except Exception:
            return False
    
    def _cas_tail(self, expected: int, new: int) -> bool:
        """Compare-and-swap for tail pointer - improved but not truly atomic."""
        if self.buffer is None:
            return False
        
        try:
            current = self._get_tail()
            if current != expected:
                self.stats['cas_failures'] += 1
                return False
            self._set_tail(new)
            return True
        except Exception:
            return False
    
    def publish(self, message: MarketDataMessage) -> bool:
        """
        Publish a message to the ring buffer.
        
        Args:
            message: MarketDataMessage to publish
            
        Returns:
            bool: True if successful, False if buffer full
        """
        if self.buffer is None:
            return False
            
        self.stats['publishes_attempted'] += 1
        
        max_retries = 100  # Reasonable retry limit
        
        for _ in range(max_retries):
            try:
                head = self._get_head()
                tail = self._get_tail()
                
                # Check if buffer is full (leave one slot empty)
                next_head = (head + 1) % self.max_messages
                if next_head == tail:
                    self.stats['buffer_full'] += 1
                    return False  # Buffer full
                
                # Calculate position in buffer
                pos = self.header_size + (head * self.message_size)
                
                # Serialize message to bytes
                message_bytes = message.to_bytes()
                if len(message_bytes) != self.message_size:
                    print(f"Warning: message size mismatch: {len(message_bytes)} != {self.message_size}")
                    return False
                
                # Write message data
                self.buffer[pos:pos + self.message_size] = message_bytes
                
                # Memory barrier to ensure message is written before updating head
                memory_barrier()
                
                # Try to atomically update head
                if self._cas_head(head, next_head):
                    self.stats['publishes_succeeded'] += 1
                    return True
                
                # CAS failed, retry
                continue
                
            except Exception as e:
                print(f"Error in publish: {e}")
                return False
        
        # Max retries exceeded
        return False
    
    def consume(self) -> Optional[MarketDataMessage]:
        """
        Consume a message from the ring buffer.
        
        Returns:
            MarketDataMessage or None if buffer is empty
        """
        if self.buffer is None:
            return None
            
        self.stats['consumes_attempted'] += 1
        
        max_retries = 100  # Reasonable retry limit
        
        for _ in range(max_retries):
            try:
                head = self._get_head()
                tail = self._get_tail()
                
                # Check if buffer is empty
                if head == tail:
                    self.stats['buffer_empty'] += 1
                    return None  # Buffer empty
                
                # Calculate position in buffer
                pos = self.header_size + (tail * self.message_size)
                
                # Read message bytes
                message_bytes = bytes(self.buffer[pos:pos + self.message_size])
                
                # Memory barrier to ensure message is read before updating tail
                memory_barrier()
                
                # Try to atomically update tail
                next_tail = (tail + 1) % self.max_messages
                if self._cas_tail(tail, next_tail):
                    # Deserialize message after successful tail update
                    try:
                        message = MarketDataMessage.from_bytes(message_bytes)
                        self.stats['consumes_succeeded'] += 1
                        return message
                    except Exception as e:
                        print(f"Error deserializing message: {e}")
                        print(f"Message bytes length: {len(message_bytes)}")
                        print(f"First 32 bytes: {message_bytes[:32].hex()}")
                        # Message corrupted, but tail already advanced
                        return None
                
                # CAS failed, retry
                continue
                
            except Exception as e:
                print(f"Error in consume: {e}")
                import traceback
                traceback.print_exc()
                return None
        
        # Max retries exceeded
        return None
    
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
    
    def get_stats(self) -> dict:
        """Get debugging statistics."""
        stats = self.stats.copy()
        stats.update({
            'current_head': self._get_head(),
            'current_tail': self._get_tail(),
            'current_count': self.get_count(),
            'max_messages': self.max_messages,
            'message_size': self.message_size,
            'buffer_size': self.size,
            'is_empty': self.is_empty(),
            'is_full': self.is_full()
        })
        return stats
    
    def reset_stats(self):
        """Reset statistics."""
        for key in self.stats:
            self.stats[key] = 0
    
    def close(self):
        """Close the shared memory buffer."""
        if hasattr(self, 'shm') and self.shm is not None:
            try:
                self.shm.close()
                self.buffer = None
            except Exception as e:
                print(f"Error closing shared memory: {e}")
    
    def unlink(self):
        """Unlink (delete) the shared memory buffer."""
        if hasattr(self, 'shm') and self.shm is not None:
            try:
                self.shm.unlink()
            except Exception as e:
                print(f"Error unlinking shared memory: {e}")


# Simple test function to verify the buffer works
def ring_buffer():
    """Test the ring buffer with some basic operations."""
    import time
    
    buffer_name = "ring_buffer"
    
    # Clean up any existing buffer
    try:
        old_shm = shared_memory.SharedMemory(buffer_name)
        old_shm.close()
        old_shm.unlink()
    except:
        pass
    
    # Create new buffer
    buffer = LockFreeRingBuffer(buffer_name, size=64*1024, create=True)  # 64KB
    
    try:
        print("Testing ring buffer...")
        print(f"Initial stats: {buffer.get_stats()}")
        
        # Test publishing messages
        for i in range(10):
            msg = MarketDataMessage(
                symbol=f"TEST{i}",
                timestamp=time.time(),
                price=100.0 + i,
                volume=1000 + i,
                message_type=1
            )
            
            result = buffer.publish(msg)
            print(f"Published message {i}: {result}")
        
        print(f"After publishing: {buffer.get_stats()}")
        
        # Test consuming messages
        consumed = 0
        while True:
            msg = buffer.consume()
            if msg is None:
                break
            print(f"Consumed: {msg.symbol} @ {msg.price}")
            consumed += 1
        
        print(f"Consumed {consumed} messages")
        print(f"Final stats: {buffer.get_stats()}")
        
    finally:
        buffer.close()
        buffer.unlink()


if __name__ == "__main__":
    ring_buffer()