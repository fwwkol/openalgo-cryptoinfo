import threading
import time
from typing import Callable, Dict, Set, Optional
from websocket_proxy.ring_buffer import LockFreeRingBuffer
from websocket_proxy.market_data import MarketDataMessage

class SharedMemorySubscriber:
    def __init__(self, buffer_name: str = "ws_proxy_buffer", retry_interval: float = 0.1):
        self.buffer_name = buffer_name
        self.retry_interval = retry_interval
        self.ring_buffer = None
        self.initialize_ring_buffer()
        self.subscriptions: Set[str] = set()  # Symbols
        self.callbacks: Dict[str, Callable] = {}
        self.running = False
        self.consumer_thread: Optional[threading.Thread] = None
        self.stats = {
            'messages_consumed': 0,
            'processing_latency_ns': 0,
            'buffer_errors': 0
        }
        
    def initialize_ring_buffer(self):
        """Initialize the ring buffer, retrying if it doesn't exist yet"""
        while True:
            try:
                self.ring_buffer = LockFreeRingBuffer(self.buffer_name, create=False)
                break
            except FileNotFoundError:
                print(f"Waiting for shared memory '{self.buffer_name}' to be created...")
                time.sleep(self.retry_interval)
    
    def subscribe(self, symbol: str, callback: Callable):
        """Subscribe to specific symbol updates"""
        sym = str(symbol)
        self.subscriptions.add(sym)
        self.callbacks[sym] = callback
    
    def unsubscribe(self, symbol: str):
        """Unsubscribe from symbol updates"""
        sym = str(symbol)
        self.subscriptions.discard(sym)
        self.callbacks.pop(sym, None)
    
    def _consumer_loop(self):
        """Main consumer loop - runs in separate thread"""
        while self.running:
            try:
                if self.ring_buffer is None:
                    self.initialize_ring_buffer()
                    continue
                    
                message = self.ring_buffer.consume()
                if message is None:
                    # No messages available, brief pause
                    time.sleep(0.000001)  # 1 microsecond
                    continue
                
                start_process_time = time.time_ns()
                
                # Check if we're subscribed to this symbol
                if message.symbol in self.subscriptions:
                    callback = self.callbacks.get(message.symbol)
                    if callback:
                        try:
                            callback(message)
                        except Exception as e:
                            print(f"Callback error for symbol {message.symbol}: {e}")
                
                # Update statistics
                end_process_time = time.time_ns()
                self.stats['messages_consumed'] += 1
                processing_latency = end_process_time - start_process_time
                self.stats['processing_latency_ns'] = (
                    (self.stats['processing_latency_ns'] * (self.stats['messages_consumed'] - 1) + processing_latency)
                    / max(1, self.stats['messages_consumed'])
                )
                
            except Exception as e:
                self.stats['buffer_errors'] = self.stats.get('buffer_errors', 0) + 1
                print(f"Error in consumer loop: {e}")
                if self.ring_buffer is not None:
                    try:
                        self.ring_buffer.close()
                    except:
                        pass
                    self.ring_buffer = None
                time.sleep(self.retry_interval)
    
    def start(self):
        """Start the consumer thread"""
        if not self.running:
            self.running = True
            self.consumer_thread = threading.Thread(target=self._consumer_loop, daemon=True)
            self.consumer_thread.start()
    
    def stop(self):
        """Stop the consumer thread"""
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=1.0)
        if self.ring_buffer is not None:
            try:
                self.ring_buffer.close()
            except Exception as e:
                print(f"Error closing ring buffer: {e}")
    
    def get_stats(self) -> dict:
        return self.stats.copy()
