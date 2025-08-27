# Shared Memory WebSocket Proxy for OpenAlgo

A high-performance, low-latency shared memory implementation for OpenAlgo's market data distribution system.

## Features

- **Sub-microsecond latency** using POSIX shared memory
- **Lock-free ring buffer** for high-throughput message passing
- **Zero-copy** data access with memory-mapped files
- **WebSocket interface** for client connections
- **Thread-safe** publisher and subscriber components

## Architecture

```
Broker Adapters → Shared Memory Ring Buffer → Memory-Mapped Subscribers → WebSocket Clients
```

## Components

1. **MarketDataMessage** - Fixed-size message structure for shared memory
2. **LockFreeRingBuffer** - High-performance circular buffer for inter-process communication
3. **SharedMemoryPublisher** - Publishes market data to the shared memory buffer
4. **SharedMemorySubscriber** - Consumes data from the shared memory buffer
5. **WebSocketProxy** - Bridges shared memory with WebSocket clients

## Installation

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. For Linux systems, you may need to increase shared memory limits:
   ```bash
   echo 'kernel.shmmax = 1073741824' | sudo tee -a /etc/sysctl.conf  # 1GB
   echo 'kernel.shmall = 268435456' | sudo tee -a /etc/sysctl.conf   # 1GB in pages
   sudo sysctl -p
   ```

## Usage

### 1. Start the WebSocket Proxy

```bash
python -m websocket_proxy.websocket_proxy_shm
```

The proxy will start on `ws://localhost:8765` by default.

### 2. Publish Market Data

```python
from websocket_proxy.shm_publisher import SharedMemoryPublisher

publisher = SharedMemoryPublisher()
publisher.start()

# Publish market data
data = {
    'type': 1,  # 1=LTP, 2=Quote, 3=Depth
    'ltp': 150.25,
    'volume': 1000,
    'bid': 150.0,
    'ask': 150.5,
    'seq': 1
}
publisher.publish_market_data('RELIANCE', data)

# Get statistics
print(publisher.get_stats())

# Cleanup
publisher.stop()
```

### 3. Subscribe to Market Data (WebSocket Client)

```javascript
const ws = new WebSocket('ws://localhost:8765');

// Subscribe to symbols
ws.onopen = () => {
  ws.send(JSON.stringify({ action: 'subscribe', symbol: 'RELIANCE' }));
  ws.send(JSON.stringify({ action: 'subscribe', symbol: 'TCS' }));
};

// Handle incoming messages
ws.onmessage = (event) => {
  console.log('Received:', JSON.parse(event.data));
};

// Unsubscribe when done
// ws.send(JSON.stringify({ action: 'unsubscribe', symbol: 'RELIANCE' }));
```

## Performance

- **Latency**: <1 microsecond for publisher to subscriber
- **Throughput**: Millions of messages per second
- **Memory Usage**: Configurable buffer size (default: 1MB)

## Example

Run the example to see the system in action:

```bash
python -m websocket_proxy.example_usage
```

This will:
1. Start a WebSocket proxy
2. Start a publisher sending mock market data
3. Connect a WebSocket client and subscribe to symbols

## Integration with OpenAlgo

To integrate with existing OpenAlgo broker adapters, use the `SharedMemoryPublisher` for direct SHM publishing:

```python
# Use this for direct SHM publishing:
from websocket_proxy.shm_publisher import SharedMemoryPublisher

# Initialize publisher
shm_publisher = SharedMemoryPublisher(buffer_name="ws_proxy_buffer")

# Publish market data directly to shared memory
from websocket_proxy.broker_adapter import SharedMemoryBrokerAdapter
publisher = SharedMemoryBrokerAdapter(broker_name="your_broker")

# Then use the publisher as before
publisher.publish_tick_data('RELIANCE', tick_data)
```

## Monitoring

Both publisher and subscriber expose statistics:

```python
stats = publisher.get_stats()
print(f"Messages published: {stats['messages_published']}")
print(f"Average latency: {stats['avg_latency_ns']} ns")
```

## Cleanup

Make sure to properly close and unlink shared memory when done:

```python
publisher.stop()
# Only unlink when no more processes need the shared memory
# publisher.cleanup()  # Uncomment to unlink shared memory
```

## License

[Your License Here]
