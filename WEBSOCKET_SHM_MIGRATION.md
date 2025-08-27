# WebSocket Proxy Migration to Shared Memory

This document outlines the changes made to remove the AdapterManager and ZeroMQ dependencies, integrating broker adapter management directly into the WebSocket proxy using shared memory.

## Changes Made

### 1. Removed AdapterManager
- **Deleted**: `services/adapter_manager.py`
- **Reason**: Centralized broker management was unnecessary complexity
- **Replacement**: Integrated broker adapter management directly into `WebSocketProxy`

### 2. Integrated Broker Management into WebSocket Proxy
- **File**: `websocket_proxy/websocket_proxy_shm.py`
- **Changes**:
  - Added broker adapter management methods (`_start_broker_adapters`, `_stop_broker_adapters`)
  - Added reference-counted subscription management (`_broker_subscribe`, `_broker_unsubscribe`)
  - Integrated Flattrade adapter lifecycle management
  - Added configuration support for broker adapters

### 3. Removed ZeroMQ Dependencies
- **Files Updated**:
  - `websocket_proxy/server.py` - Converted to compatibility wrapper
  - `websocket_proxy/base_adapter.py` - Removed ZMQ code, kept only SHM
  - `websocket_proxy/app_integration.py` - Removed ZMQ cleanup code
  - `WEBSOCKET_FIX.md` - Updated configuration examples

### 4. Created New SHM-Only Server
- **File**: `websocket_proxy/server_shm.py`
- **Purpose**: Clean implementation using only shared memory
- **Features**:
  - Simplified server lifecycle management
  - Proper configuration validation
  - Signal handling for graceful shutdown

### 5. Updated Base Adapter
- **File**: `websocket_proxy/base_adapter.py`
- **Changes**:
  - Removed all ZeroMQ-related code
  - Simplified to SHM-only publishing
  - Cleaner resource management

## Architecture Changes

### Before (ZeroMQ + AdapterManager)
```
WebSocket Proxy <-- ZeroMQ --> AdapterManager --> Broker Adapters --> SHM
```

### After (Direct SHM Integration)
```
WebSocket Proxy --> Broker Adapters --> SHM
```

## Benefits

1. **Simplified Architecture**: Removed unnecessary intermediary layers
2. **Better Performance**: Direct SHM communication without ZeroMQ overhead
3. **Reduced Dependencies**: No longer requires ZeroMQ libraries
4. **Easier Maintenance**: Single component manages both WebSocket and broker connections
5. **Better Resource Management**: Integrated lifecycle management

## Configuration Changes

### Old Configuration (ZeroMQ)
```bash
ZMQ_HOST=127.0.0.1
ZMQ_PORT=5555
```

### New Configuration (SHM Only)
```bash
SHM_BUFFER_NAME=ws_proxy_buffer
SHM_BUFFER_SIZE=1048576
ENABLE_FLATTRADE=true
FLATTRADE_USER_ID=root
```

## Migration Guide

### For Existing Code Using AdapterManager
Replace:
```python
from services.adapter_manager import AdapterManager
manager = AdapterManager.instance()
manager.subscribe(symbol, exchange, mode, depth)
```

With:
```python
# Now handled automatically by WebSocket proxy
# Subscriptions are managed when clients connect
```

### For Existing Code Using ZeroMQ
Replace:
```python
import zmq
context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:5555")
```

With:
```python
from websocket_proxy.shm_publisher import SharedMemoryPublisher
publisher = SharedMemoryPublisher(buffer_name="ws_proxy_buffer")
publisher.publish_market_data(symbol, data)
```

## Testing

Use the provided test script to verify the new implementation:
```bash
python test_shm_websocket.py
```

This will start a WebSocket server with mock market data publishing to test the SHM-based flow.

## Backward Compatibility

- The old `websocket_proxy/server.py` is kept as a compatibility wrapper
- Existing imports will continue to work but will use the new SHM implementation
- Configuration is automatically migrated where possible

## Files Added
- `websocket_proxy/server_shm.py` - New SHM-only server implementation
- `test_shm_websocket.py` - Test script for the new implementation
- `WEBSOCKET_SHM_MIGRATION.md` - This documentation

## Files Removed
- `services/adapter_manager.py` - Functionality integrated into WebSocket proxy

## Files Modified
- `websocket_proxy/websocket_proxy_shm.py` - Added broker management
- `websocket_proxy/server.py` - Converted to compatibility wrapper
- `websocket_proxy/base_adapter.py` - Removed ZMQ, SHM-only
- `websocket_proxy/app_integration.py` - Removed ZMQ cleanup
- `WEBSOCKET_FIX.md` - Updated configuration examples
- `websocket_proxy/README.md` - Updated integration examples