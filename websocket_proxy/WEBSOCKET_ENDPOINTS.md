# OpenAlgo WebSocket API Documentation

## Connection Details

**WebSocket URL**: `ws://127.0.0.1:8765` (default local development)

## Initial Connection

Upon initial connection, the server immediately sends a connection info message:

```json
{
  "type": "success",
  "event": "connected",
  "data": {
    "client_id": "client-123456",
    "server_time": 1694167834,
    "auth_required": true,
    "direct_delivery": true
  }
}
```

## Authentication

First message after connection must be authentication.

### Request
```json
{
  "action": "authenticate",
  "api_key": "your_openalgo_api_key"
}
```

### Response
```json
{
  "type": "auth",
  "status": "success",
  "message": "Authentication successful"
}
```

or on failure:
```json
{
  "type": "auth",
  "status": "error",
  "message": "Authentication failed: Invalid API key"
}
```

## Market Data Subscription

### Subscribe Request
```json
{
  "action": "subscribe",
  "symbol": "RELIANCE",
  "exchange": "NSE",
  "mode": 1  // 1=LTP, 2=Quote, 3=Depth
}
```

### Subscribe Response
```json
{
  "type": "subscribe",
  "status": "success",
  "message": "Subscription successful"
}
```

### Market Data Updates
```json
{
  "type": "market_data",
  "symbol": "RELIANCE",
  "exchange": "NSE",
  "ltp": 2500.45,
  "timestamp": 1694167834,
  // Additional fields based on subscription mode
  "depth": {
    "buy": [...],
    "sell": [...]
  },
  "quote": {
    "last_price": 2500.45,
    "change": 23.45,
    "change_percent": 0.94,
    "volume": 1234567,
    "open": 2480.00,
    "high": 2505.90,
    "low": 2478.15,
    "close": 2477.00
  }
}
```

### Unsubscribe Request
```json
{
  "action": "unsubscribe",
  "symbol": "RELIANCE",
  "exchange": "NSE",
  "mode": 1
}
```

### Unsubscribe Response
```json
{
  "type": "unsubscribe",
  "status": "success",
  "message": "Unsubscribed successfully",
  "successful": [
    {
      "symbol": "RELIANCE",
      "exchange": "NSE",
      "mode": 1
    }
  ]
}
```

## List Active Subscriptions

### List Subscriptions Request
```json
{
  "action": "list_subscriptions"
}
```

### List Subscriptions Response
```json
{
  "type": "success",
  "event": "subscriptions",
  "data": {
    "count": 2,
    "subscriptions": [
      {
        "symbol": "RELIANCE",
        "exchange": "NSE",
        "mode": 1,
        "mode_str": "LTP",
        "subscribed_at": 1694167834
      },
      {
        "symbol": "TCS",
        "exchange": "NSE",
        "mode": 2,
        "mode_str": "QUOTE",
        "subscribed_at": 1694167840
      }
    ]
  }
}
```

## Connection Heartbeat

### Ping Request
```json
{
  "action": "ping"
}
```

### Pong Response
```json
{
  "type": "pong",
  "server_time": 1694167834,
  "latency_us": 235
}
```

### Ping Request with Timestamp
```json
{
  "action": "ping",
  "timestamp": 1694167834000  // Optional client timestamp in milliseconds
}
```

### Pong Response with Latency Details
```json
{
  "type": "success",
  "event": "pong",
  "data": {
    "client_timestamp": 1694167834000,
    "server_timestamp": 1694167834050,
    "latency_ms": 50
  }
}
```

## Bulk Operations

### Bulk Subscribe Request
```json
{
  "action": "subscribe",
  "symbols": [
    {
      "symbol": "RELIANCE",
      "exchange": "NSE",
      "mode": 1
    },
    {
      "symbol": "TCS",
      "exchange": "NSE",
      "mode": 2
    }
  ]
}
```

### Bulk Unsubscribe Request
```json
{
  "action": "unsubscribe",
  "symbols": [
    {
      "symbol": "RELIANCE",
      "exchange": "NSE",
      "mode": 1
    },
    {
      "symbol": "TCS",
      "exchange": "NSE",
      "mode": 2
    }
  ]
}
```

### Bulk Operation Response
```json
{
  "type": "success",
  "event": "bulk_operation",
  "data": {
    "successful": [
      {
        "symbol": "RELIANCE",
        "exchange": "NSE",
        "mode": 1
      }
    ],
    "failed": [
      {
        "symbol": "TCS",
        "exchange": "NSE",
        "mode": 2,
        "reason": "Invalid symbol"
      }
    ]
  }
}
```

## Performance Monitoring

### Get Performance Stats Request
```json
{
  "action": "get_performance"
}
```

### Performance Stats Response

The response includes real-time performance metrics in the following format:

```json
{
  "basic_metrics": {
    "messages_received": 21,
    "messages_sent": 21,
    "active_connections": 3,
    "errors": 0,
    "dropped_messages": 0,
    "avg_latency_ns": 2299500.0,
    "avg_latency_us": 2299.5,
    "queue_depth": 0,
    "total_subscriptions": 3,
    "direct_delivery_mode": true,
    "batch_size": 50,
    "poll_interval_ms": 0.1
  },
  "performance_summary": {
    "throughput": {
      "current": 21.0,
      "peak": 21,
      "average": 21.0
    },
    "latency": {
      "avg_ms": 2.2995,
      "p95_ms": 3.44925,
      "p99_ms": 4.599,
      "max_ms": 6.8985
    },
    "buffer_utilization": 0.0
  },
  "timestamp": 1757356098,
  "uptime_seconds": 0.8547284603118896,
  "type": "success",
  "event": "combined_metrics"
}
```

### Performance Metrics Fields Description

**Time Unit Reference:**
- `ns` (nanoseconds): 1 billionth of a second (0.000000001 seconds)
- `us` (microseconds): 1 millionth of a second (0.000001 seconds)
- `ms` (milliseconds): 1 thousandth of a second (0.001 seconds)

**For context:**
- 1 millisecond (ms) = 1,000 microseconds (us)
- 1 microsecond (us) = 1,000 nanoseconds (ns)
- 1 millisecond (ms) = 1,000,000 nanoseconds (ns)

**Example latency comparisons:**
- 1ms latency: Typical good network latency
- 100us latency: Very fast internal processing time
- 2300ns latency: Ultra-low latency processing time

The metrics response includes several categories of measurements:

#### 1. Basic Metrics
- `messages_received`: Total number of messages received
- `messages_sent`: Total number of messages sent
- `active_connections`: Current number of connected clients
- `errors`: Count of errors encountered
- `dropped_messages`: Number of messages that couldn't be delivered
- `avg_latency_ns`: Average latency in nanoseconds
- `avg_latency_us`: Average latency in microseconds
- `queue_depth`: Current message queue size (0 in direct delivery mode)
- `total_subscriptions`: Total number of active market data subscriptions
- `direct_delivery_mode`: Whether direct message delivery is enabled
- `batch_size`: Size of message batches for processing
- `poll_interval_ms`: Interval between message processing cycles

#### 2. Performance Summary
**Throughput metrics:**
- `current`: Current messages per second
- `peak`: Highest messages per second achieved
- `average`: Average messages per second

**Latency metrics:**
- `avg_ms`: Average latency in milliseconds
- `p95_ms`: 95th percentile latency
- `p99_ms`: 99th percentile latency
- `max_ms`: Maximum observed latency

**Buffer utilization:**
- `buffer_utilization`: Current buffer usage (0.0-1.0)

#### 3. Metadata
- `timestamp`: Server timestamp
- `uptime_seconds`: Server uptime in seconds
- `type`: Always "success"
- `event`: Always "combined_metrics"

## Error Responses

All error responses follow this format:
```json
{
  "type": "error",
  "status": "error",
  "message": "Detailed error message",
  "code": "ERROR_CODE"  // Optional error code
}
```

Common error scenarios:
- Authentication failures
- Invalid symbols or exchanges
- Invalid subscription modes
- Connection limits exceeded
- Rate limits exceeded

## Error Codes

| Code | Description |
|------|-------------|
| AUTH_FAILED | Authentication failed |
| INVALID_SYMBOL | Invalid symbol or exchange |
| INVALID_MODE | Invalid subscription mode |
| SUB_LIMIT | Subscription limit exceeded |
| CONN_LIMIT | Connection limit exceeded |
| RATE_LIMIT | Rate limit exceeded |

## Rate Limits

- Maximum 1000 active subscriptions per connection
- Maximum 3 concurrent WebSocket connections per API key
- Reconnection backoff: Start at 1s, max 30s

## Best Practices

1. Always authenticate immediately after connecting
2. Handle reconnection with exponential backoff
3. Maintain subscription state for reconnection
4. Process market data updates asynchronously
5. Monitor performance metrics for optimization

## Security Notes

1. API keys should be kept secure and never exposed
2. Use TLS/SSL in production (wss:// instead of ws://)
3. Implement proper token rotation and expiry
4. Monitor for unusual patterns or abuse