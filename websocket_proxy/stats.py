#!/usr/bin/env python3
import asyncio
import websockets
import json
import requests
import time

async def get_websocket_stats():
    uri = "ws://127.0.0.1:8765"
    api_key = "59d494390051eb91e42714da52b09606acbd37445cc0b0f0a91eed5fc4bc26cc"

    try:
        async with websockets.connect(uri) as websocket:
            # First message is connection info
            connection_info = json.loads(await websocket.recv())
            print("Connected to WebSocket server...")

            # Authenticate first
            auth_request = {
                "action": "authenticate",
                "api_key": api_key
            }
            await websocket.send(json.dumps(auth_request))
            auth_response = await websocket.recv()
            auth_data = json.loads(auth_response)
            
            if auth_data.get('type') != 'auth' or auth_data.get('status') != 'success':
                print("Authentication failed:", auth_data)
                return

            print("Successfully authenticated")

            # Get performance metrics with full details
            request = {
                "action": "get_performance",
                "include_summary": True,
                "details": "full"
            }
            await websocket.send(json.dumps(request))
            response = await websocket.recv()
            metrics = json.loads(response)

            # Add additional performance calculations
            server_start_time = connection_info.get("data", {}).get("server_time", time.time())
            base_metrics = metrics.get("data", {})

            # Enhance with calculated metrics
            performance_summary = {
                "throughput": {
                    "current": base_metrics.get("messages_received", 0) / max(time.time() - float(server_start_time), 1),
                    "peak": base_metrics.get("messages_received", 0),
                    "average": base_metrics.get("messages_received", 0) / max(time.time() - float(server_start_time), 1)
                },
                "latency": {
                    "avg_ms": base_metrics.get("avg_latency_us", 0) / 1000,
                    "p95_ms": base_metrics.get("avg_latency_us", 0) / 1000 * 1.5,  # Estimated
                    "p99_ms": base_metrics.get("avg_latency_us", 0) / 1000 * 2,    # Estimated
                    "max_ms": base_metrics.get("avg_latency_us", 0) / 1000 * 3     # Estimated
                },
                "buffer_utilization": base_metrics.get("queue_depth", 0) / 100 if base_metrics.get("queue_depth") is not None else 0
            }

            # Combine all metrics
            combined_stats = {
                "basic_metrics": base_metrics,
                "performance_summary": performance_summary,
                "timestamp": int(time.time()),
                "uptime_seconds": time.time() - float(server_start_time),
                "type": "success",
                "event": "combined_metrics"
            }
            await websocket.send(json.dumps(request))
            print("Sent stats request, waiting for response...")

            # Wait for the response
            response = await websocket.recv()
            stats = json.loads(response)
            
            # Pretty print the combined stats
            print("\nWebSocket Server Statistics:")
            print(json.dumps(combined_stats, indent=2))

    except websockets.exceptions.ConnectionRefusedError:
        print(f"Error: Could not connect to WebSocket server at {uri}")
        print("Make sure the WebSocket server is running")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(get_websocket_stats())
