"""
Simplified WebSocket Proxy Server using Shared Memory (no ZeroMQ)
Integrates broker adapter management directly into the proxy.
"""
import asyncio
import logging
import signal
import sys
from utils.logging import get_logger
from .websocket_proxy_shm import WebSocketProxy
from .config import config

# Initialize logger
logger = get_logger("websocket_proxy_server")

class WebSocketProxyServer:
    """Main server class that manages the WebSocket proxy lifecycle."""
    
    def __init__(self):
        self.proxy = WebSocketProxy()
        self.running = False
    
    async def start(self):
        """Start the WebSocket proxy server."""
        if self.running:
            logger.warning("Server is already running")
            return
        
        logger.info("Starting WebSocket Proxy Server (SHM mode)")
        logger.info(f"Configuration: {config.to_dict()}")
        
        # Validate configuration
        if error := config.validate():
            logger.error(f"Invalid configuration: {error}")
            raise ValueError(f"Invalid configuration: {error}")
        
        self.running = True
        
        try:
            await self.proxy.start()
        except Exception as e:
            logger.error(f"Error starting WebSocket proxy: {e}")
            raise
        finally:
            self.running = False
    
    async def stop(self):
        """Stop the WebSocket proxy server."""
        if not self.running:
            return
        
        logger.info("Stopping WebSocket Proxy Server")
        self.running = False
        
        try:
            await self.proxy.shutdown()
        except Exception as e:
            logger.error(f"Error stopping WebSocket proxy: {e}")

async def main():
    """Main entry point for the WebSocket proxy server."""
    server = WebSocketProxyServer()
    
    # Handle graceful shutdown
    def signal_handler():
        logger.info("Received shutdown signal")
        asyncio.create_task(server.stop())
    
    # Set up signal handlers (if supported)
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
        logger.info("Signal handlers registered")
    except (NotImplementedError, RuntimeError):
        logger.info("Signal handlers not supported on this platform")
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise
    finally:
        await server.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)