"""
Legacy WebSocket Proxy Server - Redirects to SHM-based implementation
This file is kept for backward compatibility but now uses the new SHM-based proxy.
All ZeroMQ functionality has been removed and replaced with shared memory.
"""
import asyncio
import logging
from utils.logging import get_logger
from .server_shm import WebSocketProxyServer
from .config import config

# Initialize logger
logger = get_logger("websocket_proxy_legacy")

class WebSocketProxy:
    """
    Legacy WebSocket Proxy - redirects to new SHM-based implementation.
    Kept for backward compatibility.
    """
    
    def __init__(self, host: str = "127.0.0.1", port: int = 8765):
        """
        Initialize the WebSocket Proxy (legacy compatibility)
        
        Args:
            host: Hostname to bind the WebSocket server to (now uses config)
            port: Port number to bind the WebSocket server to (now uses config)
        """
        logger.warning("Using legacy WebSocketProxy class - redirecting to SHM-based implementation")
        logger.info(f"Legacy parameters host={host}, port={port} - using config values instead")
        
        # Update config with legacy parameters if different
        if host != config.WS_HOST:
            logger.info(f"Overriding WS_HOST from {config.WS_HOST} to {host}")
            config.WS_HOST = host
        if port != config.WS_PORT:
            logger.info(f"Overriding WS_PORT from {config.WS_PORT} to {port}")
            config.WS_PORT = port
        
        self.server = WebSocketProxyServer()
        self.host = host
        self.port = port
    
    async def start(self):
        """Start the WebSocket server (legacy compatibility)"""
        logger.info("Starting legacy WebSocket proxy (using SHM implementation)")
        await self.server.start()
    
    async def stop(self):
        """Stop the WebSocket server (legacy compatibility)"""
        logger.info("Stopping legacy WebSocket proxy")
        await self.server.stop()
    
    # Legacy methods for backward compatibility
    async def handle_client(self, websocket):
        """Legacy method - not used in new implementation"""
        logger.warning("handle_client called on legacy proxy - this should not happen")
    
    async def cleanup_client(self, client_id):
        """Legacy method - not used in new implementation"""
        logger.warning("cleanup_client called on legacy proxy - this should not happen")
    
    async def process_client_message(self, client_id, message):
        """Legacy method - not used in new implementation"""
        logger.warning("process_client_message called on legacy proxy - this should not happen")

# Legacy main function for backward compatibility
async def main():
    """Legacy main function - redirects to new implementation"""
    logger.info("Starting WebSocket proxy server (legacy main)")
    server = WebSocketProxyServer()
    
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
        import sys
        sys.exit(1)