"""
Flask Integration for Shared Memory WebSocket Proxy
Clean integration without ZeroMQ dependencies
"""

import asyncio
import threading
import platform
import os
import signal
import atexit

from utils.logging import get_logger
from .websocket_proxy_shm import WebSocketProxy

# Set correct event loop policy for Windows
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Global state
_websocket_server_started = False
_websocket_proxy_instance = None
_websocket_thread = None

logger = get_logger(__name__)

def should_start_websocket():
    """Determine if current process should start WebSocket server"""
    # In debug mode, only start in Flask child process
    if os.environ.get('FLASK_DEBUG', '').lower() in ('1', 'true'):
        return os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
    return True

def cleanup_websocket_server():
    """Clean up WebSocket server resources"""
    global _websocket_proxy_instance, _websocket_thread
    
    try:
        logger.info("Cleaning up WebSocket server...")
        
        if _websocket_proxy_instance:
            # Signal shutdown
            if hasattr(_websocket_proxy_instance, 'shutdown_event'):
                _websocket_proxy_instance.shutdown_event.set()
            
            # Close server gracefully
            try:
                if hasattr(_websocket_proxy_instance, 'server') and _websocket_proxy_instance.server:
                    _websocket_proxy_instance.server.close()
            except Exception as e:
                logger.warning(f"Error closing server: {e}")
            
            _websocket_proxy_instance = None
        
        if _websocket_thread and _websocket_thread.is_alive():
            logger.info("Waiting for WebSocket thread to finish...")
            _websocket_thread.join(timeout=3.0)
            
            if _websocket_thread.is_alive():
                logger.warning("WebSocket thread did not finish gracefully")
        
        _websocket_thread = None
        logger.info("WebSocket server cleanup completed")
        
    except Exception as e:
        logger.error(f"Error during WebSocket cleanup: {e}")
        _websocket_proxy_instance = None
        _websocket_thread = None

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    cleanup_websocket_server()
    os._exit(0)

def start_websocket_server():
    """Start WebSocket proxy server in separate thread"""
    global _websocket_proxy_instance, _websocket_thread
    
    logger.info("Starting WebSocket proxy server in separate thread")
    
    def run_websocket_server():
        """Run WebSocket server in event loop"""
        global _websocket_proxy_instance
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Create shared memory WebSocket proxy
            _websocket_proxy_instance = WebSocketProxy()
            
            # Start proxy
            loop.run_until_complete(_websocket_proxy_instance.start())
            
        except KeyboardInterrupt:
            logger.info("WebSocket server interrupted")
        except Exception as e:
            logger.exception(f"Error in WebSocket server thread: {e}")
        finally:
            # Clean up event loop
            try:
                if _websocket_proxy_instance:
                    loop.run_until_complete(_websocket_proxy_instance.shutdown())
            except Exception as e:
                logger.warning(f"Error during proxy shutdown: {e}")
            finally:
                _websocket_proxy_instance = None
                loop.close()
    
    # Start WebSocket server thread
    _websocket_thread = threading.Thread(
        target=run_websocket_server,
        daemon=False  # Allow proper cleanup
    )
    _websocket_thread.start()
    
    # Register cleanup handlers
    atexit.register(cleanup_websocket_server)
    
    # Register signal handlers
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signals_registered = ["SIGINT"]
        
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)
            signals_registered.append("SIGTERM")
        
        logger.info(f"Signal handlers registered: {', '.join(signals_registered)}")
    except Exception as e:
        logger.warning(f"Could not register signal handlers: {e}")
    
    logger.info("WebSocket proxy server thread started")
    return _websocket_thread

def start_websocket_proxy(app):
    """
    Integrate WebSocket proxy server with Flask application
    Args:
        app: Flask application instance
    """
    global _websocket_server_started
    
    if should_start_websocket():
        if not _websocket_server_started:
            _websocket_server_started = True
            logger.info("Starting WebSocket server in Flask application process")
            start_websocket_server()
            logger.info("WebSocket server integration with Flask complete")
        else:
            logger.info("WebSocket server already running, skipping initialization")
    else:
        logger.info("Skipping WebSocket server in parent/monitor process")
