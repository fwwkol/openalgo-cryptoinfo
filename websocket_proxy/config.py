"""
Configuration management for the WebSocket proxy.
Loads settings from environment variables with sensible defaults.
"""

import os
from typing import Dict, Any, Optional

class Config:
    """Global configuration for the WebSocket proxy."""
    
    # Shared memory settings
    SHM_BUFFER_NAME: str = os.getenv("SHM_BUFFER_NAME", "ws_proxy_buffer")
    SHM_BUFFER_SIZE: int = int(os.getenv("SHM_BUFFER_SIZE", "2097152"))  # 2MB default
    MAX_MESSAGES: int = int(os.getenv("MAX_MESSAGES", "2000"))
    MESSAGE_SIZE: int = int(os.getenv("MESSAGE_SIZE", "1024"))
    
    # WebSocket server settings
    WS_HOST: str = os.getenv("WS_HOST", "0.0.0.0")
    WS_PORT: int = int(os.getenv("WS_PORT", "8765"))
    WS_MAX_CONNECTIONS: int = int(os.getenv("WS_MAX_CONNECTIONS", "500"))
    WS_PING_INTERVAL: float = float(os.getenv("WS_PING_INTERVAL", "30.0"))
    WS_PING_TIMEOUT: float = float(os.getenv("WS_PING_TIMEOUT", "10.0"))
    
    # Performance settings
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))
    CONSUME_BATCH_SIZE: int = int(os.getenv("CONSUME_BATCH_SIZE", "50"))
    
    # Logging and monitoring (production optimized)
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "WARNING")
    
    # Security - Authentication required in production
    AUTH_REQUIRED: bool = os.getenv("AUTH_REQUIRED", "true").lower() == "true"
    
    # # Broker settings
    # ENABLE_FLATTRADE: bool = os.getenv("ENABLE_FLATTRADE", "true").lower() == "true"
    # FLATTRADE_USER_ID: str = os.getenv("FLATTRADE_USER_ID", "root")

    # Ultra-high-performance settings for trading
    DIRECT_DELIVERY_MODE = True  # CRITICAL: Enable direct delivery
    RING_BUFFER_BATCH_SIZE = 50  # Process 50 messages per batch
    PROCESSING_POLL_INTERVAL_MS = 0.1  # 0.1ms polling interval
    MAX_QUEUE_SIZE = 100  # Limit fallback queue size
    WS_MAX_MESSAGE_SIZE = 65536  # 64KB messages
    WS_COMPRESSION = None  # Disable compression for speed

    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Convert config to dictionary for logging/display."""
        return {
            key: value for key, value in cls.__dict__.items()
            if not key.startswith('_') and key.isupper()
        }
    
    @classmethod
    def validate(cls) -> Optional[str]:
        """Validate configuration."""
        if cls.SHM_BUFFER_SIZE < 1024:
            return "SHM_BUFFER_SIZE must be at least 1024 bytes"
        if cls.MAX_MESSAGES < 1:
            return "MAX_MESSAGES must be at least 1"
        if cls.MESSAGE_SIZE < 8:
            return "MESSAGE_SIZE must be at least 8 bytes"
        if not (0 <= cls.WS_PORT <= 65535):
            return "WS_PORT must be between 0 and 65535"
        if cls.WS_MAX_CONNECTIONS < 1:
            return "WS_MAX_CONNECTIONS must be at least 1"
        return None

# Create singleton instance
config = Config()

# Validate configuration on import
if __name__ != "__main__":
    if error := config.validate():
        raise ValueError(f"Invalid configuration: {error}")