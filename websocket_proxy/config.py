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
    SHM_BUFFER_SIZE: int = int(os.getenv("SHM_BUFFER_SIZE", "1048576"))  # 1MB default
    MAX_MESSAGES: int = int(os.getenv("MAX_MESSAGES", "1000"))
    MESSAGE_SIZE: int = int(os.getenv("MESSAGE_SIZE", "1024"))
    
    # WebSocket server settings
    WS_HOST: str = os.getenv("WS_HOST", "0.0.0.0")
    WS_PORT: int = int(os.getenv("WS_PORT", "8765"))
    WS_MAX_CONNECTIONS: int = int(os.getenv("WS_MAX_CONNECTIONS", "100"))
    WS_MAX_QUEUE: int = int(os.getenv("WS_MAX_QUEUE", "1000"))
    WS_PING_INTERVAL: float = float(os.getenv("WS_PING_INTERVAL", "30.0"))
    WS_PING_TIMEOUT: float = float(os.getenv("WS_PING_TIMEOUT", "10.0"))
    
    # Performance settings
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "50"))
    POOL_SIZE: int = int(os.getenv("POOL_SIZE", "1000"))
    
    # Logging and monitoring
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    METRICS_ENABLED: bool = os.getenv("METRICS_ENABLED", "true").lower() == "true"
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9100"))
    
    # Security
    CORS_ORIGINS: str = os.getenv("CORS_ORIGINS", "*")
    AUTH_REQUIRED: bool = os.getenv("AUTH_REQUIRED", "false").lower() == "true"
    
    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Convert config to dictionary for logging/display."""
        return {
            key: value for key, value in cls.__dict__.items() 
            if not key.startswith('_') and key.isupper()
        }
    
    @classmethod
    def validate(cls) -> Optional[str]:
        """Validate configuration.
        
        Returns:
            Optional[str]: Error message if validation fails, None otherwise
        """
        if cls.SHM_BUFFER_SIZE < 1024:
            return "SHM_BUFFER_SIZE must be at least 1024 bytes"
            
        if cls.MAX_MESSAGES < 1:
            return "MAX_MESSAGES must be at least 1"
            
        if cls.MESSAGE_SIZE < 8:  # Minimum size for a message header
            return "MESSAGE_SIZE must be at least 8 bytes"
            
        if not (0 <= cls.WS_PORT <= 65535):
            return "WS_PORT must be between 0 and 65535"
            
        if cls.WS_MAX_CONNECTIONS < 1:
            return "WS_MAX_CONNECTIONS must be at least 1"
            
        if cls.BATCH_SIZE < 1:
            return "BATCH_SIZE must be at least 1"
            
        if cls.POOL_SIZE < 1:
            return "POOL_SIZE must be at least 1"
            
        return None

# Create a singleton instance
config = Config()

# Validate configuration on import
if __name__ != "__main__":
    if error := config.validate():
        raise ValueError(f"Invalid configuration: {error}")
