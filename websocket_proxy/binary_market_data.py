"""
Enhanced Binary Market Data for ultra-fast serialization with OHLC support.
Optimized for nanosecond-level performance with fixed-size binary layout.
"""

import struct
import time
import xxhash
from dataclasses import dataclass
from typing import ClassVar, Optional, Union
from .market_data import MarketDataMessage

# Cache for symbol hash computations
_symbol_hash_cache = {}


def get_symbol_hash(symbol: str, exchange: str) -> int:
    """
    Get cached hash for symbol-exchange pair.
    
    Args:
        symbol: Trading symbol
        exchange: Exchange name
        
    Returns:
        int: 64-bit hash value
    """
    key = f"{exchange}:{symbol}"
    if key not in _symbol_hash_cache:
        _symbol_hash_cache[key] = xxhash.xxh64(key).intdigest()
    return _symbol_hash_cache[key]


def clear_symbol_hash_cache():
    """Clear the symbol hash cache."""
    global _symbol_hash_cache
    _symbol_hash_cache.clear()


@dataclass
class BinaryMarketData:
    """
    Fixed-size binary market data message with OHLC support for ultra-fast serialization.
    
    Memory layout (128 bytes total, cache-line aligned):
    - timestamp_ns: 8 bytes (nanosecond timestamp)
    - symbol_hash: 8 bytes (xxhash of exchange:symbol)
    - symbol: 32 bytes (null-padded symbol string)
    - price: 8 bytes (fixed-point price * 10000)
    - open_price: 8 bytes (fixed-point open price * 10000)
    - high_price: 8 bytes (fixed-point high price * 10000)
    - low_price: 8 bytes (fixed-point low price * 10000)
    - close_price: 8 bytes (fixed-point close price * 10000)
    - volume: 4 bytes (volume)
    - bid_price: 4 bytes (bid price * 10000)
    - ask_price: 4 bytes (ask price * 10000)
    - message_type: 1 byte (message type enum)
    - exchange_id: 1 byte (exchange identifier)
    - _padding: 26 bytes (alignment padding)
    """
    
    # Fixed binary layout - 128 bytes total (cache line aligned)
    FORMAT: ClassVar[str] = '<QQ32sQQQQQIIIBB26x'  # Little endian, padded to 128 bytes
    SIZE: ClassVar[int] = 128
    
    # Message type constants
    TYPE_LTP: ClassVar[int] = 1
    TYPE_QUOTE: ClassVar[int] = 2
    TYPE_DEPTH: ClassVar[int] = 3
    
    # Exchange ID constants
    EXCHANGE_NSE: ClassVar[int] = 1
    EXCHANGE_BSE: ClassVar[int] = 2
    EXCHANGE_MCX: ClassVar[int] = 3
    EXCHANGE_NFO: ClassVar[int] = 4
    EXCHANGE_NSE_INDEX: ClassVar[int] = 5  # Add this
    EXCHANGE_BSE_INDEX: ClassVar[int] = 6  # Add this
    
    timestamp_ns: int
    symbol_hash: int
    symbol: bytes  # 32 bytes fixed
    price: int     # Fixed point * 10000 (LTP)
    open_price: int    # Fixed point * 10000
    high_price: int    # Fixed point * 10000
    low_price: int     # Fixed point * 10000
    close_price: int   # Fixed point * 10000
    volume: int
    bid_price: int     # Fixed point * 10000
    ask_price: int     # Fixed point * 10000
    message_type: int
    exchange_id: int
    
    def __post_init__(self):
        """Validate and normalize fields after initialization."""
        # Ensure symbol is bytes and properly sized
        if isinstance(self.symbol, str):
            self.symbol = self.symbol.encode('utf-8')
        
        # Truncate or pad symbol to exactly 32 bytes
        if len(self.symbol) > 32:
            self.symbol = self.symbol[:32]
        elif len(self.symbol) < 32:
            self.symbol = self.symbol.ljust(32, b'\x00')
    
    def to_bytes(self) -> bytes:
        """
        Serialize to binary format with minimal overhead.
        
        Returns:
            bytes: 128-byte binary representation
        """
        return struct.pack(
            self.FORMAT,
            self.timestamp_ns,
            self.symbol_hash,
            self.symbol,
            self.price,
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
            self.volume,
            self.bid_price,
            self.ask_price,
            self.message_type,
            self.exchange_id
        )
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'BinaryMarketData':
        """
        Deserialize from binary format.
        
        Args:
            data: 128-byte binary data
            
        Returns:
            BinaryMarketData: Deserialized message
            
        Raises:
            ValueError: If data is not exactly 128 bytes
        """
        if len(data) != cls.SIZE:
            raise ValueError(f"Data must be exactly {cls.SIZE} bytes, got {len(data)}")
        
        unpacked = struct.unpack(cls.FORMAT, data)
        return cls(*unpacked)
    
    @classmethod
    def from_market_data_message(cls, msg: MarketDataMessage, exchange: str = "NSE") -> 'BinaryMarketData':
        """
        Convert from MarketDataMessage to BinaryMarketData.
        
        Args:
            msg: MarketDataMessage instance
            exchange: Exchange name for hash computation
            
        Returns:
            BinaryMarketData: Converted message
        """
        # Convert timestamp to nanoseconds
        timestamp_ns = int(msg.timestamp * 1_000_000_000) if msg.timestamp else int(time.time_ns())
        
        # Get symbol hash
        symbol_hash = get_symbol_hash(msg.symbol, exchange)
        
        # Convert prices to fixed-point (multiply by 10000)
        price = int(msg.price * 10000) if msg.price else 0
        open_price = int(getattr(msg, 'open_price', 0) * 10000) if hasattr(msg, 'open_price') and getattr(msg, 'open_price') else 0
        high_price = int(getattr(msg, 'high_price', 0) * 10000) if hasattr(msg, 'high_price') and getattr(msg, 'high_price') else 0
        low_price = int(getattr(msg, 'low_price', 0) * 10000) if hasattr(msg, 'low_price') and getattr(msg, 'low_price') else 0
        close_price = int(getattr(msg, 'close_price', 0) * 10000) if hasattr(msg, 'close_price') and getattr(msg, 'close_price') else 0
        bid_price = int(getattr(msg, 'bid_price', 0) * 10000) if hasattr(msg, 'bid_price') and getattr(msg, 'bid_price') else 0
        ask_price = int(getattr(msg, 'ask_price', 0) * 10000) if hasattr(msg, 'ask_price') and getattr(msg, 'ask_price') else 0
        
        # Map exchange to ID
        exchange_id = cls._get_exchange_id(exchange)
        
        return cls(
            timestamp_ns=timestamp_ns,
            symbol_hash=symbol_hash,
            symbol=msg.symbol.encode('utf-8'),
            price=price,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=msg.volume or 0,
            bid_price=bid_price,
            ask_price=ask_price,
            message_type=msg.message_type or cls.TYPE_LTP,
            exchange_id=exchange_id
        )
    
    @classmethod
    def from_normalized_data(cls, data: dict, exchange: str = "NSE") -> 'BinaryMarketData':
        """
        Convert from normalized market data dict to BinaryMarketData.
        This is the key method for preserving OHLC data from Flattrade.
        
        Args:
            data: Normalized market data dictionary
            exchange: Exchange name for hash computation
            
        Returns:
            BinaryMarketData: Converted message
        """
        symbol = data.get('symbol', '')
        timestamp = data.get('timestamp', time.time() * 1000)
        
        # Convert timestamp to nanoseconds (assuming input is in milliseconds)
        timestamp_ns = int(timestamp * 1_000_000) if timestamp else int(time.time_ns())
        
        # Get symbol hash
        symbol_hash = get_symbol_hash(symbol, exchange)
        
        # Convert all prices to fixed-point (multiply by 10000)
        price = int(float(data.get('ltp', 0) or data.get('price', 0)) * 10000)
        open_price = int(float(data.get('open', 0) or 0) * 10000)
        high_price = int(float(data.get('high', 0) or 0) * 10000)
        low_price = int(float(data.get('low', 0) or 0) * 10000)
        close_price = int(float(data.get('close', 0) or 0) * 10000)
        bid_price = int(float(data.get('bid_price', 0) or data.get('best_bid', 0) or 0) * 10000)
        ask_price = int(float(data.get('ask_price', 0) or data.get('best_ask', 0) or 0) * 10000)
        
        # Map exchange to ID
        exchange_id = cls._get_exchange_id(exchange)
        
        return cls(
            timestamp_ns=timestamp_ns,
            symbol_hash=symbol_hash,
            symbol=symbol.encode('utf-8'),
            price=price,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=int(data.get('volume', 0) or 0),
            bid_price=bid_price,
            ask_price=ask_price,
            message_type=data.get('mode', cls.TYPE_LTP),
            exchange_id=exchange_id
        )
    
    def to_market_data_message(self) -> MarketDataMessage:
        """
        Convert to MarketDataMessage format.
        
        Returns:
            MarketDataMessage: Converted message
        """
        # Convert timestamp back to seconds
        timestamp = self.timestamp_ns / 1_000_000_000
        
        # Convert symbol back to string
        symbol = self.symbol.rstrip(b'\x00').decode('utf-8')
        
        # Convert fixed-point prices back to float
        price = self.price / 10000.0
        
        return MarketDataMessage(
            symbol=symbol,
            timestamp=timestamp,
            price=price,
            volume=self.volume,
            message_type=self.message_type
        )
    
    def to_normalized_data(self) -> dict:
        """
        Convert back to normalized data dictionary with all OHLC fields.
        
        Returns:
            dict: Normalized market data with OHLC fields
        """
        symbol = self.get_symbol_string()
        
        return {
            'symbol': symbol,
            'timestamp': self.get_timestamp_milliseconds(),
            'ltp': self.get_price_float(),
            'price': self.get_price_float(),
            'open': self.get_open_price_float(),
            'high': self.get_high_price_float(),
            'low': self.get_low_price_float(),
            'close': self.get_close_price_float(),
            'volume': self.volume,
            'bid_price': self.get_bid_price_float(),
            'ask_price': self.get_ask_price_float(),
            'best_bid': self.get_bid_price_float(),
            'best_ask': self.get_ask_price_float(),
            'mode': self.message_type,
            'exchange': self._get_exchange_name()
        }
    
    @classmethod
    def create_ltp_message(cls, symbol: str, exchange: str, price: float, 
                          volume: int = 0, timestamp_ns: Optional[int] = None) -> 'BinaryMarketData':
        """
        Create LTP (Last Traded Price) message.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            price: Last traded price
            volume: Volume (optional)
            timestamp_ns: Timestamp in nanoseconds (optional, uses current time)
            
        Returns:
            BinaryMarketData: LTP message
        """
        if timestamp_ns is None:
            timestamp_ns = time.time_ns()
        
        return cls(
            timestamp_ns=timestamp_ns,
            symbol_hash=get_symbol_hash(symbol, exchange),
            symbol=symbol.encode('utf-8'),
            price=int(price * 10000),
            open_price=0,
            high_price=0,
            low_price=0,
            close_price=0,
            volume=volume,
            bid_price=0,
            ask_price=0,
            message_type=cls.TYPE_LTP,
            exchange_id=cls._get_exchange_id(exchange)
        )
    
    @classmethod
    def create_quote_message(cls, symbol: str, exchange: str, price: float, 
                           open_price: float = 0, high_price: float = 0, 
                           low_price: float = 0, close_price: float = 0,
                           bid_price: float = 0, ask_price: float = 0,
                           volume: int = 0, timestamp_ns: Optional[int] = None) -> 'BinaryMarketData':
        """
        Create quote message with full OHLC and bid/ask data.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            price: Last traded price
            open_price: Open price
            high_price: High price
            low_price: Low price
            close_price: Close price
            bid_price: Bid price
            ask_price: Ask price
            volume: Volume (optional)
            timestamp_ns: Timestamp in nanoseconds (optional)
            
        Returns:
            BinaryMarketData: Quote message
        """
        if timestamp_ns is None:
            timestamp_ns = time.time_ns()
        
        return cls(
            timestamp_ns=timestamp_ns,
            symbol_hash=get_symbol_hash(symbol, exchange),
            symbol=symbol.encode('utf-8'),
            price=int(price * 10000),
            open_price=int(open_price * 10000),
            high_price=int(high_price * 10000),
            low_price=int(low_price * 10000),
            close_price=int(close_price * 10000),
            volume=volume,
            bid_price=int(bid_price * 10000),
            ask_price=int(ask_price * 10000),
            message_type=cls.TYPE_QUOTE,
            exchange_id=cls._get_exchange_id(exchange)
        )
    
    @staticmethod
    def _get_exchange_id(exchange: str) -> int:
        """Map exchange name to ID."""
        exchange_map = {
            'NSE': BinaryMarketData.EXCHANGE_NSE,
            'BSE': BinaryMarketData.EXCHANGE_BSE,
            'MCX': BinaryMarketData.EXCHANGE_MCX,
            'NFO': BinaryMarketData.EXCHANGE_NFO,
            # Add index exchange mappings
            'NSE_INDEX': 5,  # New exchange ID for NSE indices
            'BSE_INDEX': 6,  # New exchange ID for BSE indices
        }
        return exchange_map.get(exchange.upper(), BinaryMarketData.EXCHANGE_NSE)

    def _get_exchange_name(self) -> str:
        """Get exchange name from exchange ID."""
        exchange_map = {
            self.EXCHANGE_NSE: 'NSE',
            self.EXCHANGE_BSE: 'BSE', 
            self.EXCHANGE_MCX: 'MCX',
            self.EXCHANGE_NFO: 'NFO',
            # Add reverse mappings for indices
            5: 'NSE_INDEX',
            6: 'BSE_INDEX',
        }
        return exchange_map.get(self.exchange_id, 'NSE')

    
    def get_symbol_string(self) -> str:
        """
        Get symbol as string, removing null padding.
        
        Returns:
            str: Symbol string
        """
        return self.symbol.rstrip(b'\x00').decode('utf-8')
    
    def get_price_float(self) -> float:
        """
        Get price as float (convert from fixed-point).
        
        Returns:
            float: Price as float
        """
        return self.price / 10000.0
    
    def get_open_price_float(self) -> float:
        """Get open price as float."""
        return self.open_price / 10000.0
    
    def get_high_price_float(self) -> float:
        """Get high price as float."""
        return self.high_price / 10000.0
    
    def get_low_price_float(self) -> float:
        """Get low price as float."""
        return self.low_price / 10000.0
    
    def get_close_price_float(self) -> float:
        """Get close price as float."""
        return self.close_price / 10000.0
    
    def get_bid_price_float(self) -> float:
        """
        Get bid price as float (convert from fixed-point).
        
        Returns:
            float: Bid price as float
        """
        return self.bid_price / 10000.0
    
    def get_ask_price_float(self) -> float:
        """
        Get ask price as float (convert from fixed-point).
        
        Returns:
            float: Ask price as float
        """
        return self.ask_price / 10000.0
    
    def get_timestamp_seconds(self) -> float:
        """
        Get timestamp in seconds.
        
        Returns:
            float: Timestamp in seconds
        """
        return self.timestamp_ns / 1_000_000_000
    
    def get_timestamp_milliseconds(self) -> float:
        """
        Get timestamp in milliseconds.
        
        Returns:
            float: Timestamp in milliseconds
        """
        return self.timestamp_ns / 1_000_000
    
    def is_same_symbol(self, other: 'BinaryMarketData') -> bool:
        """
        Check if two messages are for the same symbol (using hash).
        
        Args:
            other: Other BinaryMarketData message
            
        Returns:
            bool: True if same symbol
        """
        return self.symbol_hash == other.symbol_hash
    
    def __repr__(self) -> str:
        """String representation of the message."""
        symbol_str = self.get_symbol_string()
        price_float = self.get_price_float()
        timestamp_sec = self.get_timestamp_seconds()
        
        return (f"BinaryMarketData(symbol='{symbol_str}', "
                f"price={price_float:.4f}, open={self.get_open_price_float():.4f}, "
                f"high={self.get_high_price_float():.4f}, low={self.get_low_price_float():.4f}, "
                f"close={self.get_close_price_float():.4f}, volume={self.volume}, "
                f"type={self.message_type}, timestamp={timestamp_sec:.6f})")

def test_enhanced_binary_market_data():
    """Test the enhanced binary market data functionality with OHLC support."""
    print("Testing Enhanced Binary Market Data with OHLC...")
    
    # Test OHLC message creation
    msg = BinaryMarketData.create_quote_message(
        symbol="TCS",
        exchange="NSE", 
        price=3456.78,
        open_price=3400.0,
        high_price=3500.0,
        low_price=3350.0,
        close_price=3456.78,
        bid_price=3456.50,
        ask_price=3457.00,
        volume=1000
    )
    print(f"Created OHLC message: {msg}")
    
    # Test serialization
    binary_data = msg.to_bytes()
    print(f"Serialized size: {len(binary_data)} bytes")
    
    # Test deserialization
    msg2 = BinaryMarketData.from_bytes(binary_data)
    print(f"Deserialized: {msg2}")
    
    # Test conversion from normalized data (like from Flattrade)
    normalized_data = {
        'symbol': 'NATURALGAS23SEP25255CE',
        'exchange': 'MCX',
        'ltp': 17.3,
        'open': 20.7,
        'high': 23.85,
        'low': 16.15,
        'close': 17.25,
        'volume': 8965,
        'timestamp': 1756751064671.0,
        'mode': 2
    }
    
    binary_from_normalized = BinaryMarketData.from_normalized_data(normalized_data, 'MCX')
    print(f"From normalized data: {binary_from_normalized}")
    
    # Test conversion back to normalized
    back_to_normalized = binary_from_normalized.to_normalized_data()
    print(f"Back to normalized: {back_to_normalized}")
    
    # Verify OHLC data preservation
    print("\nOHLC Data Verification:")
    print(f"Original Open: {normalized_data['open']} -> Binary: {binary_from_normalized.get_open_price_float()}")
    print(f"Original High: {normalized_data['high']} -> Binary: {binary_from_normalized.get_high_price_float()}")
    print(f"Original Low: {normalized_data['low']} -> Binary: {binary_from_normalized.get_low_price_float()}")
    print(f"Original Close: {normalized_data['close']} -> Binary: {binary_from_normalized.get_close_price_float()}")
    
    print("Enhanced binary market data tests completed!")


if __name__ == "__main__":
    test_enhanced_binary_market_data()