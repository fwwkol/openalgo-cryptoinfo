import struct
from dataclasses import dataclass
from typing import Optional

@dataclass
class MarketDataMessage:
    """Fixed-size message structure for shared memory"""
    timestamp: float          # 8 bytes
    symbol_hash: int         # 8 bytes  
    message_type: int        # 4 bytes (1=LTP, 2=Quote, 3=Depth)
    price: float            # 8 bytes
    volume: int             # 8 bytes
    bid_price: float        # 8 bytes
    ask_price: float        # 8 bytes
    sequence_id: int        # 8 bytes
    reserved: bytes         # 16 bytes for future use
    
    # Total: 84 bytes per message (8+8+4+8+8+8+8+8+24=84)
    
    MESSAGE_SIZE = 84
    # Format string for struct packing/unpacking
    # d: double (8 bytes) - timestamp
    # Q: unsigned long long (8 bytes) - symbol_hash
    # I: unsigned int (4 bytes) - message_type
    # d: double (8 bytes) - price (changed from float to double for consistency)
    # Q: unsigned long long (8 bytes) - volume
    # d: double (8 bytes) - bid_price
    # d: double (8 bytes) - ask_price
    # Q: unsigned long long (8 bytes) - sequence_id
    # 24s: 24-byte string - reserved (increased from 16 to 24 to make total 84 bytes)
    FORMAT = '=dQIdQddQ24s'  # Using native byte order with '='
    
    def to_bytes(self) -> bytes:
        # Ensure reserved is exactly 24 bytes
        reserved = (self.reserved or b'\x00')[:24]
        reserved = reserved.ljust(24, b'\x00')
        
        return struct.pack(
            self.FORMAT,
            self.timestamp,
            self.symbol_hash,
            self.message_type,
            float(self.price),  # Ensure it's a float
            self.volume,
            float(self.bid_price),  # Ensure it's a float
            float(self.ask_price),  # Ensure it's a float
            self.sequence_id,
            reserved
        )
    
    @classmethod
    def from_bytes(cls, data: bytes):
        try:
            unpacked = struct.unpack(cls.FORMAT, data)
            # Ensure proper types for numeric fields
            return cls(
                timestamp=float(unpacked[0]),
                symbol_hash=int(unpacked[1]),
                message_type=int(unpacked[2]),
                price=float(unpacked[3]),
                volume=int(unpacked[4]),
                bid_price=float(unpacked[5]),
                ask_price=float(unpacked[6]),
                sequence_id=int(unpacked[7]),
                reserved=unpacked[8]
            )
        except struct.error as e:
            print(f"Error unpacking message: {e}")
            print(f"Expected format: {cls.FORMAT}, data length: {len(data)}")
            raise
