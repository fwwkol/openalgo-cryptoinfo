"""
Symbol Partitioner for distributing market data symbols across multiple processors.
Uses fast hashing to ensure even distribution and consistent routing.
"""

import xxhash
import logging
from typing import List, Dict, DefaultDict, Set, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


class SymbolPartitioner:
    """
    Partitions symbols across multiple processors using consistent hashing.
    Ensures even distribution and deterministic routing for symbols.
    """
    
    def __init__(self, num_partitions: int = 16):
        """
        Initialize symbol partitioner.
        
        Args:
            num_partitions: Number of partitions to distribute symbols across
            
        Raises:
            ValueError: If num_partitions is not positive
        """
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")
            
        self.num_partitions = num_partitions
        self._partition_cache: Dict[str, int] = {}  # Cache for frequent lookups
        self._partition_stats: DefaultDict[int, int] = defaultdict(int)
        
        logger.info(f"Initialized SymbolPartitioner with {num_partitions} partitions")
    
    def get_partition(self, symbol: str, exchange: str) -> int:
        """
        Get partition number for a symbol-exchange pair using fast hashing.
        
        Args:
            symbol: Trading symbol (e.g., "TCS", "INFY")
            exchange: Exchange name (e.g., "NSE", "BSE")
            
        Returns:
            int: Partition number (0 to num_partitions-1)
            
        Raises:
            ValueError: If symbol or exchange is empty
        """
        if not symbol or not exchange:
            raise ValueError("Symbol and exchange cannot be empty")
        
        # Create consistent key
        key = f"{exchange}:{symbol}"
        
        # Check cache first for performance
        if key in self._partition_cache:
            return self._partition_cache[key]
        
        # Fast hash-based partitioning using xxhash (faster than built-in hash)
        partition = xxhash.xxh64(key).intdigest() % self.num_partitions
        
        # Cache the result for future lookups
        self._partition_cache[key] = partition
        self._partition_stats[partition] += 1
        
        return partition
    
    def distribute_symbols(self, symbols: List[Dict]) -> Dict[int, List[Dict]]:
        """
        Distribute symbols across partitions.
        
        Args:
            symbols: List of symbol dictionaries with 'symbol' and 'exchange' keys
            
        Returns:
            Dict mapping partition_id -> list of symbols for that partition
            
        Raises:
            ValueError: If symbols list is invalid or missing required keys
        """
        if not symbols:
            logger.warning("Empty symbols list provided")
            return {}
        
        partitions: DefaultDict[int, List[Dict]] = defaultdict(list)
        invalid_symbols = []
        
        for i, symbol_info in enumerate(symbols):
            try:
                # Validate symbol_info structure
                if not isinstance(symbol_info, dict):
                    raise ValueError(f"Symbol at index {i} is not a dictionary")
                
                if 'symbol' not in symbol_info or 'exchange' not in symbol_info:
                    raise ValueError(f"Symbol at index {i} missing 'symbol' or 'exchange' key")
                
                symbol = symbol_info['symbol']
                exchange = symbol_info['exchange']
                
                if not symbol or not exchange:
                    raise ValueError(f"Symbol at index {i} has empty symbol or exchange")
                
                partition = self.get_partition(symbol, exchange)
                partitions[partition].append(symbol_info)
                
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Invalid symbol at index {i}: {e}")
                invalid_symbols.append((i, symbol_info, str(e)))
                continue
        
        if invalid_symbols:
            logger.warning(f"Skipped {len(invalid_symbols)} invalid symbols")
        
        # Log distribution statistics
        total_valid = sum(len(partition_symbols) for partition_symbols in partitions.values())
        logger.info(f"Distributed {total_valid} symbols across {len(partitions)} partitions")
        
        return dict(partitions)
    
    def get_partition_balance(self) -> Dict[str, float]:
        """
        Get partition balance statistics.
        
        Returns:
            Dict with balance metrics
        """
        if not self._partition_stats:
            return {"balance": 1.0, "std_dev": 0.0, "min_symbols": 0, "max_symbols": 0}
        
        counts = list(self._partition_stats.values())
        total_symbols = sum(counts)
        
        if total_symbols == 0:
            return {"balance": 1.0, "std_dev": 0.0, "min_symbols": 0, "max_symbols": 0}
        
        # Calculate balance metrics
        expected_per_partition = total_symbols / self.num_partitions
        variance = sum((count - expected_per_partition) ** 2 for count in counts) / len(counts)
        std_dev = variance ** 0.5
        
        balance_score = 1.0 - (std_dev / expected_per_partition) if expected_per_partition > 0 else 1.0
        
        return {
            "balance": max(0.0, balance_score),  # 1.0 = perfect balance, 0.0 = worst
            "std_dev": std_dev,
            "min_symbols": min(counts),
            "max_symbols": max(counts),
            "total_symbols": total_symbols,
            "expected_per_partition": expected_per_partition
        }
    
    def get_partition_stats(self) -> Dict[int, Dict[str, int]]:
        """
        Get detailed statistics for each partition.
        
        Returns:
            Dict mapping partition_id -> stats dict
        """
        stats = {}
        for partition_id in range(self.num_partitions):
            symbol_count = self._partition_stats.get(partition_id, 0)
            stats[partition_id] = {
                "symbol_count": symbol_count,
                "cache_hits": 0  # Could be enhanced to track cache performance
            }
        return stats
    
    def clear_cache(self):
        """Clear the partition cache and reset statistics."""
        self._partition_cache.clear()
        self._partition_stats.clear()
        logger.info("Cleared partition cache and statistics")
    
    def precompute_partitions(self, symbols: List[Dict]) -> Dict[str, int]:
        """
        Precompute partitions for a list of symbols to warm the cache.
        
        Args:
            symbols: List of symbol dictionaries
            
        Returns:
            Dict mapping "exchange:symbol" -> partition_id
        """
        partition_map = {}
        
        for symbol_info in symbols:
            try:
                symbol = symbol_info['symbol']
                exchange = symbol_info['exchange']
                key = f"{exchange}:{symbol}"
                partition = self.get_partition(symbol, exchange)
                partition_map[key] = partition
            except (KeyError, ValueError) as e:
                logger.warning(f"Skipping invalid symbol in precompute: {e}")
                continue
        
        logger.info(f"Precomputed partitions for {len(partition_map)} symbols")
        return partition_map
    
    def get_symbols_for_partition(self, symbols: List[Dict], partition_id: int) -> List[Dict]:
        """
        Get all symbols that belong to a specific partition.
        
        Args:
            symbols: List of all symbols
            partition_id: Target partition ID
            
        Returns:
            List of symbols for the specified partition
        """
        if not 0 <= partition_id < self.num_partitions:
            raise ValueError(f"Invalid partition_id: {partition_id}")
        
        partition_symbols = []
        for symbol_info in symbols:
            try:
                if self.get_partition(symbol_info['symbol'], symbol_info['exchange']) == partition_id:
                    partition_symbols.append(symbol_info)
            except (KeyError, ValueError):
                continue
        
        return partition_symbols
    
    def __repr__(self) -> str:
        """String representation of the partitioner."""
        return (f"SymbolPartitioner(num_partitions={self.num_partitions}, "
                f"cached_symbols={len(self._partition_cache)})")


def test_symbol_partitioner():
    """Test the symbol partitioner functionality."""
    print("Testing Symbol Partitioner...")
    
    # Create test symbols
    test_symbols = [
        {"symbol": "TCS", "exchange": "NSE"},
        {"symbol": "INFY", "exchange": "NSE"},
        {"symbol": "RELIANCE", "exchange": "NSE"},
        {"symbol": "HDFC", "exchange": "NSE"},
        {"symbol": "ICICIBANK", "exchange": "NSE"},
        {"symbol": "SBIN", "exchange": "NSE"},
        {"symbol": "WIPRO", "exchange": "NSE"},
        {"symbol": "MARUTI", "exchange": "NSE"},
        {"symbol": "BAJFINANCE", "exchange": "NSE"},
        {"symbol": "HCLTECH", "exchange": "NSE"},
        {"symbol": "SENSEX", "exchange": "BSE"},
        {"symbol": "NIFTY", "exchange": "NSE"},
    ]
    
    # Test partitioner
    partitioner = SymbolPartitioner(num_partitions=4)
    print(f"Created partitioner: {partitioner}")
    
    # Test individual partition lookup
    for symbol_info in test_symbols[:3]:
        partition = partitioner.get_partition(symbol_info['symbol'], symbol_info['exchange'])
        print(f"{symbol_info['exchange']}:{symbol_info['symbol']} -> Partition {partition}")
    
    # Test distribution
    partitions = partitioner.distribute_symbols(test_symbols)
    print(f"\nDistribution across {len(partitions)} partitions:")
    
    for partition_id, symbols in partitions.items():
        symbol_names = [f"{s['exchange']}:{s['symbol']}" for s in symbols]
        print(f"Partition {partition_id}: {len(symbols)} symbols - {symbol_names}")
    
    # Test balance
    balance_stats = partitioner.get_partition_balance()
    print(f"\nBalance Statistics:")
    print(f"Balance Score: {balance_stats['balance']:.3f}")
    print(f"Standard Deviation: {balance_stats['std_dev']:.2f}")
    print(f"Min/Max symbols per partition: {balance_stats['min_symbols']}/{balance_stats['max_symbols']}")
    
    # Test cache performance
    print(f"\nCache size: {len(partitioner._partition_cache)}")
    
    # Test with larger dataset
    large_symbols = []
    for i in range(1000):
        large_symbols.append({
            "symbol": f"STOCK{i:04d}",
            "exchange": "NSE" if i % 2 == 0 else "BSE"
        })
    
    large_partitions = partitioner.distribute_symbols(large_symbols)
    large_balance = partitioner.get_partition_balance()
    
    print(f"\nLarge dataset (1000 symbols):")
    print(f"Partitions used: {len(large_partitions)}")
    print(f"Balance Score: {large_balance['balance']:.3f}")
    print(f"Symbols per partition: {large_balance['min_symbols']}-{large_balance['max_symbols']}")


if __name__ == "__main__":
    test_symbol_partitioner()