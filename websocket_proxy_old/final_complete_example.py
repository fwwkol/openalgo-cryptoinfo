"""
Final Complete Example - Production-Ready Ultra-Low Latency Market Data System
Demonstrates the complete integrated system with all components working together.
"""

import time
import logging
from typing import Dict, List

from . import (
    BinaryMarketData,
    OptimizedRingBufferPool,
    MassiveSubscriptionManager,
    SubscriptionConfig,
    BackpressureAwareConsumer,
    BackpressureConfig,
    PerformanceMonitor,
    PerformanceThresholds,
    AlertConfig,
    ParallelWebSocketProcessor
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_production_config() -> Dict:
    """Create production-ready system configuration."""
    return {
        # Buffer configuration for high throughput
        "num_buffers": 12,
        "buffer_size": 4 * 1024 * 1024,  # 4MB per buffer
        
        # Subscription management for massive scale
        "max_symbols_per_connection": 1500,
        "batch_size": 75,
        "batch_delay": 0.008,
        "broker_name": "flattrade",
        
        # Consumer configuration for optimal performance
        "max_queue_size": 15000,
        "consumer_batch_size": 150,
        "adaptive_batching": True,
        "backpressure_threshold": 0.75,
        "drop_strategy": "oldest",
        
        # Performance monitoring thresholds
        "max_buffer_utilization": 0.85,
        "max_queue_utilization": 0.80,
        "max_latency_ms": 5.0,
        "min_throughput_msg_per_sec": 5000.0,
        "max_drop_rate": 0.005,  # 0.5%
        "max_error_rate": 0.02,  # 2%
        
        # Alerting configuration
        "enable_console_alerts": True,
        "enable_log_alerts": True,
        "alert_cooldown_sec": 30.0,
        
        # Parallel processing for extreme scale
        "use_parallel_processor": False,  # Set to True for 50K+ symbols
        "num_processes": 8
    }


def create_large_symbol_set(count: int = 10000) -> List[Dict]:
    """Create a large set of symbols for testing scalability."""
    symbols = []
    
    # Real NSE symbols (top 100)
    real_symbols = [
        "TCS", "INFY", "RELIANCE", "HDFC", "ICICIBANK", "SBIN", "WIPRO", "MARUTI",
        "BAJFINANCE", "HCLTECH", "ASIANPAINT", "KOTAKBANK", "LT", "AXISBANK",
        "BHARTIARTL", "ITC", "HINDUNILVR", "POWERGRID", "NESTLEIND", "ULTRACEMCO",
        "TITAN", "SUNPHARMA", "TECHM", "TATAMOTORS", "BAJAJFINSV", "DRREDDY",
        "JSWSTEEL", "HINDALCO", "ADANIPORTS", "COALINDIA", "NTPC", "ONGC",
        "GRASIM", "CIPLA", "HEROMOTOCO", "EICHERMOT", "BRITANNIA", "DIVISLAB",
        "SHREECEM", "APOLLOHOSP", "BAJAJ-AUTO", "HDFCLIFE", "SBILIFE", "PIDILITIND",
        "DABUR", "GODREJCP", "MARICO", "COLPAL", "BERGEPAINT", "ASIAN"
    ]
    
    # Add real symbols first
    for i, symbol in enumerate(real_symbols[:min(count, len(real_symbols))]):
        exchange = "NSE" if i % 4 != 0 else "BSE"
        symbols.append({
            "symbol": symbol,
            "exchange": exchange,
            "mode": 1  # LTP mode
        })
    
    # Generate additional synthetic symbols
    for i in range(len(real_symbols), count):
        symbol = f"STOCK{i:05d}"
        exchange = "NSE" if i % 3 == 0 else "BSE"
        
        symbols.append({
            "symbol": symbol,
            "exchange": exchange,
            "mode": 1
        })
    
    return symbols


class ProductionMarketDataSystem:
    """Production-ready market data system with full monitoring and alerting."""
    
    def __init__(self, config: Dict):
        """Initialize the production system."""
        self.config = config
        
        # Core components
        self.buffer_pool: OptimizedRingBufferPool = None
        self.subscription_manager: MassiveSubscriptionManager = None
        self.consumer: BackpressureAwareConsumer = None
        self.performance_monitor: PerformanceMonitor = None
        self.parallel_processor: ParallelWebSocketProcessor = None
        
        # System state
        self.running = False
        self.start_time = time.time()
        
        # Performance tracking
        self.total_messages_processed = 0
        self.business_logic_calls = 0
        
        logger.info("Initialized ProductionMarketDataSystem")
    
    def initialize_all_components(self, symbols: List[Dict]) -> bool:
        """Initialize all system components."""
        try:
            logger.info(f"Initializing production system for {len(symbols)} symbols...")
            
            # 1. Initialize buffer pool
            self.buffer_pool = OptimizedRingBufferPool(
                num_buffers=self.config['num_buffers'],
                buffer_size=self.config['buffer_size']
            )
            logger.info(f"✓ Buffer pool: {self.config['num_buffers']} x {self.config['buffer_size']//1024//1024}MB")
            
            # 2. Initialize subscription manager
            sub_config = SubscriptionConfig(
                max_symbols_per_connection=self.config['max_symbols_per_connection'],
                batch_size=self.config['batch_size'],
                batch_delay=self.config['batch_delay'],
                broker_name=self.config['broker_name']
            )
            
            self.subscription_manager = MassiveSubscriptionManager(sub_config)
            self.subscription_manager.initialize_buffer_pool(
                self.config['num_buffers'], 
                self.config['buffer_size']
            )
            logger.info("✓ Subscription manager with intelligent batching")
            
            # 3. Initialize consumer
            buffer_names = [f"optimized_market_data_{i}" for i in range(self.config['num_buffers'])]
            
            consumer_config = BackpressureConfig(
                max_queue_size=self.config['max_queue_size'],
                batch_size=self.config['consumer_batch_size'],
                adaptive_batching=self.config['adaptive_batching'],
                backpressure_threshold=self.config['backpressure_threshold'],
                drop_strategy=self.config['drop_strategy']
            )
            
            self.consumer = BackpressureAwareConsumer(buffer_names, consumer_config)
            self.consumer.set_global_message_handler(self._handle_market_data_message)
            self.consumer.set_error_handler(self._handle_consumer_error)
            logger.info("✓ Backpressure-aware consumer with adaptive batching")
            
            # 4. Initialize parallel processor (if enabled)
            if self.config.get('use_parallel_processor', False):
                self.parallel_processor = ParallelWebSocketProcessor(
                    num_processes=self.config['num_processes']
                )
                logger.info(f"✓ Parallel processor: {self.config['num_processes']} processes")
            
            # 5. Initialize performance monitor
            thresholds = PerformanceThresholds(
                max_buffer_utilization=self.config['max_buffer_utilization'],
                max_queue_utilization=self.config['max_queue_utilization'],
                max_latency_ms=self.config['max_latency_ms'],
                min_throughput_msg_per_sec=self.config['min_throughput_msg_per_sec'],
                max_drop_rate=self.config['max_drop_rate'],
                max_error_rate=self.config['max_error_rate']
            )
            
            alert_config = AlertConfig(
                enable_console_alerts=self.config['enable_console_alerts'],
                enable_log_alerts=self.config['enable_log_alerts'],
                alert_cooldown_sec=self.config['alert_cooldown_sec']
            )
            
            self.performance_monitor = PerformanceMonitor(thresholds, alert_config)
            
            # Register all components for monitoring
            self.performance_monitor.register_component("buffer_pool", self.buffer_pool)
            self.performance_monitor.register_component("consumer", self.consumer)
            self.performance_monitor.register_component("subscription_manager", self.subscription_manager)
            
            if self.parallel_processor:
                self.performance_monitor.register_component("parallel_processor", self.parallel_processor)
            
            # Add custom alert callback
            self.performance_monitor.add_alert_callback(self._custom_alert_handler)
            
            logger.info("✓ Performance monitor with comprehensive alerting")
            
            logger.info("🚀 All components initialized successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ System initialization failed: {e}")
            return False
    
    def start_production_system(self, symbols: List[Dict], auth_data: Dict) -> Dict:
        """Start the complete production system."""
        try:
            logger.info("🚀 Starting production market data system...")
            start_time = time.time()
            
            # 1. Start performance monitoring
            self.performance_monitor.start_monitoring()
            logger.info("✓ Performance monitoring started")
            
            # 2. Start consumer threads
            consumer_threads = self.consumer.start_consuming()
            logger.info(f"✓ Started {len(consumer_threads)} consumer threads")
            
            # 3. Subscribe to symbols
            if self.parallel_processor:
                # Use parallel processor for extreme scale
                success = self.parallel_processor.start_processors(
                    symbols, 
                    self.config['broker_name'],
                    auth_data
                )
                
                if not success:
                    raise Exception("Failed to start parallel processors")
                
                subscription_method = "parallel_processor"
                logger.info("✓ Parallel processors started for extreme scale")
                
            else:
                # Use subscription manager
                results = self.subscription_manager.subscribe_bulk(symbols, auth_data)
                
                if not results.get('success', False):
                    raise Exception(f"Subscription failed: {results.get('error', 'Unknown error')}")
                
                subscription_method = "subscription_manager"
                logger.info(f"✓ Subscribed to {results['subscribed']}/{results['total_symbols']} symbols")
            
            self.running = True
            startup_time = time.time() - start_time
            
            # Log system status
            logger.info("=" * 60)
            logger.info("🎉 PRODUCTION SYSTEM STARTED SUCCESSFULLY!")
            logger.info(f"📊 Symbols: {len(symbols):,}")
            logger.info(f"⚡ Buffers: {self.config['num_buffers']} x {self.config['buffer_size']//1024//1024}MB")
            logger.info(f"🔄 Consumer threads: {len(consumer_threads)}")
            logger.info(f"📈 Expected throughput: {self.config['min_throughput_msg_per_sec']:,} msg/sec")
            logger.info(f"⏱️  Startup time: {startup_time:.2f}s")
            logger.info(f"🔧 Method: {subscription_method}")
            logger.info("=" * 60)
            
            return {
                "success": True,
                "startup_time": startup_time,
                "symbols_count": len(symbols),
                "consumer_threads": len(consumer_threads),
                "subscription_method": subscription_method,
                "expected_throughput": self.config['min_throughput_msg_per_sec']
            }
            
        except Exception as e:
            logger.error(f"❌ Production system startup failed: {e}")
            self.shutdown_system()
            return {"success": False, "error": str(e)}
    
    def _handle_market_data_message(self, buffer_id: int, message: BinaryMarketData):
        """Handle incoming market data with business logic."""
        try:
            self.total_messages_processed += 1
            
            # Extract message data efficiently
            symbol = message.get_symbol_string()
            price = message.get_price_float()
            volume = message.volume
            timestamp = message.get_timestamp_seconds()
            
            # Calculate processing latency
            processing_latency_ms = (time.time() - timestamp) * 1000
            
            # Business logic examples (customize as needed)
            self._execute_business_logic(symbol, price, volume, processing_latency_ms)
            
            # Log progress periodically
            if self.total_messages_processed % 50000 == 0:
                logger.info(f"📈 Processed {self.total_messages_processed:,} messages. "
                           f"Latest: {symbol} @ ₹{price:.2f} "
                           f"(latency: {processing_latency_ms:.2f}ms)")
            
        except Exception as e:
            logger.warning(f"Error in market data handler: {e}")
    
    def _execute_business_logic(self, symbol: str, price: float, volume: int, latency_ms: float):
        """Execute custom business logic for market data."""
        self.business_logic_calls += 1
        
        # Example business logic implementations:
        
        # 1. Price Alert System
        if price > 5000:  # High-value stock alert
            if self.business_logic_calls % 1000 == 0:  # Throttle alerts
                logger.info(f"💰 High-value alert: {symbol} @ ₹{price:.2f}")
        
        # 2. Volume Spike Detection
        if volume > 100000:  # High volume alert
            if self.business_logic_calls % 2000 == 0:  # Throttle alerts
                logger.info(f"📊 High volume: {symbol} volume={volume:,}")
        
        # 3. Latency Monitoring
        if latency_ms > 10.0:  # High latency warning
            if self.business_logic_calls % 5000 == 0:  # Throttle warnings
                logger.warning(f"⚠️  High latency: {symbol} latency={latency_ms:.2f}ms")
        
        # 4. Performance Tracking
        if self.business_logic_calls % 100000 == 0:
            uptime = time.time() - self.start_time
            avg_throughput = self.total_messages_processed / uptime
            logger.info(f"🚀 Performance: {avg_throughput:.0f} msg/sec average over {uptime:.0f}s")
        
        # Add your custom business logic here:
        # - Trading strategy execution
        # - Risk management checks
        # - Portfolio updates
        # - Data persistence
        # - Real-time analytics
    
    def _handle_consumer_error(self, error: Exception, buffer_id: int):
        """Handle consumer errors with recovery logic."""
        logger.error(f"🚨 Consumer error in buffer {buffer_id}: {error}")
        
        # Implement error recovery strategies:
        # - Restart consumer thread
        # - Switch to backup buffer
        # - Alert operations team
        # - Implement circuit breaker
    
    def _custom_alert_handler(self, level: str, message: str, alert_data: Dict):
        """Custom alert handler for production monitoring."""
        # Implement custom alerting logic:
        # - Send to monitoring system (Prometheus, Grafana)
        # - Send notifications (Slack, email, SMS)
        # - Trigger automated responses
        # - Log to external systems
        
        if level == "CRITICAL":
            logger.critical(f"🚨 CRITICAL ALERT: {message}")
            # Send immediate notification to operations team
        elif level == "WARNING":
            logger.warning(f"⚠️  WARNING: {message}")
            # Log to monitoring dashboard
        
        # Example: Export metrics to external system
        # self.export_alert_to_monitoring_system(alert_data)
    
    def get_production_metrics(self) -> Dict:
        """Get comprehensive production metrics."""
        uptime = time.time() - self.start_time
        
        # Get performance monitor metrics
        perf_metrics = self.performance_monitor.get_current_metrics()
        perf_summary = self.performance_monitor.get_performance_summary()
        
        # Get component-specific metrics
        consumer_stats = self.consumer.get_consumer_stats() if self.consumer else {}
        subscription_stats = self.subscription_manager.get_subscription_stats() if self.subscription_manager else {}
        
        return {
            "system": {
                "uptime_seconds": uptime,
                "total_messages_processed": self.total_messages_processed,
                "business_logic_calls": self.business_logic_calls,
                "avg_throughput_msg_per_sec": self.total_messages_processed / uptime if uptime > 0 else 0,
                "running": self.running
            },
            "performance_monitor": perf_summary,
            "consumer": consumer_stats,
            "subscriptions": subscription_stats,
            "config": self.config
        }
    
    def shutdown_system(self):
        """Gracefully shutdown the production system."""
        logger.info("🛑 Shutting down production system...")
        
        self.running = False
        
        # Stop performance monitoring
        if self.performance_monitor:
            self.performance_monitor.stop_monitoring()
            logger.info("✓ Performance monitoring stopped")
        
        # Stop consumer
        if self.consumer:
            self.consumer.stop_consuming()
            logger.info("✓ Consumer threads stopped")
        
        # Shutdown subscription manager
        if self.subscription_manager:
            self.subscription_manager.shutdown()
            logger.info("✓ Subscription manager shutdown")
        
        # Shutdown parallel processor
        if self.parallel_processor:
            self.parallel_processor.shutdown()
            logger.info("✓ Parallel processor shutdown")
        
        # Cleanup buffer pool
        if self.buffer_pool:
            self.buffer_pool.cleanup()
            logger.info("✓ Buffer pool cleaned up")
        
        # Final metrics
        final_metrics = self.get_production_metrics()
        uptime = final_metrics['system']['uptime_seconds']
        total_messages = final_metrics['system']['total_messages_processed']
        avg_throughput = final_metrics['system']['avg_throughput_msg_per_sec']
        
        logger.info("=" * 60)
        logger.info("📊 FINAL PRODUCTION METRICS")
        logger.info(f"⏱️  Total uptime: {uptime:.1f} seconds")
        logger.info(f"📈 Messages processed: {total_messages:,}")
        logger.info(f"🚀 Average throughput: {avg_throughput:.0f} msg/sec")
        logger.info(f"🔧 Business logic calls: {self.business_logic_calls:,}")
        logger.info("✅ Production system shutdown completed")
        logger.info("=" * 60)


def main():
    """Main production demonstration."""
    print("🚀 Ultra-Low Latency Market Data System - Production Demo")
    print("=" * 70)
    
    try:
        # Create production configuration
        config = create_production_config()
        print("✓ Production configuration loaded")
        
        # Create large symbol set (10K symbols)
        symbols = create_large_symbol_set(10000)
        print(f"✓ Created {len(symbols):,} symbols for testing")
        
        # Production authentication (would be real credentials)
        auth_data = {
            "user_id": "production_user",
            "api_key": "prod_api_key_12345",
            "access_token": "prod_access_token_67890",
            "environment": "production"
        }
        
        # Initialize and run production system
        system = ProductionMarketDataSystem(config)
        
        try:
            # Initialize all components
            if not system.initialize_all_components(symbols):
                print("❌ Failed to initialize system components")
                return
            
            print("✅ All components initialized successfully")
            
            # In production, you would start the actual system:
            # results = system.start_production_system(symbols, auth_data)
            # 
            # if results["success"]:
            #     print(f"🎉 Production system started in {results['startup_time']:.2f}s")
            #     print(f"📊 Processing {len(symbols):,} symbols")
            #     
            #     # Run for production duration
            #     print("🔄 System running... (Press Ctrl+C to stop)")
            #     
            #     try:
            #         while True:
            #             time.sleep(30)  # Show metrics every 30 seconds
            #             
            #             metrics = system.get_production_metrics()
            #             print(f"📈 Throughput: {metrics['system']['avg_throughput_msg_per_sec']:.0f} msg/sec")
            #             print(f"📊 Total processed: {metrics['system']['total_messages_processed']:,}")
            #             
            #     except KeyboardInterrupt:
            #         print("\n🛑 Shutdown requested by user")
            # 
            # else:
            #     print(f"❌ Failed to start system: {results.get('error')}")
            
            # For demo, just show what would happen
            print("\n📋 PRODUCTION SYSTEM READY")
            print(f"📊 Symbols to process: {len(symbols):,}")
            print(f"⚡ Buffer capacity: {config['num_buffers']} x {config['buffer_size']//1024//1024}MB")
            print(f"🎯 Target throughput: {config['min_throughput_msg_per_sec']:,} msg/sec")
            print(f"🔧 Max symbols per connection: {config['max_symbols_per_connection']}")
            print(f"📈 Consumer batch size: {config['consumer_batch_size']}")
            print(f"⚠️  Alert thresholds configured for production monitoring")
            
            print("\n(In production environment, system would now:")
            print("  • Connect to real broker WebSocket feeds")
            print("  • Process live market data at high throughput")
            print("  • Execute business logic on every message")
            print("  • Monitor performance and generate alerts")
            print("  • Handle backpressure and error recovery)")
            
        finally:
            # Always cleanup
            system.shutdown_system()
    
    except Exception as e:
        logger.error(f"❌ Production demo failed: {e}")
        raise


if __name__ == "__main__":
    main()