"""
Performance Monitor for comprehensive system monitoring and alerting.
Provides real-time performance metrics, health checks, and alerting capabilities.
"""

import time
import threading
import logging
import statistics
from typing import List, Dict, Optional, Callable, Any, Set
from dataclasses import dataclass, field
from collections import deque, defaultdict
import json

from .optimized_ring_buffer import OptimizedRingBuffer, OptimizedRingBufferPool
from .backpressure_aware_consumer import BackpressureAwareConsumer
from .massive_subscription_manager import MassiveSubscriptionManager
from .parallel_websocket_processor import ParallelWebSocketProcessor

logger = logging.getLogger(__name__)


@dataclass
class PerformanceThresholds:
    """Performance thresholds for monitoring and alerting."""
    max_buffer_utilization: float = 0.8
    max_queue_utilization: float = 0.8
    max_latency_ms: float = 10.0
    min_throughput_msg_per_sec: float = 1000.0
    max_drop_rate: float = 0.01  # 1%
    max_error_rate: float = 0.05  # 5%
    max_memory_usage_mb: float = 8192.0  # 8GB
    max_cpu_usage_percent: float = 80.0
    connection_timeout_sec: float = 30.0


@dataclass
class AlertConfig:
    """Configuration for alerting system."""
    enable_console_alerts: bool = True
    enable_log_alerts: bool = True
    enable_callback_alerts: bool = False
    alert_cooldown_sec: float = 60.0  # Minimum time between same alerts
    critical_alert_cooldown_sec: float = 30.0
    max_alerts_per_minute: int = 10


@dataclass
class PerformanceMetrics:
    """Current performance metrics snapshot."""
    timestamp: float = field(default_factory=time.time)
    
    # Throughput metrics
    messages_per_second: float = 0.0
    peak_messages_per_second: float = 0.0
    total_messages_processed: int = 0
    
    # Latency metrics
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    
    # Buffer metrics
    buffer_utilization: Dict[str, float] = field(default_factory=dict)
    avg_buffer_utilization: float = 0.0
    max_buffer_utilization: float = 0.0
    
    # Queue metrics
    queue_utilization: Dict[str, float] = field(default_factory=dict)
    avg_queue_utilization: float = 0.0
    max_queue_utilization: float = 0.0
    
    # Error metrics
    dropped_messages: int = 0
    error_count: int = 0
    drop_rate: float = 0.0
    error_rate: float = 0.0
    
    # Connection metrics
    active_connections: int = 0
    failed_connections: int = 0
    connection_success_rate: float = 0.0
    
    # System metrics
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary for serialization."""
        return {
            'timestamp': self.timestamp,
            'throughput': {
                'messages_per_second': self.messages_per_second,
                'peak_messages_per_second': self.peak_messages_per_second,
                'total_messages_processed': self.total_messages_processed
            },
            'latency': {
                'avg_latency_ms': self.avg_latency_ms,
                'p95_latency_ms': self.p95_latency_ms,
                'p99_latency_ms': self.p99_latency_ms,
                'max_latency_ms': self.max_latency_ms
            },
            'buffers': {
                'utilization': self.buffer_utilization,
                'avg_utilization': self.avg_buffer_utilization,
                'max_utilization': self.max_buffer_utilization
            },
            'queues': {
                'utilization': self.queue_utilization,
                'avg_utilization': self.avg_queue_utilization,
                'max_utilization': self.max_queue_utilization
            },
            'errors': {
                'dropped_messages': self.dropped_messages,
                'error_count': self.error_count,
                'drop_rate': self.drop_rate,
                'error_rate': self.error_rate
            },
            'connections': {
                'active_connections': self.active_connections,
                'failed_connections': self.failed_connections,
                'success_rate': self.connection_success_rate
            },
            'system': {
                'memory_usage_mb': self.memory_usage_mb,
                'cpu_usage_percent': self.cpu_usage_percent
            }
        }


class PerformanceMonitor:
    """
    Comprehensive performance monitor for the ultra-low latency system.
    
    Features:
    - Real-time performance metrics collection
    - Configurable thresholds and alerting
    - Historical data tracking
    - Health checks and diagnostics
    - Integration with all system components
    """
    
    def __init__(self, thresholds: Optional[PerformanceThresholds] = None,
                 alert_config: Optional[AlertConfig] = None):
        """
        Initialize performance monitor.
        
        Args:
            thresholds: Performance thresholds for alerting
            alert_config: Alert configuration
        """
        self.thresholds = thresholds or PerformanceThresholds()
        self.alert_config = alert_config or AlertConfig()
        
        # Current metrics
        self.current_metrics = PerformanceMetrics()
        self.metrics_lock = threading.Lock()
        
        # Historical data (keep last 1000 samples)
        self.metrics_history: deque = deque(maxlen=1000)
        self.latency_samples: deque = deque(maxlen=10000)
        self.throughput_samples: deque = deque(maxlen=1000)
        
        # Component references
        self.monitored_components: Dict[str, Any] = {}
        
        # Monitoring control
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.monitor_interval = 1.0  # Monitor every second
        
        # Alerting
        self.alert_callbacks: List[Callable] = []
        self.alert_history: Dict[str, float] = {}  # alert_key -> last_alert_time
        self.alerts_this_minute = 0
        self.last_minute_reset = time.time()
        
        # Performance tracking
        self.start_time = time.time()
        self.last_metrics_time = time.time()
        self.last_message_count = 0
        
        logger.info("Initialized PerformanceMonitor")
    
    def register_component(self, name: str, component: Any):
        """
        Register a component for monitoring.
        
        Args:
            name: Component name
            component: Component instance to monitor
        """
        self.monitored_components[name] = component
        logger.info(f"Registered component for monitoring: {name}")
    
    def add_alert_callback(self, callback: Callable[[str, str, Dict], None]):
        """
        Add alert callback function.
        
        Args:
            callback: Function to call on alerts (level, message, metrics)
        """
        self.alert_callbacks.append(callback)
    
    def start_monitoring(self):
        """Start the monitoring thread."""
        if self.running:
            logger.warning("Performance monitor already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True,
            name="PerformanceMonitor"
        )
        self.monitor_thread.start()
        logger.info("Started performance monitoring")
    
    def stop_monitoring(self):
        """Stop the monitoring thread."""
        logger.info("Stopping performance monitor...")
        self.running = False
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)
        
        logger.info("Performance monitor stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop."""
        while self.running:
            try:
                start_time = time.perf_counter()
                
                # Collect metrics from all components
                self._collect_metrics()
                
                # Check thresholds and generate alerts
                self._check_thresholds()
                
                # Store historical data
                with self.metrics_lock:
                    self.metrics_history.append(self.current_metrics.to_dict())
                
                # Calculate sleep time to maintain interval
                elapsed = time.perf_counter() - start_time
                sleep_time = max(0, self.monitor_interval - elapsed)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"Monitoring loop took {elapsed:.3f}s, longer than interval {self.monitor_interval}s")
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(1.0)
    
    def _collect_metrics(self):
        """Collect metrics from all registered components."""
        current_time = time.time()
        
        with self.metrics_lock:
            # Reset metrics
            self.current_metrics = PerformanceMetrics()
            self.current_metrics.timestamp = current_time
            
            # Collect from buffer pools
            self._collect_buffer_metrics()
            
            # Collect from consumers
            self._collect_consumer_metrics()
            
            # Collect from subscription managers
            self._collect_subscription_metrics()
            
            # Collect from parallel processors
            self._collect_processor_metrics()
            
            # Collect system metrics
            self._collect_system_metrics()
            
            # Calculate derived metrics
            self._calculate_derived_metrics()
    
    def _collect_buffer_metrics(self):
        """Collect metrics from buffer pools and individual buffers."""
        for name, component in self.monitored_components.items():
            if isinstance(component, OptimizedRingBufferPool):
                pool_stats = component.get_pool_stats()
                
                # Buffer utilization
                for buffer_name, buffer_stats in pool_stats.get('buffers', {}).items():
                    utilization = buffer_stats.get('utilization', 0.0)
                    self.current_metrics.buffer_utilization[f"{name}_{buffer_name}"] = utilization
                
                # Overall pool utilization
                overall_util = pool_stats.get('overall_utilization', 0.0)
                self.current_metrics.buffer_utilization[f"{name}_overall"] = overall_util
                
            elif isinstance(component, OptimizedRingBuffer):
                utilization = component.get_utilization()
                self.current_metrics.buffer_utilization[name] = utilization
        
        # Calculate buffer statistics
        if self.current_metrics.buffer_utilization:
            utilizations = list(self.current_metrics.buffer_utilization.values())
            self.current_metrics.avg_buffer_utilization = statistics.mean(utilizations)
            self.current_metrics.max_buffer_utilization = max(utilizations)
    
    def _collect_consumer_metrics(self):
        """Collect metrics from backpressure aware consumers."""
        for name, component in self.monitored_components.items():
            if isinstance(component, BackpressureAwareConsumer):
                consumer_stats = component.get_consumer_stats()
                queue_stats = component.get_queue_stats()
                
                # Throughput
                total_throughput = consumer_stats.get('total_throughput_msg_per_sec', 0.0)
                self.current_metrics.messages_per_second += total_throughput
                
                # Messages processed
                total_consumed = consumer_stats.get('total_messages_consumed', 0)
                self.current_metrics.total_messages_processed += total_consumed
                
                # Dropped messages
                total_dropped = consumer_stats.get('total_messages_dropped', 0)
                self.current_metrics.dropped_messages += total_dropped
                
                # Queue utilization
                for queue_id, q_stats in queue_stats.items():
                    utilization = q_stats.get('utilization', 0.0)
                    self.current_metrics.queue_utilization[f"{name}_queue_{queue_id}"] = utilization
                
                # Latency from buffer stats
                buffer_stats = consumer_stats.get('buffer_stats', {})
                latencies = []
                for buffer_stat in buffer_stats.values():
                    avg_latency = buffer_stat.get('avg_latency_ms', 0.0)
                    if avg_latency > 0:
                        latencies.append(avg_latency)
                        self.latency_samples.append(avg_latency)
                
                if latencies:
                    self.current_metrics.avg_latency_ms = statistics.mean(latencies)
        
        # Calculate queue statistics
        if self.current_metrics.queue_utilization:
            utilizations = list(self.current_metrics.queue_utilization.values())
            self.current_metrics.avg_queue_utilization = statistics.mean(utilizations)
            self.current_metrics.max_queue_utilization = max(utilizations)
    
    def _collect_subscription_metrics(self):
        """Collect metrics from subscription managers."""
        for name, component in self.monitored_components.items():
            if isinstance(component, MassiveSubscriptionManager):
                sub_stats = component.get_subscription_stats()
                
                # Connection metrics
                active_conns = sub_stats.get('active_connections', 0)
                total_conns = sub_stats.get('total_connections', 0)
                
                self.current_metrics.active_connections += active_conns
                
                if total_conns > 0:
                    success_rate = active_conns / total_conns
                    self.current_metrics.connection_success_rate = success_rate
                    self.current_metrics.failed_connections += (total_conns - active_conns)
                
                # Error counting from connection details
                conn_details = sub_stats.get('connection_details', {})
                for conn_stats in conn_details.values():
                    error_count = conn_stats.get('error_count', 0)
                    self.current_metrics.error_count += error_count
    
    def _collect_processor_metrics(self):
        """Collect metrics from parallel processors."""
        for name, component in self.monitored_components.items():
            if isinstance(component, ParallelWebSocketProcessor):
                proc_stats = component.get_overall_stats()
                
                # Throughput
                avg_throughput = proc_stats.get('average_throughput', 0.0)
                self.current_metrics.messages_per_second += avg_throughput
                
                # Messages processed
                total_messages = proc_stats.get('total_messages_processed', 0)
                self.current_metrics.total_messages_processed += total_messages
                
                # Errors
                total_errors = proc_stats.get('total_errors', 0)
                self.current_metrics.error_count += total_errors
                
                # Connection metrics
                active_procs = proc_stats.get('active_processors', 0)
                total_procs = proc_stats.get('total_processors', 0)
                
                self.current_metrics.active_connections += active_procs
                if total_procs > 0:
                    self.current_metrics.failed_connections += (total_procs - active_procs)
    
    def _collect_system_metrics(self):
        """Collect system-level metrics."""
        try:
            import psutil
            
            # Memory usage
            process = psutil.Process()
            memory_info = process.memory_info()
            self.current_metrics.memory_usage_mb = memory_info.rss / (1024 * 1024)
            
            # CPU usage
            self.current_metrics.cpu_usage_percent = process.cpu_percent()
            
        except ImportError:
            # psutil not available, skip system metrics
            pass
        except Exception as e:
            logger.warning(f"Error collecting system metrics: {e}")
    
    def _calculate_derived_metrics(self):
        """Calculate derived metrics from collected data."""
        # Update peak throughput
        if self.current_metrics.messages_per_second > self.current_metrics.peak_messages_per_second:
            self.current_metrics.peak_messages_per_second = self.current_metrics.messages_per_second
        
        # Store throughput sample
        self.throughput_samples.append(self.current_metrics.messages_per_second)
        
        # Calculate latency percentiles
        if len(self.latency_samples) >= 10:
            sorted_latencies = sorted(list(self.latency_samples))
            n = len(sorted_latencies)
            
            self.current_metrics.p95_latency_ms = sorted_latencies[int(0.95 * n)]
            self.current_metrics.p99_latency_ms = sorted_latencies[int(0.99 * n)]
            self.current_metrics.max_latency_ms = sorted_latencies[-1]
        
        # Calculate drop rate
        total_messages = self.current_metrics.total_messages_processed + self.current_metrics.dropped_messages
        if total_messages > 0:
            self.current_metrics.drop_rate = self.current_metrics.dropped_messages / total_messages
        
        # Calculate error rate
        if self.current_metrics.total_messages_processed > 0:
            self.current_metrics.error_rate = self.current_metrics.error_count / self.current_metrics.total_messages_processed
    
    def _check_thresholds(self):
        """Check performance thresholds and generate alerts."""
        current_time = time.time()
        
        # Reset alerts per minute counter
        if current_time - self.last_minute_reset >= 60.0:
            self.alerts_this_minute = 0
            self.last_minute_reset = current_time
        
        # Check if we've exceeded alert rate limit
        if self.alerts_this_minute >= self.alert_config.max_alerts_per_minute:
            return
        
        # Buffer utilization alerts
        if self.current_metrics.max_buffer_utilization > self.thresholds.max_buffer_utilization:
            self._generate_alert(
                "WARNING",
                f"High buffer utilization: {self.current_metrics.max_buffer_utilization:.1%}",
                "buffer_utilization"
            )
        
        # Queue utilization alerts
        if self.current_metrics.max_queue_utilization > self.thresholds.max_queue_utilization:
            self._generate_alert(
                "WARNING",
                f"High queue utilization: {self.current_metrics.max_queue_utilization:.1%}",
                "queue_utilization"
            )
        
        # Latency alerts
        if self.current_metrics.p99_latency_ms > self.thresholds.max_latency_ms:
            self._generate_alert(
                "WARNING",
                f"High latency P99: {self.current_metrics.p99_latency_ms:.2f}ms",
                "latency"
            )
        
        # Throughput alerts
        if self.current_metrics.messages_per_second < self.thresholds.min_throughput_msg_per_sec:
            self._generate_alert(
                "WARNING",
                f"Low throughput: {self.current_metrics.messages_per_second:.0f} msg/sec",
                "throughput"
            )
        
        # Drop rate alerts
        if self.current_metrics.drop_rate > self.thresholds.max_drop_rate:
            self._generate_alert(
                "CRITICAL",
                f"High drop rate: {self.current_metrics.drop_rate:.2%}",
                "drop_rate"
            )
        
        # Error rate alerts
        if self.current_metrics.error_rate > self.thresholds.max_error_rate:
            self._generate_alert(
                "CRITICAL",
                f"High error rate: {self.current_metrics.error_rate:.2%}",
                "error_rate"
            )
        
        # Memory usage alerts
        if self.current_metrics.memory_usage_mb > self.thresholds.max_memory_usage_mb:
            self._generate_alert(
                "WARNING",
                f"High memory usage: {self.current_metrics.memory_usage_mb:.0f}MB",
                "memory_usage"
            )
        
        # CPU usage alerts
        if self.current_metrics.cpu_usage_percent > self.thresholds.max_cpu_usage_percent:
            self._generate_alert(
                "WARNING",
                f"High CPU usage: {self.current_metrics.cpu_usage_percent:.1f}%",
                "cpu_usage"
            )
    
    def _generate_alert(self, level: str, message: str, alert_type: str):
        """
        Generate an alert with cooldown logic.
        
        Args:
            level: Alert level (INFO, WARNING, CRITICAL)
            message: Alert message
            alert_type: Type of alert for cooldown tracking
        """
        current_time = time.time()
        
        # Check cooldown
        cooldown = (self.alert_config.critical_alert_cooldown_sec 
                   if level == "CRITICAL" 
                   else self.alert_config.alert_cooldown_sec)
        
        last_alert_time = self.alert_history.get(alert_type, 0)
        if current_time - last_alert_time < cooldown:
            return  # Still in cooldown
        
        # Update alert history
        self.alert_history[alert_type] = current_time
        self.alerts_this_minute += 1
        
        # Generate alert
        alert_data = {
            'level': level,
            'message': message,
            'alert_type': alert_type,
            'timestamp': current_time,
            'metrics': self.current_metrics.to_dict()
        }
        
        # Console alerts
        if self.alert_config.enable_console_alerts:
            print(f"[{level}] {message}")
        
        # Log alerts
        if self.alert_config.enable_log_alerts:
            if level == "CRITICAL":
                logger.critical(message)
            elif level == "WARNING":
                logger.warning(message)
            else:
                logger.info(message)
        
        # Callback alerts
        if self.alert_config.enable_callback_alerts:
            for callback in self.alert_callbacks:
                try:
                    callback(level, message, alert_data)
                except Exception as e:
                    logger.error(f"Error in alert callback: {e}")
    
    def get_current_metrics(self) -> PerformanceMetrics:
        """
        Get current performance metrics.
        
        Returns:
            PerformanceMetrics: Current metrics snapshot
        """
        with self.metrics_lock:
            return self.current_metrics
    
    def get_metrics_history(self, last_n: Optional[int] = None) -> List[Dict]:
        """
        Get historical metrics.
        
        Args:
            last_n: Number of recent samples to return (None for all)
            
        Returns:
            List of metrics dictionaries
        """
        history = list(self.metrics_history)
        if last_n is not None:
            history = history[-last_n:]
        return history
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive performance summary.
        
        Returns:
            Dict: Performance summary
        """
        uptime = time.time() - self.start_time
        
        # Calculate averages from history
        if self.throughput_samples:
            avg_throughput = statistics.mean(self.throughput_samples)
            peak_throughput = max(self.throughput_samples)
        else:
            avg_throughput = 0.0
            peak_throughput = 0.0
        
        return {
            'uptime_seconds': uptime,
            'current_metrics': self.current_metrics.to_dict(),
            'averages': {
                'avg_throughput_msg_per_sec': avg_throughput,
                'peak_throughput_msg_per_sec': peak_throughput,
            },
            'thresholds': {
                'max_buffer_utilization': self.thresholds.max_buffer_utilization,
                'max_queue_utilization': self.thresholds.max_queue_utilization,
                'max_latency_ms': self.thresholds.max_latency_ms,
                'min_throughput_msg_per_sec': self.thresholds.min_throughput_msg_per_sec,
                'max_drop_rate': self.thresholds.max_drop_rate,
                'max_error_rate': self.thresholds.max_error_rate
            },
            'alert_stats': {
                'alerts_this_minute': self.alerts_this_minute,
                'total_alert_types': len(self.alert_history),
                'last_alerts': {k: time.time() - v for k, v in self.alert_history.items()}
            },
            'monitoring': {
                'running': self.running,
                'monitored_components': list(self.monitored_components.keys()),
                'samples_collected': len(self.metrics_history)
            }
        }
    
    def monitor_buffers(self, buffers: List[OptimizedRingBuffer]):
        """
        Monitor buffer health and performance (legacy method for compatibility).
        
        Args:
            buffers: List of buffers to monitor
        """
        for i, buffer in enumerate(buffers):
            if buffer is None:
                continue
                
            utilization = buffer.get_count() / buffer.max_messages if buffer.max_messages > 0 else 0
            
            # Store in current metrics
            with self.metrics_lock:
                self.current_metrics.buffer_utilization[f'buffer_{i}'] = utilization
            
            # Generate alert if needed
            if utilization > self.thresholds.max_buffer_utilization:
                self._generate_alert(
                    "WARNING",
                    f"Buffer {i} is {utilization:.1%} full",
                    f"buffer_{i}_utilization"
                )
    
    def export_metrics_json(self, filepath: str):
        """
        Export current metrics to JSON file.
        
        Args:
            filepath: Path to export file
        """
        try:
            summary = self.get_performance_summary()
            with open(filepath, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            logger.info(f"Exported metrics to {filepath}")
        except Exception as e:
            logger.error(f"Failed to export metrics: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        self.start_monitoring()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.stop_monitoring()


def test_performance_monitor():
    """Test the performance monitor functionality."""
    print("Testing Performance Monitor...")
    
    # Create test thresholds
    thresholds = PerformanceThresholds(
        max_buffer_utilization=0.7,
        max_latency_ms=5.0,
        min_throughput_msg_per_sec=500.0
    )
    
    # Create alert config
    alert_config = AlertConfig(
        enable_console_alerts=True,
        alert_cooldown_sec=5.0
    )
    
    # Test monitor
    with PerformanceMonitor(thresholds, alert_config) as monitor:
        print(f"Created monitor with thresholds: {thresholds}")
        
        # Simulate registering components
        print("Simulating component registration...")
        
        # In real usage, you would register actual components:
        # monitor.register_component("buffer_pool", buffer_pool)
        # monitor.register_component("consumer", consumer)
        # monitor.register_component("subscription_manager", sub_manager)
        
        # Simulate monitoring
        time.sleep(2)
        
        # Get metrics
        metrics = monitor.get_current_metrics()
        print(f"Current metrics: {metrics.to_dict()}")
        
        # Get summary
        summary = monitor.get_performance_summary()
        print(f"Performance summary: {summary}")
        
        print("Performance monitor test completed")


if __name__ == "__main__":
    test_performance_monitor()