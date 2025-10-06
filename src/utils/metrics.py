import time
import logging
from functools import wraps
from typing import Callable, Any, Dict, Optional
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class PerformanceMetrics:
    """Track performance metrics for ETL operations."""
    
    def __init__(self):
        """Initialize metrics storage."""
        self.metrics: Dict[str, Dict[str, Any]] = {}
    
    def record(
        self, 
        operation: str, 
        duration_seconds: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Record a performance metric.
        
        Args:
            operation: Operation name (e.g., 'api_fetch', 'transform', 'load')
            duration_seconds: Operation duration in seconds
            metadata: Optional additional metadata
        """
        if operation not in self.metrics:
            self.metrics[operation] = {
                'count': 0,
                'total_duration': 0.0,
                'min_duration': float('inf'),
                'max_duration': 0.0,
                'last_duration': 0.0,
                'last_timestamp': None,
                'metadata': []
            }
        
        metric = self.metrics[operation]
        metric['count'] += 1
        metric['total_duration'] += duration_seconds
        metric['min_duration'] = min(metric['min_duration'], duration_seconds)
        metric['max_duration'] = max(metric['max_duration'], duration_seconds)
        metric['last_duration'] = duration_seconds
        metric['last_timestamp'] = datetime.utcnow().isoformat()
        
        if metadata:
            metric['metadata'].append({
                'timestamp': metric['last_timestamp'],
                'duration': duration_seconds,
                **metadata
            })
        
        logger.debug(
            f"Metric recorded: {operation} - {duration_seconds:.3f}s",
            extra={'operation': operation, 'duration': duration_seconds}
        )
    
    def get_metric(self, operation: str) -> Optional[Dict[str, Any]]:
        """
        Get metrics for a specific operation.
        
        Args:
            operation: Operation name
            
        Returns:
            Metric dictionary or None if not found
        """
        metric = self.metrics.get(operation)
        
        if metric and metric['count'] > 0:
            # Calculate average
            metric['avg_duration'] = metric['total_duration'] / metric['count']
        
        return metric
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all recorded metrics.
        
        Returns:
            Dictionary of all metrics
        """
        result = {}
        
        for operation, metric in self.metrics.items():
            if metric['count'] > 0:
                result[operation] = {
                    'count': metric['count'],
                    'total_duration': round(metric['total_duration'], 3),
                    'avg_duration': round(metric['total_duration'] / metric['count'], 3),
                    'min_duration': round(metric['min_duration'], 3),
                    'max_duration': round(metric['max_duration'], 3),
                    'last_duration': round(metric['last_duration'], 3),
                    'last_timestamp': metric['last_timestamp']
                }
        
        return result
    
    def log_summary(self) -> None:
        """Log a summary of all metrics."""
        summary = self.get_all_metrics()
        
        if not summary:
            logger.info("No performance metrics recorded")
            return
        
        logger.info("=== Performance Metrics Summary ===")
        for operation, metrics in summary.items():
            logger.info(
                f"{operation}: "
                f"count={metrics['count']}, "
                f"avg={metrics['avg_duration']}s, "
                f"min={metrics['min_duration']}s, "
                f"max={metrics['max_duration']}s"
            )
    
    def clear(self) -> None:
        """Clear all metrics."""
        self.metrics.clear()
        logger.debug("Performance metrics cleared")


# Global metrics instance
_metrics = PerformanceMetrics()


def get_metrics() -> PerformanceMetrics:
    """
    Get global metrics instance.
    
    Returns:
        Global PerformanceMetrics instance
    """
    return _metrics


def timing_decorator(operation_name: str = None):
    """
    Decorator to measure function execution time.
    
    Args:
        operation_name: Name for the operation (defaults to function name)
        
    Usage:
        @timing_decorator("api_fetch")
        def fetch_data():
            # code here
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            op_name = operation_name or func.__name__
            
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                _metrics.record(op_name, duration)
                logger.info(f"{op_name} completed in {duration:.3f}s")
        
        return wrapper
    return decorator


@contextmanager
def track_time(operation_name: str, metadata: Optional[Dict[str, Any]] = None):
    """
    Context manager to track execution time.
    
    Args:
        operation_name: Name of the operation
        metadata: Optional metadata to record
        
    Usage:
        with track_time("database_query", {'table': 'users'}):
            # code here
    """
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        _metrics.record(operation_name, duration, metadata)
        logger.info(
            f"{operation_name} completed in {duration:.3f}s",
            extra={'operation': operation_name, 'duration': duration}
        )


class Timer:
    """Simple timer class for manual time tracking."""
    
    def __init__(self, operation_name: str):
        """
        Initialize timer.
        
        Args:
            operation_name: Name of the operation being timed
        """
        self.operation_name = operation_name
        self.start_time = None
        self.end_time = None
    
    def start(self) -> 'Timer':
        """Start the timer."""
        self.start_time = time.time()
        logger.debug(f"Timer started: {self.operation_name}")
        return self
    
    def stop(self) -> float:
        """
        Stop the timer and record metric.
        
        Returns:
            Duration in seconds
        """
        if self.start_time is None:
            raise RuntimeError("Timer not started")
        
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        _metrics.record(self.operation_name, duration)
        logger.info(f"{self.operation_name} completed in {duration:.3f}s")
        
        return duration
    
    def __enter__(self) -> 'Timer':
        """Context manager entry."""
        return self.start()
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.stop()