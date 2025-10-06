import logging
import sys
import os
from typing import Optional
from pythonjsonlogger import jsonlogger
from datetime import datetime
from config.settings import settings


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter with additional fields."""
    
    def add_fields(self, log_record, record, message_dict):
        """Add custom fields to log record."""
        super().add_fields(log_record, record, message_dict)
        
        # Add timestamp
        log_record['timestamp'] = datetime.utcnow().isoformat()
        
        # Add log level
        log_record['level'] = record.levelname
        
        # Add module and function info
        log_record['module'] = record.module
        log_record['function'] = record.funcName
        
        # Add correlation ID if available (for request tracing)
        if hasattr(record, 'correlation_id'):
            log_record['correlation_id'] = record.correlation_id


def setup_logging(
    log_level: Optional[str] = None,
    log_format: Optional[str] = None,
    log_file: Optional[str] = None
) -> None:
    """
    Setup application-wide logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Format type ('json' or 'text')
        log_file: Optional file path for log output
    """
    # Use settings or defaults
    level = log_level or settings.log_level
    format_type = log_format or settings.log_format
    
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create logs directory if it doesn't exist
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers
    root_logger.handlers = []
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    
    # Create file handler if specified
    file_handler = None
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
    
    # Set formatter based on format type
    if format_type.lower() == 'json':
        # JSON formatter for structured logging
        formatter = CustomJsonFormatter(
            '%(timestamp)s %(level)s %(name)s %(module)s %(funcName)s %(message)s'
        )
    else:
        # Text formatter for human-readable logs
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    if file_handler:
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Set specific log levels for noisy libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    
    logging.info(f"Logging configured: level={level}, format={format_type}")


def get_logger(name: str, correlation_id: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance with optional correlation ID.
    
    Args:
        name: Logger name (typically __name__)
        correlation_id: Optional correlation ID for request tracing
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Add correlation ID to logger if provided
    if correlation_id:
        logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    return logger


class CorrelationIdFilter(logging.Filter):
    """Filter to add correlation ID to log records."""
    
    def __init__(self, correlation_id: str):
        super().__init__()
        self.correlation_id = correlation_id
    
    def filter(self, record):
        """Add correlation ID to record."""
        record.correlation_id = self.correlation_id
        return True


def add_correlation_id(logger: logging.Logger, correlation_id: str) -> None:
    """
    Add correlation ID filter to logger.
    
    Args:
        logger: Logger instance
        correlation_id: Correlation ID for request tracing
    """
    # Remove existing correlation filters
    logger.filters = [f for f in logger.filters if not isinstance(f, CorrelationIdFilter)]
    
    # Add new correlation filter
    logger.addFilter(CorrelationIdFilter(correlation_id))


# Initialize logging on module import
if not logging.getLogger().handlers:
    setup_logging()