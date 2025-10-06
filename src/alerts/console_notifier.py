import logging
from typing import Optional, Dict, Any
from datetime import datetime
from alerts.base_notifier import AlertNotifierInterface, Alert, AlertSeverity, AlertError

logger = logging.getLogger(__name__)


class ConsoleNotifier(AlertNotifierInterface):
    """
    Console/file-based alert notifier for MVP.
    
    Logs alerts to console and optionally to a dedicated alerts log file.
    Serves as placeholder for future email/Slack integration.
    """
    
    def __init__(self, log_to_file: bool = True, alert_log_path: str = "logs/alerts.log"):
        """
        Initialize console notifier.
        
        Args:
            log_to_file: If True, also log alerts to dedicated file
            alert_log_path: Path to alerts log file
        """
        self.log_to_file = log_to_file
        self.alert_log_path = alert_log_path
        
        # Setup dedicated alert logger if needed
        if self.log_to_file:
            self._setup_file_logger()
    
    def _setup_file_logger(self):
        """Setup dedicated file logger for alerts."""
        self.alert_logger = logging.getLogger('alerts')
        self.alert_logger.setLevel(logging.INFO)
        
        # Prevent propagation to root logger to avoid duplicate logs
        self.alert_logger.propagate = False
        
        # Check if handler already exists
        if not self.alert_logger.handlers:
            # Create file handler
            import os
            os.makedirs(os.path.dirname(self.alert_log_path), exist_ok=True)
            
            file_handler = logging.FileHandler(self.alert_log_path)
            file_handler.setLevel(logging.INFO)
            
            # Create formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            
            # Add handler
            self.alert_logger.addHandler(file_handler)
    
    def send_alert(
        self, 
        title: str, 
        message: str, 
        severity: AlertSeverity,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send alert to console/log file.
        
        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity level
            metadata: Optional additional context
            
        Returns:
            True if logged successfully
        """
        # Create Alert object
        alert = Alert(
            title=title,
            message=message,
            severity=severity,
            timestamp=datetime.utcnow(),
            metadata=metadata
        )
        
        return self.send_alert_object(alert)
    
    def send_alert_object(self, alert: Alert) -> bool:
        """
        Send Alert object to console/log file.
        
        Args:
            alert: Alert object to send
            
        Returns:
            True if logged successfully
        """
        try:
            # Format alert message
            alert_str = str(alert)
            
            # Determine log level based on severity
            log_level = self._get_log_level(alert.severity)
            
            # Log to console (via standard logger)
            logger.log(log_level, alert_str)
            
            # Log to dedicated alerts file
            if self.log_to_file and hasattr(self, 'alert_logger'):
                self.alert_logger.log(log_level, alert_str)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send alert: {str(e)}")
            raise AlertError(f"Failed to send console alert: {alert.title}", original_error=e)
    
    def is_available(self) -> bool:
        """
        Check if console notifier is available.
        
        Returns:
            Always True for console notifier
        """
        return True
    
    def _get_log_level(self, severity: AlertSeverity) -> int:
        """
        Map alert severity to logging level.
        
        Args:
            severity: Alert severity
            
        Returns:
            Logging level constant
        """
        severity_mapping = {
            AlertSeverity.INFO: logging.INFO,
            AlertSeverity.WARNING: logging.WARNING,
            AlertSeverity.ERROR: logging.ERROR,
            AlertSeverity.CRITICAL: logging.CRITICAL
        }
        
        return severity_mapping.get(severity, logging.INFO)
    
    def send_schema_drift_alert(
        self, 
        table_name: str, 
        new_columns: list, 
        removed_columns: list,
        type_changes: dict
    ) -> bool:
        """
        Convenience method for schema drift alerts.
        
        Args:
            table_name: Name of table with drift
            new_columns: List of new columns
            removed_columns: List of removed columns
            type_changes: Dict of type changes
            
        Returns:
            True if alert sent successfully
        """
        # Build detailed message
        message_parts = []
        
        if new_columns:
            message_parts.append(f"New columns: {', '.join(new_columns)}")
        
        if removed_columns:
            message_parts.append(f"Removed columns: {', '.join(removed_columns)}")
        
        if type_changes:
            changes = [f"{col}: {old} → {new}" for col, (old, new) in type_changes.items()]
            message_parts.append(f"Type changes: {'; '.join(changes)}")
        
        message = "\n".join(message_parts) if message_parts else "Schema validation complete"
        
        # Determine severity
        if removed_columns or type_changes:
            severity = AlertSeverity.CRITICAL
        elif new_columns:
            severity = AlertSeverity.WARNING
        else:
            severity = AlertSeverity.INFO
        
        return self.send_alert(
            title=f"Schema Drift Detected: {table_name}",
            message=message,
            severity=severity,
            metadata={
                'table_name': table_name,
                'new_columns_count': len(new_columns),
                'removed_columns_count': len(removed_columns),
                'type_changes_count': len(type_changes)
            }
        )