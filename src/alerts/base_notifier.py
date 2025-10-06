from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from enum import Enum
from dataclasses import dataclass
from datetime import datetime


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Represents an alert message."""
    
    title: str
    message: str
    severity: AlertSeverity
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None
    
    def __str__(self) -> str:
        """String representation of alert."""
        lines = [
            f"[{self.severity.value.upper()}] {self.title}",
            f"Time: {self.timestamp.isoformat()}",
            f"Message: {self.message}"
        ]
        
        if self.metadata:
            lines.append("Metadata:")
            for key, value in self.metadata.items():
                lines.append(f"  {key}: {value}")
        
        return "\n".join(lines)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            'title': self.title,
            'message': self.message,
            'severity': self.severity.value,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata or {}
        }


class AlertNotifierInterface(ABC):
    """
    Abstract interface for alert notifications.
    
    Follows ISP - allows different notification channels (console, email, Slack, etc.)
    without forcing implementations to support all methods.
    """
    
    @abstractmethod
    def send_alert(
        self, 
        title: str, 
        message: str, 
        severity: AlertSeverity,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send an alert notification.
        
        Args:
            title: Alert title/subject
            message: Alert message body
            severity: Alert severity level
            metadata: Optional additional context
            
        Returns:
            True if alert sent successfully, False otherwise
        """
        pass
    
    @abstractmethod
    def send_alert_object(self, alert: Alert) -> bool:
        """
        Send an Alert object.
        
        Args:
            alert: Alert object to send
            
        Returns:
            True if alert sent successfully, False otherwise
        """
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """
        Check if notification channel is available.
        
        Returns:
            True if available, False otherwise
        """
        pass


class AlertError(Exception):
    """Custom exception for alert sending errors."""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        self.message = message
        self.original_error = original_error
        super().__init__(self.message)