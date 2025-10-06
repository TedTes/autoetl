from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
import pandas as pd


class DriftSeverity(Enum):
    """Severity levels for schema drift."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class DriftReport:
    """Report of schema drift detection results."""
    
    table_name: str
    has_drift: bool
    new_columns: List[str]
    removed_columns: List[str]
    type_changes: Dict[str, tuple]  # column_name -> (old_type, new_type)
    severity: DriftSeverity
    message: str
    
    def __str__(self) -> str:
        """String representation of drift report."""
        lines = [
            f"Schema Drift Report for '{self.table_name}':",
            f"  Status: {'DRIFT DETECTED' if self.has_drift else 'NO DRIFT'}",
            f"  Severity: {self.severity.value.upper()}"
        ]
        
        if self.new_columns:
            lines.append(f"  New Columns ({len(self.new_columns)}): {', '.join(self.new_columns)}")
        
        if self.removed_columns:
            lines.append(f"  Removed Columns ({len(self.removed_columns)}): {', '.join(self.removed_columns)}")
        
        if self.type_changes:
            lines.append("  Type Changes:")
            for col, (old, new) in self.type_changes.items():
                lines.append(f"    - {col}: {old} -> {new}")
        
        if self.message:
            lines.append(f"  Message: {self.message}")
        
        return "\n".join(lines)
    
    def to_dict(self) -> Dict:
        """Convert report to dictionary."""
        return {
            'table_name': self.table_name,
            'has_drift': self.has_drift,
            'new_columns': self.new_columns,
            'removed_columns': self.removed_columns,
            'type_changes': self.type_changes,
            'severity': self.severity.value,
            'message': self.message
        }


class SchemaValidatorInterface(ABC):
    """
    Abstract interface for schema validation.
    
    Follows ISP - clients only depend on validation methods they need.
    """
    
    @abstractmethod
    def validate(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        expected_schema: Optional[Dict[str, str]] = None
    ) -> DriftReport:
        """
        Validate DataFrame schema and detect drift.
        
        Args:
            df: DataFrame to validate
            table_name: Name of target table
            expected_schema: Optional dict of column_name -> data_type
            
        Returns:
            DriftReport with validation results
        """
        pass
    
    @abstractmethod
    def extract_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Extract schema from DataFrame.
        
        Args:
            df: DataFrame to extract schema from
            
        Returns:
            Dict mapping column_name -> data_type
        """
        pass


class ValidationError(Exception):
    """Custom exception for validation errors."""
    
    def __init__(self, message: str, drift_report: Optional[DriftReport] = None):
        self.message = message
        self.drift_report = drift_report
        super().__init__(self.message)