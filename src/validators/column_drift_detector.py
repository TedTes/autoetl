import pandas as pd
from typing import Dict, Optional
import logging
from validators.schema_validator import (
    SchemaValidatorInterface, 
    DriftReport, 
    DriftSeverity,
    ValidationError
)

logger = logging.getLogger(__name__)


class ColumnDriftDetector(SchemaValidatorInterface):
    """
    Concrete implementation of schema validator for column drift detection.
    
    Detects:
    - New columns added to data source
    - Columns removed from data source
    - Data type changes in existing columns
    """
    
    def __init__(self, strict_mode: bool = False):
        """
        Initialize drift detector.
        
        Args:
            strict_mode: If True, raises ValidationError on any drift detection
        """
        self.strict_mode = strict_mode
    
    def validate(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        expected_schema: Optional[Dict[str, str]] = None
    ) -> DriftReport:
        """
        Validate DataFrame schema against expected schema.
        
        Args:
            df: DataFrame to validate
            table_name: Name of target table
            expected_schema: Dict of column_name -> data_type (if None, no validation)
            
        Returns:
            DriftReport with validation results
            
        Raises:
            ValidationError: If strict_mode=True and drift detected
        """
        # Extract current schema from DataFrame
        current_schema = self.extract_schema(df)
        
        # If no expected schema, return INFO report with current schema
        if expected_schema is None:
            report = DriftReport(
                table_name=table_name,
                has_drift=False,
                new_columns=list(current_schema.keys()),
                removed_columns=[],
                type_changes={},
                severity=DriftSeverity.INFO,
                message="No expected schema provided - recorded current schema"
            )
            logger.info(f"No expected schema for {table_name}, recorded {len(current_schema)} columns")
            return report
        
        # Detect drift
        new_columns = self._detect_new_columns(current_schema, expected_schema)
        removed_columns = self._detect_removed_columns(current_schema, expected_schema)
        type_changes = self._detect_type_changes(current_schema, expected_schema)
        
        # Determine if drift exists
        has_drift = bool(new_columns or removed_columns or type_changes)
        
        # Determine severity
        severity = self._determine_severity(new_columns, removed_columns, type_changes)
        
        # Build message
        message = self._build_message(new_columns, removed_columns, type_changes)
        
        # Create report
        report = DriftReport(
            table_name=table_name,
            has_drift=has_drift,
            new_columns=new_columns,
            removed_columns=removed_columns,
            type_changes=type_changes,
            severity=severity,
            message=message
        )
        
        # Log results
        if has_drift:
            logger.warning(f"Schema drift detected for {table_name}: {message}")
        else:
            logger.info(f"No schema drift detected for {table_name}")
        
        # Raise error in strict mode
        if self.strict_mode and has_drift:
            raise ValidationError(
                f"Schema drift detected for {table_name} in strict mode",
                drift_report=report
            )
        
        return report
    
    def extract_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Extract schema from DataFrame.
        
        Args:
            df: DataFrame to extract schema from
            
        Returns:
            Dict mapping column_name -> data_type (as string)
        """
        schema = {}
        
        for column in df.columns:
            dtype = df[column].dtype
            # Convert pandas dtype to string representation
            schema[column] = self._normalize_dtype(dtype)
        
        return schema
    
    def _normalize_dtype(self, dtype) -> str:
        """
        Normalize pandas dtype to standard string representation.
        
        Args:
            dtype: Pandas dtype
            
        Returns:
            Normalized type string
        """
        dtype_str = str(dtype)
        
        # Map pandas types to standard types
        type_mapping = {
            'int64': 'integer',
            'int32': 'integer',
            'float64': 'numeric',
            'float32': 'numeric',
            'object': 'text',
            'string': 'text',
            'bool': 'boolean',
            'datetime64[ns]': 'timestamp',
            'datetime64': 'timestamp',
            'date': 'date',
        }
        
        for pandas_type, standard_type in type_mapping.items():
            if pandas_type in dtype_str:
                return standard_type
        
        return dtype_str
    
    def _detect_new_columns(self, current: Dict[str, str], expected: Dict[str, str]) -> list:
        """Detect columns in current but not in expected."""
        current_cols = set(current.keys())
        expected_cols = set(expected.keys())
        return sorted(list(current_cols - expected_cols))
    
    def _detect_removed_columns(self, current: Dict[str, str], expected: Dict[str, str]) -> list:
        """Detect columns in expected but not in current."""
        current_cols = set(current.keys())
        expected_cols = set(expected.keys())
        return sorted(list(expected_cols - current_cols))
    
    def _detect_type_changes(self, current: Dict[str, str], expected: Dict[str, str]) -> Dict[str, tuple]:
        """Detect type changes in common columns."""
        type_changes = {}
        
        common_columns = set(current.keys()) & set(expected.keys())
        
        for col in common_columns:
            if current[col] != expected[col]:
                type_changes[col] = (expected[col], current[col])
        
        return type_changes
    
    def _determine_severity(
        self, 
        new_columns: list, 
        removed_columns: list, 
        type_changes: Dict[str, tuple]
    ) -> DriftSeverity:
        """
        Determine severity of drift.
        
        Rules:
        - CRITICAL: Columns removed or type changes
        - WARNING: New columns added
        - INFO: No drift
        """
        if removed_columns or type_changes:
            return DriftSeverity.CRITICAL
        elif new_columns:
            return DriftSeverity.WARNING
        else:
            return DriftSeverity.INFO
    
    def _build_message(
        self, 
        new_columns: list, 
        removed_columns: list, 
        type_changes: Dict[str, tuple]
    ) -> str:
        """Build human-readable drift message."""
        messages = []
        
        if new_columns:
            messages.append(f"{len(new_columns)} new column(s) added")
        
        if removed_columns:
            messages.append(f"{len(removed_columns)} column(s) removed")
        
        if type_changes:
            messages.append(f"{len(type_changes)} type change(s) detected")
        
        if not messages:
            return "Schema matches expected structure"
        
        return "; ".join(messages)