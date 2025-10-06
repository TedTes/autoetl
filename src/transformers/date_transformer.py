import pandas as pd
from typing import List
from datetime import datetime
import logging
from transformers.base import Transformer, TransformationError

logger = logging.getLogger(__name__)


class DateTransformer(Transformer):
    """Transforms date columns to standardized datetime format."""
    
    def __init__(self, date_columns: List[str], date_formats: List[str] = None):
        """
        Initialize DateTransformer.
        
        Args:
            date_columns: List of column names to transform
            date_formats: List of date format strings to try (e.g., ['%Y-%m-%d', '%m/%d/%Y'])
                         If None, pandas will infer the format
        """
        self.date_columns = date_columns
        self.date_formats = date_formats or [
            '%Y-%m-%d',           # 2024-01-15
            '%m/%d/%Y',           # 01/15/2024
            '%d/%m/%Y',           # 15/01/2024
            '%Y/%m/%d',           # 2024/01/15
            '%b %d, %Y',          # Jan 15, 2024
            '%B %d, %Y',          # January 15, 2024
            '%Y-%m-%d %H:%M:%S',  # 2024-01-15 14:30:00
            '%Y-%m-%dT%H:%M:%S',  # 2024-01-15T14:30:00 (ISO format)
        ]
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform date columns to datetime objects.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed date columns
        """
        self.validate_columns(df)
        
        df_copy = df.copy()
        
        for col in self.date_columns:
            if col in df_copy.columns:
                try:
                    df_copy[col] = self._parse_dates(df_copy[col])
                    logger.info(f"Successfully transformed date column: {col}")
                except Exception as e:
                    logger.error(f"Failed to transform date column {col}: {str(e)}")
                    raise TransformationError(
                        f"Failed to transform date column '{col}'",
                        original_error=e
                    )
        
        return df_copy
    
    def _parse_dates(self, series: pd.Series) -> pd.Series:
        """
        Parse dates from series trying multiple formats.
        
        Args:
            series: Pandas Series containing date strings
            
        Returns:
            Series with parsed datetime objects
        """
        # First, try pandas automatic inference
        try:
            return pd.to_datetime(series, errors='coerce')
        except Exception:
            pass
        
        # If that fails, try specific formats
        for date_format in self.date_formats:
            try:
                return pd.to_datetime(series, format=date_format, errors='coerce')
            except Exception:
                continue
        
        # If all formats fail, use coerce mode (invalid dates become NaT)
        logger.warning(f"Could not parse some dates with specified formats, using coerce mode")
        return pd.to_datetime(series, errors='coerce')
    
    def get_required_columns(self) -> List[str]:
        """
        Get list of required columns.
        
        Returns:
            List of date column names
        """
        return self.date_columns