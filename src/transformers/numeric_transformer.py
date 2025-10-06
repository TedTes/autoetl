import pandas as pd
import numpy as np
from typing import List, Optional
import logging
import re
from transformers.base import Transformer, TransformationError

logger = logging.getLogger(__name__)


class NumericTransformer(Transformer):
    """Transforms numeric columns by cleaning and normalizing values."""
    
    def __init__(
        self, 
        numeric_columns: List[str],
        remove_currency_symbols: bool = True,
        remove_commas: bool = True,
        convert_to_float: bool = True,
        fill_na_with: Optional[float] = None
    ):
        """
        Initialize NumericTransformer.
        
        Args:
            numeric_columns: List of column names to transform
            remove_currency_symbols: Remove $, €, £, etc.
            remove_commas: Remove thousand separators
            convert_to_float: Convert to float type (else keeps as object)
            fill_na_with: Value to fill NaN/None with (None = keep as NaN)
        """
        self.numeric_columns = numeric_columns
        self.remove_currency_symbols = remove_currency_symbols
        self.remove_commas = remove_commas
        self.convert_to_float = convert_to_float
        self.fill_na_with = fill_na_with
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform numeric columns to clean numeric values.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed numeric columns
        """
        self.validate_columns(df)
        
        df_copy = df.copy()
        
        for col in self.numeric_columns:
            if col in df_copy.columns:
                try:
                    df_copy[col] = self._clean_numeric(df_copy[col])
                    logger.info(f"Successfully transformed numeric column: {col}")
                except Exception as e:
                    logger.error(f"Failed to transform numeric column {col}: {str(e)}")
                    raise TransformationError(
                        f"Failed to transform numeric column '{col}'",
                        original_error=e
                    )
        
        return df_copy
    
    def _clean_numeric(self, series: pd.Series) -> pd.Series:
        """
        Clean and convert series to numeric.
        
        Args:
            series: Pandas Series containing numeric data
            
        Returns:
            Series with cleaned numeric values
        """
        # Convert to string first to handle various input types
        cleaned = series.astype(str)
        
        # Remove whitespace
        cleaned = cleaned.str.strip()
        
        # Replace common null representations
        cleaned = cleaned.replace(['', 'null', 'NULL', 'None', 'N/A', 'n/a', 'nan'], np.nan)
        
        # Remove currency symbols if requested
        if self.remove_currency_symbols:
            cleaned = cleaned.str.replace(r'[$€£¥₹]', '', regex=True)
        
        # Remove commas (thousand separators)
        if self.remove_commas:
            cleaned = cleaned.str.replace(',', '', regex=False)
        
        # Remove any other non-numeric characters except decimal point and minus sign
        cleaned = cleaned.str.replace(r'[^\d.\-]', '', regex=True)
        
        # Handle multiple decimal points (keep only the first)
        cleaned = cleaned.apply(self._fix_decimal_points)
        
        # Convert to numeric
        if self.convert_to_float:
            cleaned = pd.to_numeric(cleaned, errors='coerce')
        
        # Fill NaN values if specified
        if self.fill_na_with is not None:
            cleaned = cleaned.fillna(self.fill_na_with)
        
        return cleaned
    
    def _fix_decimal_points(self, value: str) -> str:
        """
        Fix multiple decimal points in a string.
        
        Args:
            value: String that might have multiple decimal points
            
        Returns:
            String with only one decimal point
        """
        if pd.isna(value) or value == 'nan':
            return value
        
        # Count decimal points
        decimal_count = value.count('.')
        
        if decimal_count <= 1:
            return value
        
        # If multiple decimals, keep only the last one (assuming it's the decimal separator)
        parts = value.split('.')
        if len(parts) > 1:
            return ''.join(parts[:-1]) + '.' + parts[-1]
        
        return value
    
    def get_required_columns(self) -> List[str]:
        """
        Get list of required columns.
        
        Returns:
            List of numeric column names
        """
        return self.numeric_columns