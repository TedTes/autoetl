from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Optional


class Transformer(ABC):
    """
    Abstract base class for data transformers.
    
    Follows Open/Closed Principle (OCP) - open for extension, closed for modification.
    New transformers can be added without changing existing code.
    """
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the input DataFrame.
        
        Args:
            df: Input DataFrame to transform
            
        Returns:
            Transformed DataFrame
            
        Raises:
            TransformationError: If transformation fails
        """
        pass
    
    @abstractmethod
    def get_required_columns(self) -> List[str]:
        """
        Get list of columns required by this transformer.
        
        Returns:
            List of column names required for transformation
        """
        pass
    
    def validate_columns(self, df: pd.DataFrame) -> None:
        """
        Validate that required columns exist in DataFrame.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            TransformationError: If required columns are missing
        """
        required = self.get_required_columns()
        missing = [col for col in required if col not in df.columns]
        
        if missing:
            raise TransformationError(
                f"{self.__class__.__name__} requires columns {missing} which are missing from DataFrame"
            )
    
    def can_transform(self, df: pd.DataFrame) -> bool:
        """
        Check if this transformer can be applied to the DataFrame.
        
        Args:
            df: DataFrame to check
            
        Returns:
            True if transformer can be applied, False otherwise
        """
        try:
            self.validate_columns(df)
            return True
        except TransformationError:
            return False


class TransformationError(Exception):
    """Custom exception for transformation errors."""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        self.message = message
        self.original_error = original_error
        super().__init__(self.message)