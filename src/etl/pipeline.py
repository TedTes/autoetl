import pandas as pd
from typing import List
import logging
from transformers.base import Transformer, TransformationError

logger = logging.getLogger(__name__)


class TransformerPipeline:
    """
    Orchestrates multiple transformers using Chain of Responsibility pattern.
    
    Applies transformers in sequence, passing output of one as input to next.
    """
    
    def __init__(self, transformers: List[Transformer] = None):
        """
        Initialize transformer pipeline.
        
        Args:
            transformers: List of Transformer instances to apply in order
        """
        self.transformers = transformers or []
    
    def add_transformer(self, transformer: Transformer) -> 'TransformerPipeline':
        """
        Add a transformer to the pipeline.
        
        Args:
            transformer: Transformer instance to add
            
        Returns:
            Self for method chaining
        """
        self.transformers.append(transformer)
        return self
    
    def remove_transformer(self, transformer_class: type) -> 'TransformerPipeline':
        """
        Remove all transformers of a specific class.
        
        Args:
            transformer_class: Class of transformer to remove
            
        Returns:
            Self for method chaining
        """
        self.transformers = [
            t for t in self.transformers 
            if not isinstance(t, transformer_class)
        ]
        return self
    
    def transform(self, df: pd.DataFrame, skip_on_error: bool = False) -> pd.DataFrame:
        """
        Apply all transformers in sequence.
        
        Args:
            df: Input DataFrame
            skip_on_error: If True, skip transformers that fail; if False, raise exception
            
        Returns:
            Transformed DataFrame
            
        Raises:
            TransformationError: If transformation fails and skip_on_error=False
        """
        result = df.copy()
        
        logger.info(f"Starting pipeline with {len(self.transformers)} transformer(s)")
        
        for i, transformer in enumerate(self.transformers):
            transformer_name = transformer.__class__.__name__
            
            try:
                # Check if transformer can be applied
                if not transformer.can_transform(result):
                    logger.warning(
                        f"Skipping {transformer_name}: required columns not available"
                    )
                    if not skip_on_error:
                        raise TransformationError(
                            f"{transformer_name} cannot be applied - missing required columns"
                        )
                    continue
                
                # Apply transformation
                logger.info(f"Applying transformer {i+1}/{len(self.transformers)}: {transformer_name}")
                result = transformer.transform(result)
                logger.info(f"Completed {transformer_name} successfully")
                
            except TransformationError as e:
                logger.error(f"Transformation failed in {transformer_name}: {e.message}")
                if skip_on_error:
                    logger.warning(f"Skipping {transformer_name} due to error")
                    continue
                else:
                    raise
            
            except Exception as e:
                logger.error(f"Unexpected error in {transformer_name}: {str(e)}")
                if skip_on_error:
                    logger.warning(f"Skipping {transformer_name} due to unexpected error")
                    continue
                else:
                    raise TransformationError(
                        f"Unexpected error in {transformer_name}",
                        original_error=e
                    )
        
        logger.info("Pipeline completed successfully")
        return result
    
    def validate_pipeline(self, df: pd.DataFrame) -> List[str]:
        """
        Validate that all transformers can be applied to the DataFrame.
        
        Args:
            df: DataFrame to validate against
            
        Returns:
            List of error messages (empty if all valid)
        """
        errors = []
        
        for transformer in self.transformers:
            if not transformer.can_transform(df):
                required = transformer.get_required_columns()
                missing = [col for col in required if col not in df.columns]
                errors.append(
                    f"{transformer.__class__.__name__}: missing columns {missing}"
                )
        
        return errors
    
    def get_transformer_count(self) -> int:
        """
        Get number of transformers in pipeline.
        
        Returns:
            Number of transformers
        """
        return len(self.transformers)
    
    def clear(self) -> 'TransformerPipeline':
        """
        Remove all transformers from pipeline.
        
        Returns:
            Self for method chaining
        """
        self.transformers = []
        return self