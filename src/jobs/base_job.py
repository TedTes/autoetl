from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """ETL job execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"


class JobResult:
    """Result of ETL job execution."""
    
    def __init__(
        self,
        job_name: str,
        status: JobStatus,
        start_time: datetime,
        end_time: datetime,
        records_processed: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.job_name = job_name
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.records_processed = records_processed
        self.records_failed = records_failed
        self.error_message = error_message
        self.metadata = metadata or {}
        self.duration_seconds = (end_time - start_time).total_seconds()
    
    def __str__(self) -> str:
        """String representation of job result."""
        lines = [
            f"Job: {self.job_name}",
            f"Status: {self.status.value.upper()}",
            f"Duration: {self.duration_seconds:.2f}s",
            f"Records Processed: {self.records_processed}",
        ]
        
        if self.records_failed > 0:
            lines.append(f"Records Failed: {self.records_failed}")
        
        if self.error_message:
            lines.append(f"Error: {self.error_message}")
        
        if self.metadata:
            lines.append("Metadata:")
            for key, value in self.metadata.items():
                lines.append(f"  {key}: {value}")
        
        return "\n".join(lines)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            'job_name': self.job_name,
            'status': self.status.value,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'duration_seconds': self.duration_seconds,
            'records_processed': self.records_processed,
            'records_failed': self.records_failed,
            'error_message': self.error_message,
            'metadata': self.metadata
        }


class ETLJob(ABC):
    """
    Abstract base class for ETL jobs.
    
    Follows Template Method pattern - defines job execution lifecycle,
    subclasses implement specific ETL logic.
    """
    
    def __init__(self, job_name: str):
        """
        Initialize ETL job.
        
        Args:
            job_name: Name of the job for identification
        """
        self.job_name = job_name
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    def execute(self) -> JobResult:
        """
        Execute the ETL job with lifecycle management.
        
        Template method that orchestrates job execution:
        1. Setup
        2. Extract
        3. Transform
        4. Load
        5. Cleanup
        
        Returns:
            JobResult with execution details
        """
        self.start_time = datetime.utcnow()
        status = JobStatus.RUNNING
        records_processed = 0
        records_failed = 0
        error_message = None
        metadata = {}
        
        try:
            logger.info(f"Starting ETL job: {self.job_name}")
            
            # Setup phase
            logger.info(f"[{self.job_name}] Running setup...")
            self.setup()
            
            # Extract phase
            logger.info(f"[{self.job_name}] Extracting data...")
            data = self.extract()
            metadata['extract_count'] = len(data) if hasattr(data, '__len__') else 0
            
            # Transform phase
            logger.info(f"[{self.job_name}] Transforming data...")
            transformed_data = self.transform(data)
            metadata['transform_count'] = len(transformed_data) if hasattr(transformed_data, '__len__') else 0
            
            # Load phase
            logger.info(f"[{self.job_name}] Loading data...")
            load_result = self.load(transformed_data)
            records_processed = load_result.get('records_processed', 0)
            records_failed = load_result.get('records_failed', 0)
            metadata.update(load_result.get('metadata', {}))
            
            # Determine final status
            if records_failed > 0 and records_processed > 0:
                status = JobStatus.PARTIAL_SUCCESS
            elif records_failed > 0:
                status = JobStatus.FAILED
            else:
                status = JobStatus.SUCCESS
            
            logger.info(f"[{self.job_name}] Job completed: {status.value}")
            
        except Exception as e:
            status = JobStatus.FAILED
            error_message = str(e)
            logger.error(f"[{self.job_name}] Job failed: {error_message}", exc_info=True)
            
        finally:
            # Cleanup phase (always runs)
            try:
                logger.info(f"[{self.job_name}] Running cleanup...")
                self.cleanup()
            except Exception as e:
                logger.error(f"[{self.job_name}] Cleanup failed: {str(e)}")
            
            self.end_time = datetime.utcnow()
        
        # Create and return result
        result = JobResult(
            job_name=self.job_name,
            status=status,
            start_time=self.start_time,
            end_time=self.end_time,
            records_processed=records_processed,
            records_failed=records_failed,
            error_message=error_message,
            metadata=metadata
        )
        
        logger.info(f"Job result:\n{result}")
        return result
    
    @abstractmethod
    def setup(self) -> None:
        """
        Setup phase - initialize resources, validate configuration.
        
        Called before extract phase.
        """
        pass
    
    @abstractmethod
    def extract(self) -> Any:
        """
        Extract phase - fetch data from source.
        
        Returns:
            Extracted data (DataFrame, list, dict, etc.)
        """
        pass
    
    @abstractmethod
    def transform(self, data: Any) -> Any:
        """
        Transform phase - clean and transform data.
        
        Args:
            data: Data from extract phase
            
        Returns:
            Transformed data
        """
        pass
    
    @abstractmethod
    def load(self, data: Any) -> Dict[str, Any]:
        """
        Load phase - persist data to destination.
        
        Args:
            data: Transformed data
            
        Returns:
            Dict with keys: records_processed, records_failed, metadata
        """
        pass
    
    def cleanup(self) -> None:
        """
        Cleanup phase - release resources, close connections.
        
        Default implementation does nothing. Override if needed.
        """
        pass


class JobError(Exception):
    """Custom exception for ETL job errors."""
    
    def __init__(self, message: str, job_name: str, original_error: Optional[Exception] = None):
        self.message = message
        self.job_name = job_name
        self.original_error = original_error
        super().__init__(self.message)