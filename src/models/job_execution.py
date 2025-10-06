from sqlalchemy import Column, Integer, String, DateTime, Text, JSON
from sqlalchemy.sql import func
from database.connection import Base


class JobExecution(Base):
    """Tracks ETL job execution history and metadata."""
    
    __tablename__ = "job_executions"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Job identification
    job_name = Column(String(100), nullable=False, index=True)
    status = Column(String(20), nullable=False, index=True)  # pending, running, success, failed, partial_success
    
    # Execution timing
    start_time = Column(DateTime(timezone=True), nullable=False, index=True)
    end_time = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    
    # Records tracking
    records_processed = Column(Integer, default=0, nullable=False)
    records_failed = Column(Integer, default=0, nullable=False)
    
    # Error tracking
    error_message = Column(Text, nullable=True)
    
    # Additional metadata (schema drift info, etc.)
    metadata = Column(JSON, nullable=True)
    
    # Audit fields
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<JobExecution(job_name={self.job_name}, status={self.status}, start_time={self.start_time})>"