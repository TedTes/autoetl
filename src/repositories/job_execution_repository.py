from typing import List, Optional, Any
import logging
from sqlalchemy.orm import Session
from sqlalchemy import desc
from datetime import datetime, timedelta
from models.job_execution import JobExecution
from repositories.base_repository import BaseRepository, RepositoryError

logger = logging.getLogger(__name__)


class JobExecutionRepository(BaseRepository[JobExecution]):
    """Repository for job execution tracking and history."""
    
    def __init__(self, session: Session):
        """
        Initialize JobExecution repository.
        
        Args:
            session: SQLAlchemy session
        """
        super().__init__(session)
    
    def save(self, entity: JobExecution) -> JobExecution:
        """Save a job execution record."""
        try:
            self.session.add(entity)
            self.session.flush()
            return entity
        except Exception as e:
            logger.error(f"Failed to save job execution: {str(e)}")
            raise RepositoryError("Failed to save job execution", original_error=e)
    
    def save_batch(self, entities: List[JobExecution]) -> List[JobExecution]:
        """Save multiple job execution records."""
        try:
            self.session.add_all(entities)
            self.session.flush()
            return entities
        except Exception as e:
            logger.error(f"Failed to save job executions batch: {str(e)}")
            raise RepositoryError("Failed to save job executions batch", original_error=e)
    
    def upsert(self, entity: JobExecution, unique_key: str = 'id') -> JobExecution:
        """Not typically used for job executions - use save instead."""
        return self.save(entity)
    
    def upsert_batch(self, entities: List[JobExecution], unique_key: str = 'id') -> int:
        """Not typically used for job executions - use save_batch instead."""
        self.save_batch(entities)
        return len(entities)
    
    def find_by_id(self, entity_id: int) -> Optional[JobExecution]:
        """Find job execution by ID."""
        return self.session.query(JobExecution).filter_by(id=entity_id).first()
    
    def find_all(self, limit: Optional[int] = None, offset: int = 0) -> List[JobExecution]:
        """Find all job executions with pagination."""
        query = self.session.query(JobExecution).order_by(desc(JobExecution.start_time))
        
        if offset:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def delete(self, entity: JobExecution) -> None:
        """Delete a job execution record."""
        try:
            self.session.delete(entity)
            self.session.flush()
        except Exception as e:
            logger.error(f"Failed to delete job execution: {str(e)}")
            raise RepositoryError("Failed to delete job execution", original_error=e)
    
    def count(self) -> int:
        """Get total count of job executions."""
        return self.session.query(JobExecution).count()
    
    def find_by_job_name(
        self, 
        job_name: str, 
        limit: Optional[int] = 10
    ) -> List[JobExecution]:
        """
        Find executions for a specific job.
        
        Args:
            job_name: Name of the job
            limit: Maximum number of records to return
            
        Returns:
            List of job executions ordered by most recent first
        """
        query = self.session.query(JobExecution).filter_by(
            job_name=job_name
        ).order_by(desc(JobExecution.start_time))
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def find_latest_execution(self, job_name: str) -> Optional[JobExecution]:
        """
        Find the most recent execution for a job.
        
        Args:
            job_name: Name of the job
            
        Returns:
            Latest JobExecution or None
        """
        return self.session.query(JobExecution).filter_by(
            job_name=job_name
        ).order_by(desc(JobExecution.start_time)).first()
    
    def find_by_status(
        self, 
        status: str, 
        limit: Optional[int] = None
    ) -> List[JobExecution]:
        """
        Find executions by status.
        
        Args:
            status: Job status (success, failed, etc.)
            limit: Maximum number of records to return
            
        Returns:
            List of job executions with matching status
        """
        query = self.session.query(JobExecution).filter_by(
            status=status
        ).order_by(desc(JobExecution.start_time))
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def find_failed_executions(
        self, 
        hours: int = 24, 
        limit: Optional[int] = None
    ) -> List[JobExecution]:
        """
        Find failed job executions within specified hours.
        
        Args:
            hours: Look back period in hours
            limit: Maximum number of records to return
            
        Returns:
            List of failed job executions
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        query = self.session.query(JobExecution).filter(
            JobExecution.status == 'failed',
            JobExecution.start_time >= cutoff_time
        ).order_by(desc(JobExecution.start_time))
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_execution_stats(self, job_name: str, days: int = 7) -> dict:
        """
        Get execution statistics for a job.
        
        Args:
            job_name: Name of the job
            days: Look back period in days
            
        Returns:
            Dict with stats (total, success, failed, avg_duration)
        """
        cutoff_time = datetime.utcnow() - timedelta(days=days)
        
        executions = self.session.query(JobExecution).filter(
            JobExecution.job_name == job_name,
            JobExecution.start_time >= cutoff_time
        ).all()
        
        total = len(executions)
        success = len([e for e in executions if e.status == 'success'])
        failed = len([e for e in executions if e.status == 'failed'])
        
        durations = [e.duration_seconds for e in executions if e.duration_seconds]
        avg_duration = sum(durations) / len(durations) if durations else 0
        
        return {
            'job_name': job_name,
            'period_days': days,
            'total_executions': total,
            'successful': success,
            'failed': failed,
            'success_rate': (success / total * 100) if total > 0 else 0,
            'avg_duration_seconds': round(avg_duration, 2)
        }