#!/usr/bin/env python3
"""
ETL Pipeline Entry Point

Usage:
    python main.py [job_name]
    
Example:
    python main.py building_permits
"""

import sys
import argparse
import logging
from typing import Optional

from utils.logger import setup_logging
from utils.metrics import get_metrics
from config.settings import settings
from database.connection import DatabaseConnection
from clients.ckan_client import CKANClient
from transformers.date_transformer import DateTransformer
from transformers.numeric_transformer import NumericTransformer
from etl.pipeline import TransformerPipeline
from validators.column_drift_detector import ColumnDriftDetector
from repositories.building_permit_repository import BuildingPermitRepository
from repositories.schema_repository import SchemaRepository
from repositories.job_execution_repository import JobExecutionRepository
from alerts.console_notifier import ConsoleNotifier
from jobs.building_permits_job import BuildingPermitsETLJob
from jobs.base_job import JobStatus

logger = logging.getLogger(__name__)


def create_building_permits_job() -> BuildingPermitsETLJob:
    """
    Factory function to create BuildingPermitsETLJob with all dependencies.
    
    Returns:
        Configured BuildingPermitsETLJob instance
    """
    # Create CKAN client
    ckan_client = CKANClient(
        base_url=settings.ckan_base_url,
        resource_id=settings.ckan_resource_id,
        max_retries=settings.max_retries,
        retry_delay=settings.retry_delay
    )
    
    # Create session (will be replaced by job's session in setup)
    session = DatabaseConnection.get_session_factory()()
    
    # Create repositories
    building_permit_repo = BuildingPermitRepository(session)
    schema_repo = SchemaRepository(session)
    job_execution_repo = JobExecutionRepository(session)
    
    # Create transformers
    date_transformer = DateTransformer(
        date_columns=['application_date', 'issued_date']
    )
    
    numeric_transformer = NumericTransformer(
        numeric_columns=['estimated_cost'],
        remove_currency_symbols=True,
        remove_commas=True,
        convert_to_float=True
    )
    
    # Create transformation pipeline
    pipeline = TransformerPipeline()
    pipeline.add_transformer(date_transformer)
    pipeline.add_transformer(numeric_transformer)
    
    # Create validator
    drift_detector = ColumnDriftDetector(strict_mode=False)
    
    # Create notifier
    notifier = ConsoleNotifier(log_to_file=True, alert_log_path="logs/alerts.log")
    
    # Create and return job
    job = BuildingPermitsETLJob(
        ckan_client=ckan_client,
        building_permit_repo=building_permit_repo,
        schema_repo=schema_repo,
        job_execution_repo=job_execution_repo,
        drift_detector=drift_detector,
        notifier=notifier,
        pipeline=pipeline
    )
    
    return job


def run_job(job_name: str) -> int:
    """
    Run specified ETL job.
    
    Args:
        job_name: Name of job to run
        
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger.info(f"Starting job: {job_name}")
    
    # Job factory mapping
    job_factory = {
        'building_permits': create_building_permits_job,
    }
    
    if job_name not in job_factory:
        logger.error(f"Unknown job: {job_name}")
        logger.info(f"Available jobs: {', '.join(job_factory.keys())}")
        return 1
    
    try:
        # Create job
        logger.info(f"Creating job: {job_name}")
        job = job_factory[job_name]()
        
        # Execute job
        logger.info(f"Executing job: {job_name}")
        result = job.execute()
        
        # Log metrics summary
        metrics = get_metrics()
        metrics.log_summary()
        
        # Log result
        logger.info(f"Job execution complete:\n{result}")
        
        # Return appropriate exit code
        if result.status == JobStatus.SUCCESS:
            logger.info("Job completed successfully")
            return 0
        elif result.status == JobStatus.PARTIAL_SUCCESS:
            logger.warning("Job completed with partial success")
            return 0
        else:
            logger.error(f"Job failed: {result.error_message}")
            return 1
            
    except Exception as e:
        logger.error(f"Job execution failed with exception: {str(e)}", exc_info=True)
        return 1


def main():
    """Main entry point."""
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='ETL Pipeline Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py building_permits          # Run building permits ETL
  python main.py building_permits --log-level DEBUG  # Run with debug logging
        """
    )
    
    parser.add_argument(
        'job',
        type=str,
        help='Job name to run (e.g., building_permits)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        default=None,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (default: from settings)'
    )
    
    parser.add_argument(
        '--log-format',
        type=str,
        default=None,
        choices=['json', 'text'],
        help='Log format (default: from settings)'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        default='logs/etl.log',
        help='Log file path (default: logs/etl.log)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(
        log_level=args.log_level,
        log_format=args.log_format,
        log_file=args.log_file
    )
    
    logger.info("=" * 80)
    logger.info(f"ETL Pipeline Starting - Job: {args.job}")
    logger.info("=" * 80)
    
    # Run job
    exit_code = run_job(args.job)
    
    logger.info("=" * 80)
    logger.info(f"ETL Pipeline Complete - Exit Code: {exit_code}")
    logger.info("=" * 80)
    
    # Exit with appropriate code
    sys.exit(exit_code)


if __name__ == "__main__":
    main()