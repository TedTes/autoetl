from typing import Any, Dict
import logging
import pandas as pd
from datetime import datetime

from jobs.base_job import ETLJob, JobError
from clients.ckan_client import CKANClient, CKANAPIError
from transformers.date_transformer import DateTransformer
from transformers.numeric_transformer import NumericTransformer
from etl.pipeline import TransformerPipeline
from validators.column_drift_detector import ColumnDriftDetector
from repositories.building_permit_repository import BuildingPermitRepository
from repositories.schema_repository import SchemaRepository
from alerts.console_notifier import ConsoleNotifier
from alerts.base_notifier import AlertSeverity
from models.building_permit import BuildingPermit
from database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class BuildingPermitsETLJob(ETLJob):
    """
    ETL job for Toronto Building Permits data.
    
    Orchestrates: CKAN fetch → Transform → Validate → Load → Alert
    """
    
    def __init__(
        self,
        ckan_client: CKANClient,
        building_permit_repo: BuildingPermitRepository,
        schema_repo: SchemaRepository,
        drift_detector: ColumnDriftDetector,
        notifier: ConsoleNotifier,
        pipeline: TransformerPipeline
    ):
        """
        Initialize Building Permits ETL job with dependencies.
        
        Args:
            ckan_client: CKAN API client for data extraction
            building_permit_repo: Repository for building permits
            schema_repo: Repository for schema metadata
            drift_detector: Schema drift validator
            notifier: Alert notifier
            pipeline: Transformer pipeline
        """
        super().__init__(job_name="BuildingPermitsETL")
        
        self.ckan_client = ckan_client
        self.building_permit_repo = building_permit_repo
        self.schema_repo = schema_repo
        self.drift_detector = drift_detector
        self.notifier = notifier
        self.pipeline = pipeline
        
        self.raw_data: pd.DataFrame = None
        self.transformed_data: pd.DataFrame = None
    
    def setup(self) -> None:
        """Setup phase - validate connections and configuration."""
        logger.info("Validating CKAN API connection...")
        
        if not self.ckan_client.health_check():
            raise JobError(
                "CKAN API is not accessible",
                job_name=self.job_name
            )
        
        logger.info("CKAN API connection validated successfully")
    
    def extract(self) -> pd.DataFrame:
        """Extract phase - fetch data from CKAN API."""
        try:
            logger.info(f"Fetching data from CKAN resource: {self.ckan_client.resource_id}")
            
            # Fetch data using CKAN client
            df = self.ckan_client.fetch_data()
            
            if df.empty:
                logger.warning("No data fetched from CKAN API")
                return df
            
            logger.info(f"Fetched {len(df)} records from CKAN")
            self.raw_data = df
            
            return df
            
        except CKANAPIError as e:
            raise JobError(
                f"Failed to extract data from CKAN: {e.message}",
                job_name=self.job_name,
                original_error=e
            )
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform phase - clean and transform data."""
        if data.empty:
            logger.warning("No data to transform")
            return data
        
        try:
            logger.info("Applying transformation pipeline...")
            
            # Apply transformation pipeline
            transformed = self.pipeline.transform(data, skip_on_error=False)
            
            logger.info(f"Transformation complete: {len(transformed)} records")
            self.transformed_data = transformed
            
            return transformed
            
        except Exception as e:
            raise JobError(
                f"Transformation failed: {str(e)}",
                job_name=self.job_name,
                original_error=e
            )
    
    def load(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Load phase - validate schema, detect drift, and persist to database."""
        if data.empty:
            logger.warning("No data to load")
            return {
                'records_processed': 0,
                'records_failed': 0,
                'metadata': {}
            }
        
        records_processed = 0
        records_failed = 0
        metadata = {}
        
        try:
            # Step 1: Schema validation and drift detection
            logger.info("Validating schema and detecting drift...")
            
            expected_schema = self.schema_repo.get_active_schema('building_permits')
            drift_report = self.drift_detector.validate(
                df=data,
                table_name='building_permits',
                expected_schema=expected_schema if expected_schema else None
            )
            
            metadata['schema_drift_detected'] = drift_report.has_drift
            metadata['new_columns'] = len(drift_report.new_columns)
            metadata['removed_columns'] = len(drift_report.removed_columns)
            metadata['type_changes'] = len(drift_report.type_changes)
            
            # Step 2: Send drift alert if detected
            if drift_report.has_drift:
                logger.warning(f"Schema drift detected:\n{drift_report}")
                
                self.notifier.send_schema_drift_alert(
                    table_name='building_permits',
                    new_columns=drift_report.new_columns,
                    removed_columns=drift_report.removed_columns,
                    type_changes=drift_report.type_changes
                )
                
                # Record new columns in schema metadata
                if drift_report.new_columns:
                    current_schema = self.drift_detector.extract_schema(data)
                    new_cols_schema = {
                        col: current_schema[col] 
                        for col in drift_report.new_columns
                    }
                    self.schema_repo.record_new_columns('building_permits', new_cols_schema)
                
                # Mark removed columns
                if drift_report.removed_columns:
                    self.schema_repo.mark_columns_as_removed(
                        'building_permits', 
                        drift_report.removed_columns
                    )
            
            # Step 3: If no expected schema exists, record current schema
            if not expected_schema:
                logger.info("No existing schema found, recording current schema...")
                current_schema = self.drift_detector.extract_schema(data)
                self.schema_repo.record_new_columns('building_permits', current_schema)
            
            # Step 4: Convert DataFrame to BuildingPermit entities
            logger.info("Converting data to BuildingPermit entities...")
            entities = self._dataframe_to_entities(data)
            
            # Step 5: Upsert to database
            logger.info(f"Upserting {len(entities)} building permits...")
            records_processed = self.building_permit_repo.upsert_batch(
                entities=entities,
                unique_key='permit_number'
            )
            
            logger.info(f"Successfully loaded {records_processed} records")
            
            # Step 6: Send success alert
            self.notifier.send_alert(
                title=f"ETL Job Completed: {self.job_name}",
                message=f"Successfully processed {records_processed} building permits",
                severity=AlertSeverity.INFO,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Load phase failed: {str(e)}", exc_info=True)
            
            # Send error alert
            self.notifier.send_alert(
                title=f"ETL Job Failed: {self.job_name}",
                message=f"Load phase error: {str(e)}",
                severity=AlertSeverity.ERROR
            )
            
            raise JobError(
                f"Load phase failed: {str(e)}",
                job_name=self.job_name,
                original_error=e
            )
        
        return {
            'records_processed': records_processed,
            'records_failed': records_failed,
            'metadata': metadata
        }
    
    def _dataframe_to_entities(self, df: pd.DataFrame) -> list:
        """
        Convert DataFrame to BuildingPermit entity list.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            List of BuildingPermit entities
        """
        entities = []
        
        for _, row in df.iterrows():
            entity = BuildingPermit(
                permit_number=row.get('permit_number', str(row.get('_id', ''))),
                application_date=row.get('application_date'),
                issued_date=row.get('issued_date'),
                permit_type=row.get('permit_type'),
                work_description=row.get('work_description'),
                street_number=row.get('street_number'),
                street_name=row.get('street_name'),
                postal_code=row.get('postal_code'),
                ward=row.get('ward'),
                estimated_cost=row.get('estimated_cost')
            )
            entities.append(entity)
        
        return entities
    
    def cleanup(self) -> None:
        """Cleanup phase - release resources."""
        logger.info("Cleanup: Clearing cached data...")
        self.raw_data = None
        self.transformed_data = None
        logger.info("Cleanup complete")