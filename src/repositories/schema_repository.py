from typing import List, Optional, Dict, Set
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_
from models.schema_metadata import SchemaMetadata
from repositories.base_repository import BaseRepository, RepositoryError

logger = logging.getLogger(__name__)


class SchemaRepository(BaseRepository[SchemaMetadata]):
    """Repository for schema drift detection and tracking."""
    
    def __init__(self, session: Session):
        """
        Initialize Schema repository.
        
        Args:
            session: SQLAlchemy session
        """
        super().__init__(session)
    
    def save(self, entity: SchemaMetadata) -> SchemaMetadata:
        """Save a single schema metadata entry."""
        try:
            self.session.add(entity)
            self.session.flush()
            return entity
        except Exception as e:
            logger.error(f"Failed to save schema metadata: {str(e)}")
            raise RepositoryError("Failed to save schema metadata", original_error=e)
    
    def save_batch(self, entities: List[SchemaMetadata]) -> List[SchemaMetadata]:
        """Save multiple schema metadata entries."""
        try:
            self.session.add_all(entities)
            self.session.flush()
            return entities
        except Exception as e:
            logger.error(f"Failed to save schema metadata batch: {str(e)}")
            raise RepositoryError("Failed to save schema metadata batch", original_error=e)
    
    def upsert(self, entity: SchemaMetadata, unique_key: str = 'column_name') -> SchemaMetadata:
        """Upsert not typically used for schema metadata."""
        return self.save(entity)
    
    def upsert_batch(self, entities: List[SchemaMetadata], unique_key: str = 'column_name') -> int:
        """Upsert batch not typically used for schema metadata."""
        self.save_batch(entities)
        return len(entities)
    
    def find_by_id(self, entity_id: int) -> Optional[SchemaMetadata]:
        """Find schema metadata by ID."""
        return self.session.query(SchemaMetadata).filter_by(id=entity_id).first()
    
    def find_all(self, limit: Optional[int] = None, offset: int = 0) -> List[SchemaMetadata]:
        """Find all schema metadata with pagination."""
        query = self.session.query(SchemaMetadata)
        
        if offset:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def delete(self, entity: SchemaMetadata) -> None:
        """Delete schema metadata."""
        try:
            self.session.delete(entity)
            self.session.flush()
        except Exception as e:
            logger.error(f"Failed to delete schema metadata: {str(e)}")
            raise RepositoryError("Failed to delete schema metadata", original_error=e)
    
    def count(self) -> int:
        """Get total count of schema metadata entries."""
        return self.session.query(SchemaMetadata).count()
    
    def get_active_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get current active schema for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary mapping column_name -> data_type
        """
        active_columns = self.session.query(SchemaMetadata).filter(
            and_(
                SchemaMetadata.table_name == table_name,
                SchemaMetadata.is_active == 'active'
            )
        ).all()
        
        return {col.column_name: col.data_type for col in active_columns}
    
    def detect_new_columns(self, table_name: str, incoming_columns: Dict[str, str]) -> List[str]:
        """
        Detect new columns not in the stored schema.
        
        Args:
            table_name: Name of the table
            incoming_columns: Dict of column_name -> data_type from incoming data
            
        Returns:
            List of new column names
        """
        stored_schema = self.get_active_schema(table_name)
        stored_columns = set(stored_schema.keys())
        incoming_column_names = set(incoming_columns.keys())
        
        new_columns = incoming_column_names - stored_columns
        
        if new_columns:
            logger.warning(f"Detected {len(new_columns)} new column(s) in {table_name}: {new_columns}")
        
        return list(new_columns)
    
    def detect_removed_columns(self, table_name: str, incoming_columns: Dict[str, str]) -> List[str]:
        """
        Detect columns that were removed from the data source.
        
        Args:
            table_name: Name of the table
            incoming_columns: Dict of column_name -> data_type from incoming data
            
        Returns:
            List of removed column names
        """
        stored_schema = self.get_active_schema(table_name)
        stored_columns = set(stored_schema.keys())
        incoming_column_names = set(incoming_columns.keys())
        
        removed_columns = stored_columns - incoming_column_names
        
        if removed_columns:
            logger.warning(f"Detected {len(removed_columns)} removed column(s) in {table_name}: {removed_columns}")
        
        return list(removed_columns)
    
    def detect_type_changes(self, table_name: str, incoming_columns: Dict[str, str]) -> Dict[str, tuple]:
        """
        Detect data type changes in existing columns.
        
        Args:
            table_name: Name of the table
            incoming_columns: Dict of column_name -> data_type from incoming data
            
        Returns:
            Dict mapping column_name -> (old_type, new_type)
        """
        stored_schema = self.get_active_schema(table_name)
        type_changes = {}
        
        for col_name, new_type in incoming_columns.items():
            if col_name in stored_schema:
                old_type = stored_schema[col_name]
                if old_type != new_type:
                    type_changes[col_name] = (old_type, new_type)
        
        if type_changes:
            logger.warning(f"Detected type changes in {table_name}: {type_changes}")
        
        return type_changes
    
    def record_new_columns(self, table_name: str, new_columns: Dict[str, str]) -> None:
        """
        Record newly discovered columns in schema metadata.
        
        Args:
            table_name: Name of the table
            new_columns: Dict of column_name -> data_type
        """
        metadata_entries = [
            SchemaMetadata(
                table_name=table_name,
                column_name=col_name,
                data_type=data_type,
                is_active='active'
            )
            for col_name, data_type in new_columns.items()
        ]
        
        if metadata_entries:
            self.save_batch(metadata_entries)
            logger.info(f"Recorded {len(metadata_entries)} new column(s) for {table_name}")
    
    def mark_columns_as_removed(self, table_name: str, removed_columns: List[str]) -> None:
        """
        Mark columns as removed (set is_active to 'removed').
        
        Args:
            table_name: Name of the table
            removed_columns: List of column names that were removed
        """
        for col_name in removed_columns:
            metadata = self.session.query(SchemaMetadata).filter(
                and_(
                    SchemaMetadata.table_name == table_name,
                    SchemaMetadata.column_name == col_name,
                    SchemaMetadata.is_active == 'active'
                )
            ).first()
            
            if metadata:
                metadata.is_active = 'removed'
        
        self.session.flush()
        
        if removed_columns:
            logger.info(f"Marked {len(removed_columns)} column(s) as removed for {table_name}")