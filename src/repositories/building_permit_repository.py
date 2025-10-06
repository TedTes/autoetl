from typing import List, Optional, Any
import logging
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func
from models.building_permit import BuildingPermit
from repositories.base_repository import BaseRepository, RepositoryError

logger = logging.getLogger(__name__)


class BuildingPermitRepository(BaseRepository[BuildingPermit]):
    """Repository for BuildingPermit entity with PostgreSQL-specific optimizations."""
    
    def __init__(self, session: Session):
        """
        Initialize BuildingPermit repository.
        
        Args:
            session: SQLAlchemy session
        """
        super().__init__(session)
    
    def save(self, entity: BuildingPermit) -> BuildingPermit:
        """
        Save a single building permit.
        
        Args:
            entity: BuildingPermit instance
            
        Returns:
            Saved BuildingPermit with updated fields
        """
        try:
            self.session.add(entity)
            self.session.flush()
            return entity
        except Exception as e:
            logger.error(f"Failed to save building permit: {str(e)}")
            raise RepositoryError("Failed to save building permit", original_error=e)
    
    def save_batch(self, entities: List[BuildingPermit]) -> List[BuildingPermit]:
        """
        Save multiple building permits in batch.
        
        Args:
            entities: List of BuildingPermit instances
            
        Returns:
            List of saved BuildingPermit instances
        """
        try:
            self.session.add_all(entities)
            self.session.flush()
            return entities
        except Exception as e:
            logger.error(f"Failed to save batch of building permits: {str(e)}")
            raise RepositoryError("Failed to save building permits batch", original_error=e)
    
    def upsert(self, entity: BuildingPermit, unique_key: str = 'permit_number') -> BuildingPermit:
        """
        Insert or update building permit using PostgreSQL INSERT...ON CONFLICT.
        
        Args:
            entity: BuildingPermit instance
            unique_key: Column name for conflict detection (default: permit_number)
            
        Returns:
            Upserted BuildingPermit
        """
        try:
            stmt = insert(BuildingPermit).values(
                permit_number=entity.permit_number,
                application_date=entity.application_date,
                issued_date=entity.issued_date,
                permit_type=entity.permit_type,
                work_description=entity.work_description,
                street_number=entity.street_number,
                street_name=entity.street_name,
                postal_code=entity.postal_code,
                ward=entity.ward,
                estimated_cost=entity.estimated_cost
            )
            
            # Update all fields except id and created_at on conflict
            stmt = stmt.on_conflict_do_update(
                index_elements=[unique_key],
                set_={
                    'application_date': stmt.excluded.application_date,
                    'issued_date': stmt.excluded.issued_date,
                    'permit_type': stmt.excluded.permit_type,
                    'work_description': stmt.excluded.work_description,
                    'street_number': stmt.excluded.street_number,
                    'street_name': stmt.excluded.street_name,
                    'postal_code': stmt.excluded.postal_code,
                    'ward': stmt.excluded.ward,
                    'estimated_cost': stmt.excluded.estimated_cost,
                    'updated_at': func.now()
                }
            )
            
            self.session.execute(stmt)
            self.session.flush()
            
            # Fetch and return the upserted record
            return self.session.query(BuildingPermit).filter_by(
                permit_number=entity.permit_number
            ).first()
            
        except Exception as e:
            logger.error(f"Failed to upsert building permit: {str(e)}")
            raise RepositoryError("Failed to upsert building permit", original_error=e)
    
    def upsert_batch(self, entities: List[BuildingPermit], unique_key: str = 'permit_number') -> int:
        """
        Upsert multiple building permits using bulk INSERT...ON CONFLICT.
        
        Args:
            entities: List of BuildingPermit instances
            unique_key: Column name for conflict detection
            
        Returns:
            Number of records upserted
        """
        if not entities:
            return 0
        
        try:
            values = [
                {
                    'permit_number': e.permit_number,
                    'application_date': e.application_date,
                    'issued_date': e.issued_date,
                    'permit_type': e.permit_type,
                    'work_description': e.work_description,
                    'street_number': e.street_number,
                    'street_name': e.street_name,
                    'postal_code': e.postal_code,
                    'ward': e.ward,
                    'estimated_cost': e.estimated_cost
                }
                for e in entities
            ]
            
            stmt = insert(BuildingPermit).values(values)
            
            stmt = stmt.on_conflict_do_update(
                index_elements=[unique_key],
                set_={
                    'application_date': stmt.excluded.application_date,
                    'issued_date': stmt.excluded.issued_date,
                    'permit_type': stmt.excluded.permit_type,
                    'work_description': stmt.excluded.work_description,
                    'street_number': stmt.excluded.street_number,
                    'street_name': stmt.excluded.street_name,
                    'postal_code': stmt.excluded.postal_code,
                    'ward': stmt.excluded.ward,
                    'estimated_cost': stmt.excluded.estimated_cost,
                    'updated_at': func.now()
                }
            )
            
            result = self.session.execute(stmt)
            self.session.flush()
            
            logger.info(f"Upserted {len(entities)} building permits")
            return len(entities)
            
        except Exception as e:
            logger.error(f"Failed to upsert batch: {str(e)}")
            raise RepositoryError("Failed to upsert building permits batch", original_error=e)
    
    def find_by_id(self, entity_id: int) -> Optional[BuildingPermit]:
        """Find building permit by ID."""
        return self.session.query(BuildingPermit).filter_by(id=entity_id).first()
    
    def find_by_permit_number(self, permit_number: str) -> Optional[BuildingPermit]:
        """Find building permit by permit number."""
        return self.session.query(BuildingPermit).filter_by(
            permit_number=permit_number
        ).first()
    
    def find_all(self, limit: Optional[int] = None, offset: int = 0) -> List[BuildingPermit]:
        """Find all building permits with pagination."""
        query = self.session.query(BuildingPermit)
        
        if offset:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def delete(self, entity: BuildingPermit) -> None:
        """Delete a building permit."""
        try:
            self.session.delete(entity)
            self.session.flush()
        except Exception as e:
            logger.error(f"Failed to delete building permit: {str(e)}")
            raise RepositoryError("Failed to delete building permit", original_error=e)
    
    def count(self) -> int:
        """Get total count of building permits."""
        return self.session.query(func.count(BuildingPermit.id)).scalar()