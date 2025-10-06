from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Optional, Any
from sqlalchemy.orm import Session

# Generic type for model class
T = TypeVar('T')


class BaseRepository(ABC, Generic[T]):
    """
    Abstract base repository following Repository pattern and DIP.
    
    Business logic depends on this abstraction, not concrete implementations.
    Enables easy testing with mock repositories.
    """
    
    def __init__(self, session: Session):
        """
        Initialize repository with database session.
        
        Args:
            session: SQLAlchemy session for database operations
        """
        self.session = session
    
    @abstractmethod
    def save(self, entity: T) -> T:
        """
        Save a single entity to database.
        
        Args:
            entity: Entity instance to save
            
        Returns:
            Saved entity with updated fields (e.g., id, timestamps)
        """
        pass
    
    @abstractmethod
    def save_batch(self, entities: List[T]) -> List[T]:
        """
        Save multiple entities in a batch operation.
        
        Args:
            entities: List of entity instances to save
            
        Returns:
            List of saved entities
        """
        pass
    
    @abstractmethod
    def upsert(self, entity: T, unique_key: str) -> T:
        """
        Insert or update entity based on unique key.
        
        Args:
            entity: Entity instance to upsert
            unique_key: Column name to use for conflict detection
            
        Returns:
            Upserted entity
        """
        pass
    
    @abstractmethod
    def upsert_batch(self, entities: List[T], unique_key: str) -> int:
        """
        Upsert multiple entities in batch.
        
        Args:
            entities: List of entity instances to upsert
            unique_key: Column name to use for conflict detection
            
        Returns:
            Number of records upserted
        """
        pass
    
    @abstractmethod
    def find_by_id(self, entity_id: Any) -> Optional[T]:
        """
        Find entity by primary key.
        
        Args:
            entity_id: Primary key value
            
        Returns:
            Entity if found, None otherwise
        """
        pass
    
    @abstractmethod
    def find_all(self, limit: Optional[int] = None, offset: int = 0) -> List[T]:
        """
        Find all entities with optional pagination.
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of entities
        """
        pass
    
    @abstractmethod
    def delete(self, entity: T) -> None:
        """
        Delete an entity from database.
        
        Args:
            entity: Entity instance to delete
        """
        pass
    
    @abstractmethod
    def count(self) -> int:
        """
        Get total count of entities.
        
        Returns:
            Total number of records
        """
        pass


class RepositoryError(Exception):
    """Custom exception for repository operations."""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        self.message = message
        self.original_error = original_error
        super().__init__(self.message)