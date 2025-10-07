"""
Dependency injection utilities for FastAPI.

Provides reusable dependencies for database sessions, authentication, etc.
"""

from typing import Generator
from sqlalchemy.orm import Session
from fastapi import Depends
import logging

from database.connection import DatabaseConnection
from repositories.building_permit_repository import BuildingPermitRepository

logger = logging.getLogger(__name__)


def get_db_session() -> Generator[Session, None, None]:
    """
    Dependency that provides a database session.
    
    Automatically handles:
    - Session creation
    - Commit on success
    - Rollback on error
    - Session cleanup
    
    Usage:
        @app.get("/endpoint")
        async def endpoint(db: Session = Depends(get_db_session)):
            # Use db session here
    
    Yields:
        Session: SQLAlchemy database session
    """
    session = DatabaseConnection.get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error: {str(e)}")
        raise
    finally:
        session.close()


def get_building_permit_repository(
    db: Session = Depends(get_db_session)
) -> BuildingPermitRepository:
    """
    Dependency that provides BuildingPermitRepository.
    
    Usage:
        @app.get("/building-permits")
        async def get_permits(
            repo: BuildingPermitRepository = Depends(get_building_permit_repository)
        ):
            permits = repo.find_all(limit=10)
    
    Args:
        db: Database session (injected)
    
    Returns:
        BuildingPermitRepository: Repository instance
    """
    return BuildingPermitRepository(db)