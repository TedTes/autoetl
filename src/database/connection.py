from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.pool import QueuePool
from config.settings import settings

# Base class for all ORM models
Base = declarative_base()


class DatabaseConnection:
    """Manages database connections and sessions."""
    
    _engine: Engine = None
    _session_factory: sessionmaker = None
    
    @classmethod
    def get_engine(cls) -> Engine:
        """Get or create SQLAlchemy engine with connection pooling."""
        if cls._engine is None:
            cls._engine = create_engine(
                settings.database_url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,  # Verify connections before using
                echo=False  # Set to True for SQL debugging
            )
        return cls._engine
    
    @classmethod
    def get_session_factory(cls) -> sessionmaker:
        """Get or create session factory."""
        if cls._session_factory is None:
            cls._session_factory = sessionmaker(
                bind=cls.get_engine(),
                autocommit=False,
                autoflush=False
            )
        return cls._session_factory
    
    @classmethod
    @contextmanager
    def get_session(cls) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.
        
        Usage:
            with DatabaseConnection.get_session() as session:
                # perform database operations
                session.commit()
        """
        session = cls.get_session_factory()()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    @classmethod
    def create_all_tables(cls):
        """Create all tables defined in models."""
        Base.metadata.create_all(bind=cls.get_engine())
    
    @classmethod
    def drop_all_tables(cls):
        """Drop all tables (use with caution!)."""
        Base.metadata.drop_all(bind=cls.get_engine())