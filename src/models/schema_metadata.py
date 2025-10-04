from sqlalchemy import Column, Integer, String, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from database.connection import Base


class SchemaMetadata(Base):
    """Tracks schema changes and column metadata for drift detection."""
    
    __tablename__ = "schema_metadata"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Schema information
    table_name = Column(String(100), nullable=False, index=True)
    column_name = Column(String(100), nullable=False)
    data_type = Column(String(50), nullable=False)
    
    # Tracking information
    discovered_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    is_active = Column(String(10), default='active', nullable=False)  # 'active' or 'removed'
    
    # Ensure unique combination of table and column
    __table_args__ = (
        UniqueConstraint('table_name', 'column_name', name='uq_table_column'),
    )
    
    def __repr__(self):
        return f"<SchemaMetadata(table={self.table_name}, column={self.column_name}, type={self.data_type})>"