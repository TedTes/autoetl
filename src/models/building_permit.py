from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime, Index
from sqlalchemy.sql import func
from database.connection import Base


class BuildingPermit(Base):
    """Building permit data model for Toronto building permits."""
    
    __tablename__ = "building_permits"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Unique identifier from CKAN (if available)
    permit_number = Column(String(50), unique=True, nullable=False, index=True)
    
    # Permit details
    application_date = Column(Date, nullable=True)
    issued_date = Column(Date, nullable=True, index=True)
    permit_type = Column(String(100), nullable=True, index=True)
    work_description = Column(String(500), nullable=True)
    
    # Location information
    street_number = Column(String(20), nullable=True)
    street_name = Column(String(200), nullable=True)
    postal_code = Column(String(10), nullable=True, index=True)
    ward = Column(String(50), nullable=True, index=True)
    
    # Financial information
    estimated_cost = Column(Numeric(12, 2), nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    
    # Indexes for common query patterns
    __table_args__ = (
        Index('idx_issued_date_permit_type', 'issued_date', 'permit_type'),
        Index('idx_ward_issued_date', 'ward', 'issued_date'),
    )
    
    def __repr__(self):
        return f"<BuildingPermit(permit_number={self.permit_number}, issued_date={self.issued_date})>"