"""
Pydantic schemas for Building Permit API responses.

These DTOs define the structure of data returned by API endpoints.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import date, datetime
from decimal import Decimal


class BuildingPermitResponse(BaseModel):
    """Schema for a single building permit response."""
    
    id: int = Field(..., description="Unique identifier")
    permit_number: str = Field(..., description="Permit number from source system")
    application_date: Optional[date] = Field(None, description="Date application was submitted")
    issued_date: Optional[date] = Field(None, description="Date permit was issued")
    permit_type: Optional[str] = Field(None, description="Type of building permit")
    work_description: Optional[str] = Field(None, description="Description of work to be performed")
    street_number: Optional[str] = Field(None, description="Street number")
    street_name: Optional[str] = Field(None, description="Street name")
    postal_code: Optional[str] = Field(None, description="Postal code")
    ward: Optional[str] = Field(None, description="Municipal ward")
    estimated_cost: Optional[Decimal] = Field(None, description="Estimated cost of work")
    created_at: datetime = Field(..., description="Record creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Record update timestamp")
    
    model_config = ConfigDict(
        from_attributes=True,  # Allows creation from ORM models
        json_encoders={
            Decimal: lambda v: float(v) if v else None,
            date: lambda v: v.isoformat() if v else None,
            datetime: lambda v: v.isoformat() if v else None
        }
    )


class PaginationMetadata(BaseModel):
    """Pagination metadata for list responses."""
    
    total: int = Field(..., description="Total number of records")
    limit: int = Field(..., description="Maximum records per page")
    offset: int = Field(..., description="Number of records skipped")
    returned: int = Field(..., description="Number of records in current response")


class BuildingPermitListResponse(BaseModel):
    """Schema for paginated list of building permits."""
    
    data: List[BuildingPermitResponse] = Field(..., description="List of building permits")
    pagination: PaginationMetadata = Field(..., description="Pagination information")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "data": [
                    {
                        "id": 1,
                        "permit_number": "24-123456",
                        "application_date": "2024-01-15",
                        "issued_date": "2024-02-01",
                        "permit_type": "New Building",
                        "work_description": "Construction of single family dwelling",
                        "street_number": "123",
                        "street_name": "Main Street",
                        "postal_code": "M5H 2N2",
                        "ward": "14",
                        "estimated_cost": 500000.00,
                        "created_at": "2024-02-01T10:00:00",
                        "updated_at": "2024-02-01T10:00:00"
                    }
                ],
                "pagination": {
                    "total": 1000,
                    "limit": 100,
                    "offset": 0,
                    "returned": 100
                }
            }
        }
    )


class ErrorResponse(BaseModel):
    """Schema for error responses."""
    
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "error": "NotFound",
                "message": "Building permit not found",
                "detail": "No permit exists with ID: 999999"
            }
        }
    )