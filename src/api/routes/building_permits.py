"""
Building Permits API routes.

Provides endpoints for accessing building permit data.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional
from datetime import date
import logging

from api.dependencies import get_building_permit_repository
from api.schemas.building_permit_schema import (
    BuildingPermitResponse,
    BuildingPermitListResponse,
    PaginationMetadata,
    ErrorResponse
)
from repositories.building_permit_repository import BuildingPermitRepository
from sqlalchemy import and_

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Building Permits"])


@router.get(
    "/building-permits",
    response_model=BuildingPermitListResponse,
    summary="List building permits",
    description="Retrieve a paginated list of building permits with optional filters"
)
async def list_building_permits(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    ward: Optional[str] = Query(None, description="Filter by ward"),
    permit_type: Optional[str] = Query(None, description="Filter by permit type"),
    issued_after: Optional[date] = Query(None, description="Filter by issued date (after)"),
    issued_before: Optional[date] = Query(None, description="Filter by issued date (before)"),
    repo: BuildingPermitRepository = Depends(get_building_permit_repository)
):
    """
    Get a paginated list of building permits.
    
    **Filters:**
    - ward: Municipal ward number
    - permit_type: Type of permit (e.g., "New Building", "Addition/Alteration")
    - issued_after: Issued date range start
    - issued_before: Issued date range end
    
    **Pagination:**
    - limit: Max 1000 records per request
    - offset: For pagination through results
    """
    try:
        # Build query with filters
        query = repo.session.query(repo.session.query(BuildingPermit).statement.columns)
        
        filters = []
        if ward:
            filters.append(BuildingPermit.ward == ward)
        if permit_type:
            filters.append(BuildingPermit.permit_type == permit_type)
        if issued_after:
            filters.append(BuildingPermit.issued_date >= issued_after)
        if issued_before:
            filters.append(BuildingPermit.issued_date <= issued_before)
        
        # Apply filters
        if filters:
            from models.building_permit import BuildingPermit
            query = repo.session.query(BuildingPermit).filter(and_(*filters))
        else:
            from models.building_permit import BuildingPermit
            query = repo.session.query(BuildingPermit)
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        permits = query.offset(offset).limit(limit).all()
        
        # Convert to response schema
        permit_responses = [
            BuildingPermitResponse.model_validate(permit)
            for permit in permits
        ]
        
        logger.info(
            f"Retrieved {len(permits)} building permits "
            f"(offset={offset}, limit={limit}, total={total_count})"
        )
        
        return BuildingPermitListResponse(
            data=permit_responses,
            pagination=PaginationMetadata(
                total=total_count,
                limit=limit,
                offset=offset,
                returned=len(permits)
            )
        )
        
    except Exception as e:
        logger.error(f"Error retrieving building permits: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve building permits: {str(e)}"
        )