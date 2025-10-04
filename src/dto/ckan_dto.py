from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime


class CKANField(BaseModel):
    """Represents a field/column in a CKAN resource."""
    
    id: str
    type: str
    info: Optional[Dict[str, Any]] = None


class ResourceMetadata(BaseModel):
    """Metadata about a CKAN resource."""
    
    id: str
    name: Optional[str] = None
    description: Optional[str] = None
    format: Optional[str] = None
    url: Optional[str] = None
    created: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    size: Optional[int] = None
    fields: Optional[List[CKANField]] = []
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class CKANRecord(BaseModel):
    """Represents a single record from CKAN datastore."""
    
    data: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        # Allow extra fields since CKAN records have dynamic schemas
        extra = "allow"
    
    def __getitem__(self, key: str) -> Any:
        """Allow dictionary-style access to record data."""
        return self.data.get(key)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value with default fallback."""
        return self.data.get(key, default)


class CKANResponse(BaseModel):
    """Represents the response from CKAN API."""
    
    success: bool
    result: Dict[str, Any]
    help: Optional[str] = None
    error: Optional[Dict[str, Any]] = None
    
    @property
    def records(self) -> List[Dict[str, Any]]:
        """Extract records from result."""
        return self.result.get('records', [])
    
    @property
    def total_count(self) -> int:
        """Get total count of records (if available)."""
        return self.result.get('total', len(self.records))
    
    @property
    def fields(self) -> List[Dict[str, Any]]:
        """Extract field definitions from result."""
        return self.result.get('fields', [])


class FetchDataRequest(BaseModel):
    """Request parameters for fetching data from CKAN."""
    
    resource_id: str
    sql_query: Optional[str] = None
    limit: Optional[int] = Field(default=None, ge=1, le=32000)
    offset: int = Field(default=0, ge=0)
    
    def build_sql_query(self) -> str:
        """Build SQL query from parameters if not provided."""
        if self.sql_query:
            return self.sql_query
        
        query = f'SELECT * FROM "{self.resource_id}"'
        
        if self.limit:
            query += f' LIMIT {self.limit}'
        
        if self.offset:
            query += f' OFFSET {self.offset}'
        
        return query


class FetchDataResponse(BaseModel):
    """Response containing fetched data and metadata."""
    
    records: List[Dict[str, Any]]
    total_count: int
    fields: List[Dict[str, Any]]
    resource_id: str
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }