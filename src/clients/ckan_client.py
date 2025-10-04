from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd


class CKANClientInterface(ABC):
    """Abstract interface for CKAN API client.
    
    Follows Interface Segregation Principle (ISP) and Dependency Inversion Principle (DIP).
    Enables easy testing with mocks and allows for different CKAN implementations.
    """
    
    @abstractmethod
    def fetch_data(
        self, 
        resource_id: str, 
        sql_query: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> pd.DataFrame:
        """
        Fetch data from CKAN datastore.
        
        Args:
            resource_id: CKAN resource identifier
            sql_query: Optional SQL query for filtering
            limit: Maximum number of records to fetch
            offset: Number of records to skip (for pagination)
            
        Returns:
            DataFrame containing the fetched data
            
        Raises:
            CKANAPIError: If API request fails
        """
        pass
    
    @abstractmethod
    def get_resource_info(self, resource_id: str) -> Dict[str, Any]:
        """
        Get metadata about a CKAN resource.
        
        Args:
            resource_id: CKAN resource identifier
            
        Returns:
            Dictionary containing resource metadata (fields, record count, etc.)
            
        Raises:
            CKANAPIError: If API request fails
        """
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if CKAN API is accessible.
        
        Returns:
            True if API is healthy, False otherwise
        """
        pass


class CKANAPIError(Exception):
    """Custom exception for CKAN API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[str] = None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)