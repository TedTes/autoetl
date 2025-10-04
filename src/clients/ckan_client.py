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


class CKANClient(CKANClientInterface):
    """Concrete implementation of CKAN API client using datastore_search_sql."""
    
    def __init__(self, base_url: str, resource_id: str, max_retries: int = 3, retry_delay: int = 5):
        """
        Initialize CKAN client.
        
        Args:
            base_url: Base URL of CKAN instance
            resource_id: Default resource ID to query
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.resource_id = resource_id
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.datastore_search_url = f"{self.base_url}/api/3/action/datastore_search_sql"
        self.resource_show_url = f"{self.base_url}/api/3/action/resource_show"
    
    def fetch_data(
        self, 
        resource_id: Optional[str] = None, 
        sql_query: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> pd.DataFrame:
        """
        Fetch data from CKAN datastore using SQL query.
        
        Args:
            resource_id: CKAN resource identifier (uses default if None)
            sql_query: Custom SQL query (builds default SELECT if None)
            limit: Maximum number of records to fetch
            offset: Number of records to skip
            
        Returns:
            DataFrame containing the fetched data
        """
        import requests
        import time
        
        rid = resource_id or self.resource_id
        
        # Build SQL query if not provided
        if sql_query is None:
            sql_query = f'SELECT * FROM "{rid}"'
            if limit:
                sql_query += f' LIMIT {limit}'
            if offset:
                sql_query += f' OFFSET {offset}'
        
        payload = {"sql": sql_query}
        
        # Retry logic
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    self.datastore_search_url,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if not data.get('success'):
                        raise CKANAPIError(
                            f"API returned success=false: {data.get('error', {}).get('message', 'Unknown error')}",
                            status_code=response.status_code,
                            response=response.text
                        )
                    
                    records = data.get('result', {}).get('records', [])
                    return pd.DataFrame(records)
                
                elif response.status_code == 429:  # Rate limit
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (attempt + 1))
                        continue
                    raise CKANAPIError(
                        "Rate limit exceeded",
                        status_code=response.status_code,
                        response=response.text
                    )
                
                else:
                    raise CKANAPIError(
                        f"HTTP {response.status_code}: {response.text}",
                        status_code=response.status_code,
                        response=response.text
                    )
                    
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise CKANAPIError("Request timeout after multiple retries")
            
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise CKANAPIError(f"Request failed: {str(e)}")
        
        raise CKANAPIError("Max retries exceeded")
    
    def get_resource_info(self, resource_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metadata about a CKAN resource.
        
        Args:
            resource_id: CKAN resource identifier (uses default if None)
            
        Returns:
            Dictionary containing resource metadata
        """
        import requests
        
        rid = resource_id or self.resource_id
        
        try:
            response = requests.get(
                self.resource_show_url,
                params={"id": rid},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    return data.get('result', {})
            
            raise CKANAPIError(
                f"Failed to get resource info: {response.text}",
                status_code=response.status_code
            )
            
        except requests.exceptions.RequestException as e:
            raise CKANAPIError(f"Failed to get resource info: {str(e)}")
    
    def health_check(self) -> bool:
        """
        Check if CKAN API is accessible.
        
        Returns:
            True if API is healthy, False otherwise
        """
        import requests
        
        try:
            response = requests.get(
                f"{self.base_url}/api/3/action/status_show",
                timeout=10
            )
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False