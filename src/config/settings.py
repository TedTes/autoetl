from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration using Pydantic settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
    
    # CKAN API Configuration
    ckan_base_url: str
    ckan_resource_id: str
    
    # Database Configuration
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str
    db_user: str
    db_password: str
    
    # Logging Configuration
    log_level: str = "INFO"
    log_format: str = "json"
    
    # ETL Configuration
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 5
    
    @property
    def database_url(self) -> str:
        """Construct database URL from components."""
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )
    
    @property
    def ckan_datastore_search_url(self) -> str:
        """Construct CKAN datastore search URL."""
        return f"{self.ckan_base_url}/api/3/action/datastore_search_sql"


# Global settings instance
settings = Settings()