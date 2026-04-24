"""Global configuration for Data Doctor."""
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Global application settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"
    )
    
    # Application Settings
    APP_NAME: str = "Data Doctor"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False
    
    # OpenMetadata Configuration
    OPENMETADATA_HOST_PORT: str = Field(
        default="http://localhost:8585/api",
        description="OpenMetadata API endpoint"
    )
    OPENMETADATA_JWT_TOKEN: str = Field(
        default="",
        description="JWT token for OpenMetadata authentication"
    )
    
    # API Configuration
    API_V1_PREFIX: str = "/api/v1"
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:5173"]
    
    # Demo Configuration
    DEMO_SCENARIO_FQN: str = Field(
        default="sample_data.ecommerce_db.shopify.dim_customer",
        description="Pre-staged FQN for demo scenario"
    )


settings = Settings()
