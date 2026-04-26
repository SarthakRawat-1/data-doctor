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
        default="sample_mysql_service.ecommerce_sample.dim_customer",
        description="Pre-staged FQN for demo scenario (matches setup_realistic_demo.py output)"
    )
    
    # Phase 5: AI Enhancement Layer Configuration
    GROQ_API_KEY: str = Field(
        default="",
        description="Groq API key for LLM formatting"
    )
    GROQ_MODEL: str = Field(
        default="llama-3.3-70b-versatile",
        description="Groq model to use for text generation"
    )
    SLACK_WEBHOOK_URL: str = Field(
        default="",
        description="Slack incoming webhook URL for notifications"
    )
    
    # Phase 5+: Governance Tagging Configuration (Optional)
    ENABLE_GOVERNANCE_TAGGING: bool = Field(
        default=False,
        description="Enable automatic governance tagging of unreliable assets"
    )
    GOVERNANCE_TAG_CLASSIFICATION: str = Field(
        default="DataQuality",
        description="Classification name for governance tags"
    )
    TAG_IMPACTED_ASSETS: bool = Field(
        default=True,
        description="Whether to tag downstream impacted assets"
    )


settings = Settings()
