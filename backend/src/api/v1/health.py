"""Health check endpoints."""
from datetime import datetime, timezone

from fastapi import APIRouter

from src.api.dependencies import MetadataClientDep
from src.config import settings
from src.exceptions import OpenMetadataConnectionError
from src.schemas import HealthCheckResponse

router = APIRouter()


@router.get("/health", response_model=HealthCheckResponse)
async def health_check(metadata_client: MetadataClientDep):
    """
    Health check endpoint.
    
    Verifies:
    - Application is running
    - OpenMetadata connection is working
    """
    openmetadata_connected = False
    
    try:
        openmetadata_connected = metadata_client.health_check()
    except OpenMetadataConnectionError:
        # Connection failed, but app is still running
        openmetadata_connected = False
    except Exception:
        # Unexpected error
        openmetadata_connected = False
    
    return HealthCheckResponse(
        status="healthy" if openmetadata_connected else "degraded",
        app_name=settings.APP_NAME,
        version=settings.APP_VERSION,
        openmetadata_connected=openmetadata_connected,
        timestamp=datetime.now(timezone.utc)
    )
