"""Shared dependencies for API routes."""
from typing import Annotated

from fastapi import Depends

from src.core.api_client import OpenMetadataClient, get_metadata_client


# Dependency for injecting OpenMetadata client into routes
MetadataClientDep = Annotated[OpenMetadataClient, Depends(get_metadata_client)]
