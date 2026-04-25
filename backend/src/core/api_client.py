"""OpenMetadata API client wrapper.

This module provides a clean interface to the OpenMetadata Python SDK,
handling authentication, connection management, and common API operations.

Phase 1 Implementation.
"""
from typing import Any

from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
    AuthProvider,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.entity.data.table import Table

from src.config import settings
from src.exceptions import EntityNotFoundError, OpenMetadataConnectionError


class OpenMetadataClient:
    """
    Wrapper around OpenMetadata Python SDK.
    
    Uses the OpenMetadata class with JWT authentication (v1.12+).
    Provides methods for fetching tables, pipelines, lineage, and health checks.
    """
    
    def __init__(self):
        """Initialize the OpenMetadata client."""
        self._client: OpenMetadata | None = None
    
    def connect(self) -> None:
        """
        Establish connection to OpenMetadata using OpenMetadata class.
        
        Raises:
            OpenMetadataConnectionError: If connection fails
        """
        if self._client is not None:
            return
        
        try:
            # Create connection configuration
            server_config = OpenMetadataConnection(
                hostPort=settings.OPENMETADATA_HOST_PORT,
                authProvider=AuthProvider.openmetadata,
                securityConfig=OpenMetadataJWTClientConfig(
                    jwtToken=settings.OPENMETADATA_JWT_TOKEN
                ),
            )
            
            # Initialize the client
            self._client = OpenMetadata(server_config)
            
            # Test connection with health check
            if not self._client.health_check():
                raise OpenMetadataConnectionError("Health check failed")
            
        except Exception as e:
            self._client = None
            raise OpenMetadataConnectionError(
                f"Failed to connect to OpenMetadata at {settings.OPENMETADATA_HOST_PORT}: {e}"
            )
    
    def health_check(self) -> bool:
        """
        Check if OpenMetadata connection is healthy.
        
        Returns:
            bool: True if connected and healthy
        
        Raises:
            OpenMetadataConnectionError: If health check fails
        """
        if self._client is None:
            return False
        
        try:
            return self._client.health_check()
        except Exception as e:
            raise OpenMetadataConnectionError(f"Health check failed: {e}")
    
    def get_table_by_fqn(
        self,
        fqn: str,
        fields: list[str] | None = None
    ) -> dict[str, Any]:
        """
        Get table entity by Fully Qualified Name.
        
        Args:
            fqn: Fully qualified name (e.g., "snowflake.db.schema.table")
            fields: Optional fields to include. Valid fields:
                   - columns: Column definitions
                   - profile: Table profiling data
                   - testSuite: Data quality test suite
                   - changeDescription: Schema evolution tracking
                   - owners: Table owners
                   - tags: Classification tags
        
        Returns:
            Table entity as dictionary
        
        Raises:
            EntityNotFoundError: If table not found
        """
        if self._client is None:
            raise OpenMetadataConnectionError("Client not configured. Call connect() first.")
        
        try:
            # Use get_by_name method
            table = self._client.get_by_name(
                entity=Table,
                fqn=fqn,
                fields=fields
            )
            
            if not table:
                raise EntityNotFoundError(f"Table '{fqn}' not found")
            
            # Convert Pydantic model to dict for easier manipulation
            return table.model_dump() if hasattr(table, 'model_dump') else table.dict()
            
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                raise EntityNotFoundError(f"Table '{fqn}' not found")
            raise OpenMetadataConnectionError(f"API error fetching table: {e}")
    
    def get_pipeline_by_fqn(self, fqn: str) -> dict[str, Any]:
        """
        Get pipeline entity by Fully Qualified Name.
        
        Args:
            fqn: Fully qualified name (e.g., "airflow_service.my_dag")
        
        Returns:
            Pipeline entity as dictionary
        
        Raises:
            EntityNotFoundError: If pipeline not found
        """
        if self._client is None:
            raise OpenMetadataConnectionError("Client not configured. Call connect() first.")
        
        try:
            # Use client's get method directly
            from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import IngestionPipeline
            
            pipeline = self._client.get_by_name(
                entity=IngestionPipeline,
                fqn=fqn
            )
            
            if not pipeline:
                raise EntityNotFoundError(f"Pipeline '{fqn}' not found")
            
            return pipeline.model_dump() if hasattr(pipeline, 'model_dump') else pipeline.dict()
            
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                raise EntityNotFoundError(f"Pipeline '{fqn}' not found")
            raise OpenMetadataConnectionError(f"API error fetching pipeline: {e}")
    
    def get_lineage(
        self,
        entity_type: str,
        entity_id: str,
        upstream_depth: int = 5,
        downstream_depth: int = 5
    ) -> dict[str, Any]:
        """
        Get lineage graph for an entity.
        
        Args:
            entity_type: Type of entity (e.g., "table", "pipeline", "dashboard")
            entity_id: UUID of the entity
            upstream_depth: How many hops upstream to traverse (max: 3 per API)
            downstream_depth: How many hops downstream to traverse (max: 3 per API)
        
        Returns:
            Lineage graph with structure:
            {
                "entity": {...},
                "nodes": [...],
                "upstreamEdges": [...],
                "downstreamEdges": [...]
            }
        
        Raises:
            OpenMetadataConnectionError: If lineage fetch fails
        """
        if self._client is None:
            raise OpenMetadataConnectionError("Client not configured. Call connect() first.")
        
        try:
            # Clamp depths to API limits (max 3)
            upstream_depth = min(upstream_depth, 3)
            downstream_depth = min(downstream_depth, 3)
            
            # Use client's get method for lineage
            response = self._client.client.get(
                f"/lineage/{entity_type}/{entity_id}",
                data={
                    "upstreamDepth": upstream_depth,
                    "downstreamDepth": downstream_depth
                }
            )
            
            return response
            
        except Exception as e:
            raise OpenMetadataConnectionError(f"API error fetching lineage: {e}")
    
    def get_lineage_by_fqn(
        self,
        entity_type: str,
        fqn: str,
        upstream_depth: int = 5,
        downstream_depth: int = 5
    ) -> dict[str, Any]:
        """
        Get lineage graph for an entity by FQN.
        
        Args:
            entity_type: Type of entity (e.g., "table")
            fqn: Fully qualified name
            upstream_depth: How many hops upstream to traverse
            downstream_depth: How many hops downstream to traverse
        
        Returns:
            Lineage graph
        """
        if self._client is None:
            raise OpenMetadataConnectionError("Client not configured. Call connect() first.")
        
        try:
            # Clamp depths to API limits
            upstream_depth = min(upstream_depth, 3)
            downstream_depth = min(downstream_depth, 3)
            
            # Use client's get method for lineage by FQN
            response = self._client.client.get(
                f"/lineage/{entity_type}/name/{fqn}",
                data={
                    "upstreamDepth": upstream_depth,
                    "downstreamDepth": downstream_depth
                }
            )
            
            return response
            
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                raise EntityNotFoundError(f"Entity '{fqn}' not found")
            raise OpenMetadataConnectionError(f"API error fetching lineage: {e}")
    
    def search_by_fqn(self, fqn: str, index: str = "table_search_index") -> dict[str, Any]:
        """
        Search for an entity by FQN using Elasticsearch.
        
        Args:
            fqn: Fully qualified name to search
            index: Search index to query (default: table_search_index)
        
        Returns:
            Search results with entity ID and metadata
        """
        if self._client is None:
            raise OpenMetadataConnectionError("Client not configured. Call connect() first.")
        
        try:
            # Use client's get method for search
            response = self._client.client.get(
                "/search/query",
                data={
                    "q": fqn,
                    "index": index,
                    "from": 0,
                    "size": 1
                }
            )
            
            return response
            
        except Exception as e:
            raise OpenMetadataConnectionError(f"Search API error: {e}")
    
    def get_table_versions(
        self,
        table_id: str,
        limit: int = 30
    ) -> list[dict[str, Any]]:
        """
        Get historical versions of a table for trend analysis.
        
        Args:
            table_id: UUID of the table
            limit: Maximum number of versions to retrieve
        
        Returns:
            List of table versions (newest first)
        """
        if self._client is None:
            raise OpenMetadataConnectionError("Client not configured. Call connect() first.")
        
        try:
            response = self._client.client.get(
                f"/tables/{table_id}/versions"
            )
            
            versions = response.get("versions", [])
            return versions[:limit]
            
        except Exception as e:
            raise OpenMetadataConnectionError(f"API error fetching table versions: {e}")
    
    def get_test_case_results(
        self,
        table_fqn: str
    ) -> list[dict[str, Any]]:
        """
        Get data quality test case results for a table.
        
        Args:
            table_fqn: Fully qualified name of the table
        
        Returns:
            List of test case results
        """
        if self._client is None:
            raise OpenMetadataConnectionError("Client not configured. Call connect() first.")
        
        try:
            # Search for test cases associated with this table
            response = self._client.client.get(
                "/dataQuality/testCases",
                data={
                    "entityLink": f"<#E::table::{table_fqn}>",
                    "fields": "testCaseResult,testDefinition"
                }
            )
            
            return response.get("data", [])
            
        except Exception as e:
            raise OpenMetadataConnectionError(f"API error fetching test cases: {e}")
    
    def patch_entity_tag(
        self,
        entity_type: str,
        entity_id: str,
        tag_fqn: str
    ) -> bool:
        """
        Add a governance tag to an entity.
        
        Uses OpenMetadata SDK's patch_tag method to apply classification tags
        to assets for governance and data quality tracking.
        
        Phase 5+ Governance Enhancement.
        
        Args:
            entity_type: Type of entity ("table", "pipeline", "dashboard")
            entity_id: UUID of the entity
            tag_fqn: Fully qualified tag name (e.g., "DataQuality.Critical")
        
        Returns:
            True if successful
        
        Raises:
            OpenMetadataConnectionError: If tagging fails
        
        Reference: https://docs.open-metadata.org/latest/sdk/python/ingestion/tags
        """
        if self._client is None:
            raise OpenMetadataConnectionError("Client not configured. Call connect() first.")
        
        try:
            # Import entity classes
            from metadata.generated.schema.entity.data.table import Table
            from metadata.generated.schema.entity.data.pipeline import Pipeline
            from metadata.generated.schema.entity.data.dashboard import Dashboard
            
            # Map string to entity class
            entity_class_map = {
                "table": Table,
                "pipeline": Pipeline,
                "dashboard": Dashboard,
            }
            
            entity_class = entity_class_map.get(entity_type.lower())
            if not entity_class:
                raise ValueError(f"Unsupported entity type for tagging: {entity_type}")
            
            # Use SDK's patch_tag method
            self._client.patch_tag(
                entity=entity_class,
                entity_id=entity_id,
                tag_fqn=tag_fqn
            )
            
            return True
            
        except Exception as e:
            raise OpenMetadataConnectionError(f"Failed to patch tag '{tag_fqn}' on {entity_type} {entity_id}: {e}")


# Singleton instance
_client_instance: OpenMetadataClient | None = None


def get_metadata_client() -> OpenMetadataClient:
    """
    Dependency injection function for FastAPI.
    
    Returns a singleton OpenMetadata client instance.
    Creates and connects on first call, reuses thereafter.
    """
    global _client_instance
    
    if _client_instance is None:
        _client_instance = OpenMetadataClient()
        try:
            _client_instance.connect()
        except Exception as e:
            # Reset instance on connection failure
            _client_instance = None
            raise OpenMetadataConnectionError(f"Failed to initialize OpenMetadata client: {e}")
    
    return _client_instance
