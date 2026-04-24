"""Tests for health check API endpoint."""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_check_success(client: AsyncClient):
    """Test health check endpoint returns healthy status."""
    response = await client.get("/api/v1/health")
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["status"] == "healthy"
    assert data["app_name"] == "Data Doctor"
    assert data["version"] == "0.1.0"
    assert "openmetadata_connected" in data
    assert "timestamp" in data


@pytest.mark.asyncio
async def test_health_check_structure(client: AsyncClient):
    """Test health check response has correct structure."""
    response = await client.get("/api/v1/health")
    data = response.json()
    
    # Verify all required fields are present
    required_fields = ["status", "app_name", "version", "openmetadata_connected", "timestamp"]
    for field in required_fields:
        assert field in data, f"Missing required field: {field}"
    
    # Verify types
    assert isinstance(data["status"], str)
    assert isinstance(data["app_name"], str)
    assert isinstance(data["version"], str)
    assert isinstance(data["openmetadata_connected"], bool)
    assert isinstance(data["timestamp"], str)
