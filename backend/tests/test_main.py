"""Tests for main application root endpoint."""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_root_endpoint(client: AsyncClient):
    """Test root endpoint returns welcome message."""
    response = await client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "Data Doctor" in data["message"]


@pytest.mark.asyncio
async def test_root_endpoint_structure(client: AsyncClient):
    """Test root endpoint response structure."""
    response = await client.get("/")
    data = response.json()
    
    # Verify required fields
    assert "message" in data
    assert "version" in data
    assert "docs" in data
    
    # Verify types
    assert isinstance(data["message"], str)
    assert isinstance(data["version"], str)
    assert isinstance(data["docs"], str)
    
    # Verify docs link
    assert data["docs"] == "/docs"
