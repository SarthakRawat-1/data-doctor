"""Pytest configuration and fixtures."""
import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app


# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)


@pytest.fixture
async def client() -> AsyncClient:
    """Create an async test client."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
