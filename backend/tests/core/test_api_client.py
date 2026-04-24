"""Tests for OpenMetadata API client.

Tests connection, authentication, and core API operations.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock

from src.core.api_client import OpenMetadataClient, get_metadata_client
from src.exceptions import OpenMetadataConnectionError, EntityNotFoundError


# ============================================================================
# Connection and Health Check Tests
# ============================================================================

def test_client_initialization():
    """Test that client initializes without connecting."""
    client = OpenMetadataClient()
    assert client._client is None


@patch('src.core.api_client.OpenMetadata')
def test_connect_success(mock_openmetadata_class):
    """Test successful connection to OpenMetadata."""
    # Mock the OpenMetadata instance
    mock_client = Mock()
    mock_client.health_check.return_value = True
    mock_openmetadata_class.return_value = mock_client
    
    client = OpenMetadataClient()
    client.connect()
    
    assert client._client is not None
    mock_openmetadata_class.assert_called_once()
    mock_client.health_check.assert_called_once()


@patch('src.core.api_client.OpenMetadata')
def test_connect_health_check_fails(mock_openmetadata_class):
    """Test connection fails when health check returns False."""
    mock_client = Mock()
    mock_client.health_check.return_value = False
    mock_openmetadata_class.return_value = mock_client
    
    client = OpenMetadataClient()
    
    with pytest.raises(OpenMetadataConnectionError, match="Health check failed"):
        client.connect()


@patch('src.core.api_client.OpenMetadata')
def test_connect_only_once(mock_openmetadata_class):
    """Test that connect() is idempotent."""
    mock_client = Mock()
    mock_client.health_check.return_value = True
    mock_openmetadata_class.return_value = mock_client
    
    client = OpenMetadataClient()
    client.connect()
    client.connect()  # Second call should not reconnect
    
    # Should only be called once
    assert mock_openmetadata_class.call_count == 1


@patch('src.core.api_client.OpenMetadata')
def test_health_check_when_connected(mock_openmetadata_class):
    """Test health check when client is connected."""
    mock_client = Mock()
    mock_client.health_check.return_value = True
    mock_openmetadata_class.return_value = mock_client
    
    client = OpenMetadataClient()
    client.connect()
    
    result = client.health_check()
    assert result is True


def test_health_check_when_not_connected():
    """Test health check returns False when not connected."""
    client = OpenMetadataClient()
    result = client.health_check()
    assert result is False


# ============================================================================
# get_table_by_fqn Tests
# ============================================================================

@patch('src.core.api_client.OpenMetadata')
def test_get_table_by_fqn_success(mock_openmetadata_class):
    """Test successful table retrieval."""
    mock_table = Mock()
    mock_table.model_dump.return_value = {
        "id": "123",
        "name": "customers",
        "fullyQualifiedName": "snowflake.db.schema.customers"
    }
    
    mock_client = Mock()
    mock_client.health_check.return_value = True
    mock_client.get_by_name.return_value = mock_table
    mock_openmetadata_class.return_value = mock_client
    
    client = OpenMetadataClient()
    client.connect()
    
    result = client.get_table_by_fqn("snowflake.db.schema.customers")
    
    assert result["name"] == "customers"
    mock_client.get_by_name.assert_called_once()


@patch('src.core.api_client.OpenMetadata')
def test_get_table_by_fqn_not_found(mock_openmetadata_class):
    """Test table not found raises EntityNotFoundError."""
    mock_client = Mock()
    mock_client.health_check.return_value = True
    mock_client.get_by_name.return_value = None
    mock_openmetadata_class.return_value = mock_client
    
    client = OpenMetadataClient()
    client.connect()
    
    with pytest.raises(EntityNotFoundError, match="not found"):
        client.get_table_by_fqn("nonexistent.table")


def test_get_table_by_fqn_not_connected():
    """Test that calling get_table_by_fqn without connecting raises error."""
    client = OpenMetadataClient()
    
    with pytest.raises(OpenMetadataConnectionError, match="not configured"):
        client.get_table_by_fqn("some.table")


# ============================================================================
# get_table_versions Tests
# ============================================================================

@patch('src.core.api_client.OpenMetadata')
def test_get_table_versions_success(mock_openmetadata_class):
    """Test successful retrieval of table versions."""
    mock_client = Mock()
    mock_client.health_check.return_value = True
    mock_client.client.get.return_value = {
        "versions": [
            {"version": "0.3", "profile": {"rowCount": 1000}},
            {"version": "0.2", "profile": {"rowCount": 950}},
            {"version": "0.1", "profile": {"rowCount": 900}},
        ]
    }
    mock_openmetadata_class.return_value = mock_client
    
    client = OpenMetadataClient()
    client.connect()
    
    versions = client.get_table_versions("table-id-123", limit=10)
    
    assert len(versions) == 3
    assert versions[0]["version"] == "0.3"


@patch('src.core.api_client.OpenMetadata')
def test_get_table_versions_respects_limit(mock_openmetadata_class):
    """Test that get_table_versions respects the limit parameter."""
    mock_client = Mock()
    mock_client.health_check.return_value = True
    mock_client.client.get.return_value = {
        "versions": [{"version": f"0.{i}"} for i in range(50)]
    }
    mock_openmetadata_class.return_value = mock_client
    
    client = OpenMetadataClient()
    client.connect()
    
    versions = client.get_table_versions("table-id-123", limit=5)
    
    assert len(versions) == 5


# ============================================================================
# get_test_case_results Tests
# ============================================================================

@patch('src.core.api_client.OpenMetadata')
def test_get_test_case_results_success(mock_openmetadata_class):
    """Test successful retrieval of test case results."""
    mock_client = Mock()
    mock_client.health_check.return_value = True
    mock_client.client.get.return_value = {
        "data": [
            {
                "name": "null_check",
                "testCaseResult": {"testCaseStatus": "Success"}
            },
            {
                "name": "uniqueness_check",
                "testCaseResult": {"testCaseStatus": "Failed"}
            }
        ]
    }
    mock_openmetadata_class.return_value = mock_client
    
    client = OpenMetadataClient()
    client.connect()
    
    test_cases = client.get_test_case_results("snowflake.db.schema.table")
    
    assert len(test_cases) == 2
    assert test_cases[0]["name"] == "null_check"
    assert test_cases[1]["testCaseResult"]["testCaseStatus"] == "Failed"


@patch('src.core.api_client.OpenMetadata')
def test_get_test_case_results_empty(mock_openmetadata_class):
    """Test retrieval when no test cases exist."""
    mock_client = Mock()
    mock_client.health_check.return_value = True
    mock_client.client.get.return_value = {"data": []}
    mock_openmetadata_class.return_value = mock_client
    
    client = OpenMetadataClient()
    client.connect()
    
    test_cases = client.get_test_case_results("snowflake.db.schema.table")
    
    assert len(test_cases) == 0


# ============================================================================
# get_metadata_client (Singleton) Tests
# ============================================================================

@patch('src.core.api_client.OpenMetadata')
def test_get_metadata_client_singleton(mock_openmetadata_class):
    """Test that get_metadata_client returns singleton instance."""
    mock_client = Mock()
    mock_client.health_check.return_value = True
    mock_openmetadata_class.return_value = mock_client
    
    # Reset the singleton
    import src.core.api_client
    src.core.api_client._client_instance = None
    
    client1 = get_metadata_client()
    client2 = get_metadata_client()
    
    assert client1 is client2
    # Should only connect once
    assert mock_openmetadata_class.call_count == 1


@patch('src.core.api_client.OpenMetadata')
def test_get_metadata_client_connection_failure(mock_openmetadata_class):
    """Test that get_metadata_client resets on connection failure."""
    mock_openmetadata_class.side_effect = Exception("Connection failed")
    
    # Reset the singleton
    import src.core.api_client
    src.core.api_client._client_instance = None
    
    with pytest.raises(OpenMetadataConnectionError):
        get_metadata_client()
    
    # Instance should be reset to None after failure
    assert src.core.api_client._client_instance is None


# ============================================================================
# Integration-style Tests (can be skipped if no real connection)
# ============================================================================

@pytest.mark.integration
@pytest.mark.skip(reason="Requires real OpenMetadata instance")
def test_real_connection():
    """Integration test with real OpenMetadata instance.
    
    This test is skipped by default. Run with:
    pytest tests/core/test_api_client.py -m integration
    """
    client = OpenMetadataClient()
    client.connect()
    
    assert client.health_check() is True


@pytest.mark.integration
@pytest.mark.skip(reason="Requires real OpenMetadata instance with sample data")
def test_real_table_fetch():
    """Integration test fetching real table.
    
    This test is skipped by default. Run with:
    pytest tests/core/test_api_client.py -m integration
    """
    client = OpenMetadataClient()
    client.connect()
    
    # Assumes sample_data exists in OpenMetadata
    table = client.get_table_by_fqn("sample_data.ecommerce_db.shopify.dim_customer")
    
    assert table is not None
    assert "fullyQualifiedName" in table
