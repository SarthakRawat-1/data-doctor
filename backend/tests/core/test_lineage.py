"""Tests for lineage engine functions.

Tests the lineage fetching and graph parsing functions with mock data.
Cannot test end-to-end with real OpenMetadata until lineage data is ingested.

Phase 2 Testing.
"""
import pytest
from unittest.mock import Mock, MagicMock

from src.core.lineage import (
    fetch_lineage_graph,
    fetch_lineage_graph_by_fqn,
    build_upstream_adjacency_list,
    build_downstream_adjacency_list,
    build_nodes_map,
    extract_pipeline_from_edge,
)


# Mock lineage graph data (based on data_doctor.md Section 3 example)
MOCK_LINEAGE_GRAPH = {
    "entity": {
        "id": "target-table-id",
        "type": "table",
        "name": "dim_customer",
        "fullyQualifiedName": "sample_data.ecommerce_db.shopify.dim_customer",
        "deleted": False
    },
    "nodes": [
        {
            "id": "upstream-table-id",
            "type": "table",
            "name": "raw_customer",
            "fullyQualifiedName": "sample_data.ecommerce_db.shopify.raw_customer"
        },
        {
            "id": "pipeline-id",
            "type": "pipeline",
            "name": "customer_etl_dag",
            "fullyQualifiedName": "airflow_service.customer_etl_dag"
        },
        {
            "id": "downstream-table-id",
            "type": "table",
            "name": "fct_sales",
            "fullyQualifiedName": "sample_data.ecommerce_db.shopify.fct_sales"
        }
    ],
    "upstreamEdges": [
        {
            "fromEntity": "upstream-table-id",
            "toEntity": "target-table-id",
            "lineageDetails": {
                "sqlQuery": "INSERT INTO dim_customer SELECT * FROM raw_customer",
                "pipeline": {
                    "id": "pipeline-id",
                    "type": "pipeline",
                    "name": "customer_etl_dag",
                    "fullyQualifiedName": "airflow_service.customer_etl_dag"
                }
            }
        }
    ],
    "downstreamEdges": [
        {
            "fromEntity": "target-table-id",
            "toEntity": "downstream-table-id"
        }
    ]
}


class TestFetchLineageGraph:
    """Tests for fetch_lineage_graph function."""
    
    def test_fetch_lineage_graph_success(self):
        """Test successful lineage graph fetch."""
        mock_client = Mock()
        mock_client.get_lineage.return_value = MOCK_LINEAGE_GRAPH
        
        result = fetch_lineage_graph(
            metadata_client=mock_client,
            entity_type="table",
            entity_id="target-table-id",
            upstream_depth=3,
            downstream_depth=2
        )
        
        assert result == MOCK_LINEAGE_GRAPH
        mock_client.get_lineage.assert_called_once_with(
            entity_type="table",
            entity_id="target-table-id",
            upstream_depth=3,
            downstream_depth=2
        )
    
    def test_fetch_lineage_graph_default_depths(self):
        """Test fetch with default depth parameters."""
        mock_client = Mock()
        mock_client.get_lineage.return_value = MOCK_LINEAGE_GRAPH
        
        result = fetch_lineage_graph(
            metadata_client=mock_client,
            entity_type="table",
            entity_id="target-table-id"
        )
        
        assert result == MOCK_LINEAGE_GRAPH
        mock_client.get_lineage.assert_called_once_with(
            entity_type="table",
            entity_id="target-table-id",
            upstream_depth=5,
            downstream_depth=5
        )


class TestFetchLineageGraphByFqn:
    """Tests for fetch_lineage_graph_by_fqn function."""
    
    def test_fetch_lineage_by_fqn_success(self):
        """Test successful lineage fetch by FQN."""
        mock_client = Mock()
        mock_client.get_lineage_by_fqn.return_value = MOCK_LINEAGE_GRAPH
        
        result = fetch_lineage_graph_by_fqn(
            metadata_client=mock_client,
            entity_type="table",
            fqn="sample_data.ecommerce_db.shopify.dim_customer",
            upstream_depth=3,
            downstream_depth=2
        )
        
        assert result == MOCK_LINEAGE_GRAPH
        mock_client.get_lineage_by_fqn.assert_called_once_with(
            entity_type="table",
            fqn="sample_data.ecommerce_db.shopify.dim_customer",
            upstream_depth=3,
            downstream_depth=2
        )


class TestBuildUpstreamAdjacencyList:
    """Tests for build_upstream_adjacency_list function."""
    
    def test_build_upstream_adjacency_list_single_edge(self):
        """Test building adjacency list with single edge."""
        upstream_edges = [
            {
                "fromEntity": "parent-id",
                "toEntity": "child-id"
            }
        ]
        
        result = build_upstream_adjacency_list(upstream_edges)
        
        assert "child-id" in result
        assert len(result["child-id"]) == 1
        assert result["child-id"][0]["fromEntity"] == "parent-id"
    
    def test_build_upstream_adjacency_list_multiple_parents(self):
        """Test building adjacency list with multiple parents for one child."""
        upstream_edges = [
            {"fromEntity": "parent-1", "toEntity": "child"},
            {"fromEntity": "parent-2", "toEntity": "child"},
            {"fromEntity": "parent-3", "toEntity": "child"}
        ]
        
        result = build_upstream_adjacency_list(upstream_edges)
        
        assert "child" in result
        assert len(result["child"]) == 3
        parent_ids = [edge["fromEntity"] for edge in result["child"]]
        assert "parent-1" in parent_ids
        assert "parent-2" in parent_ids
        assert "parent-3" in parent_ids
    
    def test_build_upstream_adjacency_list_multiple_children(self):
        """Test building adjacency list with multiple children."""
        upstream_edges = [
            {"fromEntity": "parent", "toEntity": "child-1"},
            {"fromEntity": "parent", "toEntity": "child-2"}
        ]
        
        result = build_upstream_adjacency_list(upstream_edges)
        
        assert "child-1" in result
        assert "child-2" in result
        assert len(result["child-1"]) == 1
        assert len(result["child-2"]) == 1
    
    def test_build_upstream_adjacency_list_with_lineage_details(self):
        """Test that lineageDetails are preserved in adjacency list."""
        upstream_edges = [
            {
                "fromEntity": "parent",
                "toEntity": "child",
                "lineageDetails": {
                    "sqlQuery": "SELECT * FROM parent",
                    "pipeline": {"id": "pipeline-id", "name": "etl_dag"}
                }
            }
        ]
        
        result = build_upstream_adjacency_list(upstream_edges)
        
        assert "child" in result
        assert "lineageDetails" in result["child"][0]
        assert result["child"][0]["lineageDetails"]["pipeline"]["id"] == "pipeline-id"
    
    def test_build_upstream_adjacency_list_empty(self):
        """Test building adjacency list with empty edges."""
        result = build_upstream_adjacency_list([])
        assert result == {}
    
    def test_build_upstream_adjacency_list_missing_to_entity(self):
        """Test handling edges with missing toEntity."""
        upstream_edges = [
            {"fromEntity": "parent"},  # Missing toEntity
            {"fromEntity": "parent-2", "toEntity": "child-2"}
        ]
        
        result = build_upstream_adjacency_list(upstream_edges)
        
        # Should only include valid edge
        assert "child-2" in result
        assert len(result) == 1


class TestBuildDownstreamAdjacencyList:
    """Tests for build_downstream_adjacency_list function."""
    
    def test_build_downstream_adjacency_list_single_edge(self):
        """Test building downstream adjacency list with single edge."""
        downstream_edges = [
            {"fromEntity": "parent-id", "toEntity": "child-id"}
        ]
        
        result = build_downstream_adjacency_list(downstream_edges)
        
        assert "parent-id" in result
        assert "child-id" in result["parent-id"]
        assert len(result["parent-id"]) == 1
    
    def test_build_downstream_adjacency_list_multiple_children(self):
        """Test building adjacency list with multiple children."""
        downstream_edges = [
            {"fromEntity": "parent", "toEntity": "child-1"},
            {"fromEntity": "parent", "toEntity": "child-2"},
            {"fromEntity": "parent", "toEntity": "child-3"}
        ]
        
        result = build_downstream_adjacency_list(downstream_edges)
        
        assert "parent" in result
        assert len(result["parent"]) == 3
        assert "child-1" in result["parent"]
        assert "child-2" in result["parent"]
        assert "child-3" in result["parent"]
    
    def test_build_downstream_adjacency_list_chain(self):
        """Test building adjacency list with chain of nodes."""
        downstream_edges = [
            {"fromEntity": "A", "toEntity": "B"},
            {"fromEntity": "B", "toEntity": "C"},
            {"fromEntity": "C", "toEntity": "D"}
        ]
        
        result = build_downstream_adjacency_list(downstream_edges)
        
        assert "A" in result
        assert "B" in result
        assert "C" in result
        assert result["A"] == ["B"]
        assert result["B"] == ["C"]
        assert result["C"] == ["D"]
    
    def test_build_downstream_adjacency_list_empty(self):
        """Test building adjacency list with empty edges."""
        result = build_downstream_adjacency_list([])
        assert result == {}
    
    def test_build_downstream_adjacency_list_missing_entities(self):
        """Test handling edges with missing entities."""
        downstream_edges = [
            {"fromEntity": "parent"},  # Missing toEntity
            {"toEntity": "child"},  # Missing fromEntity
            {"fromEntity": "valid-parent", "toEntity": "valid-child"}
        ]
        
        result = build_downstream_adjacency_list(downstream_edges)
        
        # Should only include valid edge
        assert "valid-parent" in result
        assert result["valid-parent"] == ["valid-child"]
        assert len(result) == 1


class TestBuildNodesMap:
    """Tests for build_nodes_map function."""
    
    def test_build_nodes_map_success(self):
        """Test building nodes map from nodes list."""
        nodes = [
            {"id": "node-1", "type": "table", "name": "table1"},
            {"id": "node-2", "type": "pipeline", "name": "pipeline1"},
            {"id": "node-3", "type": "table", "name": "table2"}
        ]
        
        result = build_nodes_map(nodes)
        
        assert len(result) == 3
        assert "node-1" in result
        assert "node-2" in result
        assert "node-3" in result
        assert result["node-1"]["name"] == "table1"
        assert result["node-2"]["type"] == "pipeline"
    
    def test_build_nodes_map_preserves_all_fields(self):
        """Test that all node fields are preserved."""
        nodes = [
            {
                "id": "node-1",
                "type": "table",
                "name": "table1",
                "fullyQualifiedName": "service.db.schema.table1",
                "deleted": False,
                "customField": "value"
            }
        ]
        
        result = build_nodes_map(nodes)
        
        assert result["node-1"]["fullyQualifiedName"] == "service.db.schema.table1"
        assert result["node-1"]["deleted"] is False
        assert result["node-1"]["customField"] == "value"
    
    def test_build_nodes_map_empty(self):
        """Test building nodes map with empty list."""
        result = build_nodes_map([])
        assert result == {}
    
    def test_build_nodes_map_missing_id(self):
        """Test handling nodes with missing id."""
        nodes = [
            {"id": "node-1", "name": "table1"},
            {"name": "table2"},  # Missing id
            {"id": "node-3", "name": "table3"}
        ]
        
        result = build_nodes_map(nodes)
        
        # Should only include nodes with id
        assert len(result) == 2
        assert "node-1" in result
        assert "node-3" in result


class TestExtractPipelineFromEdge:
    """Tests for extract_pipeline_from_edge function."""
    
    def test_extract_pipeline_from_edge_success(self):
        """Test extracting pipeline from edge with lineageDetails."""
        edge = {
            "fromEntity": "source",
            "toEntity": "target",
            "lineageDetails": {
                "sqlQuery": "INSERT INTO target SELECT * FROM source",
                "pipeline": {
                    "id": "pipeline-id",
                    "type": "pipeline",
                    "name": "etl_dag",
                    "fullyQualifiedName": "airflow_service.etl_dag"
                }
            }
        }
        
        result = extract_pipeline_from_edge(edge)
        
        assert result is not None
        assert result["id"] == "pipeline-id"
        assert result["name"] == "etl_dag"
        assert result["fullyQualifiedName"] == "airflow_service.etl_dag"
    
    def test_extract_pipeline_from_edge_no_lineage_details(self):
        """Test extracting pipeline from edge without lineageDetails."""
        edge = {
            "fromEntity": "source",
            "toEntity": "target"
        }
        
        result = extract_pipeline_from_edge(edge)
        assert result is None
    
    def test_extract_pipeline_from_edge_no_pipeline(self):
        """Test extracting pipeline when lineageDetails has no pipeline."""
        edge = {
            "fromEntity": "source",
            "toEntity": "target",
            "lineageDetails": {
                "sqlQuery": "SELECT * FROM source"
                # No pipeline field
            }
        }
        
        result = extract_pipeline_from_edge(edge)
        assert result is None
    
    def test_extract_pipeline_from_edge_empty_pipeline(self):
        """Test extracting pipeline when pipeline is None."""
        edge = {
            "fromEntity": "source",
            "toEntity": "target",
            "lineageDetails": {
                "pipeline": None
            }
        }
        
        result = extract_pipeline_from_edge(edge)
        assert result is None
    
    def test_extract_pipeline_from_edge_preserves_all_fields(self):
        """Test that all pipeline fields are preserved."""
        edge = {
            "fromEntity": "source",
            "toEntity": "target",
            "lineageDetails": {
                "pipeline": {
                    "id": "pipeline-id",
                    "type": "pipeline",
                    "name": "etl_dag",
                    "fullyQualifiedName": "airflow_service.etl_dag",
                    "displayName": "ETL DAG",
                    "description": "Customer ETL pipeline"
                }
            }
        }
        
        result = extract_pipeline_from_edge(edge)
        
        assert result["displayName"] == "ETL DAG"
        assert result["description"] == "Customer ETL pipeline"


class TestLineageIntegration:
    """Integration tests for lineage functions working together."""
    
    def test_full_lineage_parsing_workflow(self):
        """Test complete workflow of parsing lineage graph."""
        # Use the mock lineage graph
        lineage_graph = MOCK_LINEAGE_GRAPH
        
        # Build data structures
        nodes_map = build_nodes_map(lineage_graph["nodes"])
        upstream_adj = build_upstream_adjacency_list(lineage_graph["upstreamEdges"])
        downstream_adj = build_downstream_adjacency_list(lineage_graph["downstreamEdges"])
        
        # Verify nodes map
        assert len(nodes_map) == 3
        assert "upstream-table-id" in nodes_map
        assert "pipeline-id" in nodes_map
        assert "downstream-table-id" in nodes_map
        
        # Verify upstream adjacency
        assert "target-table-id" in upstream_adj
        assert len(upstream_adj["target-table-id"]) == 1
        assert upstream_adj["target-table-id"][0]["fromEntity"] == "upstream-table-id"
        
        # Verify downstream adjacency
        assert "target-table-id" in downstream_adj
        assert "downstream-table-id" in downstream_adj["target-table-id"]
        
        # Extract pipeline from edge
        edge = upstream_adj["target-table-id"][0]
        pipeline = extract_pipeline_from_edge(edge)
        assert pipeline is not None
        assert pipeline["id"] == "pipeline-id"
    
    def test_complex_lineage_graph(self):
        """Test parsing complex lineage graph with multiple levels."""
        complex_graph = {
            "entity": {"id": "target", "type": "table", "name": "target"},
            "nodes": [
                {"id": "A", "type": "table", "name": "A"},
                {"id": "B", "type": "table", "name": "B"},
                {"id": "C", "type": "table", "name": "C"},
                {"id": "target", "type": "table", "name": "target"},
                {"id": "D", "type": "table", "name": "D"},
                {"id": "E", "type": "table", "name": "E"}
            ],
            "upstreamEdges": [
                {"fromEntity": "A", "toEntity": "target"},
                {"fromEntity": "B", "toEntity": "target"},
                {"fromEntity": "C", "toEntity": "A"}
            ],
            "downstreamEdges": [
                {"fromEntity": "target", "toEntity": "D"},
                {"fromEntity": "target", "toEntity": "E"}
            ]
        }
        
        nodes_map = build_nodes_map(complex_graph["nodes"])
        upstream_adj = build_upstream_adjacency_list(complex_graph["upstreamEdges"])
        downstream_adj = build_downstream_adjacency_list(complex_graph["downstreamEdges"])
        
        # Target has 2 direct parents
        assert len(upstream_adj["target"]) == 2
        
        # A has 1 parent
        assert len(upstream_adj["A"]) == 1
        assert upstream_adj["A"][0]["fromEntity"] == "C"
        
        # Target has 2 children
        assert len(downstream_adj["target"]) == 2
        assert "D" in downstream_adj["target"]
        assert "E" in downstream_adj["target"]
