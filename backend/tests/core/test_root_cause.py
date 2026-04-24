"""Tests for root cause analysis engine.

Tests the BFS upstream traversal algorithm with mock data.
Cannot test end-to-end with real OpenMetadata until lineage data is ingested.

Phase 2 Testing.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch

from src.constants import AnomalyType
from src.core.root_cause import (
    find_root_cause,
    check_node_anomalies,
    check_edge_anomalies,
    find_root_cause_by_fqn,
)
from src.schemas import AnomalyDetail


# Mock lineage graph for testing
MOCK_SIMPLE_LINEAGE = {
    "entity": {"id": "target", "type": "table", "name": "target"},
    "nodes": [
        {"id": "parent", "type": "table", "name": "parent_table", "fullyQualifiedName": "db.schema.parent"},
        {"id": "target", "type": "table", "name": "target_table", "fullyQualifiedName": "db.schema.target"}
    ],
    "upstreamEdges": [
        {"fromEntity": "parent", "toEntity": "target"}
    ],
    "downstreamEdges": []
}

MOCK_LINEAGE_WITH_PIPELINE = {
    "entity": {"id": "target", "type": "table", "name": "target"},
    "nodes": [
        {"id": "parent", "type": "table", "name": "parent_table", "fullyQualifiedName": "db.schema.parent"},
        {"id": "pipeline", "type": "pipeline", "name": "etl_dag", "fullyQualifiedName": "airflow.etl_dag"},
        {"id": "target", "type": "table", "name": "target_table", "fullyQualifiedName": "db.schema.target"}
    ],
    "upstreamEdges": [
        {
            "fromEntity": "parent",
            "toEntity": "target",
            "lineageDetails": {
                "pipeline": {
                    "id": "pipeline",
                    "type": "pipeline",
                    "name": "etl_dag",
                    "fullyQualifiedName": "airflow.etl_dag"
                }
            }
        }
    ],
    "downstreamEdges": []
}

MOCK_MULTI_LEVEL_LINEAGE = {
    "entity": {"id": "target", "type": "table", "name": "target"},
    "nodes": [
        {"id": "grandparent", "type": "table", "name": "grandparent", "fullyQualifiedName": "db.schema.grandparent"},
        {"id": "parent", "type": "table", "name": "parent", "fullyQualifiedName": "db.schema.parent"},
        {"id": "target", "type": "table", "name": "target", "fullyQualifiedName": "db.schema.target"}
    ],
    "upstreamEdges": [
        {"fromEntity": "grandparent", "toEntity": "parent"},
        {"fromEntity": "parent", "toEntity": "target"}
    ],
    "downstreamEdges": []
}


class TestFindRootCause:
    """Tests for find_root_cause function."""
    
    @patch('src.core.root_cause.fetch_lineage_graph')
    @patch('src.core.root_cause.check_node_anomalies')
    @patch('src.core.root_cause.check_edge_anomalies')
    def test_find_root_cause_no_anomalies(
        self,
        mock_check_edge,
        mock_check_node,
        mock_fetch_lineage
    ):
        """Test root cause when no anomalies are found."""
        mock_client = Mock()
        mock_fetch_lineage.return_value = MOCK_SIMPLE_LINEAGE
        mock_check_edge.return_value = []
        mock_check_node.return_value = []
        
        result = find_root_cause(
            metadata_client=mock_client,
            target_entity_id="target",
            target_entity_type="table"
        )
        
        assert result["primary_root_cause"] is None
        assert result["contributing_factors"] == []
    
    @patch('src.core.root_cause.fetch_lineage_graph')
    @patch('src.core.root_cause.check_node_anomalies')
    @patch('src.core.root_cause.check_edge_anomalies')
    def test_find_root_cause_single_anomaly(
        self,
        mock_check_edge,
        mock_check_node,
        mock_fetch_lineage
    ):
        """Test root cause with single anomaly found."""
        mock_client = Mock()
        mock_fetch_lineage.return_value = MOCK_SIMPLE_LINEAGE
        mock_check_edge.return_value = []
        
        # Return anomaly for parent node
        anomaly = AnomalyDetail(
            type=AnomalyType.STALE_DATA,
            name="parent_table",
            depth=1,
            entity_id="parent",
            description="Stale data detected"
        )
        mock_check_node.return_value = [anomaly]
        
        result = find_root_cause(
            metadata_client=mock_client,
            target_entity_id="target",
            target_entity_type="table"
        )
        
        assert result["primary_root_cause"] == anomaly
        assert result["contributing_factors"] == []
    
    @patch('src.core.root_cause.fetch_lineage_graph')
    @patch('src.core.root_cause.check_node_anomalies')
    @patch('src.core.root_cause.check_edge_anomalies')
    def test_find_root_cause_multiple_anomalies_sorted_by_depth(
        self,
        mock_check_edge,
        mock_check_node,
        mock_fetch_lineage
    ):
        """Test that anomalies are sorted by depth (closest first)."""
        mock_client = Mock()
        mock_fetch_lineage.return_value = MOCK_MULTI_LEVEL_LINEAGE
        mock_check_edge.return_value = []
        
        # Return different anomalies at different depths
        def node_anomalies_side_effect(client, node, depth):
            if node["id"] == "parent":
                return [AnomalyDetail(
                    type=AnomalyType.STALE_DATA,
                    name="parent",
                    depth=depth,
                    entity_id="parent",
                    description="Parent stale"
                )]
            elif node["id"] == "grandparent":
                return [AnomalyDetail(
                    type=AnomalyType.DATA_QUALITY_FAILURE,
                    name="grandparent",
                    depth=depth,
                    entity_id="grandparent",
                    description="Grandparent quality issue"
                )]
            return []
        
        mock_check_node.side_effect = node_anomalies_side_effect
        
        result = find_root_cause(
            metadata_client=mock_client,
            target_entity_id="target",
            target_entity_type="table"
        )
        
        # Primary should be closest (depth 1)
        assert result["primary_root_cause"].depth == 1
        assert result["primary_root_cause"].name == "parent"
        
        # Contributing should be deeper (depth 2)
        assert len(result["contributing_factors"]) == 1
        assert result["contributing_factors"][0].depth == 2
        assert result["contributing_factors"][0].name == "grandparent"
    
    @patch('src.core.root_cause.fetch_lineage_graph')
    @patch('src.core.root_cause.check_node_anomalies')
    @patch('src.core.root_cause.check_edge_anomalies')
    def test_find_root_cause_edge_anomaly(
        self,
        mock_check_edge,
        mock_check_node,
        mock_fetch_lineage
    ):
        """Test root cause with pipeline failure on edge."""
        mock_client = Mock()
        mock_fetch_lineage.return_value = MOCK_LINEAGE_WITH_PIPELINE
        mock_check_node.return_value = []
        
        # Return pipeline failure on edge
        pipeline_anomaly = AnomalyDetail(
            type=AnomalyType.PIPELINE_FAILURE,
            name="etl_dag",
            depth=1,
            entity_id="pipeline",
            description="Pipeline failed"
        )
        mock_check_edge.return_value = [pipeline_anomaly]
        
        result = find_root_cause(
            metadata_client=mock_client,
            target_entity_id="target",
            target_entity_type="table"
        )
        
        assert result["primary_root_cause"] == pipeline_anomaly
        assert result["primary_root_cause"].type == AnomalyType.PIPELINE_FAILURE
    
    @patch('src.core.root_cause.fetch_lineage_graph')
    @patch('src.core.root_cause.check_node_anomalies')
    @patch('src.core.root_cause.check_edge_anomalies')
    def test_find_root_cause_both_edge_and_node_anomalies(
        self,
        mock_check_edge,
        mock_check_node,
        mock_fetch_lineage
    ):
        """Test root cause with both edge and node anomalies."""
        mock_client = Mock()
        mock_fetch_lineage.return_value = MOCK_LINEAGE_WITH_PIPELINE
        
        # Pipeline failure on edge (depth 1)
        pipeline_anomaly = AnomalyDetail(
            type=AnomalyType.PIPELINE_FAILURE,
            name="etl_dag",
            depth=1,
            entity_id="pipeline",
            description="Pipeline failed"
        )
        mock_check_edge.return_value = [pipeline_anomaly]
        
        # Data quality issue on parent node (depth 1)
        node_anomaly = AnomalyDetail(
            type=AnomalyType.DATA_QUALITY_FAILURE,
            name="parent_table",
            depth=1,
            entity_id="parent",
            description="Quality issue"
        )
        mock_check_node.return_value = [node_anomaly]
        
        result = find_root_cause(
            metadata_client=mock_client,
            target_entity_id="target",
            target_entity_type="table"
        )
        
        # Should have primary + 1 contributing (both at depth 1)
        assert result["primary_root_cause"] is not None
        assert len(result["contributing_factors"]) == 1
    
    @patch('src.core.root_cause.fetch_lineage_graph')
    def test_find_root_cause_calls_fetch_with_correct_params(
        self,
        mock_fetch_lineage
    ):
        """Test that fetch_lineage_graph is called with correct parameters."""
        mock_client = Mock()
        mock_fetch_lineage.return_value = {
            "entity": {"id": "target", "type": "table"},
            "nodes": [],
            "upstreamEdges": [],
            "downstreamEdges": []
        }
        
        find_root_cause(
            metadata_client=mock_client,
            target_entity_id="test-id",
            target_entity_type="table",
            upstream_depth=3
        )
        
        mock_fetch_lineage.assert_called_once_with(
            metadata_client=mock_client,
            entity_type="table",
            entity_id="test-id",
            upstream_depth=3,
            downstream_depth=0  # Should be 0 for root cause
        )
    
    @patch('src.core.root_cause.fetch_lineage_graph')
    @patch('src.core.root_cause.check_node_anomalies')
    @patch('src.core.root_cause.check_edge_anomalies')
    def test_find_root_cause_visited_set_prevents_cycles(
        self,
        mock_check_edge,
        mock_check_node,
        mock_fetch_lineage
    ):
        """Test that visited set prevents infinite loops in cyclic graphs."""
        # Create cyclic graph: A -> B -> C -> A
        cyclic_lineage = {
            "entity": {"id": "A", "type": "table", "name": "A"},
            "nodes": [
                {"id": "A", "type": "table", "name": "A", "fullyQualifiedName": "db.A"},
                {"id": "B", "type": "table", "name": "B", "fullyQualifiedName": "db.B"},
                {"id": "C", "type": "table", "name": "C", "fullyQualifiedName": "db.C"}
            ],
            "upstreamEdges": [
                {"fromEntity": "B", "toEntity": "A"},
                {"fromEntity": "C", "toEntity": "B"},
                {"fromEntity": "A", "toEntity": "C"}  # Creates cycle
            ],
            "downstreamEdges": []
        }
        
        mock_client = Mock()
        mock_fetch_lineage.return_value = cyclic_lineage
        mock_check_edge.return_value = []
        mock_check_node.return_value = []
        
        # Should not hang or raise error
        result = find_root_cause(
            metadata_client=mock_client,
            target_entity_id="A",
            target_entity_type="table"
        )
        
        # Should complete successfully
        assert result["primary_root_cause"] is None


class TestCheckNodeAnomalies:
    """Tests for check_node_anomalies function."""
    
    @patch('src.core.root_cause.evaluate_asset_anomalies')
    def test_check_node_anomalies_table_with_anomalies(self, mock_evaluate):
        """Test checking table node with anomalies detected."""
        mock_client = Mock()
        mock_client.get_table_by_fqn.return_value = {"id": "table-id", "name": "test_table"}
        mock_client.get_table_versions.return_value = []
        mock_client.get_test_case_results.return_value = []
        
        # Mock detection finding stale data
        mock_evaluate.return_value = [AnomalyType.STALE_DATA]
        
        node = {
            "id": "table-id",
            "type": "table",
            "name": "test_table",
            "fullyQualifiedName": "db.schema.test_table"
        }
        
        result = check_node_anomalies(mock_client, node, depth=1)
        
        assert len(result) == 1
        assert result[0].type == AnomalyType.STALE_DATA
        assert result[0].name == "test_table"
        assert result[0].depth == 1
        assert result[0].entity_id == "table-id"
    
    @patch('src.core.root_cause.evaluate_asset_anomalies')
    def test_check_node_anomalies_table_no_anomalies(self, mock_evaluate):
        """Test checking table node with no anomalies."""
        mock_client = Mock()
        mock_client.get_table_by_fqn.return_value = {"id": "table-id"}
        mock_client.get_table_versions.return_value = []
        mock_client.get_test_case_results.return_value = []
        
        mock_evaluate.return_value = []
        
        node = {
            "id": "table-id",
            "type": "table",
            "name": "test_table",
            "fullyQualifiedName": "db.schema.test_table"
        }
        
        result = check_node_anomalies(mock_client, node, depth=1)
        
        assert result == []
    
    @patch('src.core.root_cause.evaluate_asset_anomalies')
    def test_check_node_anomalies_pipeline_with_failure(self, mock_evaluate):
        """Test checking pipeline node with failure."""
        mock_client = Mock()
        mock_client.get_pipeline_by_fqn.return_value = {"id": "pipeline-id"}
        
        mock_evaluate.return_value = [AnomalyType.PIPELINE_FAILURE]
        
        node = {
            "id": "pipeline-id",
            "type": "pipeline",
            "name": "etl_dag",
            "fullyQualifiedName": "airflow.etl_dag"
        }
        
        result = check_node_anomalies(mock_client, node, depth=2)
        
        assert len(result) == 1
        assert result[0].type == AnomalyType.PIPELINE_FAILURE
        assert result[0].depth == 2
    
    @patch('src.core.root_cause.evaluate_asset_anomalies')
    def test_check_node_anomalies_multiple_anomalies(self, mock_evaluate):
        """Test checking node with multiple anomalies."""
        mock_client = Mock()
        mock_client.get_table_by_fqn.return_value = {"id": "table-id"}
        mock_client.get_table_versions.return_value = []
        mock_client.get_test_case_results.return_value = []
        
        # Multiple anomalies detected
        mock_evaluate.return_value = [
            AnomalyType.STALE_DATA,
            AnomalyType.SCHEMA_CHANGE,
            AnomalyType.DATA_QUALITY_FAILURE
        ]
        
        node = {
            "id": "table-id",
            "type": "table",
            "name": "test_table",
            "fullyQualifiedName": "db.schema.test_table"
        }
        
        result = check_node_anomalies(mock_client, node, depth=1)
        
        assert len(result) == 3
        assert result[0].type == AnomalyType.STALE_DATA
        assert result[1].type == AnomalyType.SCHEMA_CHANGE
        assert result[2].type == AnomalyType.DATA_QUALITY_FAILURE
    
    def test_check_node_anomalies_missing_node_id(self):
        """Test handling node with missing id."""
        mock_client = Mock()
        node = {"type": "table", "name": "test_table"}  # Missing id
        
        result = check_node_anomalies(mock_client, node, depth=1)
        
        assert result == []
    
    def test_check_node_anomalies_missing_node_type(self):
        """Test handling node with missing type."""
        mock_client = Mock()
        node = {"id": "table-id", "name": "test_table"}  # Missing type
        
        result = check_node_anomalies(mock_client, node, depth=1)
        
        assert result == []
    
    def test_check_node_anomalies_unsupported_type(self):
        """Test handling node with unsupported type."""
        mock_client = Mock()
        node = {
            "id": "dashboard-id",
            "type": "dashboard",  # Not supported yet
            "name": "test_dashboard",
            "fullyQualifiedName": "tableau.test_dashboard"
        }
        
        result = check_node_anomalies(mock_client, node, depth=1)
        
        assert result == []
    
    def test_check_node_anomalies_api_error_continues(self):
        """Test that API errors don't crash traversal."""
        mock_client = Mock()
        mock_client.get_table_by_fqn.side_effect = Exception("API Error")
        
        node = {
            "id": "table-id",
            "type": "table",
            "name": "test_table",
            "fullyQualifiedName": "db.schema.test_table"
        }
        
        # Should not raise exception
        result = check_node_anomalies(mock_client, node, depth=1)
        
        assert result == []


class TestCheckEdgeAnomalies:
    """Tests for check_edge_anomalies function."""
    
    @patch('src.core.root_cause.detect_pipeline_failure')
    def test_check_edge_anomalies_pipeline_failure(self, mock_detect):
        """Test detecting pipeline failure on edge."""
        mock_client = Mock()
        mock_client.get_pipeline_by_fqn.return_value = {"id": "pipeline-id"}
        mock_detect.return_value = AnomalyType.PIPELINE_FAILURE
        
        edge = {
            "fromEntity": "source",
            "toEntity": "target",
            "lineageDetails": {
                "pipeline": {
                    "id": "pipeline-id",
                    "type": "pipeline",
                    "name": "etl_dag",
                    "fullyQualifiedName": "airflow.etl_dag"
                }
            }
        }
        
        result = check_edge_anomalies(mock_client, edge, depth=1)
        
        assert len(result) == 1
        assert result[0].type == AnomalyType.PIPELINE_FAILURE
        assert result[0].name == "etl_dag"
        assert result[0].depth == 1
    
    @patch('src.core.root_cause.detect_pipeline_failure')
    def test_check_edge_anomalies_no_failure(self, mock_detect):
        """Test edge with pipeline but no failure."""
        mock_client = Mock()
        mock_client.get_pipeline_by_fqn.return_value = {"id": "pipeline-id"}
        mock_detect.return_value = None  # No failure
        
        edge = {
            "fromEntity": "source",
            "toEntity": "target",
            "lineageDetails": {
                "pipeline": {
                    "id": "pipeline-id",
                    "fullyQualifiedName": "airflow.etl_dag"
                }
            }
        }
        
        result = check_edge_anomalies(mock_client, edge, depth=1)
        
        assert result == []
    
    def test_check_edge_anomalies_no_pipeline(self):
        """Test edge without pipeline."""
        mock_client = Mock()
        edge = {
            "fromEntity": "source",
            "toEntity": "target"
        }
        
        result = check_edge_anomalies(mock_client, edge, depth=1)
        
        assert result == []
    
    def test_check_edge_anomalies_no_lineage_details(self):
        """Test edge without lineageDetails."""
        mock_client = Mock()
        edge = {
            "fromEntity": "source",
            "toEntity": "target",
            "lineageDetails": {}
        }
        
        result = check_edge_anomalies(mock_client, edge, depth=1)
        
        assert result == []
    
    def test_check_edge_anomalies_api_error_continues(self):
        """Test that API errors don't crash traversal."""
        mock_client = Mock()
        mock_client.get_pipeline_by_fqn.side_effect = Exception("API Error")
        
        edge = {
            "fromEntity": "source",
            "toEntity": "target",
            "lineageDetails": {
                "pipeline": {
                    "id": "pipeline-id",
                    "fullyQualifiedName": "airflow.etl_dag"
                }
            }
        }
        
        # Should not raise exception
        result = check_edge_anomalies(mock_client, edge, depth=1)
        
        assert result == []


class TestFindRootCauseByFqn:
    """Tests for find_root_cause_by_fqn convenience function."""
    
    @patch('src.core.root_cause.find_root_cause')
    def test_find_root_cause_by_fqn_table(self, mock_find):
        """Test finding root cause by FQN for table."""
        mock_client = Mock()
        mock_client.get_table_by_fqn.return_value = {
            "id": "table-id",
            "name": "test_table"
        }
        mock_find.return_value = {
            "primary_root_cause": None,
            "contributing_factors": []
        }
        
        result = find_root_cause_by_fqn(
            metadata_client=mock_client,
            target_fqn="db.schema.test_table",
            target_entity_type="table"
        )
        
        mock_client.get_table_by_fqn.assert_called_once_with("db.schema.test_table")
        mock_find.assert_called_once()
        assert result["primary_root_cause"] is None
    
    @patch('src.core.root_cause.find_root_cause')
    def test_find_root_cause_by_fqn_pipeline(self, mock_find):
        """Test finding root cause by FQN for pipeline."""
        mock_client = Mock()
        mock_client.get_pipeline_by_fqn.return_value = {
            "id": "pipeline-id",
            "name": "etl_dag"
        }
        mock_find.return_value = {
            "primary_root_cause": None,
            "contributing_factors": []
        }
        
        result = find_root_cause_by_fqn(
            metadata_client=mock_client,
            target_fqn="airflow.etl_dag",
            target_entity_type="pipeline"
        )
        
        mock_client.get_pipeline_by_fqn.assert_called_once_with("airflow.etl_dag")
        mock_find.assert_called_once()
    
    def test_find_root_cause_by_fqn_unsupported_type(self):
        """Test error handling for unsupported entity type."""
        mock_client = Mock()
        
        with pytest.raises(ValueError, match="Unsupported entity type"):
            find_root_cause_by_fqn(
                metadata_client=mock_client,
                target_fqn="dashboard.test",
                target_entity_type="dashboard"
            )
    
    def test_find_root_cause_by_fqn_missing_id(self):
        """Test error handling when entity has no id."""
        mock_client = Mock()
        mock_client.get_table_by_fqn.return_value = {"name": "test_table"}  # No id
        
        with pytest.raises(ValueError, match="Could not extract ID"):
            find_root_cause_by_fqn(
                metadata_client=mock_client,
                target_fqn="db.schema.test_table",
                target_entity_type="table"
            )
