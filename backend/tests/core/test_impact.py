"""Tests for impact analysis engine (downstream blast radius calculation).

Tests the downstream BFS traversal algorithm that calculates which assets
are affected by an upstream failure.

Phase 3 Testing.
"""
import pytest
from unittest.mock import Mock, MagicMock

from src.core.impact import (
    compute_blast_radius,
    categorize_impacted_assets,
    compute_blast_radius_by_fqn,
)
from src.schemas import ImpactedAssets


class TestComputeBlastRadius:
    """Tests for compute_blast_radius function."""
    
    def test_single_downstream_table(self):
        """Test impact calculation with one downstream table."""
        # Mock client
        client = Mock()
        
        # Mock lineage response: root -> table1
        lineage_graph = {
            "entity": {"id": "root-id", "type": "table", "name": "root_table"},
            "nodes": [
                {"id": "table1-id", "type": "table", "name": "downstream_table"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "root-id", "toEntity": "table1-id"}
            ]
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius(client, "root-id", "table", downstream_depth=5)
        
        # Verify
        assert isinstance(result, ImpactedAssets)
        assert len(result.tables) == 1
        assert result.tables[0]["name"] == "downstream_table"
        assert len(result.dashboards) == 0
        assert len(result.ml_models) == 0
        assert result.total_impact_count == 1
    
    def test_multiple_downstream_types(self):
        """Test impact with tables, dashboards, and ML models."""
        client = Mock()
        
        # Mock lineage: root -> table1, dashboard1, mlmodel1
        lineage_graph = {
            "entity": {"id": "root-id", "type": "table", "name": "root_table"},
            "nodes": [
                {"id": "table1-id", "type": "table", "name": "fact_sales"},
                {"id": "dash1-id", "type": "dashboard", "name": "executive_dashboard"},
                {"id": "ml1-id", "type": "mlmodel", "name": "churn_predictor"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "root-id", "toEntity": "table1-id"},
                {"fromEntity": "root-id", "toEntity": "dash1-id"},
                {"fromEntity": "root-id", "toEntity": "ml1-id"}
            ]
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius(client, "root-id", "table")
        
        # Verify all types are captured
        assert len(result.tables) == 1
        assert result.tables[0]["name"] == "fact_sales"
        assert len(result.dashboards) == 1
        assert result.dashboards[0]["name"] == "executive_dashboard"
        assert len(result.ml_models) == 1
        assert result.ml_models[0]["name"] == "churn_predictor"
        assert result.total_impact_count == 3
    
    def test_multi_hop_downstream_traversal(self):
        """Test BFS traversal across multiple hops downstream."""
        client = Mock()
        
        # Mock lineage: root -> table1 -> table2 -> dashboard1
        lineage_graph = {
            "entity": {"id": "root-id", "type": "table", "name": "root_table"},
            "nodes": [
                {"id": "table1-id", "type": "table", "name": "intermediate_table"},
                {"id": "table2-id", "type": "table", "name": "final_table"},
                {"id": "dash1-id", "type": "dashboard", "name": "report"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "root-id", "toEntity": "table1-id"},
                {"fromEntity": "table1-id", "toEntity": "table2-id"},
                {"fromEntity": "table2-id", "toEntity": "dash1-id"}
            ]
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius(client, "root-id", "table")
        
        # Verify all downstream nodes are found
        assert len(result.tables) == 2
        table_names = [t["name"] for t in result.tables]
        assert "intermediate_table" in table_names
        assert "final_table" in table_names
        assert len(result.dashboards) == 1
        assert result.dashboards[0]["name"] == "report"
        assert result.total_impact_count == 3
    
    def test_branching_downstream_graph(self):
        """Test impact with branching downstream paths."""
        client = Mock()
        
        # Mock lineage: root branches to table1 and table2, both feed dashboard
        #   root
        #   ├── table1 ──┐
        #   └── table2 ──┴─> dashboard
        lineage_graph = {
            "entity": {"id": "root-id", "type": "table", "name": "root_table"},
            "nodes": [
                {"id": "table1-id", "type": "table", "name": "branch1"},
                {"id": "table2-id", "type": "table", "name": "branch2"},
                {"id": "dash1-id", "type": "dashboard", "name": "combined_report"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "root-id", "toEntity": "table1-id"},
                {"fromEntity": "root-id", "toEntity": "table2-id"},
                {"fromEntity": "table1-id", "toEntity": "dash1-id"},
                {"fromEntity": "table2-id", "toEntity": "dash1-id"}
            ]
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius(client, "root-id", "table")
        
        # Verify all branches are captured
        assert len(result.tables) == 2
        assert len(result.dashboards) == 1
        assert result.total_impact_count == 3
    
    def test_no_downstream_impact(self):
        """Test when there are no downstream dependencies."""
        client = Mock()
        
        # Mock lineage: root has no downstream edges
        lineage_graph = {
            "entity": {"id": "root-id", "type": "table", "name": "isolated_table"},
            "nodes": [],
            "upstreamEdges": [],
            "downstreamEdges": []
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius(client, "root-id", "table")
        
        # Verify no impact
        assert len(result.tables) == 0
        assert len(result.dashboards) == 0
        assert len(result.ml_models) == 0
        assert result.total_impact_count == 0
    
    def test_pipeline_nodes_excluded_from_impact(self):
        """Test that pipeline nodes are not included in impact count."""
        client = Mock()
        
        # Mock lineage: root -> pipeline -> table
        # Pipelines are transformation logic, not consumption assets
        lineage_graph = {
            "entity": {"id": "root-id", "type": "table", "name": "root_table"},
            "nodes": [
                {"id": "pipe1-id", "type": "pipeline", "name": "etl_pipeline"},
                {"id": "table1-id", "type": "table", "name": "target_table"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "root-id", "toEntity": "pipe1-id"},
                {"fromEntity": "pipe1-id", "toEntity": "table1-id"}
            ]
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius(client, "root-id", "table")
        
        # Verify pipeline is traversed but not counted in impact
        assert len(result.tables) == 1
        assert result.tables[0]["name"] == "target_table"
        assert result.total_impact_count == 1
    
    def test_cyclic_graph_handling(self):
        """Test that cycles in downstream graph don't cause infinite loops."""
        client = Mock()
        
        # Mock lineage with cycle: root -> table1 -> table2 -> table1
        lineage_graph = {
            "entity": {"id": "root-id", "type": "table", "name": "root_table"},
            "nodes": [
                {"id": "table1-id", "type": "table", "name": "table1"},
                {"id": "table2-id", "type": "table", "name": "table2"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "root-id", "toEntity": "table1-id"},
                {"fromEntity": "table1-id", "toEntity": "table2-id"},
                {"fromEntity": "table2-id", "toEntity": "table1-id"}  # Cycle
            ]
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute - should not hang
        result = compute_blast_radius(client, "root-id", "table")
        
        # Verify both tables found exactly once
        assert len(result.tables) == 2
        assert result.total_impact_count == 2
    
    def test_root_excluded_from_impact(self):
        """Test that the root entity itself is not included in impact."""
        client = Mock()
        
        # Mock lineage where root appears in nodes
        lineage_graph = {
            "entity": {"id": "root-id", "type": "table", "name": "root_table"},
            "nodes": [
                {"id": "root-id", "type": "table", "name": "root_table"},
                {"id": "table1-id", "type": "table", "name": "downstream_table"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "root-id", "toEntity": "table1-id"}
            ]
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius(client, "root-id", "table")
        
        # Verify root is not in impact
        assert len(result.tables) == 1
        assert result.tables[0]["name"] == "downstream_table"
        assert result.total_impact_count == 1
    
    def test_custom_downstream_depth(self):
        """Test that downstream_depth parameter is passed correctly."""
        client = Mock()
        
        lineage_graph = {
            "entity": {"id": "root-id", "type": "table", "name": "root_table"},
            "nodes": [],
            "upstreamEdges": [],
            "downstreamEdges": []
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute with custom depth
        compute_blast_radius(client, "root-id", "table", downstream_depth=10)
        
        # Verify API called with correct parameters
        client.get_lineage.assert_called_once()
        call_args = client.get_lineage.call_args
        assert call_args[1]["upstream_depth"] == 0  # No upstream needed
        assert call_args[1]["downstream_depth"] == 10


class TestCategorizeImpactedAssets:
    """Tests for categorize_impacted_assets function."""
    
    def test_categorize_tables_only(self):
        """Test categorization with only table nodes."""
        nodes_map = {
            "table1-id": {"id": "table1-id", "type": "table", "name": "table1"},
            "table2-id": {"id": "table2-id", "type": "table", "name": "table2"}
        }
        visited_ids = {"table1-id", "table2-id"}
        
        result = categorize_impacted_assets(nodes_map, visited_ids)
        
        assert len(result.tables) == 2
        assert len(result.dashboards) == 0
        assert len(result.ml_models) == 0
        assert result.total_impact_count == 2
    
    def test_categorize_dashboards_only(self):
        """Test categorization with only dashboard nodes."""
        nodes_map = {
            "dash1-id": {"id": "dash1-id", "type": "dashboard", "name": "dashboard1"},
            "dash2-id": {"id": "dash2-id", "type": "dashboard", "name": "dashboard2"}
        }
        visited_ids = {"dash1-id", "dash2-id"}
        
        result = categorize_impacted_assets(nodes_map, visited_ids)
        
        assert len(result.tables) == 0
        assert len(result.dashboards) == 2
        assert len(result.ml_models) == 0
        assert result.total_impact_count == 2
    
    def test_categorize_ml_models_only(self):
        """Test categorization with only ML model nodes."""
        nodes_map = {
            "ml1-id": {"id": "ml1-id", "type": "mlmodel", "name": "model1"},
            "ml2-id": {"id": "ml2-id", "type": "mlmodel", "name": "model2"}
        }
        visited_ids = {"ml1-id", "ml2-id"}
        
        result = categorize_impacted_assets(nodes_map, visited_ids)
        
        assert len(result.tables) == 0
        assert len(result.dashboards) == 0
        assert len(result.ml_models) == 2
        assert result.total_impact_count == 2
    
    def test_categorize_mixed_types(self):
        """Test categorization with mixed asset types."""
        nodes_map = {
            "table1-id": {"id": "table1-id", "type": "table", "name": "table1"},
            "dash1-id": {"id": "dash1-id", "type": "dashboard", "name": "dashboard1"},
            "ml1-id": {"id": "ml1-id", "type": "mlmodel", "name": "model1"},
            "table2-id": {"id": "table2-id", "type": "table", "name": "table2"}
        }
        visited_ids = {"table1-id", "dash1-id", "ml1-id", "table2-id"}
        
        result = categorize_impacted_assets(nodes_map, visited_ids)
        
        assert len(result.tables) == 2
        assert len(result.dashboards) == 1
        assert len(result.ml_models) == 1
        assert result.total_impact_count == 4
    
    def test_categorize_ignores_pipelines(self):
        """Test that pipeline nodes are not categorized as impact."""
        nodes_map = {
            "table1-id": {"id": "table1-id", "type": "table", "name": "table1"},
            "pipe1-id": {"id": "pipe1-id", "type": "pipeline", "name": "pipeline1"},
            "dash1-id": {"id": "dash1-id", "type": "dashboard", "name": "dashboard1"}
        }
        visited_ids = {"table1-id", "pipe1-id", "dash1-id"}
        
        result = categorize_impacted_assets(nodes_map, visited_ids)
        
        # Pipeline should not be counted
        assert len(result.tables) == 1
        assert len(result.dashboards) == 1
        assert result.total_impact_count == 2
    
    def test_categorize_ignores_topics(self):
        """Test that topic nodes are not categorized as impact."""
        nodes_map = {
            "table1-id": {"id": "table1-id", "type": "table", "name": "table1"},
            "topic1-id": {"id": "topic1-id", "type": "topic", "name": "kafka_topic"}
        }
        visited_ids = {"table1-id", "topic1-id"}
        
        result = categorize_impacted_assets(nodes_map, visited_ids)
        
        # Topic should not be counted
        assert len(result.tables) == 1
        assert result.total_impact_count == 1
    
    def test_categorize_empty_visited(self):
        """Test categorization with no visited nodes."""
        nodes_map = {
            "table1-id": {"id": "table1-id", "type": "table", "name": "table1"}
        }
        visited_ids = set()
        
        result = categorize_impacted_assets(nodes_map, visited_ids)
        
        assert len(result.tables) == 0
        assert len(result.dashboards) == 0
        assert len(result.ml_models) == 0
        assert result.total_impact_count == 0
    
    def test_categorize_missing_nodes(self):
        """Test categorization when visited IDs are not in nodes_map."""
        nodes_map = {
            "table1-id": {"id": "table1-id", "type": "table", "name": "table1"}
        }
        visited_ids = {"table1-id", "missing-id", "another-missing-id"}
        
        result = categorize_impacted_assets(nodes_map, visited_ids)
        
        # Should only categorize nodes that exist in map
        assert len(result.tables) == 1
        assert result.total_impact_count == 1


class TestComputeBlastRadiusByFqn:
    """Tests for compute_blast_radius_by_fqn convenience function."""
    
    def test_compute_by_fqn_table(self):
        """Test computing blast radius using table FQN."""
        client = Mock()
        
        # Mock get_table_by_fqn
        client.get_table_by_fqn.return_value = {
            "id": "table-uuid",
            "name": "dim_customer",
            "fullyQualifiedName": "snowflake.analytics.dim_customer"
        }
        
        # Mock lineage
        lineage_graph = {
            "entity": {"id": "table-uuid", "type": "table", "name": "dim_customer"},
            "nodes": [
                {"id": "dash1-id", "type": "dashboard", "name": "customer_report"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "table-uuid", "toEntity": "dash1-id"}
            ]
        }
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius_by_fqn(
            client,
            "snowflake.analytics.dim_customer",
            "table"
        )
        
        # Verify
        assert len(result.dashboards) == 1
        assert result.total_impact_count == 1
        client.get_table_by_fqn.assert_called_once_with("snowflake.analytics.dim_customer")
    
    def test_compute_by_fqn_pipeline(self):
        """Test computing blast radius using pipeline FQN."""
        client = Mock()
        
        # Mock get_pipeline_by_fqn
        client.get_pipeline_by_fqn.return_value = {
            "id": "pipeline-uuid",
            "name": "customer_etl",
            "fullyQualifiedName": "airflow.customer_etl"
        }
        
        # Mock lineage
        lineage_graph = {
            "entity": {"id": "pipeline-uuid", "type": "pipeline", "name": "customer_etl"},
            "nodes": [
                {"id": "table1-id", "type": "table", "name": "target_table"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "pipeline-uuid", "toEntity": "table1-id"}
            ]
        }
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius_by_fqn(
            client,
            "airflow.customer_etl",
            "pipeline"
        )
        
        # Verify
        assert len(result.tables) == 1
        assert result.total_impact_count == 1
        client.get_pipeline_by_fqn.assert_called_once_with("airflow.customer_etl")
    
    def test_compute_by_fqn_unsupported_type(self):
        """Test error handling for unsupported entity types."""
        client = Mock()
        
        with pytest.raises(ValueError, match="Unsupported entity type"):
            compute_blast_radius_by_fqn(client, "some.fqn", "unsupported_type")
    
    def test_compute_by_fqn_missing_id(self):
        """Test error handling when entity has no ID."""
        client = Mock()
        
        # Mock entity without ID
        client.get_table_by_fqn.return_value = {
            "name": "dim_customer",
            "fullyQualifiedName": "snowflake.analytics.dim_customer"
            # Missing "id" field
        }
        
        with pytest.raises(ValueError, match="Could not extract ID"):
            compute_blast_radius_by_fqn(client, "snowflake.analytics.dim_customer", "table")
    
    def test_compute_by_fqn_custom_depth(self):
        """Test that custom downstream depth is passed through."""
        client = Mock()
        
        client.get_table_by_fqn.return_value = {"id": "table-uuid"}
        client.get_lineage.return_value = {
            "entity": {"id": "table-uuid"},
            "nodes": [],
            "upstreamEdges": [],
            "downstreamEdges": []
        }
        
        # Execute with custom depth
        compute_blast_radius_by_fqn(
            client,
            "snowflake.analytics.dim_customer",
            "table",
            downstream_depth=8
        )
        
        # Verify depth parameter passed to get_lineage
        call_args = client.get_lineage.call_args
        assert call_args[1]["downstream_depth"] == 8


class TestIntegrationScenarios:
    """Integration tests with realistic scenarios."""
    
    def test_executive_dashboard_impact(self):
        """Test realistic scenario: failed table impacts executive dashboard."""
        client = Mock()
        
        # Scenario: raw_orders fails -> dim_orders -> fact_sales -> executive_dashboard
        lineage_graph = {
            "entity": {"id": "raw-orders-id", "type": "table", "name": "raw_orders"},
            "nodes": [
                {"id": "dim-orders-id", "type": "table", "name": "dim_orders"},
                {"id": "fact-sales-id", "type": "table", "name": "fact_sales"},
                {"id": "exec-dash-id", "type": "dashboard", "name": "executive_dashboard"},
                {"id": "marketing-dash-id", "type": "dashboard", "name": "marketing_dashboard"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "raw-orders-id", "toEntity": "dim-orders-id"},
                {"fromEntity": "dim-orders-id", "toEntity": "fact-sales-id"},
                {"fromEntity": "fact-sales-id", "toEntity": "exec-dash-id"},
                {"fromEntity": "fact-sales-id", "toEntity": "marketing-dash-id"}
            ]
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius(client, "raw-orders-id", "table")
        
        # Verify complete impact chain
        assert len(result.tables) == 2  # dim_orders, fact_sales
        assert len(result.dashboards) == 2  # executive, marketing
        assert result.total_impact_count == 4
        
        # Verify specific assets
        table_names = [t["name"] for t in result.tables]
        assert "dim_orders" in table_names
        assert "fact_sales" in table_names
        
        dashboard_names = [d["name"] for d in result.dashboards]
        assert "executive_dashboard" in dashboard_names
        assert "marketing_dashboard" in dashboard_names
    
    def test_ml_model_impact_chain(self):
        """Test realistic scenario: data quality issue impacts ML model."""
        client = Mock()
        
        # Scenario: customer_features -> ml_training_table -> churn_model
        lineage_graph = {
            "entity": {"id": "features-id", "type": "table", "name": "customer_features"},
            "nodes": [
                {"id": "training-id", "type": "table", "name": "ml_training_table"},
                {"id": "model-id", "type": "mlmodel", "name": "churn_prediction_model"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "features-id", "toEntity": "training-id"},
                {"fromEntity": "training-id", "toEntity": "model-id"}
            ]
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius(client, "features-id", "table")
        
        # Verify ML model is impacted
        assert len(result.tables) == 1
        assert len(result.ml_models) == 1
        assert result.ml_models[0]["name"] == "churn_prediction_model"
        assert result.total_impact_count == 2
    
    def test_complex_branching_impact(self):
        """Test complex scenario with multiple branches and convergence."""
        client = Mock()
        
        # Scenario: root branches to A and B, both converge to C, C feeds dashboard
        #   root
        #   ├── A ──┐
        #   └── B ──┴─> C -> dashboard
        lineage_graph = {
            "entity": {"id": "root-id", "type": "table", "name": "root"},
            "nodes": [
                {"id": "a-id", "type": "table", "name": "table_a"},
                {"id": "b-id", "type": "table", "name": "table_b"},
                {"id": "c-id", "type": "table", "name": "table_c"},
                {"id": "dash-id", "type": "dashboard", "name": "combined_report"}
            ],
            "upstreamEdges": [],
            "downstreamEdges": [
                {"fromEntity": "root-id", "toEntity": "a-id"},
                {"fromEntity": "root-id", "toEntity": "b-id"},
                {"fromEntity": "a-id", "toEntity": "c-id"},
                {"fromEntity": "b-id", "toEntity": "c-id"},
                {"fromEntity": "c-id", "toEntity": "dash-id"}
            ]
        }
        
        client.get_lineage.return_value = lineage_graph
        
        # Execute
        result = compute_blast_radius(client, "root-id", "table")
        
        # Verify all downstream assets found
        assert len(result.tables) == 3  # A, B, C
        assert len(result.dashboards) == 1
        assert result.total_impact_count == 4
