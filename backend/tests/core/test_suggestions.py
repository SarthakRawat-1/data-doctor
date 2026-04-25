"""Tests for suggestion engine.

Tests the rule-based fix mapping system that generates remediation suggestions.

Phase 4 Testing.
"""
import pytest

from src.constants import AnomalyType, FixAction
from src.core.suggestions import (
    generate_suggested_fixes,
    _create_fix_for_anomaly,
    FIX_MAPPING,
)
from src.schemas import AnomalyDetail, SuggestedFix


class TestGenerateSuggestedFixes:
    """Tests for generate_suggested_fixes function."""
    
    def test_generate_fix_for_pipeline_failure(self):
        """Test generating fix for pipeline failure."""
        primary_cause = AnomalyDetail(
            type=AnomalyType.PIPELINE_FAILURE,
            name="customer_etl",
            depth=1,
            entity_id="pipeline-123",
            description="Pipeline failed"
        )
        
        fixes = generate_suggested_fixes(primary_cause, [])
        
        assert len(fixes) == 1
        assert fixes[0].action == FixAction.RERUN_PIPELINE
        assert fixes[0].target == "customer_etl"
        assert "Rerun the failed customer_etl pipeline" in fixes[0].description
    
    def test_generate_fix_for_schema_change(self):
        """Test generating fix for schema change."""
        primary_cause = AnomalyDetail(
            type=AnomalyType.SCHEMA_CHANGE,
            name="dim_customer",
            depth=1,
            entity_id="table-123"
        )
        
        fixes = generate_suggested_fixes(primary_cause, [])
        
        assert len(fixes) == 1
        assert fixes[0].action == FixAction.UPDATE_SCHEMA
        assert fixes[0].target == "dim_customer"
        assert "Remove the deleted column reference" in fixes[0].description
    
    def test_generate_fix_for_stale_data(self):
        """Test generating fix for stale data."""
        primary_cause = AnomalyDetail(
            type=AnomalyType.STALE_DATA,
            name="orders_raw",
            depth=2,
            entity_id="table-456"
        )
        
        fixes = generate_suggested_fixes(primary_cause, [])
        
        assert len(fixes) == 1
        assert fixes[0].action == FixAction.FORCE_BACKFILL
        assert fixes[0].target == "orders_raw"
        assert "Trigger a historical backfill" in fixes[0].description
    
    def test_generate_fix_for_data_quality_failure(self):
        """Test generating fix for data quality failure."""
        primary_cause = AnomalyDetail(
            type=AnomalyType.DATA_QUALITY_FAILURE,
            name="customer_table",
            depth=1,
            entity_id="table-789"
        )
        
        fixes = generate_suggested_fixes(primary_cause, [])
        
        assert len(fixes) == 1
        assert fixes[0].action == FixAction.QUARANTINE_DATA
        assert fixes[0].target == "customer_table"
        assert "Implement data contract validation" in fixes[0].description
    
    def test_generate_fixes_with_contributing_factors(self):
        """Test generating fixes for primary cause and contributing factors."""
        primary_cause = AnomalyDetail(
            type=AnomalyType.PIPELINE_FAILURE,
            name="etl_pipeline",
            depth=1,
            entity_id="pipeline-1"
        )
        
        contributing_factors = [
            AnomalyDetail(
                type=AnomalyType.DATA_QUALITY_FAILURE,
                name="source_table",
                depth=2,
                entity_id="table-1"
            ),
            AnomalyDetail(
                type=AnomalyType.STALE_DATA,
                name="upstream_table",
                depth=3,
                entity_id="table-2"
            )
        ]
        
        fixes = generate_suggested_fixes(primary_cause, contributing_factors)
        
        # Should have 3 fixes: 1 primary + 2 contributing
        assert len(fixes) == 3
        assert fixes[0].action == FixAction.RERUN_PIPELINE
        assert fixes[1].action == FixAction.QUARANTINE_DATA
        assert fixes[2].action == FixAction.FORCE_BACKFILL
    
    def test_generate_fixes_no_primary_cause(self):
        """Test that fallback fix is generated when no primary cause."""
        fixes = generate_suggested_fixes(None, [])
        
        # Should return fallback fix
        assert len(fixes) == 1
        assert fixes[0].action == FixAction.RERUN_PIPELINE
        assert fixes[0].target == "unknown"
        assert "Manual investigation required" in fixes[0].description
    
    def test_generate_fixes_only_contributing_factors(self):
        """Test generating fixes when only contributing factors exist."""
        contributing_factors = [
            AnomalyDetail(
                type=AnomalyType.STALE_DATA,
                name="table_a",
                depth=2,
                entity_id="table-a"
            ),
            AnomalyDetail(
                type=AnomalyType.SCHEMA_CHANGE,
                name="table_b",
                depth=3,
                entity_id="table-b"
            )
        ]
        
        fixes = generate_suggested_fixes(None, contributing_factors)
        
        # Should have 2 fixes from contributing factors (no fallback since we have fixes)
        assert len(fixes) == 2
        assert fixes[0].action == FixAction.FORCE_BACKFILL
        assert fixes[1].action == FixAction.UPDATE_SCHEMA
    
    def test_generate_fixes_always_non_empty(self):
        """Test that fixes list is ALWAYS non-empty (spec requirement)."""
        # No causes at all
        fixes = generate_suggested_fixes(None, [])
        assert len(fixes) > 0
        
        # Unsupported anomaly type
        unsupported = AnomalyDetail(
            type=AnomalyType.VOLUME_ANOMALY,  # Not in FIX_MAPPING
            name="test_table",
            depth=1,
            entity_id="table-1"
        )
        fixes = generate_suggested_fixes(unsupported, [])
        assert len(fixes) > 0
    
    def test_sql_script_and_markdown_none_in_phase_4(self):
        """Test that SQL and Markdown fields are None (Phase 5 feature)."""
        primary_cause = AnomalyDetail(
            type=AnomalyType.PIPELINE_FAILURE,
            name="test_pipeline",
            depth=1,
            entity_id="pipeline-1"
        )
        
        fixes = generate_suggested_fixes(primary_cause, [])
        
        assert fixes[0].sql_script is None
        assert fixes[0].markdown_details is None


class TestCreateFixForAnomaly:
    """Tests for _create_fix_for_anomaly helper function."""
    
    def test_create_fix_pipeline_failure(self):
        """Test creating fix for pipeline failure."""
        anomaly = AnomalyDetail(
            type=AnomalyType.PIPELINE_FAILURE,
            name="orders_dag",
            depth=1,
            entity_id="pipeline-1"
        )
        
        fix = _create_fix_for_anomaly(anomaly)
        
        assert fix is not None
        assert isinstance(fix, SuggestedFix)
        assert fix.action == FixAction.RERUN_PIPELINE
        assert fix.target == "orders_dag"
        assert "orders_dag" in fix.description
    
    def test_create_fix_schema_change(self):
        """Test creating fix for schema change."""
        anomaly = AnomalyDetail(
            type=AnomalyType.SCHEMA_CHANGE,
            name="customer_view",
            depth=1,
            entity_id="table-1"
        )
        
        fix = _create_fix_for_anomaly(anomaly)
        
        assert fix is not None
        assert fix.action == FixAction.UPDATE_SCHEMA
        assert fix.target == "customer_view"
    
    def test_create_fix_stale_data(self):
        """Test creating fix for stale data."""
        anomaly = AnomalyDetail(
            type=AnomalyType.STALE_DATA,
            name="events_table",
            depth=2,
            entity_id="table-2"
        )
        
        fix = _create_fix_for_anomaly(anomaly)
        
        assert fix is not None
        assert fix.action == FixAction.FORCE_BACKFILL
        assert fix.target == "events_table"
    
    def test_create_fix_data_quality(self):
        """Test creating fix for data quality failure."""
        anomaly = AnomalyDetail(
            type=AnomalyType.DATA_QUALITY_FAILURE,
            name="transactions",
            depth=1,
            entity_id="table-3"
        )
        
        fix = _create_fix_for_anomaly(anomaly)
        
        assert fix is not None
        assert fix.action == FixAction.QUARANTINE_DATA
        assert fix.target == "transactions"
    
    def test_create_fix_unsupported_type_returns_none(self):
        """Test that unsupported anomaly types return None."""
        anomaly = AnomalyDetail(
            type=AnomalyType.VOLUME_ANOMALY,  # Not in FIX_MAPPING
            name="test_table",
            depth=1,
            entity_id="table-1"
        )
        
        fix = _create_fix_for_anomaly(anomaly)
        
        assert fix is None
    
    def test_create_fix_distribution_drift_returns_none(self):
        """Test that distribution drift returns None (no mapping)."""
        anomaly = AnomalyDetail(
            type=AnomalyType.DISTRIBUTION_DRIFT,
            name="test_table",
            depth=1,
            entity_id="table-1"
        )
        
        fix = _create_fix_for_anomaly(anomaly)
        
        assert fix is None


class TestFixMapping:
    """Tests for FIX_MAPPING configuration."""
    
    def test_fix_mapping_has_required_types(self):
        """Test that FIX_MAPPING covers all critical anomaly types."""
        required_types = [
            AnomalyType.PIPELINE_FAILURE,
            AnomalyType.SCHEMA_CHANGE,
            AnomalyType.STALE_DATA,
            AnomalyType.DATA_QUALITY_FAILURE
        ]
        
        for anomaly_type in required_types:
            assert anomaly_type in FIX_MAPPING, f"{anomaly_type} missing from FIX_MAPPING"
    
    def test_fix_mapping_structure(self):
        """Test that each mapping has required fields."""
        for anomaly_type, mapping in FIX_MAPPING.items():
            assert "action" in mapping
            assert "description_template" in mapping
            assert isinstance(mapping["action"], FixAction)
            assert isinstance(mapping["description_template"], str)
            assert "{target}" in mapping["description_template"]
    
    def test_fix_mapping_actions_are_valid(self):
        """Test that all actions in mapping are valid FixAction enums."""
        for mapping in FIX_MAPPING.values():
            action = mapping["action"]
            assert action in [
                FixAction.RERUN_PIPELINE,
                FixAction.UPDATE_SCHEMA,
                FixAction.FORCE_BACKFILL,
                FixAction.QUARANTINE_DATA
            ]


class TestIntegrationScenarios:
    """Integration tests with realistic scenarios."""
    
    def test_executive_dashboard_failure_scenario(self):
        """Test realistic scenario: pipeline failure impacts dashboard."""
        primary_cause = AnomalyDetail(
            type=AnomalyType.PIPELINE_FAILURE,
            name="customer_etl_dag",
            depth=1,
            entity_id="pipeline-123",
            description="Airflow DAG failed at task transform_customers"
        )
        
        contributing_factors = [
            AnomalyDetail(
                type=AnomalyType.DATA_QUALITY_FAILURE,
                name="raw_customers",
                depth=2,
                entity_id="table-456",
                description="Null constraint violation on customer_id"
            )
        ]
        
        fixes = generate_suggested_fixes(primary_cause, contributing_factors)
        
        assert len(fixes) == 2
        
        # Primary fix: Rerun pipeline
        assert fixes[0].action == FixAction.RERUN_PIPELINE
        assert fixes[0].target == "customer_etl_dag"
        
        # Contributing fix: Quarantine data
        assert fixes[1].action == FixAction.QUARANTINE_DATA
        assert fixes[1].target == "raw_customers"
    
    def test_schema_migration_scenario(self):
        """Test realistic scenario: schema change breaks downstream."""
        primary_cause = AnomalyDetail(
            type=AnomalyType.SCHEMA_CHANGE,
            name="dim_product",
            depth=1,
            entity_id="table-789",
            description="Column 'product_category' was deleted"
        )
        
        fixes = generate_suggested_fixes(primary_cause, [])
        
        assert len(fixes) == 1
        assert fixes[0].action == FixAction.UPDATE_SCHEMA
        assert "dim_product" in fixes[0].description
    
    def test_data_freshness_issue_scenario(self):
        """Test realistic scenario: stale data detected."""
        primary_cause = AnomalyDetail(
            type=AnomalyType.STALE_DATA,
            name="fact_sales",
            depth=1,
            entity_id="table-101",
            description="Last updated 72 hours ago, SLA is 24 hours"
        )
        
        fixes = generate_suggested_fixes(primary_cause, [])
        
        assert len(fixes) == 1
        assert fixes[0].action == FixAction.FORCE_BACKFILL
        assert "fact_sales" in fixes[0].description
    
    def test_multiple_upstream_failures_scenario(self):
        """Test realistic scenario: multiple upstream issues."""
        primary_cause = AnomalyDetail(
            type=AnomalyType.PIPELINE_FAILURE,
            name="aggregation_pipeline",
            depth=1,
            entity_id="pipeline-1"
        )
        
        contributing_factors = [
            AnomalyDetail(
                type=AnomalyType.STALE_DATA,
                name="source_table_a",
                depth=2,
                entity_id="table-1"
            ),
            AnomalyDetail(
                type=AnomalyType.SCHEMA_CHANGE,
                name="source_table_b",
                depth=2,
                entity_id="table-2"
            ),
            AnomalyDetail(
                type=AnomalyType.DATA_QUALITY_FAILURE,
                name="source_table_c",
                depth=3,
                entity_id="table-3"
            )
        ]
        
        fixes = generate_suggested_fixes(primary_cause, contributing_factors)
        
        # Should have 4 fixes total
        assert len(fixes) == 4
        
        # Verify all fix types are present
        actions = [fix.action for fix in fixes]
        assert FixAction.RERUN_PIPELINE in actions
        assert FixAction.FORCE_BACKFILL in actions
        assert FixAction.UPDATE_SCHEMA in actions
        assert FixAction.QUARANTINE_DATA in actions
