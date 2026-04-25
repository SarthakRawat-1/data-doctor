"""Tests for diagnosis API endpoints.

Tests the orchestration of the complete diagnosis pipeline.

Phase 4 Testing.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from fastapi import HTTPException

from src.api.v1.diagnosis import (
    diagnose_asset,
    run_demo_scenario,
    _infer_entity_type,
    _calculate_severity,
)
from src.constants import AnomalyType, Severity, FixAction
from src.schemas import (
    DiagnosisRequest,
    DiagnosisResponse,
    AnomalyDetail,
    ImpactedAssets,
    SuggestedFix,
)


class TestDiagnoseAsset:
    """Tests for diagnose_asset endpoint."""
    
    @pytest.mark.asyncio
    @patch('src.api.v1.diagnosis.evaluate_asset_anomalies')
    @patch('src.api.v1.diagnosis.find_root_cause_by_fqn')
    @patch('src.api.v1.diagnosis.calculate_confidence_score')
    @patch('src.api.v1.diagnosis.compute_blast_radius_by_fqn')
    @patch('src.api.v1.diagnosis.generate_suggested_fixes')
    async def test_diagnose_table_with_anomalies(
        self,
        mock_generate_fixes,
        mock_compute_blast,
        mock_calc_confidence,
        mock_find_root,
        mock_evaluate
    ):
        """Test diagnosing a table with detected anomalies."""
        # Setup mocks
        mock_client = Mock()
        mock_client.get_table_by_fqn.return_value = {
            "id": "table-123",
            "name": "dim_customer",
            "fullyQualifiedName": "snowflake.analytics.dim_customer"
        }
        
        # Mock detection
        mock_evaluate.return_value = [AnomalyType.STALE_DATA]
        
        # Mock root cause
        primary_cause = AnomalyDetail(
            type=AnomalyType.PIPELINE_FAILURE,
            name="customer_etl",
            depth=1,
            entity_id="pipeline-1"
        )
        mock_find_root.return_value = {
            "primary_root_cause": primary_cause,
            "contributing_factors": []
        }
        
        # Mock confidence
        mock_calc_confidence.return_value = 0.9
        
        # Mock impact
        mock_compute_blast.return_value = ImpactedAssets(
            tables=[{"id": "table-1", "name": "fact_sales"}],
            dashboards=[{"id": "dash-1", "name": "executive_dashboard"}],
            ml_models=[],
            total_impact_count=2
        )
        
        # Mock suggestions
        mock_generate_fixes.return_value = [
            SuggestedFix(
                action=FixAction.RERUN_PIPELINE,
                target="customer_etl",
                description="Rerun the failed pipeline"
            )
        ]
        
        # Execute
        request = DiagnosisRequest(
            target_fqn="snowflake.analytics.dim_customer",
            upstream_depth=5,
            downstream_depth=5
        )
        
        response = await diagnose_asset(request, mock_client)
        
        # Verify response structure
        assert isinstance(response, DiagnosisResponse)
        assert response.target_asset == "snowflake.analytics.dim_customer"
        assert response.severity == Severity.HIGH  # Has dashboard impact
        assert response.confidence_score == 0.9
        assert response.primary_root_cause == primary_cause
        assert len(response.suggested_fixes) == 1
        assert response.execution_time_ms is not None
        assert response.execution_time_ms > 0
        
        # Verify all phases were called
        mock_client.get_table_by_fqn.assert_called_once()
        mock_evaluate.assert_called_once()
        mock_find_root.assert_called_once()
        mock_calc_confidence.assert_called_once()
        mock_compute_blast.assert_called_once()
        mock_generate_fixes.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.api.v1.diagnosis.evaluate_asset_anomalies')
    @patch('src.api.v1.diagnosis.find_root_cause_by_fqn')
    @patch('src.api.v1.diagnosis.calculate_confidence_score')
    @patch('src.api.v1.diagnosis.compute_blast_radius_by_fqn')
    @patch('src.api.v1.diagnosis.generate_suggested_fixes')
    async def test_diagnose_pipeline(
        self,
        mock_generate_fixes,
        mock_compute_blast,
        mock_calc_confidence,
        mock_find_root,
        mock_evaluate
    ):
        """Test diagnosing a pipeline entity."""
        mock_client = Mock()
        mock_client.get_pipeline_by_fqn.return_value = {
            "id": "pipeline-456",
            "name": "orders_etl",
            "fullyQualifiedName": "airflow.orders_etl"
        }
        
        mock_evaluate.return_value = [AnomalyType.PIPELINE_FAILURE]
        mock_find_root.return_value = {
            "primary_root_cause": None,
            "contributing_factors": []
        }
        mock_calc_confidence.return_value = 0.0
        mock_compute_blast.return_value = ImpactedAssets()
        mock_generate_fixes.return_value = [
            SuggestedFix(
                action=FixAction.RERUN_PIPELINE,
                target="orders_etl",
                description="Rerun pipeline"
            )
        ]
        
        request = DiagnosisRequest(
            target_fqn="airflow.orders_etl",
            upstream_depth=3,
            downstream_depth=3
        )
        
        response = await diagnose_asset(request, mock_client)
        
        assert response.target_asset == "airflow.orders_etl"
        assert response.severity == Severity.LOW  # No impact
        mock_client.get_pipeline_by_fqn.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_diagnose_entity_not_found(self):
        """Test error handling when entity is not found."""
        mock_client = Mock()
        mock_client.get_table_by_fqn.return_value = {}  # No id field
        
        request = DiagnosisRequest(
            target_fqn="snowflake.analytics.nonexistent",
            upstream_depth=5,
            downstream_depth=5
        )
        
        with pytest.raises(HTTPException) as exc_info:
            await diagnose_asset(request, mock_client)
        
        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()
    
    @pytest.mark.asyncio
    @patch('src.api.v1.diagnosis.evaluate_asset_anomalies')
    async def test_diagnose_handles_exceptions(self, mock_evaluate):
        """Test that exceptions are properly caught and returned as HTTP errors."""
        mock_client = Mock()
        mock_client.get_table_by_fqn.side_effect = Exception("API connection failed")
        
        request = DiagnosisRequest(
            target_fqn="snowflake.analytics.test_table",
            upstream_depth=5,
            downstream_depth=5
        )
        
        with pytest.raises(HTTPException) as exc_info:
            await diagnose_asset(request, mock_client)
        
        assert exc_info.value.status_code == 500
        assert "Diagnosis failed" in exc_info.value.detail
    
    @pytest.mark.asyncio
    @patch('src.api.v1.diagnosis.evaluate_asset_anomalies')
    @patch('src.api.v1.diagnosis.find_root_cause_by_fqn')
    @patch('src.api.v1.diagnosis.calculate_confidence_score')
    @patch('src.api.v1.diagnosis.compute_blast_radius_by_fqn')
    @patch('src.api.v1.diagnosis.generate_suggested_fixes')
    async def test_diagnose_with_contributing_factors(
        self,
        mock_generate_fixes,
        mock_compute_blast,
        mock_calc_confidence,
        mock_find_root,
        mock_evaluate
    ):
        """Test diagnosis with multiple contributing factors."""
        mock_client = Mock()
        mock_client.get_table_by_fqn.return_value = {
            "id": "table-789",
            "name": "fact_orders"
        }
        
        mock_evaluate.return_value = []
        
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
        
        mock_find_root.return_value = {
            "primary_root_cause": primary_cause,
            "contributing_factors": contributing_factors
        }
        
        mock_calc_confidence.return_value = 0.7
        mock_compute_blast.return_value = ImpactedAssets(
            tables=[{"id": "t1"}, {"id": "t2"}],
            dashboards=[],
            ml_models=[],
            total_impact_count=2
        )
        mock_generate_fixes.return_value = [
            SuggestedFix(action=FixAction.RERUN_PIPELINE, target="etl", description="Fix 1"),
            SuggestedFix(action=FixAction.QUARANTINE_DATA, target="source", description="Fix 2"),
            SuggestedFix(action=FixAction.FORCE_BACKFILL, target="upstream", description="Fix 3")
        ]
        
        request = DiagnosisRequest(target_fqn="snowflake.analytics.fact_orders")
        response = await diagnose_asset(request, mock_client)
        
        assert response.primary_root_cause == primary_cause
        assert len(response.contributing_factors) == 2
        assert len(response.suggested_fixes) == 3
        assert response.severity == Severity.MEDIUM  # Tables but no dashboards


class TestRunDemoScenario:
    """Tests for run_demo_scenario endpoint."""
    
    @pytest.mark.asyncio
    @patch('src.api.v1.diagnosis.diagnose_asset')
    @patch('src.api.v1.diagnosis.settings')
    async def test_run_demo_scenario_success(self, mock_settings, mock_diagnose):
        """Test running demo scenario successfully."""
        mock_settings.DEMO_SCENARIO_FQN = "sample_data.ecommerce_db.shopify.dim_customer"
        
        # Mock diagnosis response
        mock_diagnosis = DiagnosisResponse(
            incident_id="demo-123",
            target_asset="sample_data.ecommerce_db.shopify.dim_customer",
            severity=Severity.HIGH,
            confidence_score=0.9,
            primary_root_cause=AnomalyDetail(
                type=AnomalyType.PIPELINE_FAILURE,
                name="customer_etl",
                depth=1,
                entity_id="pipeline-1"
            ),
            contributing_factors=[],
            impacted_assets=ImpactedAssets(
                dashboards=[{"id": "dash-1", "name": "executive_dashboard"}],
                total_impact_count=1
            ),
            suggested_fixes=[
                SuggestedFix(
                    action=FixAction.RERUN_PIPELINE,
                    target="customer_etl",
                    description="Rerun pipeline"
                )
            ]
        )
        mock_diagnose.return_value = mock_diagnosis
        
        mock_client = Mock()
        
        # Execute
        response = await run_demo_scenario(mock_client)
        
        # Verify
        assert response.demo_fqn == "sample_data.ecommerce_db.shopify.dim_customer"
        assert "Demo scenario executed successfully" in response.message
        assert response.diagnosis == mock_diagnosis
        
        # Verify diagnose_asset was called with correct parameters
        mock_diagnose.assert_called_once()
        call_args = mock_diagnose.call_args
        assert call_args[0][0].target_fqn == "sample_data.ecommerce_db.shopify.dim_customer"
        assert call_args[0][0].upstream_depth == 5
        assert call_args[0][0].downstream_depth == 5


class TestInferEntityType:
    """Tests for _infer_entity_type helper function."""
    
    def test_infer_table_from_4_part_fqn(self):
        """Test inferring table from 4-part FQN."""
        fqn = "snowflake.analytics.public.dim_customer"
        entity_type = _infer_entity_type(fqn)
        assert entity_type == "table"
    
    def test_infer_table_from_5_part_fqn(self):
        """Test inferring table from 5-part FQN."""
        fqn = "service.database.schema.table.column"
        entity_type = _infer_entity_type(fqn)
        assert entity_type == "table"
    
    def test_infer_pipeline_from_2_part_fqn(self):
        """Test inferring pipeline from 2-part FQN."""
        fqn = "airflow.customer_etl"
        entity_type = _infer_entity_type(fqn)
        assert entity_type == "pipeline"
    
    def test_infer_table_from_3_part_fqn(self):
        """Test inferring table from 3-part FQN (edge case)."""
        fqn = "service.database.table"
        entity_type = _infer_entity_type(fqn)
        assert entity_type == "table"
    
    def test_infer_pipeline_from_single_part(self):
        """Test inferring pipeline from single-part FQN."""
        fqn = "standalone_pipeline"
        entity_type = _infer_entity_type(fqn)
        assert entity_type == "pipeline"


class TestCalculateSeverity:
    """Tests for _calculate_severity helper function."""
    
    def test_severity_high_with_dashboards(self):
        """Test HIGH severity when dashboards are impacted."""
        impacted = ImpactedAssets(
            tables=[{"id": "t1"}],
            dashboards=[{"id": "d1", "name": "executive_dashboard"}],
            ml_models=[],
            total_impact_count=2
        )
        
        severity = _calculate_severity(impacted)
        assert severity == Severity.HIGH
    
    def test_severity_high_with_ml_models(self):
        """Test HIGH severity when ML models are impacted."""
        impacted = ImpactedAssets(
            tables=[],
            dashboards=[],
            ml_models=[{"id": "ml1", "name": "churn_model"}],
            total_impact_count=1
        )
        
        severity = _calculate_severity(impacted)
        assert severity == Severity.HIGH
    
    def test_severity_high_with_many_tables(self):
        """Test HIGH severity when more than 3 tables are impacted."""
        impacted = ImpactedAssets(
            tables=[
                {"id": "t1"},
                {"id": "t2"},
                {"id": "t3"},
                {"id": "t4"}
            ],
            dashboards=[],
            ml_models=[],
            total_impact_count=4
        )
        
        severity = _calculate_severity(impacted)
        assert severity == Severity.HIGH
    
    def test_severity_medium_with_few_tables(self):
        """Test MEDIUM severity when 1-3 tables are impacted."""
        impacted = ImpactedAssets(
            tables=[{"id": "t1"}, {"id": "t2"}],
            dashboards=[],
            ml_models=[],
            total_impact_count=2
        )
        
        severity = _calculate_severity(impacted)
        assert severity == Severity.MEDIUM
    
    def test_severity_medium_with_one_table(self):
        """Test MEDIUM severity with single table."""
        impacted = ImpactedAssets(
            tables=[{"id": "t1"}],
            dashboards=[],
            ml_models=[],
            total_impact_count=1
        )
        
        severity = _calculate_severity(impacted)
        assert severity == Severity.MEDIUM
    
    def test_severity_low_with_no_impact(self):
        """Test LOW severity when no downstream impact."""
        impacted = ImpactedAssets(
            tables=[],
            dashboards=[],
            ml_models=[],
            total_impact_count=0
        )
        
        severity = _calculate_severity(impacted)
        assert severity == Severity.LOW
    
    def test_severity_high_with_both_dashboards_and_ml(self):
        """Test HIGH severity with both dashboards and ML models."""
        impacted = ImpactedAssets(
            tables=[{"id": "t1"}],
            dashboards=[{"id": "d1"}],
            ml_models=[{"id": "ml1"}],
            total_impact_count=3
        )
        
        severity = _calculate_severity(impacted)
        assert severity == Severity.HIGH


class TestIntegrationScenarios:
    """Integration tests with realistic end-to-end scenarios."""
    
    @pytest.mark.asyncio
    @patch('src.api.v1.diagnosis.evaluate_asset_anomalies')
    @patch('src.api.v1.diagnosis.find_root_cause_by_fqn')
    @patch('src.api.v1.diagnosis.calculate_confidence_score')
    @patch('src.api.v1.diagnosis.compute_blast_radius_by_fqn')
    @patch('src.api.v1.diagnosis.generate_suggested_fixes')
    async def test_executive_dashboard_failure_scenario(
        self,
        mock_generate_fixes,
        mock_compute_blast,
        mock_calc_confidence,
        mock_find_root,
        mock_evaluate
    ):
        """Test realistic scenario: Executive dashboard is broken."""
        mock_client = Mock()
        mock_client.get_table_by_fqn.return_value = {
            "id": "table-exec",
            "name": "executive_summary_table"
        }
        
        # Scenario: Pipeline failed, causing dashboard to break
        mock_evaluate.return_value = [AnomalyType.STALE_DATA]
        
        primary_cause = AnomalyDetail(
            type=AnomalyType.PIPELINE_FAILURE,
            name="executive_etl_dag",
            depth=1,
            entity_id="pipeline-exec",
            description="Airflow DAG failed at transform step"
        )
        
        mock_find_root.return_value = {
            "primary_root_cause": primary_cause,
            "contributing_factors": []
        }
        
        mock_calc_confidence.return_value = 0.9  # High confidence
        
        mock_compute_blast.return_value = ImpactedAssets(
            tables=[{"id": "t1", "name": "fact_sales"}],
            dashboards=[
                {"id": "d1", "name": "executive_dashboard"},
                {"id": "d2", "name": "marketing_dashboard"}
            ],
            ml_models=[],
            total_impact_count=3
        )
        
        mock_generate_fixes.return_value = [
            SuggestedFix(
                action=FixAction.RERUN_PIPELINE,
                target="executive_etl_dag",
                description="Rerun the failed Airflow DAG from last checkpoint"
            )
        ]
        
        request = DiagnosisRequest(
            target_fqn="snowflake.analytics.executive_summary_table"
        )
        
        response = await diagnose_asset(request, mock_client)
        
        # Verify diagnosis
        assert response.severity == Severity.HIGH
        assert response.confidence_score == 0.9
        assert response.primary_root_cause.type == AnomalyType.PIPELINE_FAILURE
        assert len(response.impacted_assets.dashboards) == 2
        assert len(response.suggested_fixes) == 1
        assert response.suggested_fixes[0].action == FixAction.RERUN_PIPELINE
