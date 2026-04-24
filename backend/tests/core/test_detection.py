"""Tests for detection engine module.

Tests all 6 detection rules:
1. Pipeline Failure
2. Stale Data
3. Schema Change
4. Data Quality Failure
5. Volume Anomaly
6. Distribution Drift
"""
import pytest
from datetime import datetime, timedelta, timezone

from src.constants import AnomalyType
from src.core.detection import (
    detect_pipeline_failure,
    detect_stale_data,
    detect_schema_change,
    detect_data_quality_failure,
    detect_volume_anomaly,
    detect_distribution_drift,
    evaluate_table_anomalies,
    evaluate_pipeline_anomalies,
    evaluate_asset_anomalies,
)


# ============================================================================
# Rule 1: Pipeline Failure Detection Tests
# ============================================================================

def test_pipeline_failure_task_status_failed():
    """Test detection of failed pipeline via taskStatus."""
    pipeline = {
        "taskStatus": {
            "executionStatus": "Failed"
        }
    }
    result = detect_pipeline_failure(pipeline)
    assert result == AnomalyType.PIPELINE_FAILURE


def test_pipeline_failure_pipeline_state_failed():
    """Test detection of failed pipeline via pipelineStatus."""
    pipeline = {
        "pipelineStatus": {
            "pipelineState": "failed"
        }
    }
    result = detect_pipeline_failure(pipeline)
    assert result == AnomalyType.PIPELINE_FAILURE


def test_pipeline_success_no_anomaly():
    """Test that successful pipeline returns None."""
    pipeline = {
        "taskStatus": {
            "executionStatus": "Success"
        },
        "pipelineStatus": {
            "pipelineState": "success"
        }
    }
    result = detect_pipeline_failure(pipeline)
    assert result is None


def test_pipeline_no_status_no_anomaly():
    """Test that pipeline without status returns None."""
    pipeline = {}
    result = detect_pipeline_failure(pipeline)
    assert result is None


# ============================================================================
# Rule 2: Stale Data Detection Tests
# ============================================================================

def test_stale_data_profile_timestamp_old():
    """Test detection of stale data via old profile timestamp."""
    # Create timestamp 72 hours ago (beyond 48h SLA)
    old_timestamp = int((datetime.now(timezone.utc) - timedelta(hours=72)).timestamp() * 1000)
    
    table = {
        "profile": {
            "timestamp": old_timestamp,
            "rowCount": 1000
        }
    }
    result = detect_stale_data(table, sla_hours=48)
    assert result == AnomalyType.STALE_DATA


def test_stale_data_system_profile_old():
    """Test detection of stale data via old system profile."""
    old_timestamp = int((datetime.now(timezone.utc) - timedelta(hours=60)).timestamp() * 1000)
    
    table = {
        "systemProfile": {
            "timestamp": old_timestamp
        }
    }
    result = detect_stale_data(table, sla_hours=48)
    assert result == AnomalyType.STALE_DATA


def test_fresh_data_no_anomaly():
    """Test that fresh data returns None."""
    # Create timestamp 12 hours ago (within 48h SLA)
    recent_timestamp = int((datetime.now(timezone.utc) - timedelta(hours=12)).timestamp() * 1000)
    
    table = {
        "profile": {
            "timestamp": recent_timestamp,
            "rowCount": 1000
        }
    }
    result = detect_stale_data(table, sla_hours=48)
    assert result is None


def test_stale_data_custom_sla():
    """Test stale data detection with custom SLA."""
    # 30 hours ago
    timestamp = int((datetime.now(timezone.utc) - timedelta(hours=30)).timestamp() * 1000)
    
    table = {
        "profile": {
            "timestamp": timestamp
        }
    }
    # Should be stale with 24h SLA
    result = detect_stale_data(table, sla_hours=24)
    assert result == AnomalyType.STALE_DATA
    
    # Should be fresh with 48h SLA
    result = detect_stale_data(table, sla_hours=48)
    assert result is None


def test_stale_data_no_profile():
    """Test that table without profile returns None."""
    table = {}
    result = detect_stale_data(table)
    assert result is None


# ============================================================================
# Rule 3: Schema Change Detection Tests
# ============================================================================

def test_schema_change_fields_deleted():
    """Test detection of schema change via deleted fields."""
    table = {
        "changeDescription": {
            "fieldsDeleted": [
                {"name": "customer_id", "oldValue": "BIGINT"}
            ]
        }
    }
    result = detect_schema_change(table)
    assert result == AnomalyType.SCHEMA_CHANGE


def test_schema_change_datatype_updated():
    """Test detection of schema change via datatype update."""
    table = {
        "changeDescription": {
            "fieldsUpdated": [
                {
                    "name": "columns.email.dataType",
                    "oldValue": "VARCHAR",
                    "newValue": "TEXT"
                }
            ]
        }
    }
    result = detect_schema_change(table)
    assert result == AnomalyType.SCHEMA_CHANGE


def test_schema_change_non_breaking_update():
    """Test that non-breaking updates don't trigger anomaly."""
    table = {
        "changeDescription": {
            "fieldsUpdated": [
                {
                    "name": "description",
                    "oldValue": "Old description",
                    "newValue": "New description"
                }
            ]
        }
    }
    result = detect_schema_change(table)
    assert result is None


def test_schema_no_change():
    """Test that table without changes returns None."""
    table = {}
    result = detect_schema_change(table)
    assert result is None


# ============================================================================
# Rule 4: Data Quality Failure Detection Tests
# ============================================================================

def test_data_quality_failure_failed_status():
    """Test detection of failed data quality test."""
    test_cases = [
        {
            "name": "null_check",
            "testCaseResult": {
                "testCaseStatus": "Failed",
                "result": "Found 150 null values"
            }
        }
    ]
    result = detect_data_quality_failure(test_cases)
    assert result == AnomalyType.DATA_QUALITY_FAILURE


def test_data_quality_failure_aborted_status():
    """Test detection of aborted data quality test."""
    test_cases = [
        {
            "name": "uniqueness_check",
            "testCaseResult": {
                "testCaseStatus": "Aborted",
                "result": "Test execution timeout"
            }
        }
    ]
    result = detect_data_quality_failure(test_cases)
    assert result == AnomalyType.DATA_QUALITY_FAILURE


def test_data_quality_success_no_anomaly():
    """Test that successful tests return None."""
    test_cases = [
        {
            "name": "null_check",
            "testCaseResult": {
                "testCaseStatus": "Success",
                "result": "No null values found"
            }
        }
    ]
    result = detect_data_quality_failure(test_cases)
    assert result is None


def test_data_quality_mixed_results():
    """Test that any failure triggers anomaly."""
    test_cases = [
        {
            "name": "test1",
            "testCaseResult": {"testCaseStatus": "Success"}
        },
        {
            "name": "test2",
            "testCaseResult": {"testCaseStatus": "Failed"}
        },
        {
            "name": "test3",
            "testCaseResult": {"testCaseStatus": "Success"}
        }
    ]
    result = detect_data_quality_failure(test_cases)
    assert result == AnomalyType.DATA_QUALITY_FAILURE


def test_data_quality_no_tests():
    """Test that empty test list returns None."""
    result = detect_data_quality_failure([])
    assert result is None


def test_data_quality_no_results():
    """Test that tests without results return None."""
    test_cases = [
        {"name": "test1"}  # No testCaseResult
    ]
    result = detect_data_quality_failure(test_cases)
    assert result is None


# ============================================================================
# Rule 5: Volume Anomaly Detection Tests
# ============================================================================

def test_volume_anomaly_spike_detected():
    """Test detection of volume spike (10x normal)."""
    # Historical: 10k rows consistently
    historical = [
        {"profile": {"rowCount": 10000}},
        {"profile": {"rowCount": 10200}},
        {"profile": {"rowCount": 9800}},
        {"profile": {"rowCount": 10100}},
        {"profile": {"rowCount": 9900}},
        {"profile": {"rowCount": 10000}},
        {"profile": {"rowCount": 10050}},
    ]
    
    # Current: 100k rows (10x spike)
    current = {
        "profile": {"rowCount": 100000}
    }
    
    result = detect_volume_anomaly(current, historical)
    assert result == AnomalyType.VOLUME_ANOMALY


def test_volume_anomaly_drop_detected():
    """Test detection of volume drop (90% decrease)."""
    # Historical: 50k rows consistently
    historical = [
        {"profile": {"rowCount": 50000}},
        {"profile": {"rowCount": 51000}},
        {"profile": {"rowCount": 49000}},
        {"profile": {"rowCount": 50500}},
        {"profile": {"rowCount": 49500}},
        {"profile": {"rowCount": 50000}},
        {"profile": {"rowCount": 50200}},
    ]
    
    # Current: 5k rows (90% drop)
    current = {
        "profile": {"rowCount": 5000}
    }
    
    result = detect_volume_anomaly(current, historical)
    assert result == AnomalyType.VOLUME_ANOMALY


def test_volume_normal_variation():
    """Test that normal variation doesn't trigger anomaly."""
    # Historical: 10k rows with normal variance
    historical = [
        {"profile": {"rowCount": 10000}},
        {"profile": {"rowCount": 10500}},
        {"profile": {"rowCount": 9500}},
        {"profile": {"rowCount": 10200}},
        {"profile": {"rowCount": 9800}},
        {"profile": {"rowCount": 10100}},
        {"profile": {"rowCount": 9900}},
    ]
    
    # Current: 10.3k rows (within 2σ)
    current = {
        "profile": {"rowCount": 10300}
    }
    
    result = detect_volume_anomaly(current, historical)
    assert result is None


def test_volume_insufficient_history():
    """Test that insufficient history returns None."""
    historical = [
        {"profile": {"rowCount": 10000}},
        {"profile": {"rowCount": 10500}},
    ]  # Only 2 data points
    
    current = {
        "profile": {"rowCount": 50000}
    }
    
    result = detect_volume_anomaly(current, historical)
    assert result is None


def test_volume_no_profile():
    """Test that table without profile returns None."""
    historical = [{"profile": {"rowCount": 10000}}] * 7
    current = {}
    
    result = detect_volume_anomaly(current, historical)
    assert result is None


# ============================================================================
# Rule 6: Distribution Drift Detection Tests
# ============================================================================

def test_distribution_drift_null_proportion_increase():
    """Test detection of null proportion increase."""
    # Historical: 2% nulls consistently
    historical = [
        {"profile": {"columnProfile": [{"name": "email", "nullProportion": 0.02}]}},
        {"profile": {"columnProfile": [{"name": "email", "nullProportion": 0.021}]}},
        {"profile": {"columnProfile": [{"name": "email", "nullProportion": 0.019}]}},
        {"profile": {"columnProfile": [{"name": "email", "nullProportion": 0.02}]}},
        {"profile": {"columnProfile": [{"name": "email", "nullProportion": 0.022}]}},
        {"profile": {"columnProfile": [{"name": "email", "nullProportion": 0.018}]}},
        {"profile": {"columnProfile": [{"name": "email", "nullProportion": 0.02}]}},
    ]
    
    # Current: 20% nulls (18% increase, above 15% threshold)
    current = {
        "profile": {
            "columnProfile": [
                {"name": "email", "nullProportion": 0.20}
            ]
        }
    }
    
    result = detect_distribution_drift(current, historical)
    assert result == AnomalyType.DISTRIBUTION_DRIFT


def test_distribution_drift_distinct_count_drop():
    """Test detection of distinct count drop."""
    # Historical: 10k distinct values
    historical = [
        {"profile": {"columnProfile": [{"name": "category", "distinctCount": 10000}]}},
        {"profile": {"columnProfile": [{"name": "category", "distinctCount": 10200}]}},
        {"profile": {"columnProfile": [{"name": "category", "distinctCount": 9800}]}},
        {"profile": {"columnProfile": [{"name": "category", "distinctCount": 10100}]}},
        {"profile": {"columnProfile": [{"name": "category", "distinctCount": 9900}]}},
        {"profile": {"columnProfile": [{"name": "category", "distinctCount": 10000}]}},
        {"profile": {"columnProfile": [{"name": "category", "distinctCount": 10050}]}},
    ]
    
    # Current: 6k distinct (40% drop, above 30% threshold)
    current = {
        "profile": {
            "columnProfile": [
                {"name": "category", "distinctCount": 6000}
            ]
        }
    }
    
    result = detect_distribution_drift(current, historical)
    assert result == AnomalyType.DISTRIBUTION_DRIFT


def test_distribution_normal_variation():
    """Test that normal variation doesn't trigger drift."""
    # Historical: 5% nulls
    historical = [
        {"profile": {"columnProfile": [{"name": "phone", "nullProportion": 0.05}]}},
        {"profile": {"columnProfile": [{"name": "phone", "nullProportion": 0.052}]}},
        {"profile": {"columnProfile": [{"name": "phone", "nullProportion": 0.048}]}},
        {"profile": {"columnProfile": [{"name": "phone", "nullProportion": 0.051}]}},
        {"profile": {"columnProfile": [{"name": "phone", "nullProportion": 0.049}]}},
        {"profile": {"columnProfile": [{"name": "phone", "nullProportion": 0.05}]}},
        {"profile": {"columnProfile": [{"name": "phone", "nullProportion": 0.05}]}},
    ]
    
    # Current: 8% nulls (3% change, below 15% threshold)
    current = {
        "profile": {
            "columnProfile": [
                {"name": "phone", "nullProportion": 0.08}
            ]
        }
    }
    
    result = detect_distribution_drift(current, historical)
    assert result is None


def test_distribution_specific_column():
    """Test drift detection for specific column."""
    historical = [
        {"profile": {"columnProfile": [
            {"name": "col1", "nullProportion": 0.02},
            {"name": "col2", "nullProportion": 0.50}  # High nulls in col2
        ]}}
    ] * 7
    
    current = {
        "profile": {
            "columnProfile": [
                {"name": "col1", "nullProportion": 0.03},  # Normal
                {"name": "col2", "nullProportion": 0.70}   # 20% increase
            ]
        }
    }
    
    # Should detect drift in col2
    result = detect_distribution_drift(current, historical, column_name="col2")
    assert result == AnomalyType.DISTRIBUTION_DRIFT
    
    # Should not detect drift in col1
    result = detect_distribution_drift(current, historical, column_name="col1")
    assert result is None


def test_distribution_insufficient_history():
    """Test that insufficient history returns None."""
    historical = [
        {"profile": {"columnProfile": [{"name": "email", "nullProportion": 0.02}]}}
    ] * 3  # Only 3 data points
    
    current = {
        "profile": {
            "columnProfile": [{"name": "email", "nullProportion": 0.50}]
        }
    }
    
    result = detect_distribution_drift(current, historical)
    assert result is None


# ============================================================================
# Integration Tests: evaluate_*_anomalies
# ============================================================================

def test_evaluate_table_anomalies_multiple_issues():
    """Test evaluation of table with multiple anomalies."""
    old_timestamp = int((datetime.now(timezone.utc) - timedelta(hours=72)).timestamp() * 1000)
    
    table = {
        "profile": {
            "timestamp": old_timestamp,
            "rowCount": 100000
        },
        "changeDescription": {
            "fieldsDeleted": [{"name": "user_id"}]
        }
    }
    
    historical = [{"profile": {"rowCount": 10000}}] * 7
    
    test_cases = [
        {"testCaseResult": {"testCaseStatus": "Failed"}}
    ]
    
    anomalies = evaluate_table_anomalies(table, historical, test_cases)
    
    # Should detect: stale_data, schema_change, data_quality_failure, volume_anomaly
    assert AnomalyType.STALE_DATA in anomalies
    assert AnomalyType.SCHEMA_CHANGE in anomalies
    assert AnomalyType.DATA_QUALITY_FAILURE in anomalies
    assert AnomalyType.VOLUME_ANOMALY in anomalies


def test_evaluate_table_anomalies_healthy():
    """Test evaluation of healthy table."""
    recent_timestamp = int((datetime.now(timezone.utc) - timedelta(hours=12)).timestamp() * 1000)
    
    table = {
        "profile": {
            "timestamp": recent_timestamp,
            "rowCount": 10000
        }
    }
    
    historical = [{"profile": {"rowCount": 10000}}] * 7
    test_cases = [{"testCaseResult": {"testCaseStatus": "Success"}}]
    
    anomalies = evaluate_table_anomalies(table, historical, test_cases)
    
    assert len(anomalies) == 0


def test_evaluate_pipeline_anomalies():
    """Test evaluation of failed pipeline."""
    pipeline = {
        "taskStatus": {"executionStatus": "Failed"}
    }
    
    anomalies = evaluate_pipeline_anomalies(pipeline)
    
    assert AnomalyType.PIPELINE_FAILURE in anomalies


def test_evaluate_asset_anomalies_routing():
    """Test that evaluate_asset_anomalies routes correctly."""
    # Test table routing
    table = {
        "profile": {
            "timestamp": int((datetime.now(timezone.utc) - timedelta(hours=72)).timestamp() * 1000)
        }
    }
    anomalies = evaluate_asset_anomalies(table, "table")
    assert AnomalyType.STALE_DATA in anomalies
    
    # Test pipeline routing
    pipeline = {
        "taskStatus": {"executionStatus": "Failed"}
    }
    anomalies = evaluate_asset_anomalies(pipeline, "pipeline")
    assert AnomalyType.PIPELINE_FAILURE in anomalies
    
    # Test unsupported type
    anomalies = evaluate_asset_anomalies({}, "dashboard")
    assert len(anomalies) == 0
