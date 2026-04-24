"""Detection engine for identifying anomalies.

Implements 6 production-grade detection rules aligned with industry standards:
1. Pipeline Failure - Hard operational failures
2. Schema Drift - Breaking structural changes
3. Data Quality Failure - Test case failures
4. Stale Data - Freshness SLA violations
5. Volume Anomaly - Unexpected row count changes
6. Distribution Drift - Field-level value distribution changes

References:
- Five Pillars of Data Observability (Monte Carlo, Datadog)
- OpenMetadata Data Quality Framework
- data_doctor.md Section 4 & research_references.md
"""
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any

from src.constants import (
    AnomalyType,
    DEFAULT_FRESHNESS_SLA_HOURS,
    VOLUME_ANOMALY_STD_DEV_THRESHOLD,
    VOLUME_ANOMALY_MIN_HISTORY_DAYS,
    DISTRIBUTION_DRIFT_NULL_THRESHOLD,
    DISTRIBUTION_DRIFT_DISTINCT_THRESHOLD,
    DISTRIBUTION_DRIFT_MIN_HISTORY_DAYS,
)


def detect_pipeline_failure(pipeline_entity: dict[str, Any]) -> AnomalyType | None:
    """
    Detect if a pipeline has failed.
    
    Rule: taskStatus.executionStatus == 'Failed' OR pipelineStatus.pipelineState == 'failed'
    
    Industry Standard: Critical for Airflow/orchestration monitoring
    Reference: data_doctor.md Section 4, Table "Detection Rule Specifications"
    
    Args:
        pipeline_entity: Pipeline entity from OpenMetadata
    
    Returns:
        AnomalyType.PIPELINE_FAILURE if failed, None otherwise
    """
    # Check taskStatus.executionStatus
    task_status = pipeline_entity.get("taskStatus")
    if task_status:
        execution_status = task_status.get("executionStatus", "").lower()
        if execution_status == "failed":
            return AnomalyType.PIPELINE_FAILURE
    
    # Check pipelineStatus.pipelineState
    pipeline_status = pipeline_entity.get("pipelineStatus")
    if pipeline_status:
        pipeline_state = pipeline_status.get("pipelineState", "").lower()
        if pipeline_state == "failed":
            return AnomalyType.PIPELINE_FAILURE
    
    return None


def detect_stale_data(
    table_entity: dict[str, Any],
    sla_hours: int = DEFAULT_FRESHNESS_SLA_HOURS
) -> AnomalyType | None:
    """
    Detect if table data is stale (not refreshed within SLA).
    
    Rule: Compare profile.timestamp or systemProfile DML timestamps against SLA
    
    Industry Standard: "Freshness" pillar of data observability
    Reference: Monte Carlo, Datadog freshness monitoring
    
    Args:
        table_entity: Table entity from OpenMetadata
        sla_hours: SLA threshold in hours (default: 48)
    
    Returns:
        AnomalyType.STALE_DATA if stale, None otherwise
    """
    now = datetime.now(timezone.utc)
    sla_threshold = now - timedelta(hours=sla_hours)
    
    # Check profile.timestamp (most recent profiling run)
    profile = table_entity.get("profile")
    if profile:
        timestamp = profile.get("timestamp")
        if timestamp:
            # Convert milliseconds to datetime
            profile_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            if profile_time < sla_threshold:
                return AnomalyType.STALE_DATA
    
    # Check systemProfile for DML activity
    system_profile = table_entity.get("systemProfile")
    if system_profile:
        # Check for recent INSERT/UPDATE operations
        operation_timestamp = system_profile.get("timestamp")
        if operation_timestamp:
            dml_time = datetime.fromtimestamp(operation_timestamp / 1000, tz=timezone.utc)
            if dml_time < sla_threshold:
                return AnomalyType.STALE_DATA
    
    return None


def detect_schema_change(table_entity: dict[str, Any]) -> AnomalyType | None:
    """
    Detect breaking schema changes.
    
    Rule: changeDescription.fieldsDeleted has values OR
          changeDescription.fieldsUpdated shows destructive datatype changes
    
    Industry Standard: "Schema" pillar of data observability
    Reference: data_doctor.md Section 4, OpenMetadata versioning system
    
    Args:
        table_entity: Table entity from OpenMetadata
    
    Returns:
        AnomalyType.SCHEMA_CHANGE if breaking change detected, None otherwise
    """
    change_description = table_entity.get("changeDescription")
    if not change_description:
        return None
    
    # Check for deleted fields (columns) - always breaking
    fields_deleted = change_description.get("fieldsDeleted", [])
    if fields_deleted:
        # Any column deletion is a breaking change
        return AnomalyType.SCHEMA_CHANGE
    
    # Check for updated fields with destructive datatype changes
    fields_updated = change_description.get("fieldsUpdated", [])
    if fields_updated:
        for field_change in fields_updated:
            field_name = field_change.get("name", "")
            
            # Check if it's a dataType change in columns
            if "columns" in field_name and "dataType" in field_name:
                # Datatype changes are potentially breaking
                return AnomalyType.SCHEMA_CHANGE
    
    return None


def detect_data_quality_failure(test_cases: list[dict[str, Any]]) -> AnomalyType | None:
    """
    Detect data quality test failures.
    
    Rule: testCaseResult.testCaseStatus == 'Failed' OR 'Aborted'
    
    Industry Standard: "Quality" pillar of data observability
    Reference: OpenMetadata Data Quality Framework
    
    Args:
        test_cases: List of test case entities from OpenMetadata
    
    Returns:
        AnomalyType.DATA_QUALITY_FAILURE if any test failed, None otherwise
    """
    if not test_cases:
        return None
    
    for test_case in test_cases:
        test_case_result = test_case.get("testCaseResult")
        if not test_case_result:
            continue
        
        status = test_case_result.get("testCaseStatus", "").lower()
        if status in ["failed", "aborted"]:
            return AnomalyType.DATA_QUALITY_FAILURE
    
    return None


def detect_volume_anomaly(
    table_entity: dict[str, Any],
    historical_versions: list[dict[str, Any]]
) -> AnomalyType | None:
    """
    Detect unexpected row count changes using statistical analysis.
    
    Rule: Compare current rowCount against historical baseline (mean ± 2σ)
    
    Industry Standard: "Volume" pillar of data observability
    Reference: Monte Carlo, Datadog volume monitoring
    
    Args:
        table_entity: Current table entity from OpenMetadata
        historical_versions: List of historical table versions
    
    Returns:
        AnomalyType.VOLUME_ANOMALY if anomaly detected, None otherwise
    """
    # Get current row count
    current_profile = table_entity.get("profile")
    if not current_profile:
        return None
    
    current_row_count = current_profile.get("rowCount")
    if current_row_count is None:
        return None
    
    # Extract historical row counts
    historical_row_counts = []
    for version in historical_versions:
        profile = version.get("profile")
        if profile:
            row_count = profile.get("rowCount")
            if row_count is not None:
                historical_row_counts.append(row_count)
    
    # Need minimum history for statistical analysis
    if len(historical_row_counts) < VOLUME_ANOMALY_MIN_HISTORY_DAYS:
        return None
    
    # Calculate baseline statistics
    try:
        baseline_mean = statistics.mean(historical_row_counts)
        baseline_std = statistics.stdev(historical_row_counts)
        
        # Detect anomaly: current value outside mean ± 2σ
        threshold = VOLUME_ANOMALY_STD_DEV_THRESHOLD * baseline_std
        deviation = abs(current_row_count - baseline_mean)
        
        if deviation > threshold:
            return AnomalyType.VOLUME_ANOMALY
    except statistics.StatisticsError:
        # Not enough variance in data
        return None
    
    return None


def detect_distribution_drift(
    table_entity: dict[str, Any],
    historical_versions: list[dict[str, Any]],
    column_name: str | None = None
) -> AnomalyType | None:
    """
    Detect field-level value distribution changes.
    
    Rule: Monitor nullProportion, distinctCount changes beyond thresholds
    
    Industry Standard: "Distribution" pillar of data observability
    Reference: Monte Carlo distribution monitoring, data_doctor.md Section 7
    
    Args:
        table_entity: Current table entity from OpenMetadata
        historical_versions: List of historical table versions
        column_name: Optional specific column to check (checks all if None)
    
    Returns:
        AnomalyType.DISTRIBUTION_DRIFT if drift detected, None otherwise
    """
    # Get current column profiles
    current_profile = table_entity.get("profile")
    if not current_profile:
        return None
    
    current_column_profiles = current_profile.get("columnProfile", [])
    if not current_column_profiles:
        return None
    
    # Build historical baseline for each column
    for current_col_profile in current_column_profiles:
        col_name = current_col_profile.get("name")
        
        # Skip if checking specific column and this isn't it
        if column_name and col_name != column_name:
            continue
        
        current_null_prop = current_col_profile.get("nullProportion", 0.0)
        current_distinct = current_col_profile.get("distinctCount", 0)
        
        # Extract historical values for this column
        historical_null_props = []
        historical_distinct_counts = []
        
        for version in historical_versions:
            profile = version.get("profile")
            if not profile:
                continue
            
            col_profiles = profile.get("columnProfile", [])
            for col_prof in col_profiles:
                if col_prof.get("name") == col_name:
                    null_prop = col_prof.get("nullProportion")
                    distinct_count = col_prof.get("distinctCount")
                    
                    if null_prop is not None:
                        historical_null_props.append(null_prop)
                    if distinct_count is not None:
                        historical_distinct_counts.append(distinct_count)
                    break
        
        # Need minimum history for at least one metric
        has_enough_null_history = len(historical_null_props) >= DISTRIBUTION_DRIFT_MIN_HISTORY_DAYS
        has_enough_distinct_history = len(historical_distinct_counts) >= DISTRIBUTION_DRIFT_MIN_HISTORY_DAYS
        
        if not (has_enough_null_history or has_enough_distinct_history):
            continue
        
        # Check null proportion drift
        if has_enough_null_history:
            baseline_null_prop = statistics.mean(historical_null_props)
            null_prop_change = abs(current_null_prop - baseline_null_prop)
            
            if null_prop_change > DISTRIBUTION_DRIFT_NULL_THRESHOLD:
                return AnomalyType.DISTRIBUTION_DRIFT
        
        # Check distinct count drift (percentage change)
        if has_enough_distinct_history:
            baseline_distinct = statistics.mean(historical_distinct_counts)
            if baseline_distinct > 0 and current_distinct >= 0:
                distinct_pct_change = abs(current_distinct - baseline_distinct) / baseline_distinct
                
                if distinct_pct_change > DISTRIBUTION_DRIFT_DISTINCT_THRESHOLD:
                    return AnomalyType.DISTRIBUTION_DRIFT
    
    return None


def evaluate_table_anomalies(
    table_entity: dict[str, Any],
    historical_versions: list[dict[str, Any]] | None = None,
    test_cases: list[dict[str, Any]] | None = None,
    sla_hours: int = DEFAULT_FRESHNESS_SLA_HOURS
) -> list[AnomalyType]:
    """
    Evaluate all applicable detection rules for a table.
    
    Args:
        table_entity: Table entity from OpenMetadata
        historical_versions: Historical versions for trend analysis
        test_cases: Data quality test cases for the table
        sla_hours: Freshness SLA in hours
    
    Returns:
        List of detected anomaly types
    """
    anomalies = []
    
    # Rule 1: Stale Data
    stale = detect_stale_data(table_entity, sla_hours)
    if stale:
        anomalies.append(stale)
    
    # Rule 2: Schema Change
    schema_change = detect_schema_change(table_entity)
    if schema_change:
        anomalies.append(schema_change)
    
    # Rule 3: Data Quality Failure
    if test_cases:
        quality_failure = detect_data_quality_failure(test_cases)
        if quality_failure:
            anomalies.append(quality_failure)
    
    # Rule 4: Volume Anomaly (requires historical data)
    if historical_versions:
        volume_anomaly = detect_volume_anomaly(table_entity, historical_versions)
        if volume_anomaly:
            anomalies.append(volume_anomaly)
    
    # Rule 5: Distribution Drift (requires historical data)
    if historical_versions:
        distribution_drift = detect_distribution_drift(table_entity, historical_versions)
        if distribution_drift:
            anomalies.append(distribution_drift)
    
    return anomalies


def evaluate_pipeline_anomalies(pipeline_entity: dict[str, Any]) -> list[AnomalyType]:
    """
    Evaluate all applicable detection rules for a pipeline.
    
    Args:
        pipeline_entity: Pipeline entity from OpenMetadata
    
    Returns:
        List of detected anomaly types
    """
    anomalies = []
    
    # Rule 1: Pipeline Failure
    failure = detect_pipeline_failure(pipeline_entity)
    if failure:
        anomalies.append(failure)
    
    return anomalies


def evaluate_asset_anomalies(
    asset_entity: dict[str, Any],
    asset_type: str,
    historical_versions: list[dict[str, Any]] | None = None,
    test_cases: list[dict[str, Any]] | None = None
) -> list[AnomalyType]:
    """
    Evaluate all applicable detection rules for an asset.
    
    Routes to appropriate detection functions based on asset type.
    
    Args:
        asset_entity: Entity from OpenMetadata
        asset_type: Type of entity ("table", "pipeline", etc.)
        historical_versions: Historical versions for trend analysis (tables only)
        test_cases: Data quality test cases (tables only)
    
    Returns:
        List of detected anomaly types
    """
    if asset_type == "table":
        return evaluate_table_anomalies(
            asset_entity,
            historical_versions=historical_versions,
            test_cases=test_cases
        )
    elif asset_type == "pipeline":
        return evaluate_pipeline_anomalies(asset_entity)
    else:
        # Other asset types not yet supported
        return []
