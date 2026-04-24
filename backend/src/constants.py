"""Global constants for Data Doctor."""
from enum import StrEnum


class AnomalyType(StrEnum):
    """Types of anomalies that can be detected."""
    PIPELINE_FAILURE = "pipeline_failure"
    STALE_DATA = "stale_data"
    SCHEMA_CHANGE = "schema_change"
    DATA_QUALITY_FAILURE = "data_quality_failure"
    VOLUME_ANOMALY = "volume_anomaly"
    DISTRIBUTION_DRIFT = "distribution_drift"


class Severity(StrEnum):
    """Severity levels for incidents."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class FixAction(StrEnum):
    """Types of suggested fix actions."""
    RERUN_PIPELINE = "rerun_pipeline"
    UPDATE_SCHEMA = "update_schema"
    FORCE_BACKFILL = "force_backfill"
    QUARANTINE_DATA = "quarantine_data"


class EntityType(StrEnum):
    """OpenMetadata entity types."""
    TABLE = "table"
    PIPELINE = "pipeline"
    DASHBOARD = "dashboard"
    ML_MODEL = "mlmodel"
    TOPIC = "topic"


# Lineage traversal depths
DEFAULT_UPSTREAM_DEPTH = 5
DEFAULT_DOWNSTREAM_DEPTH = 5

# Confidence scoring constants
BASE_CONFIDENCE_SCORE = 0.5
HIGH_SIGNAL_BOOST = 0.3
LOW_SIGNAL_BOOST = 0.1
DEPTH_ONE_BOOST = 0.2
NOISE_PENALTY = 0.1

# High-signal anomaly types for confidence scoring
HIGH_SIGNAL_TYPES = [AnomalyType.PIPELINE_FAILURE, AnomalyType.SCHEMA_CHANGE, AnomalyType.VOLUME_ANOMALY]

# Data freshness SLA (hours)
DEFAULT_FRESHNESS_SLA_HOURS = 48

# Volume anomaly detection thresholds (standard deviations)
VOLUME_ANOMALY_STD_DEV_THRESHOLD = 2.0
VOLUME_ANOMALY_MIN_HISTORY_DAYS = 7

# Distribution drift detection thresholds
DISTRIBUTION_DRIFT_NULL_THRESHOLD = 0.15  # 15% change in null proportion
DISTRIBUTION_DRIFT_DISTINCT_THRESHOLD = 0.30  # 30% change in distinct count
DISTRIBUTION_DRIFT_MIN_HISTORY_DAYS = 7
