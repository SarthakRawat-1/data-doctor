"""Tests for confidence scoring module."""
import pytest

from src.constants import AnomalyType
from src.core.confidence import calculate_confidence_score
from src.schemas import AnomalyDetail


def test_no_primary_cause_returns_zero():
    """Test that no primary cause returns 0.0 confidence."""
    score = calculate_confidence_score(None, [])
    assert score == 0.0


def test_high_signal_depth_one_no_noise():
    """Test high signal anomaly at depth 1 with no noise."""
    primary = AnomalyDetail(
        type=AnomalyType.PIPELINE_FAILURE,
        name="test_pipeline",
        depth=1
    )
    # Base (0.5) + High Signal (0.3) + Depth 1 (0.2) = 1.0
    score = calculate_confidence_score(primary, [])
    assert score == 1.0


def test_low_signal_depth_two_with_noise():
    """Test low signal anomaly at depth 2 with contributing factors."""
    primary = AnomalyDetail(
        type=AnomalyType.STALE_DATA,
        name="test_table",
        depth=2
    )
    contributing = [
        AnomalyDetail(type=AnomalyType.DATA_QUALITY_FAILURE, name="other", depth=3)
    ]
    # Base (0.5) + Low Signal (0.1) + No Depth Boost (0) - Noise (0.1) = 0.5
    score = calculate_confidence_score(primary, contributing)
    assert score == 0.5


def test_score_clamped_to_range():
    """Test that score is always clamped between 0.0 and 1.0."""
    primary = AnomalyDetail(
        type=AnomalyType.SCHEMA_CHANGE,
        name="test",
        depth=1
    )
    # Even with many contributing factors, should not go below 0.0
    many_contributing = [
        AnomalyDetail(type=AnomalyType.STALE_DATA, name=f"factor_{i}", depth=i+2)
        for i in range(20)
    ]
    score = calculate_confidence_score(primary, many_contributing)
    assert 0.0 <= score <= 1.0
