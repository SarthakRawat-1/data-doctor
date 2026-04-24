"""Confidence scoring module for diagnosis reliability.

Implements deterministic scoring from data_doctor.md Section 6:
- Base score: 0.5
- Type modifier: +0.3 (high signal) or +0.1 (low signal)
- Distance modifier: +0.2 if depth == 1
- Noise penalty: -0.1 per contributing factor
- Clamped to [0.0, 1.0]

Phase 3 Implementation.
"""
from src.constants import (
    BASE_CONFIDENCE_SCORE,
    DEPTH_ONE_BOOST,
    HIGH_SIGNAL_BOOST,
    HIGH_SIGNAL_TYPES,
    LOW_SIGNAL_BOOST,
    NOISE_PENALTY,
)
from src.schemas import AnomalyDetail


def calculate_confidence_score(
    primary_cause: AnomalyDetail | None,
    contributing_factors: list[AnomalyDetail]
) -> float:
    """
    Calculate deterministic confidence score.
    
    Formula:
    - Start with base score (0.5)
    - Add type modifier based on signal strength
    - Add distance modifier if immediately upstream
    - Subtract noise penalty for each contributing factor
    - Clamp result to [0.0, 1.0]
    
    Args:
        primary_cause: Primary root cause anomaly
        contributing_factors: List of contributing anomalies
    
    Returns:
        Confidence score between 0.0 and 1.0
    """
    if not primary_cause:
        return 0.0
    
    score = BASE_CONFIDENCE_SCORE
    
    # Type modifier: high-signal types get bigger boost
    if primary_cause.type in HIGH_SIGNAL_TYPES:
        score += HIGH_SIGNAL_BOOST
    else:
        score += LOW_SIGNAL_BOOST
    
    # Distance modifier: immediately upstream is more certain
    if primary_cause.depth == 1:
        score += DEPTH_ONE_BOOST
    
    # Noise penalty: multiple anomalies reduce certainty
    penalty = NOISE_PENALTY * len(contributing_factors)
    score -= penalty
    
    # Clamp to valid range
    return max(0.0, min(1.0, score))
