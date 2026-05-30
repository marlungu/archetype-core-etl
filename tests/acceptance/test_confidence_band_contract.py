"""Acceptance tests for the confidence-band routing contract.

These assert the externally visible behaviour of
:func:`archetype_core_etl.transform.quality_gate.confidence_band` against the
approved spec: routing, reason text, boundary handling, validation errors,
and the stability of the band string values.

Reason expectations are built by interpolating the module's own threshold
constants, so the tests stay correct if a threshold is ever moved.
"""

from __future__ import annotations

import pytest

from archetype_core_etl.transform.quality_gate import (
    AUTO_APPROVE_THRESHOLD,
    REJECT_THRESHOLD,
    BandDecision,
    confidence_band,
)


# Criterion 1: high confidence routes to auto_approve with matching reason.
def test_high_confidence_auto_approves() -> None:
    confidence = 0.95
    result = confidence_band(confidence)
    assert result.band == "auto_approve"
    assert result.reason == (
        f"confidence {confidence} at or above auto-approve threshold "
        f"{AUTO_APPROVE_THRESHOLD}"
    )


# Criterion 2: mid confidence routes to human_review with matching reason.
def test_mid_confidence_human_review() -> None:
    confidence = 0.72
    result = confidence_band(confidence)
    assert result.band == "human_review"
    assert result.reason == (
        f"confidence {confidence} between reject threshold {REJECT_THRESHOLD} "
        f"and auto-approve threshold {AUTO_APPROVE_THRESHOLD}"
    )


# Criterion 3: low confidence routes to reject with matching reason.
def test_low_confidence_rejects() -> None:
    confidence = 0.40
    result = confidence_band(confidence)
    assert result.band == "reject"
    assert result.reason == (
        f"confidence {confidence} below reject threshold {REJECT_THRESHOLD}"
    )


# Criterion 4: the auto-approve threshold itself is inclusive.
def test_auto_approve_threshold_is_inclusive() -> None:
    assert confidence_band(AUTO_APPROVE_THRESHOLD).band == "auto_approve"


# Criterion 5: just below the auto-approve threshold falls to human_review.
def test_just_below_auto_approve_is_human_review() -> None:
    assert confidence_band(0.8499).band == "human_review"


# Criterion 6: the reject threshold itself routes to human_review.
def test_reject_threshold_is_human_review() -> None:
    assert confidence_band(REJECT_THRESHOLD).band == "human_review"


# Criterion 7: just below the reject threshold routes to reject.
def test_just_below_reject_threshold_rejects() -> None:
    assert confidence_band(0.5999).band == "reject"


# Criterion 8: confidence outside [0.0, 1.0] raises ValueError with the
# spec'd message.
@pytest.mark.parametrize("bad_confidence", [-0.01, 1.01, 2.0])
def test_out_of_range_confidence_raises(bad_confidence: float) -> None:
    with pytest.raises(ValueError) as excinfo:
        confidence_band(bad_confidence)
    assert str(excinfo.value) == (
        f"confidence must be in [0.0, 1.0], got {bad_confidence}"
    )


# Criterion 9: the return type is BandDecision.
def test_return_type_is_band_decision() -> None:
    assert isinstance(confidence_band(0.95), BandDecision)


# Criterion 10: the three band string values are exactly as published.
def test_band_values_are_unchanged() -> None:
    assert confidence_band(0.95).band == "auto_approve"
    assert confidence_band(0.72).band == "human_review"
    assert confidence_band(0.40).band == "reject"
