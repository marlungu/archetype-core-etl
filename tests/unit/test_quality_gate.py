"""Tests for archetype_core_etl.transform.quality_gate."""

from __future__ import annotations

from copy import deepcopy

import pytest

from archetype_core_etl.transform.quality_gate import (
    AUTO_APPROVE_THRESHOLD,
    REJECT_THRESHOLD,
    BandDecision,
    GateResult,
    QualityGate,
    confidence_band,
)


class TestQualityGatePass:
    def test_empty_batch_passes(self):
        gate = QualityGate()
        result = gate.validate([])
        assert result == GateResult(passed=True, total=0, failed=0, failure_details=[])

    def test_all_valid_batch_passes(self, valid_record_batch):
        gate = QualityGate()
        result = gate.validate(valid_record_batch)
        assert result.passed is True
        assert result.total == 3
        assert result.failed == 0
        assert result.failure_details == []


class TestQualityGateFail:
    def test_invalid_agency_fails(self, valid_record_batch):
        batch = deepcopy(valid_record_batch)
        batch[0]["agency"] = "NASA"
        gate = QualityGate()
        result = gate.validate(batch)
        assert result.passed is False
        assert result.failed >= 1
        agency_failures = [d for d in result.failure_details if d["column"] == "agency"]
        assert len(agency_failures) == 1
        assert "NASA" in agency_failures[0]["unexpected_values"]

    def test_pages_less_than_one_fails(self, valid_record_batch):
        batch = deepcopy(valid_record_batch)
        batch[1]["pages"] = 0
        gate = QualityGate()
        result = gate.validate(batch)
        assert result.passed is False
        pages_failures = [d for d in result.failure_details if d["column"] == "pages"]
        assert len(pages_failures) == 1
        assert pages_failures[0]["unexpected_count"] >= 1

    def test_null_record_id_fails(self, valid_record_batch):
        batch = deepcopy(valid_record_batch)
        batch[2]["record_id"] = None
        gate = QualityGate()
        result = gate.validate(batch)
        assert result.passed is False
        rid_failures = [d for d in result.failure_details if d["column"] == "record_id"]
        assert len(rid_failures) == 1

    def test_short_document_text_fails(self, valid_record_batch):
        batch = deepcopy(valid_record_batch)
        batch[0]["document_text"] = "short"
        gate = QualityGate()
        result = gate.validate(batch)
        assert result.passed is False
        text_failures = [d for d in result.failure_details if d["column"] == "document_text"]
        assert len(text_failures) == 1


class TestConfidenceBand:
    def test_high_confidence_auto_approves(self):
        result = confidence_band(0.95)
        assert result.band == "auto_approve"
        assert result.reason == (
            f"confidence {0.95} at or above auto-approve threshold {AUTO_APPROVE_THRESHOLD}"
        )

    def test_mid_confidence_routes_to_human_review(self):
        result = confidence_band(0.72)
        assert result.band == "human_review"
        assert result.reason == (
            f"confidence {0.72} between reject threshold {REJECT_THRESHOLD} "
            f"and auto-approve threshold {AUTO_APPROVE_THRESHOLD}"
        )

    def test_low_confidence_rejects(self):
        result = confidence_band(0.40)
        assert result.band == "reject"
        assert result.reason == (f"confidence {0.40} below reject threshold {REJECT_THRESHOLD}")

    def test_auto_approve_threshold_is_inclusive(self):
        result = confidence_band(AUTO_APPROVE_THRESHOLD)
        assert result.band == "auto_approve"
        assert result.reason == (
            f"confidence {AUTO_APPROVE_THRESHOLD} at or above auto-approve threshold "
            f"{AUTO_APPROVE_THRESHOLD}"
        )

    def test_just_below_auto_approve_is_human_review(self):
        result = confidence_band(0.8499)
        assert result.band == "human_review"
        assert result.reason == (
            f"confidence {0.8499} between reject threshold {REJECT_THRESHOLD} "
            f"and auto-approve threshold {AUTO_APPROVE_THRESHOLD}"
        )

    def test_reject_threshold_is_human_review(self):
        # The boundary itself is the floor of human review, not a reject.
        result = confidence_band(REJECT_THRESHOLD)
        assert result.band == "human_review"
        assert result.reason == (
            f"confidence {REJECT_THRESHOLD} between reject threshold {REJECT_THRESHOLD} "
            f"and auto-approve threshold {AUTO_APPROVE_THRESHOLD}"
        )

    def test_just_below_reject_threshold_rejects(self):
        result = confidence_band(0.5999)
        assert result.band == "reject"
        assert result.reason == (f"confidence {0.5999} below reject threshold {REJECT_THRESHOLD}")

    def test_returns_band_decision(self):
        assert isinstance(confidence_band(0.95), BandDecision)

    @pytest.mark.parametrize("score", [-0.01, 1.01, 2.0])
    def test_out_of_range_raises(self, score):
        with pytest.raises(ValueError):
            confidence_band(score)
