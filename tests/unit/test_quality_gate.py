"""Tests for archetype_core_etl.transform.quality_gate."""

from __future__ import annotations

import uuid
from copy import deepcopy

import pytest

from archetype_core_etl.transform.quality_gate import GateResult, QualityGate


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
        agency_failures = [
            d for d in result.failure_details if d["column"] == "agency"
        ]
        assert len(agency_failures) == 1
        assert "NASA" in agency_failures[0]["unexpected_values"]

    def test_pages_less_than_one_fails(self, valid_record_batch):
        batch = deepcopy(valid_record_batch)
        batch[1]["pages"] = 0
        gate = QualityGate()
        result = gate.validate(batch)
        assert result.passed is False
        pages_failures = [
            d for d in result.failure_details if d["column"] == "pages"
        ]
        assert len(pages_failures) == 1
        assert pages_failures[0]["unexpected_count"] >= 1

    def test_null_record_id_fails(self, valid_record_batch):
        batch = deepcopy(valid_record_batch)
        batch[2]["record_id"] = None
        gate = QualityGate()
        result = gate.validate(batch)
        assert result.passed is False
        rid_failures = [
            d for d in result.failure_details if d["column"] == "record_id"
        ]
        assert len(rid_failures) == 1

    def test_short_document_text_fails(self, valid_record_batch):
        batch = deepcopy(valid_record_batch)
        batch[0]["document_text"] = "short"
        gate = QualityGate()
        result = gate.validate(batch)
        assert result.passed is False
        text_failures = [
            d for d in result.failure_details if d["column"] == "document_text"
        ]
        assert len(text_failures) == 1
