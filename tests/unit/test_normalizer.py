"""Tests for archetype_core_etl.transform.normalizer."""

from __future__ import annotations

import pytest

from archetype_core_etl.common.exceptions import TransformationError
from archetype_core_etl.transform.normalizer import normalize_record


class TestNormalizeRecordValid:
    def test_valid_record_normalizes(self, valid_record_dict):
        result = normalize_record(valid_record_dict)
        assert str(result.record_id) == valid_record_dict["record_id"]
        assert result.agency == "USCIS"

    def test_whitespace_is_collapsed(self, valid_record_dict):
        valid_record_dict["document_text"] = "  lots   of    spaces   here  "
        result = normalize_record(valid_record_dict)
        assert result.document_text == "lots of spaces here"

    def test_agency_is_uppercased(self, valid_record_dict):
        valid_record_dict["agency"] = "uscis"
        result = normalize_record(valid_record_dict)
        assert result.agency == "USCIS"

    def test_case_status_is_lowercased(self, valid_record_dict):
        valid_record_dict["case_status"] = "PENDING"
        result = normalize_record(valid_record_dict)
        assert result.case_status == "pending"

    def test_priority_tier_is_lowercased(self, valid_record_dict):
        valid_record_dict["priority_tier"] = "EXPEDITE"
        result = normalize_record(valid_record_dict)
        assert result.priority_tier == "expedite"

    def test_document_text_truncated_at_10000(self, valid_record_dict):
        valid_record_dict["document_text"] = "A" * 15_000
        result = normalize_record(valid_record_dict)
        assert len(result.document_text) == 10_000

    def test_input_dict_not_mutated(self, valid_record_dict):
        original_agency = valid_record_dict["agency"]
        normalize_record(valid_record_dict)
        assert valid_record_dict["agency"] == original_agency


class TestNormalizeRecordErrors:
    def test_missing_required_field_raises_transformation_error(self, valid_record_dict):
        del valid_record_dict["record_id"]
        with pytest.raises(TransformationError, match="<unknown>"):
            normalize_record(valid_record_dict)

    def test_missing_field_with_record_id_in_message(self, valid_record_dict):
        record_id = valid_record_dict["record_id"]
        del valid_record_dict["agency"]
        with pytest.raises(TransformationError, match=record_id):
            normalize_record(valid_record_dict)

    def test_invalid_agency_raises_transformation_error(self, valid_record_dict):
        valid_record_dict["agency"] = "NASA"
        with pytest.raises(TransformationError):
            normalize_record(valid_record_dict)
