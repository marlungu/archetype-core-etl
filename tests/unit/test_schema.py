"""Tests for archetype_core_etl.extract.schema.FederalDocumentRecord."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from archetype_core_etl.extract.schema import FederalDocumentRecord


class TestFederalDocumentRecordValid:
    def test_instantiates_with_all_fields(self, valid_record_dict):
        record = FederalDocumentRecord.model_validate(valid_record_dict)
        assert record.agency == "USCIS"
        assert record.pages == 3
        assert record.form_number == "I-130"
        assert isinstance(record.record_id, uuid.UUID)

    def test_optional_fields_default_to_none_or_empty(self):
        minimal = {
            "record_id": str(uuid.uuid4()),
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "document_type": "notice",
            "agency": "CBP",
            "applicant_id": "B-99999999",
            "case_status": "approved",
            "priority_tier": "expedite",
            "document_text": "A notice document with sufficient length.",
            "pages": 1,
        }
        record = FederalDocumentRecord.model_validate(minimal)
        assert record.form_number is None
        assert record.officer_notes is None
        assert record.flags == []


class TestFederalDocumentRecordRejection:
    def test_extra_fields_rejected(self, valid_record_dict):
        valid_record_dict["surprise_field"] = "should not be here"
        with pytest.raises(ValidationError, match="surprise_field"):
            FederalDocumentRecord.model_validate(valid_record_dict)

    def test_document_text_below_min_length(self, valid_record_dict):
        valid_record_dict["document_text"] = "short"
        with pytest.raises(ValidationError, match="document_text"):
            FederalDocumentRecord.model_validate(valid_record_dict)

    def test_invalid_priority_tier(self, valid_record_dict):
        valid_record_dict["priority_tier"] = "critical"
        with pytest.raises(ValidationError, match="priority_tier"):
            FederalDocumentRecord.model_validate(valid_record_dict)

    def test_invalid_agency(self, valid_record_dict):
        valid_record_dict["agency"] = "NASA"
        with pytest.raises(ValidationError, match="agency"):
            FederalDocumentRecord.model_validate(valid_record_dict)

    def test_pages_less_than_one(self, valid_record_dict):
        valid_record_dict["pages"] = 0
        with pytest.raises(ValidationError, match="pages"):
            FederalDocumentRecord.model_validate(valid_record_dict)
