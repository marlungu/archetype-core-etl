"""Shared pytest fixtures for archetype-core-etl test suites."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest


@pytest.fixture()
def valid_record_dict() -> dict:
    """A minimal, valid raw record dict for FederalDocumentRecord."""
    return {
        "record_id": str(uuid.uuid4()),
        "submitted_at": datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc).isoformat(),
        "document_type": "application",
        "agency": "USCIS",
        "applicant_id": "A-12345678",
        "case_status": "pending",
        "priority_tier": "standard",
        "document_text": "This is a valid federal document with enough characters.",
        "form_number": "I-130",
        "pages": 3,
        "flags": ["expedite_requested"],
        "officer_notes": "Reviewed on intake.",
    }


@pytest.fixture()
def valid_record_batch(valid_record_dict: dict) -> list[dict]:
    """A batch of 3 valid records with distinct record_ids."""
    records = []
    for _ in range(3):
        record = dict(valid_record_dict)
        record["record_id"] = str(uuid.uuid4())
        records.append(record)
    return records
