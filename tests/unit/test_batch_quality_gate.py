"""Tests for the field-presence check used by the run_quality_gate DAG task."""

from __future__ import annotations

from archetype_core_etl.transform.field_presence import check_required_fields

_REQUIRED = {
    "record_id",
    "agency",
    "applicant_id",
    "case_status",
    "priority_tier",
    "document_type",
    "document_text",
    "pages",
    "submitted_at",
}


def _base_record() -> dict:
    return {
        "record_id": "rec-1",
        "agency": "DHS",
        "applicant_id": "app-1",
        "case_status": "open",
        "priority_tier": "tier_1",
        "document_type": "application",
        "document_text": "some text",
        "pages": 1,
        "submitted_at": "2024-01-01T00:00:00Z",
    }


# ---------------------------------------------------------------------------
# Test 1: pages=0 is falsy but must pass (key is present and value is not None)
# ---------------------------------------------------------------------------


def test_pages_zero_passes():
    record = _base_record()
    record["pages"] = 0

    missing = check_required_fields(record, _REQUIRED)

    assert missing == [], f"Expected no missing fields, got: {missing}"


# ---------------------------------------------------------------------------
# Test 2: absent key is detected as missing
# ---------------------------------------------------------------------------


def test_absent_key_fails():
    record = _base_record()
    del record["applicant_id"]

    missing = check_required_fields(record, _REQUIRED)

    assert len(missing) == 1 and missing[0] == "applicant_id", (
        f"Expected ['applicant_id'], got: {missing}"
    )


# ---------------------------------------------------------------------------
# Test 3: key present with value None is detected as missing
# ---------------------------------------------------------------------------


def test_explicit_none_fails():
    record = _base_record()
    record["applicant_id"] = None

    missing = check_required_fields(record, _REQUIRED)

    assert len(missing) == 1 and missing[0] == "applicant_id", (
        f"Expected ['applicant_id'], got: {missing}"
    )
