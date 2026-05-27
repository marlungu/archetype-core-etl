"""Tests for AuditWriter._build_entries record-id-keyed lookup."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from archetype_core_etl.classify.bedrock_classifier import ClassificationResult
from archetype_core_etl.load.audit_writer import AuditWriter

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
_MODEL = "us.anthropic.claude-sonnet-4-6"


def _result(record_id: str) -> ClassificationResult:
    return ClassificationResult(
        record_id=record_id,
        compliance_score=0.9,
        risk_tier="low",
        policy_alignment="aligned",
        reasoning="ok",
        input_tokens=100,
        output_tokens=50,
        tokens_used=150,
        model_id=_MODEL,
        classified_at=_NOW,
    )


def _record_dict(record_id: str) -> dict:
    return {
        "record_id": record_id,
        "agency": "DHS",
        "applicant_id": f"app-{record_id}",
        "case_status": "open",
        "priority_tier": "tier_1",
        "document_type": "application",
        "document_text": f"text for {record_id}",
        "pages": 3,
        "submitted_at": "2024-01-01T00:00:00Z",
    }


def _sha256(d: dict) -> str:
    return hashlib.sha256(json.dumps(d, sort_keys=True, default=str).encode()).hexdigest()


def _make_writer() -> AuditWriter:
    writer = AuditWriter(dsn="postgresql://unused/unused")
    writer._table_ready = True  # skip DDL
    return writer


def _submitted_at(*record_ids: str) -> dict[str, datetime]:
    return dict.fromkeys(record_ids, _NOW)


# ---------------------------------------------------------------------------
# Test 1: partial classification success — B skipped, C must map to C's hash
# ---------------------------------------------------------------------------


def test_partial_classification_correct_hashes():
    rec_a = _record_dict("A")
    rec_b = _record_dict("B")
    rec_c = _record_dict("C")

    result_a = _result("A")
    result_c = _result("C")

    captured_rows: list = []

    def fake_execute_values(cur, sql, rows):
        captured_rows.extend(rows)

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_cur.__enter__ = MagicMock(return_value=mock_cur)
    mock_cur.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cur

    with (
        patch("archetype_core_etl.load.audit_writer.psycopg2.connect", return_value=mock_conn),
        patch(
            "archetype_core_etl.load.audit_writer.execute_values", side_effect=fake_execute_values
        ),
    ):
        writer = _make_writer()
        writer.write(
            pipeline_run_id="run-1",
            results=[result_a, result_c],
            submitted_at_by_record=_submitted_at("A", "C"),
            quality_gate_passed=True,
            input_records=[rec_a, rec_b, rec_c],
        )

    # rows are tuples; input_record_hash is index 16
    hashes_by_id = {row[0]: row[16] for row in captured_rows}

    assert hashes_by_id["C"] == _sha256(rec_c), "C must map to rec_c's hash"
    assert _sha256(rec_b) not in hashes_by_id.values(), "rec_b hash must not appear"


# ---------------------------------------------------------------------------
# Test 2: missing record in lookup logs warning and uses fallback hash
# ---------------------------------------------------------------------------


def test_missing_lookup_logs_warning_and_fallback(caplog):
    result_x = _result("X")
    result_y = _result("Y")

    # Only provide input_record for X; Y is missing
    rec_x = _record_dict("X")

    captured_rows: list = []

    def fake_execute_values(cur, sql, rows):
        captured_rows.extend(rows)

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_cur.__enter__ = MagicMock(return_value=mock_cur)
    mock_cur.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cur

    import logging

    with (
        patch("archetype_core_etl.load.audit_writer.psycopg2.connect", return_value=mock_conn),
        patch(
            "archetype_core_etl.load.audit_writer.execute_values", side_effect=fake_execute_values
        ),
        caplog.at_level(logging.WARNING),
    ):
        writer = _make_writer()
        writer.write(
            pipeline_run_id="run-2",
            results=[result_x, result_y],
            submitted_at_by_record=_submitted_at("X", "Y"),
            quality_gate_passed=True,
            input_records=[rec_x],
        )

    warning_messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any("audit_writer.input_lookup_miss" in m for m in warning_messages), (
        "Expected a warning with action 'audit_writer.input_lookup_miss'"
    )

    hashes_by_id = {row[0]: row[16] for row in captured_rows}
    fallback_hash = _sha256({"record_id": "Y"})
    assert hashes_by_id["Y"] == fallback_hash, "Y must use fallback hash"


# ---------------------------------------------------------------------------
# Test 3: empty input_records list — fallback hash + one warning per result
# ---------------------------------------------------------------------------


def test_empty_input_records_uses_fallback_for_all(caplog):
    result_p = _result("P")
    result_q = _result("Q")

    captured_rows: list = []

    def fake_execute_values(cur, sql, rows):
        captured_rows.extend(rows)

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_cur.__enter__ = MagicMock(return_value=mock_cur)
    mock_cur.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cur

    import logging

    with (
        patch("archetype_core_etl.load.audit_writer.psycopg2.connect", return_value=mock_conn),
        patch(
            "archetype_core_etl.load.audit_writer.execute_values", side_effect=fake_execute_values
        ),
        caplog.at_level(logging.WARNING),
    ):
        writer = _make_writer()
        writer.write(
            pipeline_run_id="run-3",
            results=[result_p, result_q],
            submitted_at_by_record=_submitted_at("P", "Q"),
            quality_gate_passed=True,
            input_records=[],
        )

    hashes_by_id = {row[0]: row[16] for row in captured_rows}
    assert hashes_by_id["P"] == _sha256({"record_id": "P"})
    assert hashes_by_id["Q"] == _sha256({"record_id": "Q"})

    miss_warnings = [
        r
        for r in caplog.records
        if r.levelname == "WARNING" and "audit_writer.input_lookup_miss" in r.getMessage()
    ]
    assert len(miss_warnings) == 2, "Expected one warning per missing record"
