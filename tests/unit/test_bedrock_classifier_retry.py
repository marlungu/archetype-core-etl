"""Tests for BedrockClassifier._classify_with_retry attempt/backoff logic."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, call, patch

from archetype_core_etl.classify.bedrock_classifier import (
    BedrockClassifier,
    ClassificationResult,
)
from archetype_core_etl.common.exceptions import ClassificationError
from archetype_core_etl.extract.schema import FederalDocumentRecord

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
_MODEL = "us.anthropic.claude-sonnet-4-6"


_UUID_1 = "00000000-0000-0000-0000-000000000001"


def _make_record(record_id: str = _UUID_1) -> FederalDocumentRecord:
    return FederalDocumentRecord(
        record_id=record_id,
        agency="USCIS",
        applicant_id="app-1",
        case_status="open",
        priority_tier="standard",
        document_type="application",
        document_text="some text here for compliance review",
        pages=1,
        submitted_at=_NOW,
    )


def _make_result(record_id: str = _UUID_1) -> ClassificationResult:
    return ClassificationResult(
        record_id=record_id,
        compliance_score=0.85,
        risk_tier="low",
        policy_alignment="aligned",
        reasoning="ok",
        input_tokens=100,
        output_tokens=50,
        tokens_used=150,
        model_id=_MODEL,
        classified_at=_NOW,
    )


def _make_classifier() -> BedrockClassifier:
    """Return a BedrockClassifier whose Bedrock client is a MagicMock."""
    mock_client = MagicMock()
    return BedrockClassifier(client=mock_client, model_id=_MODEL)


# ---------------------------------------------------------------------------
# Test 1: max_attempts=1 — single try, no sleep, no retry
# ---------------------------------------------------------------------------


def test_max_attempts_1_tries_once_no_sleep():
    classifier = _make_classifier()
    record = _make_record()

    with (
        patch.object(
            classifier, "_classify_one", side_effect=ClassificationError("boom")
        ) as mock_one,
        patch("archetype_core_etl.classify.bedrock_classifier.time", autospec=True) as mock_time,
    ):
        result = classifier._classify_with_retry(record, max_attempts=1)

    assert result is None
    assert mock_one.call_count == 1
    mock_time.sleep.assert_not_called()


# ---------------------------------------------------------------------------
# Test 2: max_attempts=3 — three tries, two backoff sleeps (10.0 and 20.0)
# ---------------------------------------------------------------------------


def test_max_attempts_3_retries_twice_with_backoff():
    classifier = _make_classifier()
    record = _make_record()

    with (
        patch.object(
            classifier, "_classify_one", side_effect=ClassificationError("boom")
        ) as mock_one,
        patch("archetype_core_etl.classify.bedrock_classifier.time", autospec=True) as mock_time,
    ):
        result = classifier._classify_with_retry(record, max_attempts=3)

    assert result is None
    assert mock_one.call_count == 3
    assert mock_time.sleep.call_count == 2
    mock_time.sleep.assert_has_calls([call(10.0), call(20.0)])


# ---------------------------------------------------------------------------
# Test 3: success on second attempt — result returned, no further calls
# ---------------------------------------------------------------------------


def test_success_on_second_attempt():
    classifier = _make_classifier()
    record = _make_record()
    expected = _make_result()

    with (
        patch.object(
            classifier,
            "_classify_one",
            side_effect=[ClassificationError("transient"), expected],
        ) as mock_one,
        patch("archetype_core_etl.classify.bedrock_classifier.time", autospec=True) as mock_time,
    ):
        result = classifier._classify_with_retry(record, max_attempts=3)

    assert result is expected
    assert mock_one.call_count == 2
    assert mock_time.sleep.call_count == 1
    mock_time.sleep.assert_called_once_with(10.0)
