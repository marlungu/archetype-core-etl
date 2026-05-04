"""Tests for the dead letter writer."""

from __future__ import annotations

from unittest.mock import MagicMock

from archetype_core_etl.common.dead_letter import DeadLetterWriter


def test_write_creates_ndjson_in_s3():
    mock_client = MagicMock()
    writer = DeadLetterWriter(bucket="test-bucket", client=mock_client)

    key = writer.write(
        stage="classification",
        pipeline_run_id="run-123",
        records=[{"record_id": "abc", "data": "test"}],
        error_message="Model returned invalid JSON",
    )

    assert key.startswith("s3://test-bucket/dead-letter/classification/")
    mock_client.put_object.assert_called_once()
    call_kwargs = mock_client.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "test-bucket"
    assert "run-123" in call_kwargs["Key"]
    assert call_kwargs["ContentType"] == "application/x-ndjson"


def test_write_empty_records_returns_empty():
    writer = DeadLetterWriter(bucket="test-bucket", client=MagicMock())
    key = writer.write(
        stage="classification",
        pipeline_run_id="run-123",
        records=[],
        error_message="nothing failed",
    )
    assert key == ""


def test_write_never_raises_on_s3_error():
    mock_client = MagicMock()
    mock_client.put_object.side_effect = Exception("S3 is down")
    writer = DeadLetterWriter(bucket="test-bucket", client=mock_client)

    key = writer.write(
        stage="classification",
        pipeline_run_id="run-123",
        records=[{"record_id": "abc"}],
        error_message="test error",
    )

    assert key == ""
