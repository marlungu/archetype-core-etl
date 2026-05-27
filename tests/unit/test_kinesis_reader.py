"""Tests for KinesisReader shard decoding and error isolation."""

from __future__ import annotations

import base64
import json
import logging
from unittest.mock import MagicMock

import pytest

from archetype_core_etl.extract.kinesis_reader import KinesisCheckpoint, KinesisReader


def test_kinesis_checkpoint_update_and_get() -> None:
    checkpoint = KinesisCheckpoint()
    assert checkpoint.get("shard-1") is None
    checkpoint.update("shard-1", "seq-123")
    assert checkpoint.get("shard-1") == "seq-123"


def test_kinesis_reader_read_batches_success() -> None:
    mock_client = MagicMock()
    mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shard-0001"}]}
    mock_client.get_shard_iterator.return_value = {"ShardIterator": "iterator-abc"}

    # 1. First call to get_records returns one record and next iterator
    # 2. Second call returns no records, breaking the loop for the shard
    record_data = json.dumps({"record_id": "rec-1", "content": "hello"}).encode("utf-8")
    mock_client.get_records.side_effect = [
        {
            "Records": [
                {
                    "SequenceNumber": "seq-999",
                    "Data": base64.b64encode(record_data).decode("utf-8"),
                }
            ],
            "NextShardIterator": "iterator-def",
        },
        {"Records": [], "NextShardIterator": None},
    ]

    reader = KinesisReader(stream_name="my-stream", client=mock_client)
    batches = list(reader.read_batches())

    assert len(batches) == 1
    assert len(batches[0]) == 1
    record = batches[0][0]
    assert record["record_id"] == "rec-1"
    assert record["_source_bucket"] == "my-stream"
    assert record["_source_key"] == "shard-shard-0001/seq-999"
    assert record["_source_line_number"] == 1
    assert reader.checkpoint.get("shard-0001") == "seq-999"


def test_kinesis_reader_read_batches_json_error_isolated(caplog: pytest.LogCaptureFixture) -> None:
    mock_client = MagicMock()
    mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shard-0002"}]}
    mock_client.get_shard_iterator.return_value = {"ShardIterator": "iterator-xyz"}

    # One bad record (invalid json) followed by one good record in the same batch
    bad_data = b"invalid json structure{"
    good_data = json.dumps({"record_id": "rec-good"}).encode("utf-8")

    mock_client.get_records.side_effect = [
        {
            "Records": [
                {
                    "SequenceNumber": "seq-bad",
                    "Data": base64.b64encode(bad_data).decode("utf-8"),
                },
                {
                    "SequenceNumber": "seq-good",
                    "Data": base64.b64encode(good_data).decode("utf-8"),
                },
            ],
            "NextShardIterator": "iterator-next",
        },
        {"Records": [], "NextShardIterator": None},
    ]

    reader = KinesisReader(stream_name="my-stream", client=mock_client)
    with caplog.at_level(logging.ERROR):
        batches = list(reader.read_batches())

    # The bad record must be skipped, but the good record should succeed!
    assert len(batches) == 1
    assert len(batches[0]) == 1
    assert batches[0][0]["record_id"] == "rec-good"
    assert batches[0][0]["_source_key"] == "shard-shard-0002/seq-good"

    # Checkpoint should still advance to the last sequence number processed in get_records
    assert reader.checkpoint.get("shard-0002") == "seq-good"

    # Verify that the JSON decoding error was logged
    error_logs = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert any("kinesis_reader.decode_failed" in log for log in error_logs)
