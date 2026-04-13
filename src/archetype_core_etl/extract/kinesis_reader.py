"""Kinesis stream reader with an in-memory checkpoint.

Reads records batch-by-batch from every shard of a stream, decoding each
record's base64/JSON payload into a dict. A simple
:class:`KinesisCheckpoint` tracks the last sequence number seen per shard
so subsequent invocations can resume with ``AFTER_SEQUENCE_NUMBER``.

Durable checkpoint storage is intentionally out of scope: callers that
need to survive process restarts should serialize
``reader.checkpoint.sequence_numbers`` to their own store (DynamoDB,
Postgres, S3) and rehydrate it before constructing the next reader.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, cast

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]

from archetype_core_etl.common.exceptions import ExtractionError
from archetype_core_etl.common.logging import get_logger
from archetype_core_etl.config import get_settings

logger = get_logger(__name__)

_DEFAULT_BATCH_SIZE = 500
_DEFAULT_ITERATOR_TYPE = "TRIM_HORIZON"


def _build_client() -> Any:
    """Construct a Kinesis client from the active application settings."""
    settings = get_settings()
    kwargs: dict[str, Any] = {"region_name": settings.aws.region}
    if settings.aws.endpoint_url:
        kwargs["endpoint_url"] = settings.aws.endpoint_url
    if settings.aws.access_key_id and settings.aws.secret_access_key:
        kwargs["aws_access_key_id"] = settings.aws.access_key_id.get_secret_value()
        kwargs["aws_secret_access_key"] = settings.aws.secret_access_key.get_secret_value()
    return boto3.client("kinesis", **kwargs)


@dataclass
class KinesisCheckpoint:
    """In-memory mapping of shard id -> last consumed sequence number."""

    sequence_numbers: dict[str, str] = field(default_factory=dict)

    def update(self, shard_id: str, sequence_number: str) -> None:
        self.sequence_numbers[shard_id] = sequence_number

    def get(self, shard_id: str) -> str | None:
        return self.sequence_numbers.get(shard_id)


class KinesisReader:
    """Batch reader for an AWS Kinesis stream."""

    def __init__(
        self,
        stream_name: str,
        *,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        checkpoint: KinesisCheckpoint | None = None,
        client: Any | None = None,
    ) -> None:
        self._stream_name = stream_name
        self._batch_size = batch_size
        self._checkpoint = checkpoint or KinesisCheckpoint()
        self._client = client or _build_client()

    @property
    def checkpoint(self) -> KinesisCheckpoint:
        return self._checkpoint

    def read_batches(self) -> Iterator[list[dict[str, Any]]]:
        """Yield batches of decoded records across every shard of the stream.

        For each shard, resumes from the last checkpointed sequence number
        (``AFTER_SEQUENCE_NUMBER``) when one is present, otherwise starts
        at ``TRIM_HORIZON``. After every successful ``get_records`` call
        the checkpoint is advanced to the latest sequence number in the
        batch.
        """
        for shard_id in self._list_shards():
            iterator = self._get_shard_iterator(shard_id)
            while iterator:
                records, iterator = self._get_records(shard_id, iterator)
                if not records:
                    break
                yield [self._decode(r) for r in records]

    def _list_shards(self) -> list[str]:
        try:
            response = self._client.list_shards(StreamName=self._stream_name)
            return [s["ShardId"] for s in response.get("Shards", [])]
        except (BotoCoreError, ClientError) as exc:
            logger.exception(
                "kinesis_reader.list_shards_failed",
                extra={"stream": self._stream_name},
            )
            raise ExtractionError(
                f"Failed to list shards for stream {self._stream_name}: {exc}"
            ) from exc

    def _get_shard_iterator(self, shard_id: str) -> str | None:
        last_seq = self._checkpoint.get(shard_id)
        try:
            if last_seq:
                response = self._client.get_shard_iterator(
                    StreamName=self._stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                    StartingSequenceNumber=last_seq,
                )
            else:
                response = self._client.get_shard_iterator(
                    StreamName=self._stream_name,
                    ShardId=shard_id,
                    ShardIteratorType=_DEFAULT_ITERATOR_TYPE,
                )
        except (BotoCoreError, ClientError) as exc:
            logger.exception(
                "kinesis_reader.get_shard_iterator_failed",
                extra={"stream": self._stream_name, "shard": shard_id},
            )
            raise ExtractionError(f"Failed to get shard iterator for {shard_id}: {exc}") from exc
        return cast(str | None, response.get("ShardIterator"))

    def _get_records(
        self,
        shard_id: str,
        iterator: str,
    ) -> tuple[list[dict[str, Any]], str | None]:
        try:
            response = self._client.get_records(
                ShardIterator=iterator,
                Limit=self._batch_size,
            )
        except (BotoCoreError, ClientError) as exc:
            logger.exception(
                "kinesis_reader.get_records_failed",
                extra={"stream": self._stream_name, "shard": shard_id},
            )
            raise ExtractionError(f"Failed to get records from shard {shard_id}: {exc}") from exc

        records = response.get("Records", [])
        if records:
            self._checkpoint.update(shard_id, records[-1]["SequenceNumber"])
            logger.info(
                "kinesis_reader.batch_received",
                extra={
                    "stream": self._stream_name,
                    "shard": shard_id,
                    "count": len(records),
                    "last_sequence": records[-1]["SequenceNumber"],
                },
            )
        return records, response.get("NextShardIterator")

    @staticmethod
    def _decode(record: dict[str, Any]) -> dict[str, Any]:
        data = record["Data"]
        payload = data if isinstance(data, (bytes, bytearray)) else base64.b64decode(data)
        try:
            return cast(dict[str, Any], json.loads(payload))
        except json.JSONDecodeError as exc:
            raise ExtractionError(
                f"Malformed JSON in Kinesis record {record.get('SequenceNumber')}: {exc}"
            ) from exc


__all__ = ["KinesisCheckpoint", "KinesisReader"]
