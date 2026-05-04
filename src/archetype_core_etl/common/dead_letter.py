"""Dead letter handler for failed pipeline records.

Records that fail at any stage — ingestion, quality gate, classification,
or load — are written to S3 as NDJSON files partitioned by stage and
timestamp. This ensures no data is silently lost and supports manual
inspection and replay of failed records.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import boto3

from archetype_core_etl.common.logging import get_logger

logger = get_logger(__name__)


class DeadLetterWriter:
    """Write failed records to an S3 dead letter path."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "dead-letter",
        client: Any | None = None,
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._client = client or boto3.client("s3")

    def write(
        self,
        *,
        stage: str,
        pipeline_run_id: str,
        records: list[dict[str, Any]],
        error_message: str,
    ) -> str:
        """Write failed records to S3.

        Returns the S3 key where the dead letter file was written.
        Dead letter writes never raise — if S3 is unavailable the failure
        is logged and an empty string is returned so the pipeline continues.
        """
        if not records:
            return ""

        now = datetime.now(tz=UTC)
        partition = now.strftime("%Y/%m/%d")
        timestamp = now.strftime("%Y%m%dT%H%M%S")
        key = f"{self._prefix}/{stage}/{partition}/{pipeline_run_id}_{timestamp}.ndjson"

        lines = []
        for record in records:
            entry = {
                "record": record,
                "failure_stage": stage,
                "pipeline_run_id": pipeline_run_id,
                "error_message": error_message,
                "failed_at": now.isoformat(),
            }
            lines.append(json.dumps(entry, default=str))

        body = "\n".join(lines)

        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/x-ndjson",
            )
        except Exception:
            logger.exception(
                "dead_letter.write_failed",
                extra={
                    "bucket": self._bucket,
                    "key": key,
                    "stage": stage,
                    "record_count": len(records),
                },
            )
            return ""

        logger.info(
            "dead_letter.write.complete",
            extra={
                "bucket": self._bucket,
                "key": key,
                "stage": stage,
                "record_count": len(records),
            },
        )
        return f"s3://{self._bucket}/{key}"


__all__ = ["DeadLetterWriter"]
