"""S3 record reader supporting batch and date-incremental modes.

Reads newline-delimited JSON (NDJSON) objects out of an S3 bucket and
yields them as dicts. The caller is responsible for validating each dict
against :class:`archetype_core_etl.extract.schema.FederalDocumentRecord`.

All AWS configuration — region, endpoint override, and optional static
credentials — is pulled from :func:`get_settings`, so LocalStack and
production share the same code path.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from archetype_core_etl.common.aws import build_boto3_client
from archetype_core_etl.common.exceptions import ExtractionError
from archetype_core_etl.common.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_PAGE_SIZE = 1000


class S3Reader:
    """Stream NDJSON records from an S3 bucket.

    Two usage patterns:

    * :meth:`read_batch` iterates every object under a fixed prefix.
    * :meth:`read_incremental` iterates only the keys whose prefix matches
      a specific calendar day (``<prefix>/YYYY/MM/DD/``).
    """

    def __init__(
        self,
        bucket: str,
        *,
        page_size: int = _DEFAULT_PAGE_SIZE,
        client: Any | None = None,
    ) -> None:
        self._bucket = bucket
        self._page_size = page_size
        self._client = client or build_boto3_client("s3")

    def read_batch(self, prefix: str = "") -> Iterator[dict[str, Any]]:
        """Yield every record under ``prefix`` as a decoded dict."""
        logger.info(
            "s3_reader.read_batch.start",
            extra={"bucket": self._bucket, "prefix": prefix},
        )
        count = 0
        for record in self._iter_records(prefix):
            count += 1
            yield record
        logger.info(
            "s3_reader.read_batch.complete",
            extra={"bucket": self._bucket, "prefix": prefix, "records": count},
        )

    def read_incremental(
        self,
        prefix: str,
        target_date: date,
    ) -> Iterator[dict[str, Any]]:
        """Yield records written under ``<prefix>/YYYY/MM/DD/`` for one day."""
        dated_prefix = f"{prefix.rstrip('/')}/{target_date:%Y/%m/%d}/"
        logger.info(
            "s3_reader.read_incremental.start",
            extra={
                "bucket": self._bucket,
                "prefix": dated_prefix,
                "target_date": target_date.isoformat(),
            },
        )
        count = 0
        for record in self._iter_records(dated_prefix):
            count += 1
            yield record
        logger.info(
            "s3_reader.read_incremental.complete",
            extra={
                "bucket": self._bucket,
                "prefix": dated_prefix,
                "target_date": target_date.isoformat(),
                "records": count,
            },
        )

    def _iter_records(self, prefix: str) -> Iterator[dict[str, Any]]:
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self._bucket,
                Prefix=prefix,
                PaginationConfig={"PageSize": self._page_size},
            )
            for page in pages:
                for obj in page.get("Contents", []):
                    yield from self._read_object(obj["Key"])
        except (BotoCoreError, ClientError) as exc:
            logger.exception(
                "s3_reader.list_failed",
                extra={"bucket": self._bucket, "prefix": prefix},
            )
            raise ExtractionError(f"Failed to list s3://{self._bucket}/{prefix}: {exc}") from exc

    def _read_object(self, key: str) -> Iterator[dict[str, Any]]:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            body = response["Body"]
            for raw_line in body.iter_lines():
                if not raw_line:
                    continue
                yield json.loads(raw_line)
        except (BotoCoreError, ClientError) as exc:
            logger.exception(
                "s3_reader.get_failed",
                extra={"bucket": self._bucket, "key": key},
            )
            raise ExtractionError(f"Failed to read s3://{self._bucket}/{key}: {exc}") from exc
        except json.JSONDecodeError as exc:
            logger.exception(
                "s3_reader.parse_failed",
                extra={"bucket": self._bucket, "key": key},
            )
            raise ExtractionError(
                f"Malformed JSON line in s3://{self._bucket}/{key}: {exc}"
            ) from exc


__all__ = ["S3Reader"]
