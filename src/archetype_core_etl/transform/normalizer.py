"""Record normalization and validation.

Takes raw dicts (as produced by the readers in
:mod:`archetype_core_etl.extract`) and returns validated, canonicalized
:class:`FederalDocumentRecord` instances. Any Pydantic validation error
is re-raised as :class:`TransformationError` with the offending record id
embedded so downstream audit logs can trace individual failures.
"""

from __future__ import annotations

import re
from typing import Any

from pydantic import ValidationError

from archetype_core_etl.common.exceptions import TransformationError
from archetype_core_etl.common.logging import get_logger
from archetype_core_etl.extract.schema import FederalDocumentRecord

logger = get_logger(__name__)

_MAX_DOCUMENT_TEXT_LENGTH = 10_000
_WHITESPACE_RE = re.compile(r"\s+")

_TEXT_FIELDS = (
    "document_type",
    "applicant_id",
    "case_status",
    "document_text",
    "form_number",
    "officer_notes",
)


def _normalize_whitespace(value: str) -> str:
    """Collapse runs of whitespace to a single space and strip the result."""
    return _WHITESPACE_RE.sub(" ", value).strip()


def normalize_record(raw: dict[str, Any]) -> FederalDocumentRecord:
    """Normalize and validate a raw record dict.

    Steps applied in order:

    1. Strip/collapse whitespace in every text field.
    2. Uppercase ``agency``.
    3. Lowercase ``case_status`` and ``priority_tier``.
    4. Truncate ``document_text`` to 10_000 characters.
    5. Validate the result against :class:`FederalDocumentRecord`.

    The input dict is not mutated. On validation failure the function
    raises :class:`TransformationError` with the original ``record_id``
    (or ``<unknown>`` when absent) embedded in the message.
    """
    record = dict(raw)

    for field_name in _TEXT_FIELDS:
        value = record.get(field_name)
        if isinstance(value, str):
            record[field_name] = _normalize_whitespace(value)

    agency = record.get("agency")
    if isinstance(agency, str):
        record["agency"] = agency.upper()

    for field_name in ("case_status", "priority_tier"):
        value = record.get(field_name)
        if isinstance(value, str):
            record[field_name] = value.lower()

    document_text = record.get("document_text")
    if isinstance(document_text, str) and len(document_text) > _MAX_DOCUMENT_TEXT_LENGTH:
        record["document_text"] = document_text[:_MAX_DOCUMENT_TEXT_LENGTH]

    try:
        return FederalDocumentRecord.model_validate(record)
    except ValidationError as exc:
        record_id = record.get("record_id", "<unknown>")
        logger.exception(
            "normalize_record.validation_failed",
            extra={"record_id": str(record_id)},
        )
        raise TransformationError(f"Failed to normalize record {record_id}: {exc}") from exc


__all__ = ["normalize_record"]
