"""Great Expectations-backed quality gate for federal document records.

The gate exposes a stable :class:`GateResult` contract so downstream code
is insulated from the Great Expectations API surface. The expectation
suite is assembled in memory on every :meth:`QualityGate.validate` call
— the gate is designed for batch use inside a task, not for incremental
expectation store management.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from great_expectations.dataset import PandasDataset

from archetype_core_etl.common.logging import get_logger

logger = get_logger(__name__)

_ALLOWED_AGENCIES: list[str] = ["USCIS", "CBP", "ICE", "TSA", "FEMA"]
_ALLOWED_PRIORITY_TIERS: list[str] = ["standard", "expedite", "emergency"]
_ALLOWED_DOCUMENT_TYPES: list[str] = [
    "application",
    "petition",
    "notice",
    "decision",
    "evidence",
    "correspondence",
]


@dataclass
class GateResult:
    """Outcome of a :class:`QualityGate.validate` run."""

    passed: bool
    total: int
    failed: int
    failure_details: list[dict[str, Any]] = field(default_factory=list)


class QualityGate:
    """Validate a batch of raw record dicts against a fixed expectation suite.

    The suite enforces:

    * ``record_id`` and ``submitted_at`` are non-null.
    * ``document_type`` is in an allowed set (customizable per instance).
    * ``agency`` is in ``["USCIS", "CBP", "ICE", "TSA", "FEMA"]``.
    * ``priority_tier`` is in ``["standard", "expedite", "emergency"]``.
    * ``pages`` is ``>= 1``.
    * ``document_text`` has length ``>= 10``.
    """

    def __init__(
        self,
        *,
        allowed_document_types: list[str] | None = None,
        allowed_agencies: list[str] | None = None,
        allowed_priority_tiers: list[str] | None = None,
    ) -> None:
        self._document_types = list(allowed_document_types or _ALLOWED_DOCUMENT_TYPES)
        self._agencies = list(allowed_agencies or _ALLOWED_AGENCIES)
        self._priority_tiers = list(allowed_priority_tiers or _ALLOWED_PRIORITY_TIERS)

    def validate(self, records: list[dict[str, Any]]) -> GateResult:
        """Run the expectation suite against ``records``.

        ``failed`` is the sum of ``unexpected_count`` across every failing
        expectation, so a single row that trips multiple expectations will
        contribute to the count more than once. This matches Great
        Expectations' own reporting semantics.
        """
        total = len(records)
        if total == 0:
            logger.info("quality_gate.validate.empty_batch")
            return GateResult(passed=True, total=0, failed=0)

        dataset = PandasDataset(pd.DataFrame(records))

        results = [
            dataset.expect_column_values_to_not_be_null("record_id"),
            dataset.expect_column_values_to_not_be_null("submitted_at"),
            dataset.expect_column_values_to_be_in_set(
                "document_type", value_set=self._document_types
            ),
            dataset.expect_column_values_to_be_in_set(
                "agency", value_set=self._agencies
            ),
            dataset.expect_column_values_to_be_in_set(
                "priority_tier", value_set=self._priority_tiers
            ),
            dataset.expect_column_values_to_be_between("pages", min_value=1),
            dataset.expect_column_value_lengths_to_be_between(
                "document_text", min_value=10
            ),
        ]

        failure_details: list[dict[str, Any]] = []
        failed = 0
        for r in results:
            if r.success:
                continue
            info = r.result or {}
            unexpected = int(info.get("unexpected_count", 0))
            failed += unexpected
            failure_details.append(
                {
                    "expectation": r.expectation_config.expectation_type,
                    "column": r.expectation_config.kwargs.get("column"),
                    "unexpected_count": unexpected,
                    "unexpected_values": info.get("partial_unexpected_list", []),
                }
            )

        passed = not failure_details
        logger.info(
            "quality_gate.validate.complete",
            extra={
                "total": total,
                "failed": failed,
                "passed": passed,
                "failing_expectations": len(failure_details),
            },
        )
        return GateResult(
            passed=passed,
            total=total,
            failed=failed,
            failure_details=failure_details,
        )


__all__ = ["GateResult", "QualityGate"]
