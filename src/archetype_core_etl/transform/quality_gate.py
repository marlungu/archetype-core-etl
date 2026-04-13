"""Great Expectations-backed quality gate (GX 1.x fluent API).

The gate exposes a stable :class:`GateResult` contract so downstream code
is insulated from the Great Expectations API surface. Internally it uses
the GX 1.x fluent flow: an ephemeral in-memory :class:`DataContext`, a
pandas data source with a whole-dataframe batch definition, and a
programmatically assembled :class:`ExpectationSuite` validated against
each incoming batch.

The suite is built once per :class:`QualityGate` instance and reused
across calls. A fresh ephemeral context is created on every
:meth:`validate` invocation so each batch runs in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

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

_SUITE_NAME = "archetype_federal_document_suite"
_DATASOURCE_NAME = "archetype_pandas"
_ASSET_NAME = "federal_document_batch"
_BATCH_DEFINITION_NAME = "whole_dataframe"


@dataclass
class GateResult:
    """Outcome of a :class:`QualityGate.validate` run."""

    passed: bool
    total: int
    failed: int
    failure_details: list[dict[str, Any]] = field(default_factory=list)


class QualityGate:
    """Validate a batch of raw record dicts against a fixed expectation suite.

    Enforces:

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

    def _build_suite(self, context: Any) -> gx.ExpectationSuite:
        """Assemble the expectation suite within an active GX context."""
        suite = context.suites.add(gx.ExpectationSuite(name=_SUITE_NAME))
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="record_id"))
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="submitted_at"))
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(column="document_type", value_set=self._document_types)
        )
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(column="agency", value_set=self._agencies)
        )
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(column="priority_tier", value_set=self._priority_tiers)
        )
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="pages", min_value=1))
        suite.add_expectation(
            gxe.ExpectColumnValueLengthsToBeBetween(column="document_text", min_value=10)
        )
        return suite

    def validate(self, records: list[dict[str, Any]]) -> GateResult:
        """Run the expectation suite against ``records``.

        ``failed`` is the sum of ``unexpected_count`` across every failing
        expectation, matching Great Expectations' own reporting semantics
        — a row that trips multiple expectations contributes to the count
        more than once.
        """
        total = len(records)
        if total == 0:
            logger.info("quality_gate.validate.empty_batch")
            return GateResult(passed=True, total=0, failed=0)

        dataframe = pd.DataFrame(records)

        # Fresh ephemeral context per batch isolates state and avoids any
        # cross-batch leakage of data source / asset registration.
        context = gx.get_context(mode="ephemeral")
        suite = self._build_suite(context)
        data_source = context.data_sources.add_pandas(name=_DATASOURCE_NAME)
        data_asset = data_source.add_dataframe_asset(name=_ASSET_NAME)
        batch_definition = data_asset.add_batch_definition_whole_dataframe(_BATCH_DEFINITION_NAME)
        batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})
        suite_result = batch.validate(suite)

        failure_details: list[dict[str, Any]] = []
        failed = 0
        for expectation_result in suite_result.results:
            if expectation_result.success:
                continue
            info = expectation_result.result or {}
            unexpected = int(info.get("unexpected_count", 0))
            failed += unexpected
            config = expectation_result.expectation_config
            failure_details.append(
                {
                    "expectation": config.type if config is not None else "unknown",
                    "column": config.kwargs.get("column") if config is not None else None,
                    "unexpected_count": unexpected,
                    "unexpected_values": info.get("partial_unexpected_list", []),
                }
            )

        passed = bool(suite_result.success) and not failure_details
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
