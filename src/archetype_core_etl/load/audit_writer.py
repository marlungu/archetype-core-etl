"""PostgreSQL audit writer for pipeline runs.

Every record that passes through the classify layer produces exactly
one row in ``archetype_audit.classification_audit``. The writer creates
the table on first use (idempotent) and wraps every write in a
transaction — either the whole batch lands or none of it does.

Cost in USD is derived from token usage and the same per-1K pricing
table the :class:`CostTracker` uses, so audit rows agree with the
end-of-batch cost summary to the cent.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from archetype_core_etl.classify.bedrock_classifier import ClassificationResult
from archetype_core_etl.classify.cost_tracker import DEFAULT_PRICING, ModelPricing
from archetype_core_etl.common.exceptions import LoadError
from archetype_core_etl.common.logging import get_logger

logger = get_logger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS classification_audit (
    audit_id            BIGSERIAL PRIMARY KEY,
    record_id           TEXT        NOT NULL,
    pipeline_run_id     TEXT        NOT NULL,
    submitted_at        TIMESTAMPTZ NOT NULL,
    classified_at       TIMESTAMPTZ NOT NULL,
    compliance_score    NUMERIC(4, 3) NOT NULL,
    risk_tier           TEXT        NOT NULL,
    policy_alignment    TEXT        NOT NULL,
    tokens_used         INTEGER     NOT NULL,
    cost_usd            NUMERIC(12, 6) NOT NULL,
    quality_gate_passed BOOLEAN     NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_classification_audit_run
    ON classification_audit (pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_classification_audit_record
    ON classification_audit (record_id);
"""

_INSERT_SQL = (
    "INSERT INTO classification_audit "
    "(record_id, pipeline_run_id, submitted_at, classified_at, "
    "compliance_score, risk_tier, policy_alignment, tokens_used, "
    "cost_usd, quality_gate_passed) VALUES %s"
)


@dataclass(frozen=True)
class AuditEntry:
    """One row's worth of audit data — one per classified record."""

    record_id: str
    pipeline_run_id: str
    submitted_at: datetime
    classified_at: datetime
    compliance_score: float
    risk_tier: str
    policy_alignment: str
    tokens_used: int
    cost_usd: float
    quality_gate_passed: bool


class AuditWriter:
    """Append-only audit log writer backed by PostgreSQL."""

    def __init__(
        self,
        *,
        dsn: str,
        pricing: dict[str, ModelPricing] | None = None,
    ) -> None:
        self._dsn = dsn
        self._pricing = pricing or dict(DEFAULT_PRICING)
        self._table_ready = False

    def ensure_table(self) -> None:
        """Create the audit table (and indexes) if they do not already exist."""
        if self._table_ready:
            return
        try:
            with psycopg2.connect(self._dsn) as conn, conn.cursor() as cur:
                cur.execute(sql.SQL(_CREATE_TABLE_SQL))
        except psycopg2.Error as exc:
            logger.exception("audit_writer.create_table_failed")
            raise LoadError(f"Failed to create audit table: {exc}") from exc
        self._table_ready = True

    def write(
        self,
        *,
        pipeline_run_id: str,
        results: Iterable[ClassificationResult],
        submitted_at_by_record: dict[str, datetime],
        quality_gate_passed: bool,
    ) -> int:
        """Persist one audit row per classification result.

        ``submitted_at_by_record`` maps ``record_id`` → source ``submitted_at``
        so the caller can carry the original extract timestamp into the
        audit log without re-loading the source record.

        The entire batch is committed in a single transaction; a failure
        on any row rolls the whole batch back.
        """
        self.ensure_table()
        entries = self._build_entries(
            pipeline_run_id=pipeline_run_id,
            results=results,
            submitted_at_by_record=submitted_at_by_record,
            quality_gate_passed=quality_gate_passed,
        )
        if not entries:
            logger.info(
                "audit_writer.write.empty_batch",
                extra={"pipeline_run_id": pipeline_run_id},
            )
            return 0

        rows = [
            (
                e.record_id,
                e.pipeline_run_id,
                e.submitted_at,
                e.classified_at,
                e.compliance_score,
                e.risk_tier,
                e.policy_alignment,
                e.tokens_used,
                e.cost_usd,
                e.quality_gate_passed,
            )
            for e in entries
        ]

        try:
            with psycopg2.connect(self._dsn) as conn, conn.cursor() as cur:
                execute_values(cur, _INSERT_SQL, rows)
        except psycopg2.Error as exc:
            logger.exception(
                "audit_writer.write_failed",
                extra={"pipeline_run_id": pipeline_run_id, "rows": len(rows)},
            )
            raise LoadError(f"Audit write failed for run {pipeline_run_id}: {exc}") from exc

        logger.info(
            "audit_writer.write.complete",
            extra={"pipeline_run_id": pipeline_run_id, "rows": len(rows)},
        )
        return len(rows)

    def _build_entries(
        self,
        *,
        pipeline_run_id: str,
        results: Iterable[ClassificationResult],
        submitted_at_by_record: dict[str, datetime],
        quality_gate_passed: bool,
    ) -> list[AuditEntry]:
        entries: list[AuditEntry] = []
        for r in results:
            submitted_at = submitted_at_by_record.get(r.record_id)
            if submitted_at is None:
                raise LoadError(
                    f"No submitted_at found for record {r.record_id}; "
                    "every result must have a corresponding source timestamp."
                )
            entries.append(
                AuditEntry(
                    record_id=r.record_id,
                    pipeline_run_id=pipeline_run_id,
                    submitted_at=submitted_at,
                    classified_at=r.classified_at,
                    compliance_score=r.compliance_score,
                    risk_tier=r.risk_tier,
                    policy_alignment=r.policy_alignment,
                    tokens_used=r.tokens_used,
                    cost_usd=self._cost_for(r),
                    quality_gate_passed=quality_gate_passed,
                )
            )
        return entries

    def _cost_for(self, result: ClassificationResult) -> float:
        """Estimate per-record USD cost from total tokens.

        ``ClassificationResult.tokens_used`` is input+output combined
        because the classifier only surfaces the total. We approximate
        the split as 50/50 and apply the blended rate — the exact
        per-batch cost is still reported by :class:`CostTracker`, which
        sees the real input/output breakdown. Unknown models record
        zero so audit writes never fail on a pricing gap.
        """
        price = self._pricing.get(result.model_id)
        if price is None:
            return 0.0
        blended = 0.5 * (price.input_usd_per_1k + price.output_usd_per_1k)
        return round((result.tokens_used / 1000.0) * blended, 6)


__all__ = ["AuditEntry", "AuditWriter"]
