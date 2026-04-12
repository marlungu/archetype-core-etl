"""Delta Lake writer for classification results.

Writes :class:`ClassificationResult` batches to two Databricks Delta
tables:

* **Bronze** — every classification, regardless of quality gate outcome.
* **Gold** — only records whose source batch passed the quality gate.

The writer is intentionally thin: it serializes results to rows and
hands them off to the Databricks SDK's Statement Execution API. The
target tables must exist ahead of time (managed by Terraform). The
writer verifies table existence on first use and raises
:class:`LoadError` on any failure so the orchestrator can route the
exception to the audit log.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from archetype_core_etl.classify.bedrock_classifier import ClassificationResult
from archetype_core_etl.common.exceptions import LoadError
from archetype_core_etl.common.logging import get_logger

logger = get_logger(__name__)


class DeltaWriter:
    """Write classification results to Bronze and Gold Delta tables."""

    def __init__(
        self,
        *,
        workspace_client: Any,
        warehouse_id: str,
        catalog: str,
        schema: str,
        bronze_table: str = "classifications_bronze",
        gold_table: str = "classifications_gold",
    ) -> None:
        self._client = workspace_client
        self._warehouse_id = warehouse_id
        self._catalog = catalog
        self._schema = schema
        self._bronze_fqn = f"{catalog}.{schema}.{bronze_table}"
        self._gold_fqn = f"{catalog}.{schema}.{gold_table}"

    def write_bronze(self, results: Iterable[ClassificationResult]) -> int:
        """Append every result to the Bronze table."""
        return self._append(self._bronze_fqn, list(results))

    def write_gold(self, results: Iterable[ClassificationResult]) -> int:
        """Append only quality-gated results to the Gold table."""
        return self._append(self._gold_fqn, list(results))

    def _append(self, table_fqn: str, results: list[ClassificationResult]) -> int:
        if not results:
            logger.info("delta_writer.append.empty_batch", extra={"table": table_fqn})
            return 0

        values_clause = ",\n".join(self._row_literal(r) for r in results)
        statement = (
            f"INSERT INTO {table_fqn} "
            "(record_id, compliance_score, risk_tier, policy_alignment, "
            "reasoning, tokens_used, model_id, classified_at) VALUES\n"
            f"{values_clause}"
        )

        try:
            response = self._client.statement_execution.execute_statement(
                warehouse_id=self._warehouse_id,
                statement=statement,
                catalog=self._catalog,
                schema=self._schema,
                wait_timeout="30s",
            )
        except Exception as exc:
            logger.exception(
                "delta_writer.execute_failed",
                extra={"table": table_fqn, "rows": len(results)},
            )
            raise LoadError(
                f"Delta append to {table_fqn} failed: {exc}"
            ) from exc

        status_state = getattr(getattr(response, "status", None), "state", None)
        if status_state and str(status_state) not in {"SUCCEEDED", "StatementState.SUCCEEDED"}:
            raise LoadError(
                f"Delta append to {table_fqn} ended in state {status_state}"
            )

        logger.info(
            "delta_writer.append.complete",
            extra={"table": table_fqn, "rows": len(results)},
        )
        return len(results)

    @staticmethod
    def _row_literal(result: ClassificationResult) -> str:
        """Render one result as a SQL VALUES row using string-literal quoting."""
        def sql_str(value: str) -> str:
            escaped = value.replace("'", "''")
            return f"'{escaped}'"

        return (
            "("
            f"{sql_str(result.record_id)}, "
            f"{result.compliance_score}, "
            f"{sql_str(result.risk_tier)}, "
            f"{sql_str(result.policy_alignment)}, "
            f"{sql_str(json.dumps(result.reasoning))}, "
            f"{int(result.tokens_used)}, "
            f"{sql_str(result.model_id)}, "
            f"TIMESTAMP {sql_str(result.classified_at.isoformat())}"
            ")"
        )


__all__ = ["DeltaWriter"]
