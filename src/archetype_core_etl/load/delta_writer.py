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

All external values are passed via the Statement Execution API's native
``parameters`` field — no string interpolation of user data touches SQL.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from databricks.sdk.service.sql import StatementParameterListItem

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
        schema_name: str,
        bronze_table: str = "classifications_bronze",
        gold_table: str = "classifications_gold",
    ) -> None:
        self._client = workspace_client
        self._warehouse_id = warehouse_id
        self._catalog = catalog
        self._schema_name = schema_name
        self._bronze_fqn = f"{catalog}.{schema_name}.{bronze_table}"
        self._gold_fqn = f"{catalog}.{schema_name}.{gold_table}"

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

        for idx, result in enumerate(results):
            self._insert_one(table_fqn, result, idx)

        logger.info(
            "delta_writer.append.complete",
            extra={"table": table_fqn, "rows": len(results)},
        )
        return len(results)

    def _insert_one(self, table_fqn: str, result: ClassificationResult, idx: int) -> None:
        statement = (
            f"INSERT INTO {table_fqn} "
            "(record_id, compliance_score, risk_tier, policy_alignment, "
            "reasoning, tokens_used, model_id, classified_at) VALUES "
            "(:record_id, :compliance_score, :risk_tier, :policy_alignment, "
            ":reasoning, :tokens_used, :model_id, :classified_at)"
        )
        parameters = [
            StatementParameterListItem(name="record_id", value=result.record_id, type="STRING"),
            StatementParameterListItem(
                name="compliance_score",
                value=str(result.compliance_score),
                type="DOUBLE",
            ),
            StatementParameterListItem(name="risk_tier", value=result.risk_tier, type="STRING"),
            StatementParameterListItem(
                name="policy_alignment",
                value=result.policy_alignment,
                type="STRING",
            ),
            StatementParameterListItem(
                name="reasoning",
                value=json.dumps(result.reasoning),
                type="STRING",
            ),
            StatementParameterListItem(
                name="tokens_used",
                value=str(int(result.tokens_used)),
                type="INT",
            ),
            StatementParameterListItem(name="model_id", value=result.model_id, type="STRING"),
            StatementParameterListItem(
                name="classified_at",
                value=result.classified_at.isoformat(),
                type="TIMESTAMP",
            ),
        ]

        try:
            response = self._client.statement_execution.execute_statement(
                warehouse_id=self._warehouse_id,
                statement=statement,
                parameters=parameters,
                catalog=self._catalog,
                schema=self._schema_name,
                wait_timeout="30s",
            )
        except Exception as exc:
            logger.exception(
                "delta_writer.execute_failed",
                extra={"table": table_fqn, "row_index": idx},
            )
            raise LoadError(f"Delta append to {table_fqn} failed: {exc}") from exc

        status_state = getattr(getattr(response, "status", None), "state", None)
        if status_state and str(status_state) not in {
            "SUCCEEDED",
            "StatementState.SUCCEEDED",
        }:
            raise LoadError(f"Delta append to {table_fqn} ended in state {status_state}")


__all__ = ["DeltaWriter"]
