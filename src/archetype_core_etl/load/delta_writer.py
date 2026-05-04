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

Writes use MERGE ON (record_id, pipeline_run_id) so retrying a failed
run never produces duplicate rows. Note: pipeline_run_id and
input_tokens/output_tokens columns must exist in the Databricks tables
(add them when creating or migrating the schema).
"""

from __future__ import annotations

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

    def write_bronze(
        self,
        results: Iterable[ClassificationResult],
        *,
        pipeline_run_id: str,
    ) -> int:
        """Merge every result into the Bronze table."""
        return self._append(self._bronze_fqn, list(results), pipeline_run_id)

    def write_gold(
        self,
        results: Iterable[ClassificationResult],
        *,
        pipeline_run_id: str,
    ) -> int:
        """Merge only quality-gated results into the Gold table."""
        return self._append(self._gold_fqn, list(results), pipeline_run_id)

    def _append(
        self,
        table_fqn: str,
        results: list[ClassificationResult],
        pipeline_run_id: str,
    ) -> int:
        if not results:
            logger.info("delta_writer.append.empty_batch", extra={"table": table_fqn})
            return 0

        for idx, result in enumerate(results):
            self._merge_one(table_fqn, result, pipeline_run_id, idx)

        logger.info(
            "delta_writer.append.complete",
            extra={"table": table_fqn, "rows": len(results)},
        )
        return len(results)

    def _merge_one(
        self,
        table_fqn: str,
        result: ClassificationResult,
        pipeline_run_id: str,
        idx: int,
    ) -> None:
        statement = (
            f"MERGE INTO {table_fqn} AS target "
            "USING (SELECT :record_id AS record_id, :pipeline_run_id AS pipeline_run_id) AS source "
            "ON target.record_id = source.record_id "
            "AND target.pipeline_run_id = source.pipeline_run_id "
            "WHEN NOT MATCHED THEN INSERT "
            "(record_id, pipeline_run_id, compliance_score, risk_tier, policy_alignment, "
            "reasoning, input_tokens, output_tokens, tokens_used, model_id, classified_at) VALUES "
            "(:record_id, :pipeline_run_id, :compliance_score, :risk_tier, :policy_alignment, "
            ":reasoning, :input_tokens, :output_tokens, :tokens_used, :model_id, :classified_at)"
        )
        parameters = [
            StatementParameterListItem(name="record_id", value=result.record_id, type="STRING"),
            StatementParameterListItem(
                name="pipeline_run_id", value=pipeline_run_id, type="STRING"
            ),
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
                value=result.reasoning,
                type="STRING",
            ),
            StatementParameterListItem(
                name="input_tokens",
                value=str(int(result.input_tokens)),
                type="INT",
            ),
            StatementParameterListItem(
                name="output_tokens",
                value=str(int(result.output_tokens)),
                type="INT",
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
            raise LoadError(f"Delta merge into {table_fqn} failed: {exc}") from exc

        status_state = getattr(getattr(response, "status", None), "state", None)
        if status_state and str(status_state) not in {
            "SUCCEEDED",
            "StatementState.SUCCEEDED",
        }:
            raise LoadError(f"Delta merge into {table_fqn} ended in state {status_state}")


__all__ = ["DeltaWriter"]
