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

import time
from collections.abc import Iterable
from typing import Any, cast

from databricks.sdk.service.sql import StatementParameterListItem, StatementResponse, StatementState

from archetype_core_etl.classify.bedrock_classifier import ClassificationResult
from archetype_core_etl.common.exceptions import LoadError
from archetype_core_etl.common.logging import get_logger

logger = get_logger(__name__)

# States where the statement is still running — keep polling.
_RUNNING_STATES = {StatementState.PENDING, StatementState.RUNNING}

# States where the statement is done — stop polling.
_TERMINAL_STATES = {
    StatementState.SUCCEEDED,
    StatementState.FAILED,
    StatementState.CANCELED,
    StatementState.CLOSED,
}


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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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

    def _execute_and_poll(
        self,
        statement: str,
        parameters: list[StatementParameterListItem],
        *,
        table_fqn: str,
        row_index: int,
        poll_interval: float = 3.0,
        max_poll_seconds: float = 300.0,
    ) -> StatementResponse:
        """Execute a SQL statement asynchronously and poll until terminal.

        Submits with ``wait_timeout="0s"`` so Databricks returns
        immediately with a ``statement_id``.  Then polls
        ``get_statement`` until the state is terminal (SUCCEEDED,
        FAILED, CANCELED, CLOSED).

        This avoids the 50-second ``wait_timeout`` ceiling that causes
        PENDING failures on cold warehouses or heavy MERGE operations.
        """
        # ── Submit async ─────────────────────────────────────────────
        try:
            response = self._client.statement_execution.execute_statement(
                warehouse_id=self._warehouse_id,
                statement=statement,
                parameters=parameters,
                catalog=self._catalog,
                schema=self._schema_name,
                wait_timeout="0s",
            )
        except Exception as exc:
            logger.exception(
                "delta_writer.execute_failed",
                extra={"table": table_fqn, "row_index": row_index},
            )
            raise LoadError(f"Delta merge into {table_fqn} failed: {exc}") from exc

        statement_id = response.statement_id
        state = response.status.state if response.status else None

        logger.info(
            "delta_writer.statement_submitted",
            extra={
                "statement_id": statement_id,
                "initial_state": state.value if state else "UNKNOWN",
                "table": table_fqn,
                "row_index": row_index,
            },
        )

        # ── If already terminal, return or raise immediately ─────────
        if state in _TERMINAL_STATES:
            self._check_terminal(response, table_fqn)
            return cast(StatementResponse, response)

        # ── Poll loop ────────────────────────────────────────────────
        elapsed = 0.0
        while elapsed < max_poll_seconds:
            time.sleep(poll_interval)
            elapsed += poll_interval

            try:
                response = self._client.statement_execution.get_statement(
                    statement_id=statement_id,
                )
            except Exception as exc:
                logger.warning(
                    "delta_writer.poll_error",
                    extra={
                        "statement_id": statement_id,
                        "elapsed": elapsed,
                        "error": str(exc),
                    },
                )
                # Transient network hiccup — keep polling
                continue

            state = response.status.state if response.status else None
            logger.debug(
                "delta_writer.polling",
                extra={
                    "statement_id": statement_id,
                    "state": state.value if state else "UNKNOWN",
                    "elapsed_s": round(elapsed, 1),
                },
            )

            if state in _TERMINAL_STATES:
                self._check_terminal(response, table_fqn)
                return cast(StatementResponse, response)

        # ── Local timeout — cancel and raise ─────────────────────────
        try:
            self._client.statement_execution.cancel_execution(
                statement_id=statement_id,
            )
        except Exception:
            logger.warning(
                "delta_writer.cancel_failed",
                extra={"statement_id": statement_id},
            )

        raise LoadError(
            f"Statement {statement_id} still PENDING/RUNNING after "
            f"{max_poll_seconds}s — canceled. Table: {table_fqn}"
        )

    @staticmethod
    def _check_terminal(response: StatementResponse, table_fqn: str) -> None:
        """Raise :class:`LoadError` if terminal state is not SUCCEEDED."""
        state = response.status.state if response.status else None
        if state == StatementState.SUCCEEDED:
            return
        error_msg = ""
        if response.status and response.status.error:
            error_msg = response.status.error.message or ""
        raise LoadError(
            f"Delta merge into {table_fqn} ended in state "
            f"{state.value if state else 'UNKNOWN'}: {error_msg}"
        )

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
            "WHEN MATCHED THEN UPDATE SET "
            "target.compliance_score = :compliance_score, "
            "target.risk_tier = :risk_tier, "
            "target.policy_alignment = :policy_alignment, "
            "target.reasoning = :reasoning, "
            "target.input_tokens = :input_tokens, "
            "target.output_tokens = :output_tokens, "
            "target.tokens_used = :tokens_used, "
            "target.model_id = :model_id, "
            "target.classified_at = :classified_at "
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

        self._execute_and_poll(
            statement,
            parameters,
            table_fqn=table_fqn,
            row_index=idx,
        )


__all__ = ["DeltaWriter"]
