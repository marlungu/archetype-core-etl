"""Batch pipeline DAG: S3 ingest → quality gate → classify → Delta bronze → audit.

Runs every 6 hours. All configuration is sourced from
:func:`archetype_core_etl.config.get_settings`. Heavy imports are
deferred inside ``@task`` bodies so the DAG file parses quickly during
Airflow's scheduler scan.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from dags.common.dag_defaults import default_args


def configure_mwaa_runtime() -> None:
    """Set non-secret environment defaults and assert required secrets are present.

    Non-secret configuration (region, bucket names, model IDs, Databricks host/warehouse)
    is defaulted here so the pipeline works out-of-the-box in dev without extra setup.

    Secret values (DATABRICKS_TOKEN, ARCHETYPE_DB_AUDIT_URL) must be injected by the
    operator via MWAA Airflow Variables, AWS Secrets Manager, or a local .env file.
    They are never defaulted here.
    """

    os.environ.setdefault("ARCHETYPE_ENVIRONMENT", "dev")

    os.environ.setdefault("ARCHETYPE_AWS_REGION", "us-east-1")
    os.environ.setdefault("ARCHETYPE_AWS_RAW_BUCKET", "archetype-core-etl-dev-raw")
    os.environ.setdefault("ARCHETYPE_AWS_PROCESSED_BUCKET", "archetype-core-etl-dev-processed")
    os.environ.setdefault("ARCHETYPE_AWS_AUDIT_BUCKET", "archetype-core-etl-dev-audit")

    os.environ.setdefault("ARCHETYPE_AIRFLOW_DAG_BUCKET", "archetype-core-etl-dev-dags")
    os.environ.setdefault("ARCHETYPE_AIRFLOW_ENVIRONMENT_NAME", "archetype-core-etl-dev")

    os.environ.setdefault("ARCHETYPE_BEDROCK_REGION", "us-east-1")
    os.environ.setdefault(
        "ARCHETYPE_BEDROCK_MODEL_ID",
        "us.anthropic.claude-sonnet-4-6",
    )

    os.environ.setdefault("ARCHETYPE_DATABRICKS_CATALOG", "archetype_core")
    os.environ.setdefault("ARCHETYPE_DATABRICKS_SCHEMA_NAME", "default")
    os.environ.setdefault(
        "ARCHETYPE_DATABRICKS_HOST",
        "https://dbc-34205be5-3b63.cloud.databricks.com",
    )
    os.environ.setdefault(
        "ARCHETYPE_DATABRICKS_WAREHOUSE_ID",
        "04ec71b7c548b8bf",
    )
    os.environ.setdefault(
        "DATABRICKS_HOST",
        "https://dbc-34205be5-3b63.cloud.databricks.com",
    )

    required_secrets = ("DATABRICKS_TOKEN", "ARCHETYPE_DB_AUDIT_URL")
    missing = [name for name in required_secrets if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Set these via MWAA Airflow Variables, AWS Secrets Manager, or a local .env file. "
            "Do not commit credential values to source."
        )


# Only validate secrets when running under Airflow. During pytest the DAG file
# is imported without AIRFLOW_HOME set, so this guard prevents a RuntimeError
# from breaking the test suite when real secrets are not present.
if os.environ.get("AIRFLOW_HOME"):
    configure_mwaa_runtime()


@dag(
    dag_id="archetype_batch_pipeline",
    schedule="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["archetype", "batch", "etl"],
    doc_md=__doc__,
)
def batch_pipeline() -> None:
    @task()
    def generate_run_id(**context: Any) -> str:
        """Return the Airflow dag_run.run_id, falling back to a UUID.

        The returned value is used as the application-level pipeline_run_id.
        The task argument is intentionally not named ``run_id`` because Airflow
        reserves that key in its task context.
        """
        dag_run = context.get("dag_run")
        if dag_run and getattr(dag_run, "run_id", None):
            return str(dag_run.run_id)

        import uuid

        return str(uuid.uuid4())

    @task()
    def ingest_from_s3(pipeline_run_id: str) -> list[dict[str, Any]]:
        """Read raw NDJSON documents from the raw S3 bucket."""
        from archetype_core_etl.common.logging import get_logger
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.extract import S3Reader

        logger = get_logger(__name__)
        logger.info("ingest_from_s3.start", extra={"pipeline_run_id": pipeline_run_id})

        settings = get_settings()
        reader = S3Reader(bucket=settings.aws.raw_bucket)
        records = list(reader.read_batch(prefix="federal-documents/"))

        logger.info(
            "ingest_from_s3.complete",
            extra={"pipeline_run_id": pipeline_run_id, "record_count": len(records)},
        )
        return records

    @task()
    def run_quality_gate(
        records: list[dict[str, Any]],
        pipeline_run_id: str,
    ) -> list[dict[str, Any]]:
        """Lightweight cloud-demo quality gate.

        Production version uses Great Expectations. This demo version validates
        required fields without requiring the Great Expectations package inside
        the MWAA runtime.
        """
        from archetype_core_etl.common.logging import get_logger
        from archetype_core_etl.transform.field_presence import check_required_fields

        logger = get_logger(__name__)
        logger.info(
            "run_quality_gate.start",
            extra={
                "pipeline_run_id": pipeline_run_id,
                "record_count": len(records),
            },
        )

        required_fields = {
            "record_id",
            "agency",
            "applicant_id",
            "case_status",
            "priority_tier",
            "document_type",
            "document_text",
            "pages",
            "submitted_at",
        }

        failed_records = []
        for record in records:
            missing_fields = check_required_fields(record, required_fields)
            if missing_fields:
                failed_records.append(
                    {
                        "record_id": record.get("record_id", "unknown"),
                        "missing_fields": missing_fields,
                    }
                )

        if failed_records:
            raise RuntimeError(
                f"Quality gate failed for {len(failed_records)} records: {failed_records}"
            )

        logger.info(
            "run_quality_gate.passed",
            extra={
                "pipeline_run_id": pipeline_run_id,
                "record_count": len(records),
            },
        )
        return records

    @task()
    def classify_records(
        records: list[dict[str, Any]],
        pipeline_run_id: str,
    ) -> dict[str, Any]:
        """Normalize, classify via Bedrock, and return serialized results."""
        from archetype_core_etl.classify import BedrockClassifier
        from archetype_core_etl.classify.rate_limiter import RateLimiter
        from archetype_core_etl.common.aws import build_boto3_client
        from archetype_core_etl.common.dead_letter import DeadLetterWriter
        from archetype_core_etl.common.logging import get_logger
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.transform import normalize_record

        logger = get_logger(__name__)
        logger.info("classify_records.start", extra={"pipeline_run_id": pipeline_run_id})

        settings = get_settings()
        client = build_boto3_client("bedrock-runtime")
        rate_limiter = RateLimiter(
            requests_per_minute=20,
            tokens_per_minute=40_000,
        )
        classifier = BedrockClassifier(
            client=client,
            model_id=settings.bedrock.model_id,
            rate_limiter=rate_limiter,
        )

        from dags.common.serialization import serialize_classification_payload

        source_keys = list({r.get("_source_key", "unknown") for r in records})
        validated = [normalize_record(r) for r in records]
        results = classifier.classify_batch(validated)

        failed_count = len(validated) - len(results)
        if failed_count > 0:
            dead_letter = DeadLetterWriter(bucket=settings.aws.raw_bucket)
            result_ids = {r.record_id for r in results}
            failed_records = [r.model_dump() for r in validated if r.record_id not in result_ids]
            if failed_records:
                dead_letter.write(
                    stage="classification",
                    pipeline_run_id=pipeline_run_id,
                    records=failed_records,
                    error_message=f"{failed_count} records failed classification",
                )

        prompt_hash = BedrockClassifier.prompt_hash()
        return serialize_classification_payload(
            results,
            validated,
            pipeline_run_id=pipeline_run_id,
            prompt_hash=prompt_hash,
            source_keys=source_keys,
        )

    @task()
    def write_delta(payload: dict[str, Any], pipeline_run_id: str) -> dict[str, Any]:
        """Write classification results to Databricks Delta Lake bronze and gold tables."""
        from dags.common.serialization import deserialize_classification_payload
        from databricks.sdk import WorkspaceClient

        from archetype_core_etl.common.logging import get_logger
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.load import DeltaWriter

        logger = get_logger(__name__)
        logger.info("write_delta.start", extra={"pipeline_run_id": pipeline_run_id})

        settings = get_settings()
        ws = WorkspaceClient(
            host=settings.databricks.host,
            token=os.environ["DATABRICKS_TOKEN"],
        )
        writer = DeltaWriter(
            workspace_client=ws,
            warehouse_id=settings.databricks.warehouse_id,
            catalog=settings.databricks.catalog,
            schema_name=settings.databricks.schema_name,
        )

        results, _, _, _, _ = deserialize_classification_payload(payload)
        writer.write_bronze(results, pipeline_run_id=pipeline_run_id)
        writer.write_gold(results, pipeline_run_id=pipeline_run_id)
        return payload

    @task()
    def write_audit(payload: dict[str, Any], pipeline_run_id: str) -> None:
        """Persist audit rows to PostgreSQL."""
        from dags.common.serialization import deserialize_classification_payload

        from archetype_core_etl.common.logging import get_logger
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.load import AuditWriter

        logger = get_logger(__name__)
        logger.info("write_audit.start", extra={"pipeline_run_id": pipeline_run_id})

        settings = get_settings()
        audit = AuditWriter(
            dsn=settings.database.audit_url.get_secret_value(),
        )

        results, submitted_at_by_record, _, prompt_hash, input_records = (
            deserialize_classification_payload(payload)
        )
        audit.write(
            pipeline_run_id=pipeline_run_id,
            results=results,
            submitted_at_by_record=submitted_at_by_record,
            quality_gate_passed=True,
            source_bucket=settings.aws.raw_bucket,
            source_key=", ".join(payload.get("source_keys", ["unknown"])),
            prompt_hash=prompt_hash,
            input_records=input_records,
        )

    # Chain: generate_run_id → ingest → gate → classify → delta (bronze+gold) → audit
    pipeline_run_id = generate_run_id()
    raw = ingest_from_s3(pipeline_run_id=pipeline_run_id)
    gated = run_quality_gate(records=raw, pipeline_run_id=pipeline_run_id)
    classified = classify_records(records=gated, pipeline_run_id=pipeline_run_id)
    delta_done = write_delta(payload=classified, pipeline_run_id=pipeline_run_id)
    write_audit(payload=delta_done, pipeline_run_id=pipeline_run_id)


batch_pipeline()
