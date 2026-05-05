"""Batch pipeline DAG: S3 ingest → quality gate → classify → Delta bronze → audit.

Runs every 6 hours. All configuration is sourced from
:func:`archetype_core_etl.config.get_settings`. Heavy imports are
deferred inside ``@task`` bodies so the DAG file parses quickly during
Airflow's scheduler scan.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from dags.common.dag_defaults import default_args


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
    def generate_run_id(**context) -> str:
        """Return the Airflow dag_run.run_id, falling back to a UUID.

        Using the Airflow-assigned run ID means Airflow's own retry and
        back-fill mechanics produce stable, predictable IDs rather than a
        fresh UUID on every parse of the DAG file.
        """
        dag_run = context.get("dag_run")
        if dag_run and getattr(dag_run, "run_id", None):
            return str(dag_run.run_id)
        import uuid

        return str(uuid.uuid4())

    @task()
    def ingest_from_s3(run_id: str) -> list[dict]:
        """Read raw NDJSON documents from the raw S3 bucket."""
        from archetype_core_etl.common.logging import get_logger
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.extract import S3Reader

        logger = get_logger(__name__)
        logger.info("ingest_from_s3.start", extra={"pipeline_run_id": run_id})
        settings = get_settings()
        reader = S3Reader(bucket=settings.aws.raw_bucket)
        records = list(reader.read_batch(prefix="federal-documents/"))
        return records

    @task()
    def run_quality_gate(records: list[dict], run_id: str) -> list[dict]:
        """Validate records against the Great Expectations suite."""
        from archetype_core_etl.common.dead_letter import DeadLetterWriter
        from archetype_core_etl.common.logging import get_logger
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.transform import QualityGate

        logger = get_logger(__name__)
        logger.info("run_quality_gate.start", extra={"pipeline_run_id": run_id})
        gate = QualityGate()
        result = gate.validate(records)
        if not result.passed:
            settings = get_settings()
            dead_letter = DeadLetterWriter(bucket=settings.aws.raw_bucket)
            dead_letter.write(
                stage="quality_gate",
                pipeline_run_id=run_id,
                records=records,
                error_message=(
                    f"Quality gate failed: {result.failed}/{result.total} records — "
                    f"{result.failure_details}"
                ),
            )
            raise RuntimeError(
                f"Quality gate failed: {result.failed}/{result.total} records — "
                f"{result.failure_details}"
            )
        return records

    @task()
    def classify_records(records: list[dict], run_id: str) -> dict:
        """Normalize, classify via Bedrock, and return serialized results."""
        import boto3

        from archetype_core_etl.classify import BedrockClassifier
        from archetype_core_etl.common.dead_letter import DeadLetterWriter
        from archetype_core_etl.common.logging import get_logger
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.transform import normalize_record

        logger = get_logger(__name__)
        logger.info("classify_records.start", extra={"pipeline_run_id": run_id})
        settings = get_settings()
        client = boto3.client(
            "bedrock-runtime",
            region_name=settings.bedrock.region,
        )
        classifier = BedrockClassifier(
            client=client,
            model_id=settings.bedrock.model_id,
        )

        from dags.common.serialization import serialize_classification_payload

        source_keys = list({r.get("_source_key", "unknown") for r in records})
        validated = [normalize_record(r) for r in records]
        results = classifier.classify_batch(validated)

        # Route failed records to dead letter if any were skipped
        failed_count = len(validated) - len(results)
        if failed_count > 0:
            dead_letter = DeadLetterWriter(bucket=settings.aws.raw_bucket)
            result_ids = {r.record_id for r in results}
            failed_records = [r.model_dump() for r in validated if r.record_id not in result_ids]
            if failed_records:
                dead_letter.write(
                    stage="classification",
                    pipeline_run_id=run_id,
                    records=failed_records,
                    error_message=f"{failed_count} records failed classification",
                )

        prompt_hash = BedrockClassifier.prompt_hash()
        return serialize_classification_payload(
            results,
            validated,
            pipeline_run_id=run_id,
            prompt_hash=prompt_hash,
            source_keys=source_keys,
        )

    @task()
    def write_delta(payload: dict, run_id: str) -> dict:
        """Write classification results to Databricks Delta Lake bronze and gold tables."""
        from dags.common.serialization import deserialize_classification_payload
        from databricks.sdk import WorkspaceClient

        from archetype_core_etl.common.logging import get_logger
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.load import DeltaWriter

        logger = get_logger(__name__)
        logger.info("write_delta.start", extra={"pipeline_run_id": run_id})
        settings = get_settings()
        ws = WorkspaceClient(host=settings.databricks.host)
        writer = DeltaWriter(
            workspace_client=ws,
            warehouse_id=settings.databricks.warehouse_id,
            catalog=settings.databricks.catalog,
            schema_name=settings.databricks.schema_name,
        )

        results, _, _, _, _ = deserialize_classification_payload(payload)
        writer.write_bronze(results, pipeline_run_id=run_id)
        writer.write_gold(results, pipeline_run_id=run_id)
        return payload

    @task()
    def write_audit(payload: dict, run_id: str) -> None:
        """Persist audit rows to PostgreSQL."""
        from dags.common.serialization import deserialize_classification_payload

        from archetype_core_etl.common.logging import get_logger
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.load import AuditWriter

        logger = get_logger(__name__)
        logger.info("write_audit.start", extra={"pipeline_run_id": run_id})
        settings = get_settings()
        audit = AuditWriter(
            dsn=settings.database.audit_url.get_secret_value(),
        )

        results, submitted_at_by_record, _, prompt_hash, input_records = (
            deserialize_classification_payload(payload)
        )
        audit.write(
            pipeline_run_id=run_id,
            results=results,
            submitted_at_by_record=submitted_at_by_record,
            quality_gate_passed=True,
            source_bucket=settings.aws.raw_bucket,
            source_key=", ".join(payload.get("source_keys", ["unknown"])),
            prompt_hash=prompt_hash,
            input_records=input_records,
        )

    # Chain: generate_run_id → ingest → gate → classify → delta (bronze+gold) → audit
    run_id = generate_run_id()
    raw = ingest_from_s3(run_id)
    gated = run_quality_gate(raw, run_id)
    classified = classify_records(gated, run_id)
    delta_done = write_delta(classified, run_id)
    write_audit(delta_done, run_id)


batch_pipeline()
