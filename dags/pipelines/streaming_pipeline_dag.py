"""Streaming pipeline DAG: Kinesis ingest → classify → audit.

Runs every 5 minutes. Reads new records from a Kinesis stream using an
in-memory checkpoint (state does not survive across DAG runs — a
production deployment should persist the checkpoint to DynamoDB or
Postgres). All configuration is sourced from
:func:`archetype_core_etl.config.get_settings`.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from airflow.decorators import dag, task
from dags.common.dag_defaults import default_args


@dag(
    dag_id="archetype_streaming_pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["archetype", "streaming", "etl"],
    doc_md=__doc__,
)
def streaming_pipeline() -> None:
    @task()
    def ingest_from_kinesis() -> list[dict]:
        """Read batches from Kinesis and flatten into a single list of dicts."""
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.extract import KinesisReader

        settings = get_settings()
        stream_name = settings.aws.kinesis_stream_name
        if not stream_name:
            raise RuntimeError(
                "ARCHETYPE_AWS_KINESIS_STREAM_NAME is not set; cannot run the streaming pipeline."
            )

        reader = KinesisReader(stream_name=stream_name)
        records: list[dict] = []
        for batch in reader.read_batches():
            records.extend(batch)
        return records

    @task()
    def classify_records(records: list[dict]) -> dict:
        """Normalize, classify via Bedrock, and return serialized results."""
        import boto3

        from archetype_core_etl.classify import BedrockClassifier
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.transform import normalize_record

        if not records:
            return {"results": [], "submitted_at_by_record": {}}

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

        validated = [normalize_record(r) for r in records]
        results = classifier.classify_batch(validated)

        return serialize_classification_payload(results, validated)

    @task()
    def write_audit(payload: dict) -> None:
        """Persist audit rows to PostgreSQL."""
        from dags.common.serialization import deserialize_classification_payload

        from archetype_core_etl.config import get_settings
        from archetype_core_etl.load import AuditWriter

        if not payload["results"]:
            return

        settings = get_settings()
        audit = AuditWriter(
            dsn=settings.database.audit_url.get_secret_value(),
        )

        results, submitted_at_by_record = deserialize_classification_payload(payload)
        audit.write(
            pipeline_run_id=str(uuid.uuid4()),
            results=results,
            submitted_at_by_record=submitted_at_by_record,
            quality_gate_passed=True,
        )

    # Chain: ingest → classify → audit
    raw = ingest_from_kinesis()
    classified = classify_records(raw)
    write_audit(classified)


streaming_pipeline()
