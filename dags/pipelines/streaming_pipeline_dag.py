"""Streaming pipeline DAG: Kinesis ingest → classify → audit.

Runs every 5 minutes. Reads new records from a Kinesis stream using an
in-memory checkpoint (state does not survive across DAG runs — a
production deployment should persist the checkpoint to DynamoDB or
Postgres). All configuration is sourced from
:func:`archetype_core_etl.config.get_settings`.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

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

        validated = [normalize_record(r) for r in records]
        results = classifier.classify_batch(validated)

        return {
            "results": [
                {
                    "record_id": cr.record_id,
                    "compliance_score": cr.compliance_score,
                    "risk_tier": cr.risk_tier,
                    "policy_alignment": cr.policy_alignment,
                    "reasoning": cr.reasoning,
                    "tokens_used": cr.tokens_used,
                    "model_id": cr.model_id,
                    "classified_at": cr.classified_at.isoformat(),
                }
                for cr in results
            ],
            "submitted_at_by_record": {
                str(r.record_id): r.submitted_at.isoformat() for r in validated
            },
        }

    @task()
    def write_audit(payload: dict) -> None:
        """Persist audit rows to PostgreSQL."""
        from datetime import datetime as dt

        from archetype_core_etl.classify.bedrock_classifier import ClassificationResult
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.load import AuditWriter

        if not payload["results"]:
            return

        settings = get_settings()
        audit = AuditWriter(
            dsn=settings.database.audit_url.get_secret_value(),
        )

        pipeline_run_id = str(uuid.uuid4())
        results = [
            ClassificationResult(
                record_id=r["record_id"],
                compliance_score=r["compliance_score"],
                risk_tier=r["risk_tier"],
                policy_alignment=r["policy_alignment"],
                reasoning=r["reasoning"],
                tokens_used=r["tokens_used"],
                model_id=r["model_id"],
                classified_at=dt.fromisoformat(r["classified_at"]).replace(
                    tzinfo=UTC,
                ),
            )
            for r in payload["results"]
        ]
        submitted_at_by_record = {
            k: dt.fromisoformat(v).replace(tzinfo=UTC)
            for k, v in payload["submitted_at_by_record"].items()
        }

        audit.write(
            pipeline_run_id=pipeline_run_id,
            results=results,
            submitted_at_by_record=submitted_at_by_record,
            quality_gate_passed=True,
        )

    # Chain: ingest → classify → audit
    raw = ingest_from_kinesis()
    classified = classify_records(raw)
    write_audit(classified)


streaming_pipeline()
