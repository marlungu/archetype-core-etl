"""Batch pipeline DAG: S3 ingest → quality gate → classify → Delta bronze → audit.

Runs every 6 hours. All configuration is sourced from
:func:`archetype_core_etl.config.get_settings`. Heavy imports are
deferred inside ``@task`` bodies so the DAG file parses quickly during
Airflow's scheduler scan.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

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
    def ingest_from_s3() -> list[dict]:
        """Read raw NDJSON documents from the raw S3 bucket."""
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.extract import S3Reader

        settings = get_settings()
        reader = S3Reader(bucket=settings.aws.raw_bucket)
        records = list(reader.read_batch(prefix="federal-documents/"))
        return records

    @task()
    def run_quality_gate(records: list[dict]) -> list[dict]:
        """Validate records against the Great Expectations suite."""
        from archetype_core_etl.transform import QualityGate

        gate = QualityGate()
        result = gate.validate(records)
        if not result.passed:
            raise RuntimeError(
                f"Quality gate failed: {result.failed}/{result.total} records — "
                f"{result.failure_details}"
            )
        return records

    @task()
    def classify_records(records: list[dict]) -> dict:
        """Normalize, classify via Bedrock, and return serialized results."""
        import boto3

        from archetype_core_etl.classify import BedrockClassifier
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.transform import normalize_record

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

        # Serialize for XCom; downstream tasks reconstruct as needed.
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
    def write_delta_bronze(payload: dict) -> dict:
        """Write all classification results to the Bronze Delta table."""
        from datetime import datetime as dt

        from databricks.sdk import WorkspaceClient

        from archetype_core_etl.classify.bedrock_classifier import ClassificationResult
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.load import DeltaWriter

        settings = get_settings()
        ws = WorkspaceClient(host=settings.databricks.host)
        writer = DeltaWriter(
            workspace_client=ws,
            warehouse_id=settings.databricks.warehouse_id,
            catalog=settings.databricks.catalog,
            schema_name=settings.databricks.schema_name,
        )

        results = [
            ClassificationResult(
                record_id=r["record_id"],
                compliance_score=r["compliance_score"],
                risk_tier=r["risk_tier"],
                policy_alignment=r["policy_alignment"],
                reasoning=r["reasoning"],
                tokens_used=r["tokens_used"],
                model_id=r["model_id"],
                classified_at=dt.fromisoformat(r["classified_at"]).replace(tzinfo=UTC),
            )
            for r in payload["results"]
        ]

        writer.write_bronze(results)
        return payload

    @task()
    def write_audit(payload: dict) -> None:
        """Persist audit rows to PostgreSQL."""
        from datetime import datetime as dt

        from archetype_core_etl.classify.bedrock_classifier import ClassificationResult
        from archetype_core_etl.config import get_settings
        from archetype_core_etl.load import AuditWriter

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
                classified_at=dt.fromisoformat(r["classified_at"]).replace(tzinfo=UTC),
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

    # Chain: ingest → gate → classify → bronze → audit
    raw = ingest_from_s3()
    gated = run_quality_gate(raw)
    classified = classify_records(gated)
    bronze_done = write_delta_bronze(classified)
    write_audit(bronze_done)


batch_pipeline()
