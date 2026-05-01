"""XCom serialization helpers for classification payloads.

Centralises the dict encoding/decoding of :class:`ClassificationResult`
so both the batch and streaming DAGs share a single implementation.
"""

from __future__ import annotations

from datetime import UTC, datetime

from archetype_core_etl.classify.bedrock_classifier import ClassificationResult
from archetype_core_etl.extract.schema import FederalDocumentRecord


def serialize_classification_payload(
    results: list[ClassificationResult],
    validated_records: list[FederalDocumentRecord],
    *,
    pipeline_run_id: str,
) -> dict:
    """Serialize classification results and source timestamps for XCom transport."""
    return {
        "pipeline_run_id": pipeline_run_id,
        "results": [
            {
                "record_id": cr.record_id,
                "compliance_score": cr.compliance_score,
                "risk_tier": cr.risk_tier,
                "policy_alignment": cr.policy_alignment,
                "reasoning": cr.reasoning,
                "input_tokens": cr.input_tokens,
                "output_tokens": cr.output_tokens,
                "tokens_used": cr.tokens_used,
                "model_id": cr.model_id,
                "classified_at": cr.classified_at.isoformat(),
            }
            for cr in results
        ],
        "submitted_at_by_record": {
            str(r.record_id): r.submitted_at.isoformat() for r in validated_records
        },
    }


def deserialize_classification_payload(
    payload: dict,
) -> tuple[list[ClassificationResult], dict[str, datetime], str]:
    """Reconstruct ClassificationResult list, submitted_at map, and pipeline_run_id."""
    pipeline_run_id: str = payload["pipeline_run_id"]
    results = [
        ClassificationResult(
            record_id=r["record_id"],
            compliance_score=r["compliance_score"],
            risk_tier=r["risk_tier"],
            policy_alignment=r["policy_alignment"],
            reasoning=r["reasoning"],
            input_tokens=r["input_tokens"],
            output_tokens=r["output_tokens"],
            tokens_used=r["tokens_used"],
            model_id=r["model_id"],
            classified_at=datetime.fromisoformat(r["classified_at"]).replace(tzinfo=UTC),
        )
        for r in payload["results"]
    ]
    submitted_at_by_record = {
        k: datetime.fromisoformat(v).replace(tzinfo=UTC)
        for k, v in payload["submitted_at_by_record"].items()
    }
    return results, submitted_at_by_record, pipeline_run_id
