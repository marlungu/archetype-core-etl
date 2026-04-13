"""Bedrock-backed compliance classifier for federal document records.

Wraps Amazon Bedrock's Anthropic Claude runtime to score each record's
compliance posture, risk tier, and policy alignment. The classifier is
batch-oriented: callers hand in a list of
:class:`FederalDocumentRecord` instances, the classifier chunks the
batch, invokes the model once per record, parses the strict-JSON
response, and returns one :class:`ClassificationResult` per input
record.

Responses must be valid JSON with every documented field present.
Anything else raises :class:`ClassificationError` so the orchestrator
can route the failure to the audit log.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from archetype_core_etl.classify.cost_tracker import CostTracker
from archetype_core_etl.classify.rate_limiter import RateLimiter
from archetype_core_etl.common.exceptions import ClassificationError
from archetype_core_etl.common.logging import get_logger
from archetype_core_etl.extract.schema import FederalDocumentRecord

logger = get_logger(__name__)

RiskTier = Literal["low", "medium", "high"]
PolicyAlignment = Literal["aligned", "partial", "non_compliant"]

_REQUIRED_FIELDS = (
    "compliance_score",
    "risk_tier",
    "policy_alignment",
    "reasoning",
)

_ALLOWED_RISK_TIERS: frozenset[str] = frozenset({"low", "medium", "high"})
_ALLOWED_ALIGNMENTS: frozenset[str] = frozenset({"aligned", "partial", "non_compliant"})

_SYSTEM_PROMPT = (
    "You are a federal compliance analyst. Evaluate the supplied document "
    "record and respond with a single JSON object and nothing else — no "
    "prose, no markdown fences, no preamble. The JSON object MUST contain "
    "exactly these keys:\n"
    '  "compliance_score" (float between 0.0 and 1.0),\n'
    '  "risk_tier" (one of "low", "medium", "high"),\n'
    '  "policy_alignment" (one of "aligned", "partial", "non_compliant"),\n'
    '  "reasoning" (string, 1-3 sentences).\n'
    "Do not include any other keys. Do not wrap the JSON in code fences."
)


@dataclass(frozen=True)
class ClassificationResult:
    """Structured output for a single classified record."""

    record_id: str
    compliance_score: float
    risk_tier: RiskTier
    policy_alignment: PolicyAlignment
    reasoning: str
    tokens_used: int
    model_id: str
    classified_at: datetime


class BedrockClassifier:
    """Classify federal document records via Amazon Bedrock / Claude."""

    #: Default Bedrock model: Cross-Region Inference profile for Claude Sonnet 4.6.
    DEFAULT_MODEL_ID: str = "us.anthropic.claude-sonnet-4-6"

    def __init__(
        self,
        *,
        client: Any,
        model_id: str = DEFAULT_MODEL_ID,
        rate_limiter: RateLimiter | None = None,
        cost_tracker: CostTracker | None = None,
        max_tokens: int = 512,
        temperature: float = 0.0,
    ) -> None:
        self._client = client
        self._model_id = model_id
        self._rate_limiter = rate_limiter
        self._cost_tracker = cost_tracker or CostTracker(model_id=model_id)
        self._max_tokens = max_tokens
        self._temperature = temperature

    @property
    def cost_tracker(self) -> CostTracker:
        return self._cost_tracker

    def classify_batch(
        self,
        records: list[FederalDocumentRecord],
        batch_size: int = 25,
    ) -> list[ClassificationResult]:
        """Classify ``records`` in chunks of ``batch_size``.

        Emits a structured cost summary at the end of the run via
        :meth:`CostTracker.emit_summary`.
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        results: list[ClassificationResult] = []
        total = len(records)
        logger.info(
            "bedrock_classifier.classify_batch.start",
            extra={
                "model_id": self._model_id,
                "total": total,
                "batch_size": batch_size,
            },
        )
        for offset in range(0, total, batch_size):
            chunk = records[offset : offset + batch_size]
            for record in chunk:
                results.append(self._classify_one(record))

        self._cost_tracker.emit_summary()
        logger.info(
            "bedrock_classifier.classify_batch.complete",
            extra={"model_id": self._model_id, "total": total},
        )
        return results

    def _classify_one(self, record: FederalDocumentRecord) -> ClassificationResult:
        user_message = self._build_user_message(record)
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": self._max_tokens,
            "temperature": self._temperature,
            "system": _SYSTEM_PROMPT,
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_message}],
                }
            ],
        }

        if self._rate_limiter is not None:
            # Rough pre-call estimate: prompt text length plus the output cap.
            estimated = max(len(user_message) // 4, 1) + self._max_tokens
            self._rate_limiter.acquire(estimated)

        try:
            response = self._client.invoke_model(
                modelId=self._model_id,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(body),
            )
        except Exception as exc:  # boto3 errors vary; classify uniformly
            logger.exception(
                "bedrock_classifier.invoke_failed",
                extra={"record_id": str(record.record_id)},
            )
            raise ClassificationError(
                f"Bedrock invocation failed for record {record.record_id}: {exc}"
            ) from exc

        payload = self._parse_envelope(response, record)
        return self._build_result(record, payload)

    @staticmethod
    def _build_user_message(record: FederalDocumentRecord) -> str:
        return (
            "Evaluate this federal document record for compliance.\n"
            f"record_id: {record.record_id}\n"
            f"agency: {record.agency}\n"
            f"document_type: {record.document_type}\n"
            f"case_status: {record.case_status}\n"
            f"priority_tier: {record.priority_tier}\n"
            f"pages: {record.pages}\n"
            f"flags: {', '.join(record.flags) if record.flags else 'none'}\n"
            f"document_text:\n{record.document_text}"
        )

    def _parse_envelope(
        self,
        response: dict[str, Any],
        record: FederalDocumentRecord,
    ) -> dict[str, Any]:
        """Extract the JSON payload + usage from a Bedrock response envelope."""
        try:
            raw_body = response["body"].read()
            envelope = json.loads(raw_body)
        except (KeyError, ValueError, TypeError) as exc:
            raise ClassificationError(
                f"Malformed Bedrock envelope for record {record.record_id}: {exc}"
            ) from exc

        try:
            text = envelope["content"][0]["text"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ClassificationError(
                f"Missing content block for record {record.record_id}: {exc}"
            ) from exc

        usage = envelope.get("usage") or {}
        input_tokens = int(usage.get("input_tokens", 0))
        output_tokens = int(usage.get("output_tokens", 0))
        self._cost_tracker.record(input_tokens, output_tokens)

        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ClassificationError(
                f"Model returned non-JSON for record {record.record_id}: {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise ClassificationError(f"Model JSON for record {record.record_id} is not an object")
        missing = [f for f in _REQUIRED_FIELDS if f not in payload]
        if missing:
            raise ClassificationError(
                f"Model response for record {record.record_id} missing fields: {missing}"
            )

        payload["_input_tokens"] = input_tokens
        payload["_output_tokens"] = output_tokens
        return payload

    def _build_result(
        self,
        record: FederalDocumentRecord,
        payload: dict[str, Any],
    ) -> ClassificationResult:
        risk_tier = payload["risk_tier"]
        if risk_tier not in _ALLOWED_RISK_TIERS:
            raise ClassificationError(
                f"Invalid risk_tier '{risk_tier}' for record {record.record_id}"
            )
        alignment = payload["policy_alignment"]
        if alignment not in _ALLOWED_ALIGNMENTS:
            raise ClassificationError(
                f"Invalid policy_alignment '{alignment}' for record {record.record_id}"
            )
        try:
            score = float(payload["compliance_score"])
        except (TypeError, ValueError) as exc:
            raise ClassificationError(
                f"Invalid compliance_score for record {record.record_id}: {exc}"
            ) from exc
        if not 0.0 <= score <= 1.0:
            raise ClassificationError(
                f"compliance_score {score} out of range for record {record.record_id}"
            )

        input_tokens = int(payload.get("_input_tokens", 0))
        output_tokens = int(payload.get("_output_tokens", 0))
        return ClassificationResult(
            record_id=str(record.record_id),
            compliance_score=score,
            risk_tier=risk_tier,  # type: ignore[arg-type]
            policy_alignment=alignment,  # type: ignore[arg-type]
            reasoning=str(payload["reasoning"]),
            tokens_used=input_tokens + output_tokens,
            model_id=self._model_id,
            classified_at=datetime.now(UTC),
        )


__all__ = [
    "BedrockClassifier",
    "ClassificationResult",
    "PolicyAlignment",
    "RiskTier",
]
