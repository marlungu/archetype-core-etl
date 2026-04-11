"""USD cost accounting for Claude 3.5 Sonnet on Amazon Bedrock.

Accumulates input and output token counts across a classification run
and emits a structured cost summary at batch end. Prices are expressed
as USD per 1,000 tokens so the conversion matches AWS billing docs
without intermediate floating-point scaling.

Pricing source: Bedrock on-demand pricing for
``anthropic.claude-3-5-sonnet-20241022-v2:0`` as of this commit. Update
:data:`DEFAULT_PRICING` when AWS publishes changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from archetype_core_etl.common.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class ModelPricing:
    """Per-1K-token USD pricing for a single Bedrock model."""

    input_usd_per_1k: float
    output_usd_per_1k: float


# Bedrock on-demand pricing, USD per 1,000 tokens.
DEFAULT_PRICING: dict[str, ModelPricing] = {
    "anthropic.claude-3-5-sonnet-20241022-v2:0": ModelPricing(
        input_usd_per_1k=0.003,
        output_usd_per_1k=0.015,
    ),
}


@dataclass
class CostSummary:
    """Snapshot of accumulated cost for a classification run."""

    model_id: str
    input_tokens: int
    output_tokens: int
    input_cost_usd: float
    output_cost_usd: float
    total_cost_usd: float
    requests: int


@dataclass
class CostTracker:
    """Running cost accumulator for one classification run."""

    model_id: str
    pricing: dict[str, ModelPricing] = field(
        default_factory=lambda: dict(DEFAULT_PRICING)
    )
    input_tokens: int = 0
    output_tokens: int = 0
    requests: int = 0

    def record(self, input_tokens: int, output_tokens: int) -> None:
        """Add one request's token counts to the running totals."""
        if input_tokens < 0 or output_tokens < 0:
            raise ValueError("token counts must be non-negative")
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        self.requests += 1

    def summary(self) -> CostSummary:
        """Materialize the current totals as a :class:`CostSummary`."""
        price = self.pricing.get(self.model_id)
        if price is None:
            raise KeyError(
                f"No pricing configured for model {self.model_id}; "
                "add an entry to CostTracker.pricing."
            )
        input_cost = (self.input_tokens / 1000.0) * price.input_usd_per_1k
        output_cost = (self.output_tokens / 1000.0) * price.output_usd_per_1k
        return CostSummary(
            model_id=self.model_id,
            input_tokens=self.input_tokens,
            output_tokens=self.output_tokens,
            input_cost_usd=round(input_cost, 6),
            output_cost_usd=round(output_cost, 6),
            total_cost_usd=round(input_cost + output_cost, 6),
            requests=self.requests,
        )

    def emit_summary(self) -> CostSummary:
        """Log the summary via structured logging and return it."""
        summary = self.summary()
        logger.info(
            "cost_tracker.summary",
            extra={
                "model_id": summary.model_id,
                "requests": summary.requests,
                "input_tokens": summary.input_tokens,
                "output_tokens": summary.output_tokens,
                "input_cost_usd": summary.input_cost_usd,
                "output_cost_usd": summary.output_cost_usd,
                "total_cost_usd": summary.total_cost_usd,
            },
        )
        return summary


__all__ = ["CostSummary", "CostTracker", "DEFAULT_PRICING", "ModelPricing"]
