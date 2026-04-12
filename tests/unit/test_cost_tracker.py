"""Tests for archetype_core_etl.classify.cost_tracker."""

from __future__ import annotations

import pytest

from archetype_core_etl.classify.cost_tracker import CostTracker, ModelPricing

MODEL_ID = "us.anthropic.claude-sonnet-4-6"


@pytest.fixture()
def tracker() -> CostTracker:
    return CostTracker(model_id=MODEL_ID)


class TestTokenAccumulation:
    def test_single_record(self, tracker):
        tracker.record(input_tokens=1000, output_tokens=200)
        assert tracker.input_tokens == 1000
        assert tracker.output_tokens == 200
        assert tracker.requests == 1

    def test_multiple_records_accumulate(self, tracker):
        tracker.record(input_tokens=500, output_tokens=100)
        tracker.record(input_tokens=300, output_tokens=50)
        tracker.record(input_tokens=200, output_tokens=50)
        assert tracker.input_tokens == 1000
        assert tracker.output_tokens == 200
        assert tracker.requests == 3

    def test_negative_tokens_rejected(self, tracker):
        with pytest.raises(ValueError, match="non-negative"):
            tracker.record(input_tokens=-1, output_tokens=0)


class TestCostCalculation:
    def test_sonnet_46_pricing(self, tracker):
        # 10,000 input tokens → (10,000 / 1,000) * $0.003 = $0.03
        # 2,000 output tokens → (2,000 / 1,000) * $0.015 = $0.03
        # total = $0.06
        tracker.record(input_tokens=10_000, output_tokens=2_000)
        summary = tracker.summary()
        assert summary.model_id == MODEL_ID
        assert summary.input_tokens == 10_000
        assert summary.output_tokens == 2_000
        assert summary.input_cost_usd == pytest.approx(0.03)
        assert summary.output_cost_usd == pytest.approx(0.03)
        assert summary.total_cost_usd == pytest.approx(0.06)
        assert summary.requests == 1

    def test_zero_tokens_zero_cost(self, tracker):
        summary = tracker.summary()
        assert summary.total_cost_usd == 0.0
        assert summary.requests == 0

    def test_large_batch_cost(self, tracker):
        # Simulate 100 requests averaging 1,500 input + 300 output tokens each.
        for _ in range(100):
            tracker.record(input_tokens=1_500, output_tokens=300)
        summary = tracker.summary()
        # 150,000 input → $0.45, 30,000 output → $0.45, total → $0.90
        assert summary.input_cost_usd == pytest.approx(0.45)
        assert summary.output_cost_usd == pytest.approx(0.45)
        assert summary.total_cost_usd == pytest.approx(0.90)
        assert summary.requests == 100

    def test_unknown_model_raises(self):
        tracker = CostTracker(model_id="unknown-model")
        tracker.record(input_tokens=100, output_tokens=100)
        with pytest.raises(KeyError, match="unknown-model"):
            tracker.summary()

    def test_custom_pricing(self):
        custom = {
            "custom-model": ModelPricing(
                input_usd_per_1k=0.01,
                output_usd_per_1k=0.05,
            )
        }
        tracker = CostTracker(model_id="custom-model", pricing=custom)
        tracker.record(input_tokens=1_000, output_tokens=1_000)
        summary = tracker.summary()
        assert summary.input_cost_usd == pytest.approx(0.01)
        assert summary.output_cost_usd == pytest.approx(0.05)
        assert summary.total_cost_usd == pytest.approx(0.06)
