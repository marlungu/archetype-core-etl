"""Tests for archetype_core_etl.classify.rate_limiter."""

from __future__ import annotations

import pytest

from archetype_core_etl.classify.rate_limiter import RateLimiter


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_acquire_raises_when_estimated_tokens_exceeds_capacity() -> None:
    limiter = RateLimiter(requests_per_minute=60, tokens_per_minute=100)

    with pytest.raises(ValueError, match="exceeds limiter token bucket capacity"):
        limiter.acquire(estimated_tokens=101)


def test_acquire_allows_zero_token_requests() -> None:
    limiter = RateLimiter(requests_per_minute=2, tokens_per_minute=10)

    limiter.acquire(estimated_tokens=0)
    limiter.acquire(estimated_tokens=0)


def test_acquire_waits_for_refill_using_mocked_clock() -> None:
    clock = FakeClock()
    limiter = RateLimiter(
        requests_per_minute=60,
        tokens_per_minute=60,
        clock=clock.monotonic,
        sleeper=clock.sleep,
    )

    limiter.acquire(estimated_tokens=60)
    limiter.acquire(estimated_tokens=1)

    assert clock.now == pytest.approx(1.0)