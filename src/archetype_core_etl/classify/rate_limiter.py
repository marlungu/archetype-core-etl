"""Token-bucket rate limiter for Bedrock API calls.

Enforces two limits simultaneously: requests-per-minute and
tokens-per-minute. Callers pass the estimated token cost of the upcoming
request to :meth:`acquire`; the call blocks until both buckets can admit
it. Thread-safe.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass
class _Bucket:
    capacity: float
    refill_per_second: float
    tokens: float
    last_refill: float

    def refill(self, now: float) -> None:
        elapsed = now - self.last_refill
        if elapsed <= 0:
            return
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_second)
        self.last_refill = now


class RateLimiter:
    """Dual token-bucket enforcing RPM and TPM limits.

    Usage::

        limiter = RateLimiter(requests_per_minute=60, tokens_per_minute=40_000)
        limiter.acquire(estimated_tokens=1500)
        client.invoke_model(...)
    """

    def __init__(self, requests_per_minute: int, tokens_per_minute: int) -> None:
        if requests_per_minute <= 0 or tokens_per_minute <= 0:
            raise ValueError("rate limits must be positive")
        now = time.monotonic()
        self._request_bucket = _Bucket(
            capacity=float(requests_per_minute),
            refill_per_second=requests_per_minute / 60.0,
            tokens=float(requests_per_minute),
            last_refill=now,
        )
        self._token_bucket = _Bucket(
            capacity=float(tokens_per_minute),
            refill_per_second=tokens_per_minute / 60.0,
            tokens=float(tokens_per_minute),
            last_refill=now,
        )
        self._lock = threading.Lock()

    def acquire(self, estimated_tokens: int) -> None:
        """Block until both buckets can admit one request of ``estimated_tokens``."""
        if estimated_tokens < 0:
            raise ValueError("estimated_tokens must be non-negative")

        while True:
            with self._lock:
                now = time.monotonic()
                self._request_bucket.refill(now)
                self._token_bucket.refill(now)

                have_req = self._request_bucket.tokens >= 1.0
                have_tok = self._token_bucket.tokens >= estimated_tokens
                if have_req and have_tok:
                    self._request_bucket.tokens -= 1.0
                    self._token_bucket.tokens -= estimated_tokens
                    return

                req_wait = (
                    0.0
                    if have_req
                    else (1.0 - self._request_bucket.tokens)
                    / self._request_bucket.refill_per_second
                )
                tok_wait = (
                    0.0
                    if have_tok
                    else (estimated_tokens - self._token_bucket.tokens)
                    / self._token_bucket.refill_per_second
                )
                wait = max(req_wait, tok_wait)

            time.sleep(wait)


__all__ = ["RateLimiter"]
