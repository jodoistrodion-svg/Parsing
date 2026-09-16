from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass(slots=True)
class RateLimitState:
    limit: int | None = None
    remaining: int | None = None
    reset_at: float | None = None

    @property
    def exhausted(self) -> bool:
        return self.remaining == 0


class AdaptiveRateLimiter:
    """Header-aware limiter that stays out of the hot path until a bucket is exhausted.

    The important property for autobuy is that normal responses with remaining
    quota never sleep. A wait is introduced only after the server explicitly
    reports an exhausted bucket, using X-RateLimit-Reset when available.
    """

    def __init__(self, safety_ms: int = 5):
        self._states: dict[str, RateLimitState] = {}
        self._lock = asyncio.Lock()
        self._safety = max(0.0, safety_ms / 1000.0)

    async def before_request(self, bucket: str) -> None:
        async with self._lock:
            state = self._states.get(bucket)
            if state is None or not state.exhausted or state.reset_at is None:
                return
            delay = state.reset_at - time.time() + self._safety
        if delay > 0:
            await asyncio.sleep(delay)

    async def observe(self, bucket: str, headers) -> RateLimitState:
        def _int(name: str) -> int | None:
            try:
                value = headers.get(name)
                return int(value) if value is not None else None
            except (TypeError, ValueError):
                return None

        def _reset() -> float | None:
            raw = headers.get("X-RateLimit-Reset")
            if raw is None:
                return None
            try:
                value = float(raw)
            except (TypeError, ValueError):
                return None
            # APIs commonly expose epoch seconds; tolerate a relative value too.
            return value if value > time.time() - 60 else time.time() + max(0.0, value)

        state = RateLimitState(_int("X-RateLimit-Limit"), _int("X-RateLimit-Remaining"), _reset())
        async with self._lock:
            self._states[bucket] = state
        return state

    def snapshot(self) -> dict[str, RateLimitState]:
        return dict(self._states)
