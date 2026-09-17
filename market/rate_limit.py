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
    """Header-aware limiter with a zero-cost path while quota remains."""

    def __init__(self, safety_ms: int = 5):
        self._states: dict[str, RateLimitState] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._safety = max(0.0, safety_ms / 1000.0)

    def _bucket_lock(self, bucket: str) -> asyncio.Lock:
        lock = self._locks.get(bucket)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[bucket] = lock
        return lock

    async def before_request(self, bucket: str) -> None:
        state = self._states.get(bucket)
        if state is None or not state.exhausted or state.reset_at is None:
            return
        async with self._bucket_lock(bucket):
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

        raw_reset = headers.get("X-RateLimit-Reset")
        reset_at: float | None = None
        try:
            if raw_reset is not None:
                value = float(raw_reset)
                # LZT documents this header as a reset timestamp. Do not guess
                # relative semantics: an unknown format is safer to ignore than
                # to add a long accidental sleep to the autobuy hot path.
                if value > 0:
                    reset_at = value
        except (TypeError, ValueError):
            pass

        state = RateLimitState(
            _int("X-RateLimit-Limit"),
            _int("X-RateLimit-Remaining"),
            reset_at,
        )
        self._states[bucket] = state
        return state

    def snapshot(self) -> dict[str, RateLimitState]:
        return dict(self._states)
