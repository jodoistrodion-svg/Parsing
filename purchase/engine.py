from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass(frozen=True, slots=True)
class PurchasePolicy:
    """Policy only; the actual HTTP implementation remains in the hot path."""

    account_check: bool = False
    parallel_first_wave: int = 24
    max_duration_sec: float = 2.8


PurchaseRunner = Callable[..., Awaitable[tuple[bool, str]]]


class PurchaseEngine:
    """Thin seam for migrating the legacy buyer without changing its latency."""

    def __init__(self, runner: PurchaseRunner, policy: PurchasePolicy | None = None):
        self.runner = runner
        self.policy = policy or PurchasePolicy()

    async def purchase(self, source: dict[str, Any], item: dict[str, Any], found_perf: float | None = None):
        # Deliberately do not perform account validation here. The default
        # strategy is the fast path and delegates directly to the proven runner.
        return await self.runner(source, item, found_perf=found_perf)
