from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LatencyEvent:
    name: str
    elapsed_ms: int
    item_key: str | None = None
    source: str | None = None


def elapsed_ms(start: float) -> int:
    return max(0, int((time.perf_counter() - start) * 1000))
