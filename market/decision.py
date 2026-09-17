from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True, slots=True)
class Decision:
    accepted: bool
    reason: str


class DecisionEngine:
    """Cheap synchronous decision boundary for the discovery hot path."""

    def __init__(self, predicate: Callable[[dict[str, Any]], bool] | None = None):
        self._predicate = predicate

    def decide(self, item: dict[str, Any]) -> Decision:
        if self._predicate is not None and not self._predicate(item):
            return Decision(False, "filtered")
        return Decision(True, "accepted")
