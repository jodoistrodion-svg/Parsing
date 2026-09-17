from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from filters.engine import FilterEngine


@dataclass(frozen=True, slots=True)
class Decision:
    accepted: bool
    reason: str


class DecisionEngine:
    """Pure discovery decision layer; never performs network or I/O."""

    def __init__(self, filters: FilterEngine | None = None):
        self.filters = filters or FilterEngine()

    def decide(self, item: dict[str, Any], *, already_seen: bool = False, already_attempted: bool = False) -> Decision:
        if already_seen:
            return Decision(False, "seen")
        if already_attempted:
            return Decision(False, "purchase_already_attempted")
        if not self.filters.accept(item):
            return Decision(False, "filtered")
        return Decision(True, "accepted")
