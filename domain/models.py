from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class MarketSource:
    url: str
    name: str = ""
    enabled: bool = True
    autobuy: bool = False


@dataclass(frozen=True, slots=True)
class MarketItem:
    item_id: int | str | None
    title: str = ""
    price: Any = None
    raw: dict[str, Any] = field(default_factory=dict, compare=False, hash=False)

    @property
    def key(self) -> str:
        if self.item_id not in (None, ""):
            return f"id::{self.item_id}"
        return f"noid::{self.title}::{self.price}"


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    item: MarketItem
    source: MarketSource
    found_at: float


@dataclass(frozen=True, slots=True)
class PurchaseResult:
    ok: bool
    item_key: str
    status: int = 0
    state: str = ""
    info: str = ""
    elapsed_ms: int = 0
