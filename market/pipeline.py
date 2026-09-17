from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from domain.decision import DecisionEngine


@dataclass(slots=True)
class PipelineItem:
    item: dict[str, Any]
    source: dict[str, Any]
    found_perf: float


@dataclass(slots=True)
class PipelineStats:
    discovered: int = 0
    duplicates: int = 0
    filtered: int = 0
    accepted: int = 0
    queued: int = 0


FetchSource = Callable[[dict[str, Any]], Awaitable[tuple[dict[str, Any], list[dict[str, Any]], str | None]]]
EnqueueAutobuy = Callable[[dict[str, Any], dict[str, Any], float], Awaitable[None]]
MarkSeen = Callable[[str], Awaitable[None]]
IsSeen = Callable[[str], bool]
IsAttempted = Callable[[str], bool]
MakeKey = Callable[[dict[str, Any]], str]


class DiscoveryPipeline:
    """Hot-path discovery pipeline.

    Responsibilities are deliberately narrow:
    source fetch -> item ordering/cap -> dedup/decision -> autobuy enqueue -> seen.

    The pipeline does not perform account checks, purchase HTTP, Telegram I/O or
    sleeps. Those remain outside the discovery hot path. Source requests are
    consumed as soon as each source completes, so a fast autobuy source does not
    wait for slower sources.
    """

    def __init__(
        self,
        *,
        fetch_sources: Callable[..., AsyncIterator[tuple[dict[str, Any], list[dict[str, Any]], str | None]]],
        make_key: MakeKey,
        is_seen: IsSeen,
        is_attempted: IsAttempted,
        mark_seen: MarkSeen,
        enqueue_autobuy: EnqueueAutobuy,
        decision: DecisionEngine | None = None,
        max_items_per_source: int = 200,
        max_new_items_per_cycle: int = 1000,
    ):
        self._fetch_sources = fetch_sources
        self._make_key = make_key
        self._is_seen = is_seen
        self._is_attempted = is_attempted
        self._mark_seen = mark_seen
        self._enqueue_autobuy = enqueue_autobuy
        self._decision = decision or DecisionEngine()
        self._max_items_per_source = max(0, int(max_items_per_source))
        self._max_new_items_per_cycle = max(0, int(max_new_items_per_cycle))

    async def run(self, user_id: int, *, include_non_autobuy: bool) -> tuple[list[PipelineItem], PipelineStats, list[tuple[str, str, str]]]:
        stats = PipelineStats()
        accepted: list[PipelineItem] = []
        errors: list[tuple[str, str, str]] = []
        seen_this_cycle: set[str] = set()

        async for source, items, err in self._fetch_sources(user_id, include_non_autobuy=include_non_autobuy):
            if err:
                errors.append((str(source.get("name") or "UNKNOWN"), str(source.get("url") or "UNKNOWN"), str(err)))
                continue
            if not items:
                continue

            ordered = sorted(items, key=_item_sort_key, reverse=True)
            if self._max_items_per_source > 0:
                ordered = ordered[: self._max_items_per_source]

            for item in ordered:
                if self._max_new_items_per_cycle > 0 and stats.accepted >= self._max_new_items_per_cycle:
                    break

                stats.discovered += 1
                key = self._make_key(item)
                if key in seen_this_cycle or self._is_seen(key):
                    stats.duplicates += 1
                    continue

                decision = self._decision.decide(
                    item,
                    already_seen=False,
                    already_attempted=self._is_attempted(key),
                )
                if not decision.accepted:
                    if decision.reason == "filtered":
                        stats.filtered += 1
                    else:
                        stats.duplicates += 1
                    continue

                found_perf = perf_counter()
                seen_this_cycle.add(key)
                await self._mark_seen(key)
                stats.accepted += 1

                if source.get("autobuy", False) and not self._is_attempted(key):
                    await self._enqueue_autobuy(source, item, found_perf)
                    stats.queued += 1

                accepted.append(PipelineItem(item=item, source=source, found_perf=found_perf))

        return accepted, stats, errors


def _item_sort_key(item: dict[str, Any]) -> tuple[int, int]:
    published_at = item.get("published_at") or item.get("created_at") or item.get("date") or item.get("time")
    try:
        ts = int(float(published_at))
    except Exception:
        ts = 0
    item_id = item.get("item_id") or item.get("id")
    try:
        iid = int(item_id)
    except Exception:
        iid = 0
    return ts, iid
