from __future__ import annotations

import asyncio
from typing import Awaitable, Callable


async def parse_urls_concurrent(
    sources: list[dict],
    parse_func: Callable[[dict], Awaitable[tuple[dict, list, str | None]]],
) -> list[tuple[dict, list, str | None]]:
    tasks = [asyncio.create_task(parse_func(src)) for src in sources]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    parsed: list[tuple[dict, list, str | None]] = []
    for item in results:
        if isinstance(item, Exception):
            parsed.append(({"idx": -1, "url": "UNKNOWN", "name": "UNKNOWN"}, [], str(item)))
            continue
        parsed.append(item)
    return parsed
