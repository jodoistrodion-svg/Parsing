from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass
class CacheEntry:
    value: object
    expires_at: float


class TTLCache:
    def __init__(self, ttl_sec: float = 1.0, max_size: int = 5000):
        self.ttl_sec = max(0.01, float(ttl_sec))
        self.max_size = max(1, int(max_size))
        self._lock = asyncio.Lock()
        self._items: dict[str, CacheEntry] = {}

    async def get(self, key: str):
        async with self._lock:
            entry = self._items.get(key)
            if not entry:
                return None
            now = time.monotonic()
            if entry.expires_at <= now:
                self._items.pop(key, None)
                return None
            return entry.value

    async def set(self, key: str, value):
        async with self._lock:
            if len(self._items) >= self.max_size:
                self._purge_expired_locked()
                if len(self._items) >= self.max_size:
                    self._items.pop(next(iter(self._items)), None)
            self._items[key] = CacheEntry(value=value, expires_at=time.monotonic() + self.ttl_sec)

    async def clear(self):
        async with self._lock:
            self._items.clear()

    def _purge_expired_locked(self):
        now = time.monotonic()
        stale = [k for k, v in self._items.items() if v.expires_at <= now]
        for k in stale:
            self._items.pop(k, None)
