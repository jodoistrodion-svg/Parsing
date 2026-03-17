from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

logger = logging.getLogger(__name__)

JobHandler = Callable[[int, Any], Awaitable[None]]


class UserAutobuyQueueManager:
    def __init__(self, maxsize: int = 2000):
        self._maxsize = maxsize
        self._queues: dict[int, asyncio.Queue] = {}
        self._workers: dict[int, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    def _get_queue(self, user_id: int) -> asyncio.Queue:
        q = self._queues.get(user_id)
        if q is None:
            q = asyncio.Queue(maxsize=self._maxsize)
            self._queues[user_id] = q
        return q

    async def ensure_worker(self, user_id: int, handler: JobHandler):
        async with self._lock:
            task = self._workers.get(user_id)
            if task and not task.done():
                return
            self._workers[user_id] = asyncio.create_task(self._worker_loop(user_id, handler))

    async def enqueue(self, user_id: int, payload: Any, handler: JobHandler):
        await self.ensure_worker(user_id, handler)
        q = self._get_queue(user_id)

        if q.full():
            dropped = 0
            while q.full() and not q.empty() and dropped < 200:
                try:
                    q.get_nowait()
                    q.task_done()
                    dropped += 1
                except Exception:
                    break
            if dropped:
                logger.warning("AUTOBUY_QUEUE_DROP user_id=%s dropped=%s", user_id, dropped)

        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            logger.warning("AUTOBUY_QUEUE_FULL user_id=%s", user_id)

    async def stop_user(self, user_id: int):
        async with self._lock:
            task = self._workers.pop(user_id, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def shutdown(self):
        user_ids = list(self._workers.keys())
        for uid in user_ids:
            await self.stop_user(uid)

    async def _worker_loop(self, user_id: int, handler: JobHandler):
        q = self._get_queue(user_id)
        while True:
            try:
                payload = await q.get()
            except asyncio.CancelledError:
                break

            try:
                await handler(user_id, payload)
            except asyncio.CancelledError:
                q.task_done()
                break
            except Exception:
                logger.exception("AUTOBUY_QUEUE_WORKER_ERR user_id=%s", user_id)
            finally:
                q.task_done()
