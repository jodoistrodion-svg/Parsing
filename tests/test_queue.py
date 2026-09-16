import asyncio

from buyer.queue import UserAutobuyQueueManager


def test_queue_runs_jobs_concurrently():
    async def scenario():
        manager = UserAutobuyQueueManager(maxsize=32, workers_per_user=4)
        started = 0
        peak = 0
        lock = asyncio.Lock()

        async def handler(_user_id, _payload):
            nonlocal started, peak
            async with lock:
                started += 1
                peak = max(peak, started)
            await asyncio.sleep(0.03)
            async with lock:
                started -= 1

        for i in range(4):
            await manager.enqueue(1, i, handler)
        await manager._get_queue(1).join()
        await manager.shutdown()
        return peak

    assert asyncio.run(scenario()) >= 2
