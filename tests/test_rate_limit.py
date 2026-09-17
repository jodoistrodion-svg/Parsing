import asyncio
import time

from market.rate_limit import AdaptiveRateLimiter


def test_remaining_quota_has_zero_wait():
    async def scenario():
        limiter = AdaptiveRateLimiter()
        await limiter.observe("buy", {"X-RateLimit-Remaining": "10", "X-RateLimit-Reset": str(time.time() + 10)})
        started = time.perf_counter()
        await limiter.before_request("buy")
        return time.perf_counter() - started

    assert asyncio.run(scenario()) < 0.05


def test_exhausted_bucket_waits_until_reset():
    async def scenario():
        limiter = AdaptiveRateLimiter(safety_ms=0)
        await limiter.observe("buy", {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(time.time() + 0.01)})
        started = time.perf_counter()
        await limiter.before_request("buy")
        return time.perf_counter() - started

    assert asyncio.run(scenario()) >= 0.005
