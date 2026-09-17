import asyncio

from purchase.idempotency import PurchaseIdempotency


def test_only_one_concurrent_claim_wins():
    async def scenario():
        guard = PurchaseIdempotency()
        results = await asyncio.gather(*(guard.claim("id::42") for _ in range(8)))
        await guard.release("id::42")
        return sum(results)

    assert asyncio.run(scenario()) == 1
