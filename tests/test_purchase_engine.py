import asyncio

from purchase.engine import PurchaseEngine, PurchasePolicy


def test_purchase_engine_delegates_without_extra_account_check():
    async def scenario():
        calls = []

        async def runner(source, item, found_perf=None):
            calls.append((source, item, found_perf))
            return True, "ok"

        engine = PurchaseEngine(runner, PurchasePolicy(account_check=False, parallel_first_wave=24))
        result = await engine.purchase({"name": "src"}, {"id": 42}, found_perf=1.25)
        return result, calls

    result, calls = asyncio.run(scenario())
    assert result == (True, "ok")
    assert calls == [({"name": "src"}, {"id": 42}, 1.25)]
