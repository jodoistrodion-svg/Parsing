import asyncio

from domain.decision import DecisionEngine
from market.pipeline import DiscoveryPipeline


def test_pipeline_deduplicates_filters_and_enqueues_autobuy():
    async def scenario():
        seen = set()
        attempted = set()
        queued = []
        marked = []

        async def fetch_sources(user_id, *, include_non_autobuy):
            yield {"idx": 1, "url": "u1", "name": "auto", "enabled": True, "autobuy": True}, [
                {"id": 2, "title": "new", "price": 10, "published_at": 2},
                {"id": 1, "title": "old", "price": 20, "published_at": 1},
            ], None
            yield {"idx": 2, "url": "u2", "name": "plain", "enabled": True, "autobuy": False}, [
                {"id": 2, "title": "duplicate", "price": 10, "published_at": 3},
            ], None

        async def mark_seen(key):
            marked.append(key)
            seen.add(key)

        async def enqueue(source, item, found_perf):
            queued.append((source["name"], item["id"], found_perf))

        pipeline = DiscoveryPipeline(
            fetch_sources=fetch_sources,
            make_key=lambda item: f"id::{item['id']}",
            is_seen=lambda key: key in seen,
            is_attempted=lambda key: key in attempted,
            mark_seen=mark_seen,
            enqueue_autobuy=enqueue,
            decision=DecisionEngine(),
            max_items_per_source=200,
            max_new_items_per_cycle=1000,
        )

        accepted, stats, errors = await pipeline.run(1, include_non_autobuy=True)
        return accepted, stats, errors, queued, marked

    accepted, stats, errors, queued, marked = asyncio.run(scenario())
    assert errors == []
    assert [x.item["id"] for x in accepted] == [2, 1]
    assert stats.discovered == 3
    assert stats.duplicates == 1
    assert stats.accepted == 2
    assert stats.queued == 2
    assert [x[:2] for x in queued] == [("auto", 2), ("auto", 1)]
    assert marked == ["id::2", "id::1"]


def test_pipeline_never_runs_account_check_or_purchase_http():
    async def scenario():
        calls = []

        async def fetch_sources(user_id, *, include_non_autobuy):
            yield {"idx": 1, "url": "u1", "name": "auto", "autobuy": True}, [{"id": 7}], None

        async def mark_seen(key):
            pass

        async def enqueue(source, item, found_perf):
            calls.append((source["name"], item["id"]))

        pipeline = DiscoveryPipeline(
            fetch_sources=fetch_sources,
            make_key=lambda item: f"id::{item['id']}",
            is_seen=lambda key: False,
            is_attempted=lambda key: False,
            mark_seen=mark_seen,
            enqueue_autobuy=enqueue,
        )
        accepted, _, _ = await pipeline.run(1, include_non_autobuy=False)
        return accepted, calls

    accepted, calls = asyncio.run(scenario())
    assert [x.item["id"] for x in accepted] == [7]
    assert calls == [("auto", 7)]
