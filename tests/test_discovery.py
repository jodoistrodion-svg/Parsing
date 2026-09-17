import asyncio

from market.discovery import iter_sources_split


def test_autobuy_sources_run_as_first_concurrent_wave():
    async def scenario():
        started = []
        release = asyncio.Event()

        async def fetcher(source):
            started.append(source["name"])
            await release.wait()
            return source, [{"id": 1}], None

        sources = [
            {"name": "plain-1", "autobuy": False},
            {"name": "buy-1", "autobuy": True},
            {"name": "buy-2", "autobuy": True},
        ]

        iterator = iter_sources_split(sources, fetcher, include_non_autobuy=False)
        task = asyncio.create_task(iterator.__anext__())
        await asyncio.sleep(0)
        first_wave = set(started)
        release.set()
        try:
            await task
        except StopAsyncIteration:
            pass
        return first_wave

    assert asyncio.run(scenario()) == {"buy-1", "buy-2"}


def test_plain_sources_are_skipped_when_not_requested():
    async def scenario():
        seen = []

        async def fetcher(source):
            seen.append(source["name"])
            return source, [], None

        async for _ in iter_sources_split(
            [{"name": "plain", "autobuy": False}],
            fetcher,
            include_non_autobuy=False,
        ):
            pass
        return seen

    assert asyncio.run(scenario()) == []
