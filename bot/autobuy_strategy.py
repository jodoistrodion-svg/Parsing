from __future__ import annotations


def build_buy_urls(source_url: str, item_id: int) -> list[str]:
    """Build strict buy race endpoints for a lot.

    We intentionally keep only two low-latency endpoints on prod-api and
    race them in parallel for the same item.
    """
    _ = source_url  # kept for backward compatible signature
    base = "https://prod-api.lzt.market"
    return [
        f"{base}/{item_id}/confirm-buy",
        f"{base}/{item_id}/fast-buy",
    ]


def prioritize_buy_urls(all_urls: list[str], preferred_urls: list[str] | None = None) -> list[str]:
    preferred_urls = preferred_urls or []
    preferred = [u for u in preferred_urls if u in all_urls]
    return preferred + [u for u in all_urls if u not in preferred]
