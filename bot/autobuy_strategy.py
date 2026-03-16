from __future__ import annotations

from urllib.parse import urlsplit


def build_buy_urls(source_url: str, item_id: int) -> list[str]:
    """Build ordered list of possible buy endpoints.

    The order prioritizes API endpoints and low-latency paths first.
    """
    source_url = (source_url or "").strip()
    source_base = ""
    try:
        parts = urlsplit(source_url)
        if parts.scheme and parts.netloc:
            source_base = f"{parts.scheme}://{parts.netloc}"
    except Exception:
        source_base = ""

    base_hosts = [
        "https://prod-api.lzt.market",
        "https://api.lzt.market",
        "https://api.lolz.live",
    ]

    source_low = source_url.lower()
    source_is_api = source_base and any(marker in source_low for marker in ("api.", "prod-api."))
    if source_base and source_is_api:
        base_hosts.insert(0, source_base)
    elif source_base:
        base_hosts.append(source_base)

    dedup_bases: list[str] = []
    seen_bases: set[str] = set()
    for base in base_hosts:
        if base in seen_bases:
            continue
        seen_bases.add(base)
        dedup_bases.append(base)

    fast_paths = [
        "{id}/confirm-buy",
        "market/{id}/confirm-buy",
        "{id}/fast-buy",
        "market/{id}/fast-buy",
        "{id}/buy",
        "market/{id}/buy",
    ]
    medium_paths = [
        "items/{id}/confirm-buy",
        "items/{id}/fast-buy",
        "items/{id}/buy",
        "item/{id}/confirm-buy",
        "item/{id}/fast-buy",
        "item/{id}/buy",
    ]
    fallback_paths = [
        "{id}/purchase",
        "market/{id}/purchase",
        "item/{id}/purchase",
        "items/{id}/purchase",
    ]

    urls: list[str] = []
    seen: set[str] = set()
    for path_list in (fast_paths, medium_paths, fallback_paths):
        # Важен порядок: сначала одинаковый path по всем хостам,
        # чтобы в первой волне параллельных запросов покрыть максимум API.
        for tpl in path_list:
            for base in dedup_bases:
                url = f"{base}/{tpl.format(id=item_id)}"
                if url in seen:
                    continue
                seen.add(url)
                urls.append(url)
    return urls


def prioritize_buy_urls(all_urls: list[str], preferred_urls: list[str] | None = None) -> list[str]:
    preferred_urls = preferred_urls or []
    preferred = [u for u in preferred_urls if u in all_urls]
    return preferred + [u for u in all_urls if u not in preferred]
