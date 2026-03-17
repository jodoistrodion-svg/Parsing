from __future__ import annotations

from urllib.parse import urlsplit


def build_buy_urls(source_url: str, item_id: int) -> list[str]:
    """Build ordered list of possible buy endpoints.

    Keep only the fastest confirmed purchase endpoints.
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
    ]

    if source_base in {"https://prod-api.lzt.market", "https://api.lzt.market"}:
        base_hosts.insert(0, source_base)

    dedup_bases: list[str] = []
    seen_bases: set[str] = set()
    for base in base_hosts:
        if base in seen_bases:
            continue
        seen_bases.add(base)
        dedup_bases.append(base)

    buy_paths = [
        "{id}/confirm-buy",
    ]

    urls: list[str] = []
    seen: set[str] = set()
    for tpl in buy_paths:
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
