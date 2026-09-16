from __future__ import annotations

from urllib.parse import urlsplit


# Hot-path note:
# confirm-buy is intentionally first. The official LZT API documents it as a
# purchase operation that DOES NOT check account validity. This keeps the
# no-account-check path fast. fast-buy remains the next official fallback.
# The caller is allowed to fire the first wave concurrently; this module does
# not add sleeps or client-side rate throttling.


def _source_base(source_url: str) -> str:
    try:
        parts = urlsplit((source_url or "").strip())
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}"
    except Exception:
        pass
    return ""


def _ordered_bases(source_url: str) -> list[str]:
    source_base = _source_base(source_url)
    base_hosts = [
        "https://prod-api.lzt.market",
        "https://api.lzt.market",
        "https://api.lolz.live",
    ]

    source_low = (source_url or "").lower()
    source_is_api = bool(source_base) and any(
        marker in source_low for marker in ("api.", "prod-api.")
    )
    if source_base:
        if source_is_api:
            base_hosts.insert(0, source_base)
        else:
            base_hosts.append(source_base)

    result: list[str] = []
    seen: set[str] = set()
    for base in base_hosts:
        if base and base not in seen:
            seen.add(base)
            result.append(base)
    return result


def build_buy_urls(source_url: str, item_id: int) -> list[str]:
    """Build ordered buy endpoints for the low-latency autobuy hot path.

    Official endpoints are deliberately first:
      * /{item_id}/confirm-buy — purchase without account-validity check
      * /{item_id}/fast-buy — official check+buy fallback

    Legacy URL shapes are retained after the official paths for compatibility
    with the behaviour of the original project. No delay is introduced here;
    the caller controls concurrency and deadlines.
    """
    bases = _ordered_bases(source_url)

    # IMPORTANT: confirm-buy stays before fast-buy because it does not perform
    # an account-validity check. This is the critical speed path.
    official_paths = [
        "{id}/confirm-buy",
        "market/{id}/confirm-buy",
        "{id}/fast-buy",
        "market/{id}/fast-buy",
    ]

    legacy_paths = [
        "{id}/buy",
        "market/{id}/buy",
        "items/{id}/confirm-buy",
        "items/{id}/fast-buy",
        "items/{id}/buy",
        "item/{id}/confirm-buy",
        "item/{id}/fast-buy",
        "item/{id}/buy",
        "{id}/purchase",
        "market/{id}/purchase",
        "item/{id}/purchase",
        "items/{id}/purchase",
    ]

    urls: list[str] = []
    seen: set[str] = set()
    for path_list in (official_paths, legacy_paths):
        # Preserve the original first-wave behaviour: the same endpoint shape
        # is fanned out across available hosts, so one slow host does not block
        # the other candidates.
        for tpl in path_list:
            for base in bases:
                url = f"{base}/{tpl.format(id=item_id)}"
                if url in seen:
                    continue
                seen.add(url)
                urls.append(url)
    return urls


def prioritize_buy_urls(
    all_urls: list[str], preferred_urls: list[str] | None = None
) -> list[str]:
    preferred_urls = preferred_urls or []
    preferred = [u for u in preferred_urls if u in all_urls]
    return preferred + [u for u in all_urls if u not in preferred]
