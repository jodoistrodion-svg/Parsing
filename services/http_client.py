from __future__ import annotations

import aiohttp


def build_connector() -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(
        limit=256,
        limit_per_host=128,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        keepalive_timeout=30,
        force_close=False,
    )


def default_headers(api_key: str) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; ParsingBot/2.0)",
        "Referer": "https://zelenka.guru/",
        "Connection": "keep-alive",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers
