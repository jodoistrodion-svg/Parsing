from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


_ALIASES = {
    "genshinlevelmin": "genshin_level_min",
    "genshinlevelmax": "genshin_level_max",
    "brawl_cupmin": "brawl_cup_min",
    "brawl_cupmax": "brawl_cup_max",
    "clash_cupmin": "clash_cup_min",
    "clash_cupmax": "clash_cup_max",
    "orderby": "order_by",
}


def normalize_market_url(url: str) -> str:
    """Canonicalize known legacy query aliases without changing values."""
    raw = (url or "").strip()
    if not raw:
        return raw
    parts = urlsplit(raw)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    normalized = [(_ALIASES.get(k, k), v) for k, v in pairs]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(normalized, doseq=True), parts.fragment))
