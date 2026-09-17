from market.normalize import normalize_market_url


def test_normalize_known_query_aliases():
    url = "https://api.lzt.market/mihoyo?genshinlevelmin=20&orderby=pdate_to_down"
    normalized = normalize_market_url(url)
    assert "genshin_level_min=20" in normalized
    assert "order_by=pdate_to_down" in normalized
    assert "genshinlevelmin" not in normalized
