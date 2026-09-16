from domain.models import MarketItem


def test_item_key_prefers_id():
    assert MarketItem(123, "x", 10).key == "id::123"


def test_item_key_is_stable_without_id():
    assert MarketItem(None, "x", 10).key == "noid::x::10"
