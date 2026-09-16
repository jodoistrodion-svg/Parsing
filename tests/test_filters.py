from filters.engine import FilterEngine, FilterSpec


def test_filter_spec_accepts_matching_item():
    engine = FilterEngine(FilterSpec.from_dict({
        "include_title": ["genshin"],
        "price_max": 100,
        "region": ["eu"],
        "email_provider": ["gmail"],
    }))
    assert engine.accept({
        "title": "Genshin account",
        "price": 80,
        "region": "eu",
        "email_provider": "gmail",
    })


def test_filter_spec_rejects_price_and_title():
    engine = FilterEngine(FilterSpec.from_dict({
        "include_title": ["genshin"],
        "exclude_title": ["scam"],
        "price_min": 50,
        "price_max": 100,
    }))
    assert not engine.accept({"title": "Genshin scam", "price": 80})
    assert not engine.accept({"title": "Genshin", "price": 120})
