from market.decision import DecisionEngine


def test_decision_accepts_without_predicate():
    result = DecisionEngine().decide({"id": 1})
    assert result.accepted is True


def test_decision_rejects_with_predicate():
    result = DecisionEngine(lambda item: item.get("price", 0) <= 10).decide({"price": 11})
    assert result.accepted is False
    assert result.reason == "filtered"
