from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\adaptive_portfolio_intelligence_layer.json"
)

def test_adaptive_portfolio_intelligence_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "adaptive-portfolio-intelligence-layer"
    assert d["portfolio_intelligence_enabled"] is True
    assert d["portfolio_domain_count"] > 0

    assert d["portfolio_contract"]["decision_support_only"] is True
    assert d["portfolio_contract"]["execution_not_allowed"] is True
    assert d["portfolio_contract"]["human_review_required"] is True

    assert d["governance"]["portfolio_intelligence_governed"] is True
    assert d["governance"]["autonomous_execution_allowed"] is False
    assert d["governance"]["investment_advice_generated"] is False
