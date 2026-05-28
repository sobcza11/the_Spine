from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\executive_counterfactual_engine.json"
)

def test_executive_counterfactual_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "executive-counterfactual-engine"
    assert d["counterfactual_engine_enabled"] is True
    assert d["counterfactual_domain_count"] > 0

    assert d["counterfactual_contract"]["scenario_based_only"] is True
    assert d["counterfactual_contract"]["counterfactual_not_prediction"] is True
    assert d["counterfactual_contract"]["human_review_required"] is True

    assert d["governance"]["counterfactual_governance_enabled"] is True
    assert d["governance"]["autonomous_execution_allowed"] is False
    assert d["governance"]["decision_support_only"] is True
