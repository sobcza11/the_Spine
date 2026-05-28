from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\institutional_macro_simulation_lab.json"
)

def test_institutional_macro_simulation_lab():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-macro-simulation-lab"
    assert d["macro_simulation_lab_enabled"] is True
    assert d["simulation_domain_count"] > 0

    assert d["simulation_contract"]["scenario_based_only"] is True
    assert d["simulation_contract"]["simulation_not_prediction"] is True
    assert d["simulation_contract"]["human_review_required"] is True

    assert d["governance"]["simulation_governance_enabled"] is True
    assert d["governance"]["autonomous_execution_allowed"] is False
    assert d["governance"]["decision_support_only"] is True
