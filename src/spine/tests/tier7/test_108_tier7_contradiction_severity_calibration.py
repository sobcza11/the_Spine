from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_contradiction_severity_calibration.json"
)

def test_tier7_contradiction_severity_calibration():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-contradiction-severity-calibration"
    assert d["contradiction_severity_calibration_enabled"] is True
    assert d["contradiction_component_count"] >= 7

    assert d["total_component_weight"] == 1.0

    assert "rates_vs_equities" in d["contradiction_components"]
    assert "credit_vs_equities" in d["contradiction_components"]
    assert "policy_vs_market_pricing" in d["contradiction_components"]

    assert d["severity_contract"]["component_weights_required"] is True
    assert d["severity_contract"]["weights_sum_to_one"] is True

    assert d["governance"]["contradiction_calibration_governed"] is True
    assert d["governance"]["contradictions_must_survive"] is True
