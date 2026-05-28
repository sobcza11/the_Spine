from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\operator_trust_calibration.json"
)

def test_operator_trust_calibration():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "operator-trust-calibration"
    assert d["operator_trust_calibration_enabled"] is True
    assert d["trust_calibration_dimension_count"] >= 7

    assert "forecast_reliability" in d["trust_calibration_dimensions"]
    assert "failure_history_visibility" in d["trust_calibration_dimensions"]

    assert d["calibration_contract"]["trust_boundaries_required"] is True
    assert d["calibration_contract"]["override_tracking_required"] is True

    assert d["governance"]["blind_trust_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
