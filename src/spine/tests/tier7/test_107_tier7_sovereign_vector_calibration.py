from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_sovereign_vector_calibration.json"
)

def test_tier7_sovereign_vector_calibration():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-sovereign-vector-calibration"
    assert d["sovereign_vector_calibration_enabled"] is True
    assert d["sovereign_vector_component_count"] >= 7

    assert d["total_component_weight"] == 1.0

    assert "fx_reserve_pressure" in d["sovereign_vector_components"]
    assert "sovereign_spread_pressure" in d["sovereign_vector_components"]
    assert "regional_contagion_pressure" in d["sovereign_vector_components"]

    assert d["calibration_contract"]["component_weights_required"] is True
    assert d["calibration_contract"]["weights_sum_to_one"] is True

    assert d["governance"]["sovereign_calibration_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
