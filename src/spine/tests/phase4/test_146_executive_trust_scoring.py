from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\executive_trust_scoring.json"
)

def test_executive_trust_scoring():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "executive-trust-scoring"
    assert d["executive_trust_scoring_enabled"] is True
    assert d["trust_component_count"] >= 8

    assert "forecast_accuracy" in d["trust_components"]
    assert "source_traceability" in d["trust_components"]
    assert "historical_stability" in d["trust_components"]

    assert d["trust_contract"]["confidence_calibration_required"] is True
    assert d["trust_contract"]["historical_stability_required"] is True

    assert d["governance"]["operator_override_visible"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
