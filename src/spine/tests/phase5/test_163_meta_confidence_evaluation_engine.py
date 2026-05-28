from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\meta_confidence_evaluation_engine.json"
)

def test_meta_confidence_evaluation_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "meta-confidence-evaluation-engine"
    assert d["meta_confidence_evaluation_enabled"] is True
    assert d["meta_confidence_component_count"] >= 7

    assert "historical_calibration_quality" in d["meta_confidence_components"]
    assert "forecast_error_history" in d["meta_confidence_components"]
    assert "operator_override_frequency" in d["meta_confidence_components"]

    assert d["meta_confidence_contract"]["confidence_quality_scoring_required"] is True
    assert d["meta_confidence_contract"]["forecast_error_history_required"] is True

    assert d["governance"]["confidence_about_confidence_is_advisory"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
