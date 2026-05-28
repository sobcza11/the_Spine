from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\decision_improvement_measurement.json"
)

def test_decision_improvement_measurement():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "decision-improvement-measurement"
    assert d["decision_improvement_measurement_enabled"] is True
    assert d["improvement_metric_count"] >= 7

    assert "decision_speed_improvement" in d["improvement_metrics"]
    assert "risk_detection_improvement" in d["improvement_metrics"]
    assert "operator_confidence_improvement" in d["improvement_metrics"]

    assert d["measurement_contract"]["baseline_comparison_required"] is True
    assert d["measurement_contract"]["decision_quality_tracking_required"] is True

    assert d["governance"]["decision_execution_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
