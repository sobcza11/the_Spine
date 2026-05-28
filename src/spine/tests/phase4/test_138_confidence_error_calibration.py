from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\confidence_error_calibration.json"
)

def test_confidence_error_calibration():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "confidence-error-calibration"
    assert d["confidence_error_calibration_enabled"] is True
    assert d["confidence_error_metric_count"] >= 7

    assert "calibration_error" in d["confidence_error_metrics"]
    assert "overconfidence_rate" in d["confidence_error_metrics"]
    assert "brier_score" in d["confidence_error_metrics"]

    assert d["calibration_contract"]["actual_accuracy_required"] is True
    assert d["calibration_contract"]["overconfidence_detection_required"] is True

    assert d["governance"]["confidence_is_not_truth"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
