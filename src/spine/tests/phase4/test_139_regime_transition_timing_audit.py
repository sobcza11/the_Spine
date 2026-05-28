from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\regime_transition_timing_audit.json"
)

def test_regime_transition_timing_audit():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "regime-transition-timing-audit"
    assert d["regime_transition_timing_audit_enabled"] is True
    assert d["timing_audit_metric_count"] >= 7

    assert "early_detection_days" in d["timing_audit_metrics"]
    assert "late_detection_days" in d["timing_audit_metrics"]
    assert "missed_transition_count" in d["timing_audit_metrics"]

    assert d["timing_contract"]["transition_ground_truth_required"] is True
    assert d["timing_contract"]["early_late_measurement_required"] is True

    assert d["governance"]["late_detection_visible"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
