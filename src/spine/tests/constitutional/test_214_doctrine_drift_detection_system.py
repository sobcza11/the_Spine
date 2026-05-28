from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\doctrine_drift_detection_system.json"
)

def test_doctrine_drift_detection_system():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "doctrine-drift-detection-system"
    assert d["doctrine_drift_detection_enabled"] is True
    assert d["drift_signal_count"] >= 7

    assert "confidence_inflation" in d["drift_signals"]
    assert "governance_boundary_erosion" in d["drift_signals"]

    assert d["drift_contract"]["baseline_doctrine_required"] is True
    assert d["drift_contract"]["deviation_measurement_required"] is True

    assert d["governance"]["silent_doctrine_mutation_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
