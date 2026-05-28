from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\out_of_sample_degradation_tracking.json"
)

def test_out_of_sample_degradation_tracking():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "out-of-sample-degradation-tracking"
    assert d["out_of_sample_tracking_enabled"] is True
    assert d["degradation_metric_count"] >= 5

    assert d["tracking_contract"]["train_test_split_required"] is True
    assert d["tracking_contract"]["performance_decay_visible"] is True
    assert d["governance"]["overfit_risk_visible"] is True
