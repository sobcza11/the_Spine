from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\runtime_living\runtime_anomalies.json"
)

def test_runtime_anomaly_detection():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "runtime-anomaly-detection"

    assert d["runtime_monitoring_enabled"] is True

    assert d["systems_monitored"] > 0

    assert len(d["anomalies"]) > 0

    assert d["governance"]["drift_detection_enabled"] is True

    assert d["governance"]["runtime_corruption_detection"] is True
