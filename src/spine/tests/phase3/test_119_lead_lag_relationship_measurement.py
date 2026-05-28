from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\lead_lag_relationship_measurement.json"
)

def test_lead_lag_relationship_measurement():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "lead-lag-relationship-measurement"
    assert d["lead_lag_measurement_enabled"] is True
    assert d["lead_lag_test_count"] >= 4

    assert d["measurement_contract"]["lead_windows_declared"] is True
    assert d["measurement_contract"]["outcomes_declared_before_scoring"] is True
    assert d["measurement_contract"]["descriptive_only_scoring_forbidden"] is True
