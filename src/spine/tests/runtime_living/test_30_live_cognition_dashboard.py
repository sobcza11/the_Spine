from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\runtime_living\live_cognition_dashboard.json"
)

def test_live_cognition_dashboard():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "live-cognition-dashboard"

    assert d["persistent_live_cognition"] is True

    assert d["dashboard_count"] > 0

    assert len(d["dashboards"]) > 0

    assert d["runtime_features"]["event_driven_updates"] is True

    assert d["governance"]["dashboard_governance_enabled"] is True
