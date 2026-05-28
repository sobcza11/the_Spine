from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\release_candidate\executive_release_dashboard.json"
)

def test_executive_release_dashboard():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "executive-release-dashboard"
    assert d["executive_release_dashboard_enabled"] is True
    assert d["dashboard_panel_count"] >= 8

    assert d["dashboard_status"] == "RC1_EXECUTIVE_VIEW_READY"

    assert "readiness_scorecard" in d["dashboard_panels"]
    assert "real_world_validation_gap" in d["dashboard_panels"]

    assert d["dashboard_contract"]["rc1_status_visible"] is True
    assert d["governance"]["release_candidate_not_marked_done"] is True
