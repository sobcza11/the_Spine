from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\institutional_os_dashboard.json"
)

def test_institutional_os_dashboard():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-os-dashboard"
    assert d["institutional_os_dashboard_enabled"] is True
    assert d["dashboard_section_count"] > 0

    assert d["dashboard_contract"]["single_os_view_enabled"] is True
    assert d["dashboard_contract"]["integration_readiness_visible"] is True
    assert d["dashboard_contract"]["decision_support_only"] is True

    assert d["governance"]["dashboard_governance_enabled"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["autonomous_execution_allowed"] is False
