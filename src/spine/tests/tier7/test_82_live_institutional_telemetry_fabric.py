from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\live_institutional_telemetry_fabric.json"
)

def test_live_institutional_telemetry_fabric():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "live-institutional-telemetry-fabric"
    assert d["telemetry_fabric_enabled"] is True
    assert d["telemetry_domain_count"] > 0

    assert d["telemetry_contract"]["runtime_visibility_required"] is True
    assert d["telemetry_contract"]["signal_freshness_visible"] is True
    assert d["telemetry_contract"]["audit_events_visible"] is True

    assert d["governance"]["telemetry_fabric_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["autonomous_execution_allowed"] is False
