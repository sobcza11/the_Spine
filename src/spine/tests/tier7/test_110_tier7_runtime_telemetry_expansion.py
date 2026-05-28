from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_runtime_telemetry_expansion.json"
)

def test_tier7_runtime_telemetry_expansion():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-runtime-telemetry-expansion"
    assert d["runtime_telemetry_expansion_enabled"] is True
    assert d["telemetry_channel_count"] >= 9

    assert "artifact_freshness" in d["telemetry_channels"]
    assert "runtime_latency" in d["telemetry_channels"]
    assert "governance_escalations" in d["telemetry_channels"]

    assert d["telemetry_contract"]["freshness_monitoring_required"] is True
    assert d["telemetry_contract"]["dashboard_monitoring_required"] is True

    assert d["alert_policy"]["missing_file_alert"] is True
    assert d["alert_policy"]["failed_pipeline_alert"] is True

    assert d["governance"]["telemetry_expansion_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
