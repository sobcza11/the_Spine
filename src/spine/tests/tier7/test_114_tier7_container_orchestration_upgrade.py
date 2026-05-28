from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_container_orchestration_upgrade.json"
)

def test_tier7_container_orchestration_upgrade():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-container-orchestration-upgrade"
    assert d["container_orchestration_upgrade_enabled"] is True
    assert d["runtime_segment_count"] >= 7

    assert "api_runtime_container" in d["runtime_segments"]
    assert "frontend_runtime_container" in d["runtime_segments"]
    assert "telemetry_runtime_container" in d["runtime_segments"]

    assert d["orchestration_contract"]["runtime_segmentation_required"] is True
    assert d["orchestration_contract"]["local_docker_first"] is True
    assert d["orchestration_contract"]["kubernetes_future_ready"] is True

    assert d["deployment_policy"]["single_monolith_avoided"] is True

    assert d["governance"]["container_orchestration_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
