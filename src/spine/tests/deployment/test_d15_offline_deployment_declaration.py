from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\offline_deployment_declaration.json"
)

def test_offline_deployment_declaration():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "offline-deployment-declaration"
    assert d["offline_deployment_declaration_enabled"] is True
    assert d["deployment_stage"] == "OFFLINE_RC1"
    assert d["declaration_component_count"] >= 9

    assert "offline_replay_operational" in d["declaration_components"]

    assert d["deployment_contract"]["deterministic_runtime_required"] is True
    assert d["governance"]["live_runtime_not_yet_authorized"] is True
