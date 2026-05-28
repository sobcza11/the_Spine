from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\deployment_veto_system.json"
)

def test_deployment_veto_system():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "deployment-veto-system"
    assert d["deployment_veto_enabled"] is True
    assert d["veto_trigger_count"] >= 7

    assert "constitutional_violation" in d["veto_triggers"]
    assert "failed_validation_gate" in d["veto_triggers"]

    assert d["veto_contract"]["unsafe_promotion_blocked"] is True
    assert d["veto_contract"]["veto_reason_required"] is True

    assert d["governance"]["autonomous_deployment_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
