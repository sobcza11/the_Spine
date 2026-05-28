from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\live_shadow_deployment.json"
)

def test_live_shadow_deployment():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "live-shadow-deployment"
    assert d["live_shadow_deployment_enabled"] is True
    assert d["shadow_runtime_rule_count"] >= 5

    assert d["shadow_contract"]["decision_execution_forbidden"] is True
    assert d["shadow_contract"]["continuous_runtime_required"] is True
    assert d["shadow_contract"]["outcome_logging_required"] is True

    assert d["governance"]["shadow_deployment_governed"] is True
    assert d["governance"]["execution_blocked"] is True
