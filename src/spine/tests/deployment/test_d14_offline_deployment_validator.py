from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\offline_deployment_validator.json"
)

def test_offline_deployment_validator():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "offline-deployment-validator"
    assert d["offline_deployment_validator_enabled"] is True
    assert d["validation_area_count"] >= 10

    assert "replay_engine" in d["validation_areas"]

    assert d["validator_contract"]["replayability_validation_required"] is True
    assert d["governance"]["unsafe_deployment_declaration_forbidden"] is True
