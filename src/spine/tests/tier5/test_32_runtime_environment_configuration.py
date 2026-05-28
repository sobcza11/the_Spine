from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier5\runtime_environment_configuration.json"
)

def test_runtime_environment_configuration():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "runtime-environment-configuration"

    assert d["environment_governance_enabled"] is True

    assert len(d["environments"]) > 0

    assert d["governance"]["production_locking_enabled"] is True
