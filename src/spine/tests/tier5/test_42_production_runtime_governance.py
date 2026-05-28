from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier5\production_runtime_governance.json"
)

def test_production_runtime_governance():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "production-runtime-governance"

    assert d["production_governance_enabled"] is True

    assert d["control_count"] > 0

    assert len(d["governance_controls"]) > 0

    assert d["runtime_features"]["production_locking"] is True

    assert d["governance"]["runtime_governance_enforced"] is True
