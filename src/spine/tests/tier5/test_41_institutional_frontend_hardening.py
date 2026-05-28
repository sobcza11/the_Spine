from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier5\institutional_frontend_hardening.json"
)

def test_institutional_frontend_hardening():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-frontend-hardening"

    assert d["frontend_hardening_enabled"] is True

    assert d["control_count"] > 0

    assert len(d["frontend_controls"]) > 0

    assert d["runtime_features"]["live_runtime_rendering"] is True

    assert d["governance"]["frontend_governance_required"] is True
