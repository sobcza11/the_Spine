from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\offline_contradiction_package.json"
)

def test_offline_contradiction_package():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "offline-contradiction-package"
    assert d["offline_contradiction_package_enabled"] is True
    assert d["contradiction_class_count"] >= 6

    assert "sovereign_spread_instability" in d["contradiction_classes"]

    assert d["contradiction_contract"]["historical_replay_required"] is True
    assert d["governance"]["silent_escalation_forbidden"] is True
