from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\offline_geoscen_package.json"
)

def test_offline_geoscen_package():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "offline-geoscen-package"
    assert d["offline_geoscen_package_enabled"] is True
    assert d["geoscen_component_count"] >= 6

    assert "sovereign_spread_analysis" in d["geoscen_components"]

    assert d["geoscen_contract"]["historical_replay_required"] is True
    assert d["governance"]["silent_escalation_forbidden"] is True
