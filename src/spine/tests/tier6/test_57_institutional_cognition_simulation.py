from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\institutional_cognition_simulation.json"
)

def test_institutional_cognition_simulation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-cognition-simulation"

    assert d["institutional_simulation_enabled"] is True

    assert d["scenario_count"] > 0

    assert d["governance"]["simulation_not_live_execution"] is True
