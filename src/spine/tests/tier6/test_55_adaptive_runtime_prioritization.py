from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\adaptive_runtime_prioritization.json"
)

def test_adaptive_runtime_prioritization():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "adaptive-runtime-prioritization"

    assert d["adaptive_prioritization_enabled"] is True

    assert d["priority_rule_count"] > 0

    assert d["governance"]["runtime_resource_governed"] is True
