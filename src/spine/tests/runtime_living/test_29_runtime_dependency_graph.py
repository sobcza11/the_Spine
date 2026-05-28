from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\runtime_living\runtime_dependency_graph.json"
)

def test_runtime_dependency_graph():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "runtime-dependency-graph"

    assert d["dependency_tracking_enabled"] is True

    assert d["dependency_count"] > 0

    assert len(d["dependencies"]) > 0

    assert d["governance"]["runtime_propagation_governed"] is True
