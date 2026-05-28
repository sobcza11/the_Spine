from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier5\dockerized_runtime_segments.json"
)

def test_dockerized_runtime_segments():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "dockerized-runtime-segments"

    assert d["containerization_enabled"] is True

    assert len(d["segments"]) > 0

    assert d["governance"]["runtime_isolation_enabled"] is True
