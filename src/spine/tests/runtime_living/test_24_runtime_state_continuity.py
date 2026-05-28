from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\runtime_living\runtime_state_continuity.json"
)

def test_runtime_state_continuity():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "runtime-state-continuity"

    assert d["persistent_runtime_memory"] is True

    assert len(d["snapshots"]) > 0

    assert d["governance"]["runtime_state_persistence"] is True

    assert d["governance"]["llm_writeback_allowed"] is False
