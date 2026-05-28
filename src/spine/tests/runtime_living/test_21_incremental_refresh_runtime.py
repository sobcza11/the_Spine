from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\runtime_living\incremental_runtime_state.json")

def test_incremental_refresh_runtime():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "incremental-refresh-runtime"
    assert d["refresh_mode"] == "partial_cognition_mutation"
    assert len(d["watched_artifacts"]) > 0
    assert d["governance"]["partial_refresh_only"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
