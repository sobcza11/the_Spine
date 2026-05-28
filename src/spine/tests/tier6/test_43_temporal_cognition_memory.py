from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\temporal_cognition_memory.json")

def test_temporal_cognition_memory():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "temporal-cognition-memory"
    assert d["temporal_memory_enabled"] is True
    assert d["memory_depth"] > 0
    assert d["governance"]["llm_writeback_allowed"] is False
