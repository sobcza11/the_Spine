from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\agents\base_agent_runtime.json")

def test_base_agent_runtime():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "base-agent-runtime"
    assert d["governance"]["read_only"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
