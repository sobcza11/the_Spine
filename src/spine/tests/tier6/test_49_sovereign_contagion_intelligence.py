from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\sovereign_contagion_intelligence.json")

def test_sovereign_contagion_intelligence():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "sovereign-contagion-intelligence"
    assert d["contagion_intelligence_enabled"] is True
    assert d["channel_count"] > 0
    assert d["governance"]["decision_support_only"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
