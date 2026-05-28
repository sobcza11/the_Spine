from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\autonomous_contradiction_arbitration.json")

def test_autonomous_contradiction_arbitration():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "autonomous-contradiction-arbitration"
    assert d["arbitration_enabled"] is True
    assert d["rule_count"] > 0
    assert d["governance"]["autonomous_resolution_is_advisory"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
