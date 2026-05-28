from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\narrative_drift_intelligence.json")

def test_narrative_drift_intelligence():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "narrative-drift-intelligence"
    assert d["narrative_drift_enabled"] is True
    assert d["channel_count"] > 0
    assert d["governance"]["citation_required"] is True
