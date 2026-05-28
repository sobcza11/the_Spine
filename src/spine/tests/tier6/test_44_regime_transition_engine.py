from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\regime_transition_engine.json")

def test_regime_transition_engine():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "regime-transition-engine"
    assert d["regime_transition_enabled"] is True
    assert d["state_count"] > 0
    assert d["governance"]["probabilistic_not_deterministic"] is True
