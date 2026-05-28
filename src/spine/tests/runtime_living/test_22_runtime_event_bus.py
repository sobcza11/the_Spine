from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\runtime_living\runtime_event_bus.json")

def test_runtime_event_bus():
    assert P.exists()

    d = json.loads(P.read_text(encoding="utf-8"))

    assert d["module"] == "runtime-event-bus"
    assert d["event_count"] == len(d["events"])
    assert d["event_count"] > 0

    assert d["governance"]["event_routing_enabled"] is True
    assert d["governance"]["event_audit_required"] is True
