from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\runtime_living\websocket_synchronization.json")

def test_websocket_synchronization():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "websocket-synchronization"
    assert d["status"] == "local_sync_skeleton"
    assert len(d["channels"]) >= 3
    assert d["governance"]["read_only_streaming"] is True
    assert d["governance"]["state_mutation_requires_event"] is True
