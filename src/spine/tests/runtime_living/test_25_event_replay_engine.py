from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\runtime_living\event_replay_log.json"
)

def test_event_replay_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "event-replay-engine"

    assert d["replay_supported"] is True

    assert d["replay_event_count"] > 0

    assert len(d["replay_events"]) > 0

    assert d["governance"]["runtime_reconstruction_enabled"] is True
