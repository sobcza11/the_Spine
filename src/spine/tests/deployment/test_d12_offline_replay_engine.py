from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\offline_replay_engine.json"
)

def test_offline_replay_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "offline-replay-engine"
    assert d["offline_replay_engine_enabled"] is True
    assert d["replay_stage_count"] >= 6

    assert "audit_reconstruction" in d["replay_stages"]

    assert d["replay_contract"]["deterministic_replay_required"] is True
    assert d["governance"]["snapshot_mutation_forbidden"] is True
