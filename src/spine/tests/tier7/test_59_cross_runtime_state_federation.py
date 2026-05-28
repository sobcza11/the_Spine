from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\cross_runtime_state_federation.json"
)

def test_cross_runtime_state_federation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cross-runtime-state-federation"
    assert d["state_federation_enabled"] is True
    assert d["runtime_count"] > 0

    assert d["state_contract"]["shared_runtime_state_required"] is True
    assert d["state_contract"]["event_replay_supported"] is True
    assert d["state_contract"]["runtime_continuity_required"] is True

    assert d["governance"]["state_federation_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["audit_trail_required"] is True
