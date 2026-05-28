from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\cross_agent_delegation_hierarchy.json"
)

def test_cross_agent_delegation_hierarchy():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cross-agent-delegation-hierarchy"
    assert d["agent_delegation_enabled"] is True
    assert d["agent_count"] > 0

    assert d["delegation_contract"]["agents_read_only"] is True
    assert d["delegation_contract"]["delegation_trace_required"] is True
    assert d["delegation_contract"]["agent_conflict_visible"] is True

    assert d["governance"]["agent_delegation_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["autonomous_execution_allowed"] is False
