from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\belief_revision_protocol.json"
)

def test_belief_revision_protocol():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "belief-revision-protocol"
    assert d["belief_revision_protocol_enabled"] is True
    assert d["revision_trigger_count"] >= 7

    assert "forecast_failure" in d["revision_triggers"]
    assert "contradicting_evidence_accumulation" in d["revision_triggers"]

    assert d["revision_contract"]["belief_change_requires_trigger"] is True
    assert d["governance"]["silent_belief_revision_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
