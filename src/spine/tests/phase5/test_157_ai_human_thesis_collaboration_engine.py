from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\ai_human_thesis_collaboration_engine.json"
)

def test_ai_human_thesis_collaboration_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "ai-human-thesis-collaboration-engine"
    assert d["ai_human_thesis_collaboration_enabled"] is True
    assert d["collaboration_stage_count"] >= 8

    assert "ai_hypothesis_generation" in d["collaboration_stages"]
    assert "human_approval" in d["collaboration_stages"]

    assert d["collaboration_contract"]["human_approval_required"] is True
    assert d["collaboration_contract"]["contradiction_review_required"] is True

    assert d["governance"]["human_authority_preserved"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
