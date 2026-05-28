from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\human_analyst_collaboration_workflows.json"
)

def test_human_analyst_collaboration_workflows():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "human-analyst-collaboration-workflows"
    assert d["analyst_collaboration_enabled"] is True
    assert d["analyst_workflow_count"] >= 7

    assert "analyst_signal_review" in d["analyst_workflows"]
    assert "analyst_thesis_creation" in d["analyst_workflows"]
    assert "analyst_post_mortem_entry" in d["analyst_workflows"]

    assert d["collaboration_contract"]["thesis_lifecycle_supported"] is True
    assert d["governance"]["human_authority_preserved"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
