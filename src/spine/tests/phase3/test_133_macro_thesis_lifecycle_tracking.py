from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\macro_thesis_lifecycle_tracking.json"
)

def test_macro_thesis_lifecycle_tracking():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "macro-thesis-lifecycle-tracking"
    assert d["thesis_lifecycle_tracking_enabled"] is True
    assert d["thesis_stage_count"] >= 8

    assert "thesis_creation" in d["thesis_stages"]
    assert "outcome_tracking" in d["thesis_stages"]
    assert "post_mortem_review" in d["thesis_stages"]

    assert d["thesis_contract"]["signal_support_required"] is True
    assert d["thesis_contract"]["post_mortem_required"] is True

    assert d["governance"]["human_approval_required"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
