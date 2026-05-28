from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\truth_hierarchy_engine.json"
)

def test_truth_hierarchy_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "truth-hierarchy-engine"
    assert d["truth_hierarchy_enabled"] is True
    assert d["truth_hierarchy_level_count"] >= 7

    assert d["truth_hierarchy"]["tier_1"] == "audited_or_official_data"
    assert d["truth_hierarchy"]["tier_7"] == "unverified_claims"

    assert d["hierarchy_contract"]["model_hypotheses_not_truth"] is True
    assert d["hierarchy_contract"]["unverified_claims_lowest_authority"] is True

    assert d["governance"]["information_democracy_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
