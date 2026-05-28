from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\autonomous_macro_hypothesis_engine.json"
)

def test_autonomous_macro_hypothesis_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "autonomous-macro-hypothesis-engine"
    assert d["autonomous_macro_hypothesis_engine_enabled"] is True
    assert d["hypothesis_domain_count"] >= 7

    assert "liquidity_fragility" in d["hypothesis_domains"]
    assert "cross_asset_contradictions" in d["hypothesis_domains"]

    assert d["hypothesis_contract"]["testable_hypotheses_required"] is True
    assert d["governance"]["autonomous_execution_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
