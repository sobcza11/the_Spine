from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\strategic_macro_foresight_theater.json"
)

def test_strategic_macro_foresight_theater():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "strategic-macro-foresight-theater"
    assert d["strategic_foresight_enabled"] is True
    assert d["foresight_domain_count"] >= 7

    assert "reserve_currency_shifts" in d["foresight_domains"]
    assert "global_fragmentation" in d["foresight_domains"]

    assert d["foresight_contract"]["civilization_scale_reasoning_required"] is True
    assert d["governance"]["false_certainty_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
