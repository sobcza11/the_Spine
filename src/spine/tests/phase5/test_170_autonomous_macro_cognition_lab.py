from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\autonomous_macro_cognition_lab.json"
)

def test_autonomous_macro_cognition_lab():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "autonomous-macro-cognition-lab"
    assert d["autonomous_macro_cognition_lab_enabled"] is True
    assert d["cognition_lab_system_count"] >= 8

    assert "hypothesis_generation" in d["cognition_lab_systems"]
    assert "causal_validation" in d["cognition_lab_systems"]
    assert "institutional_memory_graph" in d["cognition_lab_systems"]

    assert d["lab_contract"]["governed_research_required"] is True
    assert d["lab_contract"]["persistent_memory_required"] is True

    assert d["governance"]["human_authority_preserved"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
