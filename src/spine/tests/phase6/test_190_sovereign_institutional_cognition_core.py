from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\sovereign_institutional_cognition_core.json"
)

def test_sovereign_institutional_cognition_core():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "sovereign-institutional-cognition-core"
    assert d["sovereign_cognition_core_enabled"] is True
    assert d["core_system_count"] >= 7

    assert "institutional_epistemology_framework" in d["core_systems"]
    assert "recursive_institutional_learning_engine" in d["core_systems"]

    assert d["core_contract"]["institutional_sovereignty_required"] is True
    assert d["governance"]["human_authority_supremacy"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
