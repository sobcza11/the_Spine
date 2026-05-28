from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\runtime_living\runtime_mutation_governance.json"
)

def test_runtime_mutation_governance():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "runtime-mutation-governance"

    assert d["governed_runtime_mutation"] is True

    assert len(d["mutation_rules"]) > 0

    assert "write_to_the_spine" in d["blocked_actions"]

    assert d["governance"]["runtime_protection_enabled"] is True

    assert d["governance"]["llm_writeback_allowed"] is False
