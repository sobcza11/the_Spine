from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\institutional_cognition_compiler.json"
)

def test_institutional_cognition_compiler():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-cognition-compiler"
    assert d["cognition_compiler_enabled"] is True
    assert d["compiler_input_count"] > 0

    assert d["compiler_contract"]["deterministic_inputs_authoritative"] is True
    assert d["compiler_contract"]["compiled_output_traceable"] is True
    assert d["compiler_contract"]["contradictions_preserved"] is True

    assert d["governance"]["cognition_compiler_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["uncited_synthesis_allowed"] is False
