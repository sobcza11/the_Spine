from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\advisory_only_enforcement_layer.json"
)

def test_advisory_only_enforcement_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "advisory-only-enforcement-layer"
    assert d["advisory_only_enforcement_enabled"] is True
    assert d["advisory_boundary_count"] >= 7

    assert "no_autonomous_execution" in d["advisory_boundaries"]
    assert "decision_support_only" in d["advisory_boundaries"]

    assert d["enforcement_contract"]["autonomous_execution_forbidden"] is True
    assert d["enforcement_contract"]["capital_deployment_forbidden"] is True

    assert d["governance"]["decision_authority_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
