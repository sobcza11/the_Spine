from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\narrative_influence_mapping_engine.json"
)

def test_narrative_influence_mapping_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "narrative-influence-mapping-engine"
    assert d["narrative_mapping_enabled"] is True
    assert d["narrative_domain_count"] > 0

    assert d["narrative_contract"]["source_traceability_required"] is True
    assert d["narrative_contract"]["uncited_synthesis_allowed"] is False
    assert d["narrative_contract"]["narrative_drift_visible"] is True

    assert d["governance"]["narrative_mapping_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["neutrality_required"] is True
