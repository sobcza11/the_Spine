from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\adaptive_ontology_evolution.json"
)

def test_adaptive_ontology_evolution():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "adaptive-ontology-evolution"
    assert d["adaptive_ontology_evolution_enabled"] is True
    assert d["ontology_evolution_domain_count"] >= 7

    assert "new_signal_family_candidates" in d["ontology_evolution_domains"]
    assert "deprecated_ontology_terms" in d["ontology_evolution_domains"]

    assert d["ontology_contract"]["ontology_versioning_required"] is True
    assert d["ontology_contract"]["semantic_drift_review_required"] is True

    assert d["governance"]["uncontrolled_ontology_growth_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
