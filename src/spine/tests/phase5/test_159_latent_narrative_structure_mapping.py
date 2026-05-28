from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\latent_narrative_structure_mapping.json"
)

def test_latent_narrative_structure_mapping():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "latent-narrative-structure-mapping"
    assert d["latent_narrative_structure_mapping_enabled"] is True
    assert d["narrative_structure_count"] >= 7

    assert "policy_language_clusters" in d["narrative_structures"]
    assert "contradictory_macro_narratives" in d["narrative_structures"]

    assert d["mapping_contract"]["latent_structure_detection_required"] is True
    assert d["mapping_contract"]["source_traceability_required"] is True

    assert d["governance"]["neutrality_required"] is True
    assert d["governance"]["uncited_synthesis_allowed"] is False
