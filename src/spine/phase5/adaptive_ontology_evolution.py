from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase5"
OUT_PATH = OUT_DIR / "adaptive_ontology_evolution.json"


ONTOLOGY_EVOLUTION_DOMAINS = [
    "new_macro_entity_candidates",
    "new_regime_category_candidates",
    "new_signal_family_candidates",
    "new_contradiction_class_candidates",
    "new_sovereign_risk_terms",
    "new_narrative_structure_terms",
    "deprecated_ontology_terms",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "adaptive-ontology-evolution",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "adaptive_ontology_evolution_enabled": True,

        "ontology_evolution_domains": ONTOLOGY_EVOLUTION_DOMAINS,
        "ontology_evolution_domain_count": len(ONTOLOGY_EVOLUTION_DOMAINS),

        "ontology_objective": (
            "Control the expansion, revision, and deprecation of macro ontology terms "
            "as new entities, regimes, signals, contradictions, sovereign risks, and narrative structures emerge."
        ),

        "ontology_contract": {
            "new_terms_require_review": True,
            "deprecated_terms_tracked": True,
            "ontology_versioning_required": True,
            "semantic_drift_review_required": True,
            "human_approval_required": True,
        },

        "governance": {
            "ontology_evolution_governed": True,
            "uncontrolled_ontology_growth_forbidden": True,
            "human_approval_required": True,
            "llm_writeback_allowed": False,
            "audit_trail_required": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
