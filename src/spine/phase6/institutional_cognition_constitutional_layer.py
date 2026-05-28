from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase6"
OUT_PATH = OUT_DIR / "institutional_cognition_constitutional_layer.json"


CONSTITUTIONAL_PRINCIPLES = [
    "human_authority_supremacy",
    "evidence_before_conviction",
    "uncertainty_visibility",
    "contradiction_preservation",
    "causal_validation",
    "historical_accountability",
    "governed_adaptation",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-cognition-constitutional-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "constitutional_layer_enabled": True,

        "constitutional_principles": CONSTITUTIONAL_PRINCIPLES,
        "constitutional_principle_count": len(CONSTITUTIONAL_PRINCIPLES),

        "constitutional_objective": (
            "Define the foundational institutional cognition principles governing "
            "authority, evidence, uncertainty, contradiction, causality, accountability, "
            "and adaptation."
        ),

        "constitutional_contract": {
            "human_authority_required": True,
            "evidence_governance_required": True,
            "uncertainty_visibility_required": True,
            "constitutional_alignment_required": True,
            "human_review_required": True,
        },

        "governance": {
            "constitutional_governance_enabled": True,
            "constitutional_violations_visible": True,
            "ungoverned_mutation_forbidden": True,
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
