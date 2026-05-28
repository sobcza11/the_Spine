from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "release_candidate"
OUT_PATH = OUT_DIR / "release_candidate_declaration.json"


DECLARATION_AREAS = [
    "institutional_architecture_complete",
    "governance_controls_operational",
    "constitutional_layer_operational",
    "truth_calibration_operational",
    "research_infrastructure_operational",
    "validation_framework_operational",
    "production_validation_incomplete",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "release-candidate-declaration",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "release_candidate_declaration_enabled": True,

        "release_candidate_version": "RC1",

        "declaration_areas": DECLARATION_AREAS,
        "declaration_area_count": len(DECLARATION_AREAS),

        "release_status": (
            "Institutional cognition release candidate. "
            "Architecturally advanced, governance-complete, "
            "validation-expanding, production deployment not finalized."
        ),

        "declaration_contract": {
            "rc1_visibility_required": True,
            "validation_gap_visibility_required": True,
            "production_gap_visibility_required": True,
            "governance_visibility_required": True,
            "human_review_required": True,
        },

        "governance": {
            "release_candidate_declaration_governed": True,
            "system_not_declared_finished": True,
            "truthful_maturity_reporting_required": True,
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
