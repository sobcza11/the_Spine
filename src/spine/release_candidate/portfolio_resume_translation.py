from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "release_candidate"
OUT_PATH = OUT_DIR / "portfolio_resume_translation.json"


TRANSLATION_AREAS = [
    "institutional_governance_architecture",
    "macro_cognition_research",
    "runtime_orchestration",
    "truth_calibration_frameworks",
    "constitutional_ai_controls",
    "forecast_validation_systems",
    "executive_decision_infrastructure",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "portfolio-resume-translation",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "portfolio_resume_translation_enabled": True,

        "translation_areas": TRANSLATION_AREAS,
        "translation_area_count": len(TRANSLATION_AREAS),

        "translation_objective": (
            "Translate institutional cognition infrastructure into credible external "
            "portfolio, research, architecture, governance, and executive-language narratives."
        ),

        "translation_contract": {
            "architecture_translation_required": True,
            "governance_translation_required": True,
            "research_translation_required": True,
            "executive_translation_required": True,
            "human_review_required": True,
        },

        "governance": {
            "portfolio_translation_governed": True,
            "inflated_claims_forbidden": True,
            "real_vs_scaffold_boundaries_required": True,
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
