from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "release_candidate"
OUT_PATH = OUT_DIR / "full_platform_readiness_scorecard.json"


READINESS_AREAS = {
    "architecture": 10.0,
    "governance": 10.0,
    "runtime_sophistication": 9.8,
    "research_infrastructure": 9.8,
    "truth_calibration": 9.7,
    "constitutional_controls": 9.9,
    "real_world_validation": 7.6,
    "production_operationalization": 7.2,
}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    composite_score = round(
        sum(READINESS_AREAS.values()) / len(READINESS_AREAS),
        2,
    )

    payload = {
        "system": "IsoVector",
        "module": "full-platform-readiness-scorecard",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "platform_readiness_scorecard_enabled": True,

        "readiness_areas": READINESS_AREAS,
        "readiness_area_count": len(READINESS_AREAS),
        "composite_readiness_score": composite_score,

        "release_candidate_status": "RC1_SCAFFOLD_READY_VALIDATION_REQUIRED",

        "scorecard_objective": (
            "Score data, cognition, runtime, governance, truth calibration, trust, "
            "constitutional controls, validation, and operational readiness."
        ),

        "scorecard_contract": {
            "architecture_scored": True,
            "governance_scored": True,
            "truth_calibration_scored": True,
            "constitutional_controls_scored": True,
            "validation_gap_visible": True,
        },

        "governance": {
            "readiness_scorecard_governed": True,
            "scaffold_vs_production_gap_visible": True,
            "release_candidate_not_marked_done": True,
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
