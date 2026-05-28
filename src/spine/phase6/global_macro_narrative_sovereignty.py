from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase6"
OUT_PATH = OUT_DIR / "global_macro_narrative_sovereignty.json"


NARRATIVE_DEFENSE_SYSTEMS = [
    "propaganda_detection",
    "narrative_distortion_tracking",
    "cross_source_validation",
    "source_diversification",
    "consensus_bias_detection",
    "information_contamination_review",
    "contradictory_source_preservation",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "GeoScen",
        "module": "global-macro-narrative-sovereignty",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "global_macro_narrative_sovereignty_enabled": True,

        "narrative_defense_systems": NARRATIVE_DEFENSE_SYSTEMS,
        "narrative_defense_system_count": len(NARRATIVE_DEFENSE_SYSTEMS),

        "narrative_objective": (
            "Protect institutional cognition from propaganda, distortion, contamination, "
            "consensus bias, and external narrative capture."
        ),

        "narrative_contract": {
            "cross_source_validation_required": True,
            "consensus_bias_detection_required": True,
            "contradictory_sources_preserved": True,
            "human_review_required": True,
            "audit_required": True,
        },

        "governance": {
            "narrative_sovereignty_governed": True,
            "single_source_dependence_forbidden": True,
            "narrative_capture_blocked": True,
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
