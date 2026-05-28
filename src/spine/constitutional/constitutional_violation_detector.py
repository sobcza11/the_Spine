from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "constitutional_violation_detector.json"


VIOLATION_CLASSES = [
    "evidence_violation",
    "confidence_violation",
    "source_integrity_violation",
    "human_authority_violation",
    "uncertainty_suppression_violation",
    "narrative_capture_violation",
    "constitutional_boundary_violation",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "constitutional-violation-detector",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "constitutional_violation_detector_enabled": True,

        "violation_classes": VIOLATION_CLASSES,
        "violation_class_count": len(VIOLATION_CLASSES),

        "detector_objective": (
            "Detect constitutional failures across evidence, confidence, sources, "
            "human authority, uncertainty handling, narrative capture, and governance boundaries."
        ),

        "detector_contract": {
            "violation_detection_required": True,
            "severity_scoring_required": True,
            "escalation_required": True,
            "audit_required": True,
            "human_review_required": True,
        },

        "governance": {
            "constitutional_violation_detection_governed": True,
            "unresolved_violations_forbidden": True,
            "constitutional_enforcement_required": True,
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
