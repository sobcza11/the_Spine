from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "macro_thesis_lifecycle_tracking.json"


THESIS_STAGES = [
    "thesis_creation",
    "signal_support_attachment",
    "confidence_assignment",
    "contradiction_review",
    "operator_approval",
    "outcome_tracking",
    "post_mortem_review",
    "memory_archive",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "macro-thesis-lifecycle-tracking",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "thesis_lifecycle_tracking_enabled": True,

        "thesis_stages": THESIS_STAGES,
        "thesis_stage_count": len(THESIS_STAGES),

        "thesis_objective": (
            "Track macro theses from creation through signal support, confidence assignment, "
            "contradiction review, approval, outcome tracking, post-mortem review, and memory archive."
        ),

        "thesis_contract": {
            "thesis_creation_required": True,
            "signal_support_required": True,
            "confidence_required": True,
            "outcome_tracking_required": True,
            "post_mortem_required": True,
        },

        "governance": {
            "thesis_tracking_governed": True,
            "human_approval_required": True,
            "decision_support_only": True,
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
