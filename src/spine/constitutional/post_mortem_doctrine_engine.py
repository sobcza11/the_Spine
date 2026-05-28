from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "post_mortem_doctrine_engine.json"


POST_MORTEM_FIELDS = [
    "failure_id",
    "failure_class",
    "original_claim_or_forecast",
    "expected_outcome",
    "realized_outcome",
    "root_cause",
    "doctrine_implication",
    "learning_action",
    "review_status",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "post-mortem-doctrine-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "post_mortem_doctrine_enabled": True,

        "post_mortem_fields": POST_MORTEM_FIELDS,
        "post_mortem_field_count": len(POST_MORTEM_FIELDS),

        "post_mortem_objective": (
            "Convert institutional cognition failures into doctrine learning through "
            "failure records, outcome comparison, root cause, doctrine implications, and review."
        ),

        "post_mortem_contract": {
            "failure_record_required": True,
            "realized_outcome_required": True,
            "root_cause_required": True,
            "doctrine_implication_required": True,
            "human_review_required": True,
        },

        "governance": {
            "post_mortem_doctrine_governed": True,
            "failure_learning_required": True,
            "post_mortem_suppression_forbidden": True,
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
