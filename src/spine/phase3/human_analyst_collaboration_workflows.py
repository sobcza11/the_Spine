from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "human_analyst_collaboration_workflows.json"


ANALYST_WORKFLOWS = [
    "analyst_signal_review",
    "analyst_override_commentary",
    "analyst_thesis_creation",
    "analyst_thesis_update",
    "analyst_false_positive_review",
    "analyst_post_mortem_entry",
    "analyst_escalation_recommendation",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "human-analyst-collaboration-workflows",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "analyst_collaboration_enabled": True,

        "analyst_workflows": ANALYST_WORKFLOWS,
        "analyst_workflow_count": len(ANALYST_WORKFLOWS),

        "collaboration_objective": (
            "Integrate human analyst workflows into macro intelligence review through "
            "signal review, overrides, thesis creation, thesis updates, false-positive "
            "review, post-mortems, and escalation recommendations."
        ),

        "collaboration_contract": {
            "analyst_review_required": True,
            "override_commentary_supported": True,
            "thesis_lifecycle_supported": True,
            "post_mortem_entry_supported": True,
            "escalation_recommendation_supported": True,
        },

        "governance": {
            "analyst_collaboration_governed": True,
            "human_authority_preserved": True,
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
