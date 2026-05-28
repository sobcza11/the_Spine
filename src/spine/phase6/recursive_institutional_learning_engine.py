from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase6"
OUT_PATH = OUT_DIR / "recursive_institutional_learning_engine.json"


LEARNING_DOMAINS = [
    "forecast_failure_learning",
    "governance_failure_learning",
    "signal_decay_learning",
    "belief_revision_learning",
    "operator_feedback_learning",
    "historical_replay_learning",
    "cross_cycle_adaptation_learning",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "recursive-institutional-learning-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "recursive_learning_enabled": True,

        "learning_domains": LEARNING_DOMAINS,
        "learning_domain_count": len(LEARNING_DOMAINS),

        "learning_objective": (
            "Continuously improve institutional cognition by recursively learning from "
            "forecast failures, governance failures, signal decay, operator feedback, "
            "historical replay, and cross-cycle adaptation."
        ),

        "learning_contract": {
            "failure_learning_required": True,
            "historical_replay_required": True,
            "belief_revision_tracking_required": True,
            "human_review_required": True,
            "audit_required": True,
        },

        "governance": {
            "recursive_learning_governed": True,
            "untracked_learning_forbidden": True,
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
