from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase4"
OUT_PATH = OUT_DIR / "false_positive_reduction_engine.json"


REDUCTION_CONTROLS = [
    "minimum_signal_persistence_threshold",
    "multi_signal_confirmation_required",
    "confidence_penalty_for_single_source_alert",
    "cooldown_window_after_false_positive",
    "operator_review_before_escalation",
    "historical_false_positive_memory_check",
    "severity_threshold_adjustment",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "false-positive-reduction-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "false_positive_reduction_enabled": True,

        "reduction_controls": REDUCTION_CONTROLS,
        "reduction_control_count": len(REDUCTION_CONTROLS),

        "reduction_objective": (
            "Reduce unnecessary escalation noise by requiring persistence, confirmation, "
            "confidence penalties, cooldowns, operator review, false-positive memory, "
            "and severity threshold adjustment."
        ),

        "reduction_contract": {
            "false_positive_memory_required": True,
            "multi_signal_confirmation_required": True,
            "operator_review_required": True,
            "alert_noise_reduction_required": True,
            "human_review_required": True,
        },

        "governance": {
            "false_positive_reduction_governed": True,
            "suppression_requires_audit": True,
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
