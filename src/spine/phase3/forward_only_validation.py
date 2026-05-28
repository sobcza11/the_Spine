from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "forward_only_validation.json"


VALIDATION_RULES = [
    "signals_must_be_computed_before_outcome_window",
    "future_data_access_forbidden",
    "formula_changes_after_test_forbidden",
    "train_test_time_split_required",
    "outcome_window_declared_before_scoring",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "forward-only-validation",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "forward_only_validation_enabled": True,

        "validation_rules": VALIDATION_RULES,

        "validation_rule_count": len(VALIDATION_RULES),

        "forward_validation_contract": {
            "future_data_forbidden": True,
            "pre_outcome_signal_required": True,
            "time_split_required": True,
            "formula_freeze_required": True,
            "human_review_required": True,
        },

        "governance": {
            "forward_validation_governed": True,
            "hindsight_bias_blocked": True,
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
