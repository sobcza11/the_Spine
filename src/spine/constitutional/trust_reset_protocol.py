from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "trust_reset_protocol.json"


TRUST_RESET_TRIGGERS = [
    "major_forecast_failure",
    "confidence_legitimacy_failure",
    "source_integrity_failure",
    "constitutional_violation",
    "operator_trust_breakdown",
    "high_impact_false_positive",
    "unresolved_failure_post_mortem",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "trust-reset-protocol",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "trust_reset_protocol_enabled": True,

        "trust_reset_triggers": TRUST_RESET_TRIGGERS,
        "trust_reset_trigger_count": len(TRUST_RESET_TRIGGERS),

        "reset_objective": (
            "Recover institutional trust after major failures by requiring reset triggers, post-mortems, "
            "confidence recalibration, operator review, and controlled trust restoration."
        ),

        "reset_contract": {
            "trust_reset_trigger_required": True,
            "post_mortem_required": True,
            "confidence_recalibration_required": True,
            "operator_review_required": True,
            "human_review_required": True,
        },

        "governance": {
            "trust_reset_governed": True,
            "automatic_trust_restoration_forbidden": True,
            "failure_resolution_required": True,
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
