from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "failure_admission_protocol.json"


FAILURE_CLASSES = [
    "forecast_failure",
    "confidence_failure",
    "source_integrity_failure",
    "causal_claim_failure",
    "regime_detection_failure",
    "escalation_failure",
    "operator_trust_failure",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "failure-admission-protocol",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "failure_admission_protocol_enabled": True,

        "failure_classes": FAILURE_CLASSES,
        "failure_class_count": len(FAILURE_CLASSES),

        "protocol_objective": (
            "Force institutional cognition to record failures across forecasts, confidence, "
            "sources, causality, regimes, escalations, and operator trust."
        ),

        "protocol_contract": {
            "failure_record_required": True,
            "failure_type_required": True,
            "root_cause_required": True,
            "post_mortem_required": True,
            "human_review_required": True,
        },

        "governance": {
            "failure_admission_governed": True,
            "failure_suppression_forbidden": True,
            "failure_visibility_required": True,
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
