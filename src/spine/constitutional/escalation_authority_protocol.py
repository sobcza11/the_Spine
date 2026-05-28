from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "escalation_authority_protocol.json"


ESCALATION_CLASSES = [
    "liquidity_stress_escalation",
    "sovereign_risk_escalation",
    "regime_transition_escalation",
    "confidence_failure_escalation",
    "source_integrity_escalation",
    "constitutional_violation_escalation",
    "operator_review_escalation",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "escalation-authority-protocol",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "escalation_authority_protocol_enabled": True,

        "escalation_classes": ESCALATION_CLASSES,
        "escalation_class_count": len(ESCALATION_CLASSES),

        "escalation_objective": (
            "Govern who or what can escalate institutional signals, failures, risks, source issues, "
            "and constitutional violations into human review workflows."
        ),

        "escalation_contract": {
            "escalation_classification_required": True,
            "human_review_required": True,
            "escalation_reason_required": True,
            "authority_boundary_required": True,
            "audit_required": True,
        },

        "governance": {
            "escalation_protocol_governed": True,
            "unreviewed_escalation_forbidden": True,
            "operator_review_required": True,
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
