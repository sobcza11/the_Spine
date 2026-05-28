from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "belief_revision_protocol.json"


REVISION_TRIGGERS = [
    "new_high_authority_evidence",
    "contradicting_evidence_accumulation",
    "forecast_failure",
    "regime_shift_detection",
    "source_integrity_degradation",
    "confidence_calibration_failure",
    "operator_review_request",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "belief-revision-protocol",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "belief_revision_protocol_enabled": True,

        "revision_triggers": REVISION_TRIGGERS,
        "revision_trigger_count": len(REVISION_TRIGGERS),

        "revision_objective": (
            "Govern when institutional beliefs can change based on new evidence, contradiction, "
            "forecast failure, regime shifts, source degradation, calibration failure, or operator review."
        ),

        "revision_contract": {
            "belief_change_requires_trigger": True,
            "revision_reason_required": True,
            "evidence_delta_required": True,
            "confidence_update_required": True,
            "human_review_required": True,
        },

        "governance": {
            "belief_revision_governed": True,
            "silent_belief_revision_forbidden": True,
            "revision_history_required": True,
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
