from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase5"
OUT_PATH = OUT_DIR / "institutional_cognition_doctrine_layer.json"


DOCTRINE_PRINCIPLES = [
    "deterministic_measurements_authoritative",
    "confidence_must_be_calibrated",
    "causality_required_for_escalation",
    "human_authority_preserved",
    "contradictions_must_remain_visible",
    "historical_failures_must_be_preserved",
    "governance_overrides_model_speed",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-cognition-doctrine-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "institutional_cognition_doctrine_enabled": True,

        "doctrine_principles": DOCTRINE_PRINCIPLES,
        "doctrine_principle_count": len(DOCTRINE_PRINCIPLES),

        "doctrine_objective": (
            "Formalize institutional macro cognition operating principles governing "
            "measurement authority, calibrated confidence, causality, contradiction visibility, "
            "historical preservation, and human authority."
        ),

        "doctrine_contract": {
            "human_authority_required": True,
            "confidence_calibration_required": True,
            "causal_validation_required": True,
            "contradiction_visibility_required": True,
            "governance_required": True,
        },

        "governance": {
            "doctrine_layer_governed": True,
            "ungoverned_cognition_forbidden": True,
            "human_override_required": True,
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
