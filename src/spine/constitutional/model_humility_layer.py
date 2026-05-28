from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "model_humility_layer.json"


HUMILITY_REQUIREMENTS = [
    "known_limitations",
    "blind_spot_disclosure",
    "uncertainty_statement",
    "confidence_boundary",
    "alternative_explanations",
    "failure_history_reference",
    "human_review_flag",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "model-humility-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "model_humility_enabled": True,

        "humility_requirements": HUMILITY_REQUIREMENTS,
        "humility_requirement_count": len(HUMILITY_REQUIREMENTS),

        "humility_objective": (
            "Require cognition outputs to disclose limitations, blind spots, uncertainty, "
            "confidence boundaries, alternatives, failure history, and human review needs."
        ),

        "humility_contract": {
            "limitations_required": True,
            "blind_spots_required": True,
            "uncertainty_required": True,
            "alternative_explanations_required": True,
            "human_review_required": True,
        },

        "governance": {
            "model_humility_governed": True,
            "false_omniscience_blocked": True,
            "limits_must_be_visible": True,
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
