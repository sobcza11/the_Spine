from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "narrative_capture_defense_system.json"


CAPTURE_VECTORS = [
    "political_pressure",
    "market_consensus_pressure",
    "media_narrative_pressure",
    "executive_confirmation_bias",
    "single_source_dependency",
    "social_sentiment_contamination",
    "institutional_groupthink",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "narrative-capture-defense-system",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "narrative_capture_defense_enabled": True,

        "capture_vectors": CAPTURE_VECTORS,
        "capture_vector_count": len(CAPTURE_VECTORS),

        "defense_objective": (
            "Protect institutional cognition from narrative capture caused by political pressure, "
            "market consensus, media influence, executive bias, social sentiment, and groupthink."
        ),

        "defense_contract": {
            "capture_detection_required": True,
            "multi_source_validation_required": True,
            "contradiction_preservation_required": True,
            "independent_reasoning_required": True,
            "human_review_required": True,
        },

        "governance": {
            "narrative_capture_governed": True,
            "groupthink_resistance_required": True,
            "consensus_pressure_override_blocked": True,
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
