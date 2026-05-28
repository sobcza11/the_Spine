from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase4"
OUT_PATH = OUT_DIR / "narrative_distortion_detection.json"


DISTORTION_CHANNELS = [
    "source_concentration_bias",
    "political_framing_bias",
    "market_panic_language",
    "policy_misinterpretation_risk",
    "propaganda_contamination_risk",
    "single_narrative_overdominance",
    "unsupported_causal_claims",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "FedSpeak",
        "module": "narrative-distortion-detection",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "narrative_distortion_detection_enabled": True,

        "distortion_channels": DISTORTION_CHANNELS,
        "distortion_channel_count": len(DISTORTION_CHANNELS),

        "distortion_objective": (
            "Detect narrative distortion from source concentration, political framing, "
            "panic language, policy misinterpretation, propaganda contamination, narrative "
            "overdominance, and unsupported causal claims."
        ),

        "distortion_contract": {
            "source_concentration_check_required": True,
            "bias_detection_required": True,
            "unsupported_causality_detection_required": True,
            "narrative_overdominance_detection_required": True,
            "human_review_required": True,
        },

        "governance": {
            "narrative_distortion_governed": True,
            "neutrality_required": True,
            "uncited_synthesis_allowed": False,
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
