from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase4"
OUT_PATH = OUT_DIR / "cognitive_uncertainty_decomposition.json"


UNCERTAINTY_COMPONENTS = {
    "data_uncertainty": "missing_or_stale_measurements",
    "model_uncertainty": "unstable_or_weak_signal_relationships",
    "regime_uncertainty": "unclear_macro_state_transition",
    "narrative_uncertainty": "conflicting_or_distorted_language_signals",
    "source_uncertainty": "low_quality_or_concentrated_sources",
    "contradiction_uncertainty": "high_cross_asset_fracture",
    "operator_uncertainty": "human_review_or_override_disagreement",
}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "cognitive-uncertainty-decomposition",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "uncertainty_decomposition_enabled": True,

        "uncertainty_components": UNCERTAINTY_COMPONENTS,
        "uncertainty_component_count": len(UNCERTAINTY_COMPONENTS),

        "uncertainty_objective": (
            "Explain why confidence changes by decomposing uncertainty into data, model, "
            "regime, narrative, source, contradiction, and operator uncertainty."
        ),

        "uncertainty_contract": {
            "confidence_change_explanation_required": True,
            "uncertainty_components_required": True,
            "contradiction_uncertainty_required": True,
            "source_uncertainty_required": True,
            "human_review_required": True,
        },

        "governance": {
            "uncertainty_decomposition_governed": True,
            "confidence_not_overstated": True,
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
