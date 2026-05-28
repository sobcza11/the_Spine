from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "uncertainty_preservation_layer.json"


UNCERTAINTY_TYPES = [
    "data_uncertainty",
    "model_uncertainty",
    "regime_uncertainty",
    "causal_uncertainty",
    "source_uncertainty",
    "forecast_uncertainty",
    "operator_uncertainty",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "uncertainty-preservation-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "uncertainty_preservation_enabled": True,

        "uncertainty_types": UNCERTAINTY_TYPES,
        "uncertainty_type_count": len(UNCERTAINTY_TYPES),

        "uncertainty_objective": (
            "Prevent false certainty compression by preserving uncertainty across data, model, "
            "regime, causality, sources, forecasts, and operator disagreement."
        ),

        "uncertainty_contract": {
            "uncertainty_visibility_required": True,
            "false_certainty_forbidden": True,
            "uncertainty_type_required": True,
            "confidence_limits_required": True,
            "human_review_required": True,
        },

        "governance": {
            "uncertainty_preservation_governed": True,
            "certainty_compression_blocked": True,
            "overconfident_claims_flagged": True,
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
