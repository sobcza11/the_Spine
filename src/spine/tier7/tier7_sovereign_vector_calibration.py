from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

TIER7_DIR = ROOT / "tier7"
OUT_PATH = TIER7_DIR / "tier7_sovereign_vector_calibration.json"


SOVEREIGN_VECTOR_COMPONENTS = {
    "external_debt_pressure": {
        "weight": 0.18,
        "direction": "higher_is_riskier",
    },
    "fx_reserve_pressure": {
        "weight": 0.18,
        "direction": "higher_is_riskier",
    },
    "currency_depreciation_pressure": {
        "weight": 0.16,
        "direction": "higher_is_riskier",
    },
    "inflation_instability_pressure": {
        "weight": 0.14,
        "direction": "higher_is_riskier",
    },
    "real_rate_pressure": {
        "weight": 0.12,
        "direction": "higher_is_riskier",
    },
    "sovereign_spread_pressure": {
        "weight": 0.14,
        "direction": "higher_is_riskier",
    },
    "regional_contagion_pressure": {
        "weight": 0.08,
        "direction": "higher_is_riskier",
    },
}


def main():
    TIER7_DIR.mkdir(parents=True, exist_ok=True)

    total_weight = round(
        sum(v["weight"] for v in SOVEREIGN_VECTOR_COMPONENTS.values()),
        4,
    )

    payload = {
        "system": "GeoScen",
        "module": "tier7-sovereign-vector-calibration",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "sovereign_vector_calibration_enabled": True,

        "sovereign_vector_components": SOVEREIGN_VECTOR_COMPONENTS,

        "sovereign_vector_component_count": len(SOVEREIGN_VECTOR_COMPONENTS),

        "total_component_weight": total_weight,

        "calibration_objective": (
            "Calibrate GeoScen sovereign pressure scoring through weighted components "
            "covering external debt, reserves, currency depreciation, inflation instability, "
            "real-rate pressure, sovereign spreads, and regional contagion."
        ),

        "calibration_contract": {
            "component_weights_required": True,
            "weights_sum_to_one": total_weight == 1.0,
            "directionality_required": True,
            "historical_validation_required": True,
            "human_review_required": True,
        },

        "governance": {
            "sovereign_calibration_governed": True,
            "model_is_diagnostic": True,
            "llm_writeback_allowed": False,
            "human_review_required": True,
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
