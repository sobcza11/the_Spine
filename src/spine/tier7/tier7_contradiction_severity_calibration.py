from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

TIER7_DIR = ROOT / "tier7"
OUT_PATH = TIER7_DIR / "tier7_contradiction_severity_calibration.json"


CONTRADICTION_COMPONENTS = {
    "rates_vs_equities": {
        "weight": 0.18,
        "severity_channel": "duration_risk_vs_growth_expectations",
    },
    "credit_vs_equities": {
        "weight": 0.18,
        "severity_channel": "default_risk_vs_risk_appetite",
    },
    "fx_vs_rates": {
        "weight": 0.14,
        "severity_channel": "currency_pressure_vs_policy_path",
    },
    "commodities_vs_inflation": {
        "weight": 0.14,
        "severity_channel": "input_costs_vs_price_regime",
    },
    "liquidity_vs_volatility": {
        "weight": 0.18,
        "severity_channel": "funding_conditions_vs_market_stress",
    },
    "sovereign_vs_capital_flows": {
        "weight": 0.10,
        "severity_channel": "country_risk_vs_cross_border_pressure",
    },
    "policy_vs_market_pricing": {
        "weight": 0.08,
        "severity_channel": "central_bank_path_vs_asset_pricing",
    },
}


def main():
    TIER7_DIR.mkdir(parents=True, exist_ok=True)

    total_weight = round(
        sum(v["weight"] for v in CONTRADICTION_COMPONENTS.values()),
        4,
    )

    payload = {
        "system": "IsoVector",
        "module": "tier7-contradiction-severity-calibration",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "contradiction_severity_calibration_enabled": True,

        "contradiction_components": CONTRADICTION_COMPONENTS,

        "contradiction_component_count": len(CONTRADICTION_COMPONENTS),

        "total_component_weight": total_weight,

        "calibration_objective": (
            "Calibrate empirical contradiction severity across rates, equities, credit, "
            "FX, commodities, inflation, liquidity, volatility, sovereign pressure, "
            "capital flows, policy, and market pricing."
        ),

        "severity_contract": {
            "component_weights_required": True,
            "weights_sum_to_one": total_weight == 1.0,
            "cross_asset_channels_required": True,
            "severity_weighting_required": True,
            "historical_validation_required": True,
        },

        "governance": {
            "contradiction_calibration_governed": True,
            "contradictions_must_survive": True,
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
