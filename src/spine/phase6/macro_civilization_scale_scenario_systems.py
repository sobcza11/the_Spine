from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase6"
OUT_PATH = OUT_DIR / "macro_civilization_scale_scenario_systems.json"


SCENARIO_DOMAINS = [
    "demographic_transition",
    "deglobalization_fragmentation",
    "energy_system_transition",
    "reserve_currency_transition",
    "sovereign_debt_supercycle",
    "technological_productivity_shift",
    "climate_macro_stress",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "GeoScen",
        "module": "macro-civilization-scale-scenario-systems",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "civilization_scale_scenarios_enabled": True,

        "scenario_domains": SCENARIO_DOMAINS,
        "scenario_domain_count": len(SCENARIO_DOMAINS),

        "scenario_objective": (
            "Model very long-duration strategic macro scenarios across demographics, "
            "fragmentation, energy transition, reserve currencies, debt, productivity, and climate stress."
        ),

        "scenario_contract": {
            "long_duration_modeling_required": True,
            "uncertainty_visibility_required": True,
            "scenario_not_prediction": True,
            "human_review_required": True,
            "audit_required": True,
        },

        "governance": {
            "civilization_scenarios_governed": True,
            "false_certainty_blocked": True,
            "scenario_execution_forbidden": True,
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
