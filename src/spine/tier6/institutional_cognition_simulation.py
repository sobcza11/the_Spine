from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier6"
OUT_PATH = OUT_DIR / "institutional_cognition_simulation.json"


SIMULATION_SCENARIOS = [
    {
        "scenario": "global_liquidity_shock",
        "severity": "high",
    },
    {
        "scenario": "commodity_supply_crisis",
        "severity": "medium",
    },
    {
        "scenario": "regional_sovereign_default",
        "severity": "high",
    },
    {
        "scenario": "policy_reversal_cycle",
        "severity": "medium",
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-cognition-simulation",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "institutional_simulation_enabled": True,

        "simulation_scenarios": SIMULATION_SCENARIOS,

        "scenario_count": len(
            SIMULATION_SCENARIOS
        ),

        "governance": {
            "simulation_not_live_execution": True,
            "human_review_required": True,
            "stress_testing_enabled": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
