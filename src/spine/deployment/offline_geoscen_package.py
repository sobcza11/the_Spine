from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "deployment"
OUT_PATH = OUT_DIR / "offline_geoscen_package.json"


GEOSCEN_COMPONENTS = [
    "sovereign_stress_monitoring",
    "cross_border_liquidity_tracking",
    "policy_divergence_detection",
    "fx_pressure_mapping",
    "sovereign_spread_analysis",
    "macro_fragility_scoring",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "offline-geoscen-package",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "offline_geoscen_package_enabled": True,

        "geoscen_components": GEOSCEN_COMPONENTS,
        "geoscen_component_count": len(GEOSCEN_COMPONENTS),

        "geoscen_objective": (
            "Render governed offline sovereign cognition snapshots "
            "for replayable institutional geopolitical stress analysis."
        ),

        "geoscen_contract": {
            "offline_execution_required": True,
            "sovereign_monitoring_required": True,
            "historical_replay_required": True,
            "audit_visibility_required": True,
            "human_review_required": True,
        },

        "governance": {
            "offline_geoscen_governed": True,
            "ungoverned_distribution_forbidden": True,
            "silent_escalation_forbidden": True,
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
