from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "political_pressure_firewall.json"


PRESSURE_SCENARIOS = [
    "policy_alignment_pressure",
    "electoral_cycle_pressure",
    "national_interest_pressure",
    "executive_reputation_pressure",
    "market_stability_pressure",
    "public_relations_pressure",
    "regulatory_influence_pressure",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "political-pressure-firewall",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "political_pressure_firewall_enabled": True,

        "pressure_scenarios": PRESSURE_SCENARIOS,
        "pressure_scenario_count": len(PRESSURE_SCENARIOS),

        "firewall_objective": (
            "Prevent institutional cognition outputs from being distorted by political, electoral, "
            "reputational, regulatory, or public-relations pressure."
        ),

        "firewall_contract": {
            "pressure_detection_required": True,
            "independent_validation_required": True,
            "evidence_supremacy_required": True,
            "human_review_required": True,
            "audit_required": True,
        },

        "governance": {
            "political_pressure_firewall_governed": True,
            "evidence_overrides_politics": True,
            "external_pressure_override_blocked": True,
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
