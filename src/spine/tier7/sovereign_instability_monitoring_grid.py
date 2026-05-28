from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "sovereign_instability_monitoring_grid.json"


SOVEREIGN_DOMAINS = [
    "debt_sustainability_pressure",
    "fx_reserve_pressure",
    "capital_flight_pressure",
    "policy_instability_pressure",
    "regional_contagion_pressure",
    "external_funding_pressure",
    "inflation_social_stress_pressure",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "sovereign-instability-monitoring-grid",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "sovereign_monitoring_enabled": True,

        "sovereign_domains": SOVEREIGN_DOMAINS,

        "sovereign_domain_count": len(SOVEREIGN_DOMAINS),

        "monitoring_objective": (
            "Monitor sovereign instability pressure across debt, reserves, capital "
            "flight, policy instability, regional contagion, external funding, and "
            "inflation-linked social stress."
        ),

        "monitoring_contract": {
            "sovereign_pressure_visible": True,
            "regional_contagion_supported": True,
            "deterministic_inputs_required": True,
            "executive_escalation_supported": True,
            "source_traceability_required": True,
        },

        "governance": {
            "sovereign_monitoring_governed": True,
            "deterministic_inputs_authoritative": True,
            "human_review_required": True,
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
