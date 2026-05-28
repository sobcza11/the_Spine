from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "global_transmission_topology_engine.json"


TRANSMISSION_DOMAINS = [
    "rates_transmission",
    "fx_transmission",
    "commodity_transmission",
    "sovereign_transmission",
    "credit_transmission",
    "liquidity_transmission",
    "policy_transmission",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "global-transmission-topology-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "transmission_topology_enabled": True,

        "transmission_domains": TRANSMISSION_DOMAINS,

        "transmission_domain_count": len(TRANSMISSION_DOMAINS),

        "transmission_objective": (
            "Map global macro transmission topology across rates, FX, commodities, "
            "sovereign systems, credit, liquidity, and policy channels."
        ),

        "transmission_contract": {
            "cross_market_linkages_visible": True,
            "regional_spillover_supported": True,
            "contagion_paths_supported": True,
            "source_traceability_required": True,
            "human_review_required": True,
        },

        "governance": {
            "transmission_topology_governed": True,
            "deterministic_inputs_authoritative": True,
            "llm_writeback_allowed": False,
            "autonomous_execution_allowed": False,
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
