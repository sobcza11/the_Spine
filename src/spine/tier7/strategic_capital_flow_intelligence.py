from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "strategic_capital_flow_intelligence.json"


CAPITAL_FLOW_DOMAINS = [
    "cross_border_flows",
    "safe_haven_rotation",
    "usd_funding_pressure",
    "em_capital_pressure",
    "reserve_currency_pressure",
    "commodity_linked_capital_flows",
    "sovereign_spread_capital_pressure",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "strategic-capital-flow-intelligence",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "capital_flow_intelligence_enabled": True,

        "capital_flow_domains": CAPITAL_FLOW_DOMAINS,

        "capital_flow_domain_count": len(CAPITAL_FLOW_DOMAINS),

        "capital_flow_objective": (
            "Create governed strategic capital-flow intelligence across cross-border "
            "flows, safe-haven rotation, USD funding pressure, EM capital pressure, "
            "reserve currency pressure, commodity-linked flows, and sovereign spreads."
        ),

        "capital_flow_contract": {
            "cross_border_pressure_visible": True,
            "safe_haven_rotation_supported": True,
            "sovereign_capital_pressure_supported": True,
            "source_traceability_required": True,
            "human_review_required": True,
        },

        "governance": {
            "capital_flow_intelligence_governed": True,
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
