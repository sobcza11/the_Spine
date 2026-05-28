from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "global_liquidity_intelligence_core.json"


LIQUIDITY_DOMAINS = [
    "usd_liquidity",
    "treasury_market_liquidity",
    "central_bank_liquidity",
    "cross_border_liquidity",
    "funding_stress",
    "credit_liquidity",
    "commodity_liquidity_pressure",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "global-liquidity-intelligence-core",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "liquidity_intelligence_enabled": True,

        "liquidity_domains": LIQUIDITY_DOMAINS,

        "liquidity_domain_count": len(LIQUIDITY_DOMAINS),

        "liquidity_objective": (
            "Create a governed global liquidity intelligence core covering USD "
            "liquidity, Treasury liquidity, central-bank liquidity, cross-border "
            "funding pressure, credit liquidity, and commodity-linked stress."
        ),

        "liquidity_contract": {
            "deterministic_liquidity_inputs_required": True,
            "cross_market_pressure_visible": True,
            "funding_stress_detection_supported": True,
            "source_traceability_required": True,
            "executive_escalation_supported": True,
        },

        "governance": {
            "liquidity_intelligence_governed": True,
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
