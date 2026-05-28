from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "adaptive_portfolio_intelligence_layer.json"


PORTFOLIO_DOMAINS = [
    "macro_exposure_mapping",
    "cross_asset_sensitivity",
    "regime_adjustment_context",
    "liquidity_risk_context",
    "sovereign_exposure_context",
    "inflation_sensitivity_context",
    "policy_risk_context",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "adaptive-portfolio-intelligence-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "portfolio_intelligence_enabled": True,

        "portfolio_domains": PORTFOLIO_DOMAINS,

        "portfolio_domain_count": len(PORTFOLIO_DOMAINS),

        "portfolio_objective": (
            "Map macro regime conditions, cross-asset sensitivity, liquidity pressure, "
            "sovereign exposure, inflation sensitivity, and policy risk into governed "
            "portfolio intelligence context."
        ),

        "portfolio_contract": {
            "decision_support_only": True,
            "execution_not_allowed": True,
            "portfolio_context_traceable": True,
            "risk_exposure_visible": True,
            "human_review_required": True,
        },

        "governance": {
            "portfolio_intelligence_governed": True,
            "autonomous_execution_allowed": False,
            "investment_advice_generated": False,
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
