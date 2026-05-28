from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "narrative_influence_mapping_engine.json"


NARRATIVE_DOMAINS = [
    "central_bank_narrative",
    "inflation_narrative",
    "growth_narrative",
    "sovereign_risk_narrative",
    "liquidity_narrative",
    "credit_stress_narrative",
    "geopolitical_transmission_narrative",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "narrative-influence-mapping-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "narrative_mapping_enabled": True,

        "narrative_domains": NARRATIVE_DOMAINS,

        "narrative_domain_count": len(NARRATIVE_DOMAINS),

        "narrative_objective": (
            "Map institutional macro narratives across central-bank language, inflation, "
            "growth, sovereign risk, liquidity, credit stress, and geopolitical transmission."
        ),

        "narrative_contract": {
            "source_traceability_required": True,
            "uncited_synthesis_allowed": False,
            "narrative_drift_visible": True,
            "influence_mapping_supported": True,
            "human_review_required": True,
        },

        "governance": {
            "narrative_mapping_governed": True,
            "llm_writeback_allowed": False,
            "autonomous_execution_allowed": False,
            "neutrality_required": True,
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
