from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase6"
OUT_PATH = OUT_DIR / "long_duration_uncertainty_persistence.json"


UNCERTAINTY_PERSISTENCE_DOMAINS = [
    "long_horizon_forecasts",
    "regime_transition_uncertainty",
    "sovereign_instability_uncertainty",
    "policy_path_uncertainty",
    "cross_asset_uncertainty",
    "geopolitical_uncertainty",
    "structural_break_uncertainty",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "GeoScen",
        "module": "long-duration-uncertainty-persistence",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "long_duration_uncertainty_enabled": True,

        "uncertainty_persistence_domains": UNCERTAINTY_PERSISTENCE_DOMAINS,
        "uncertainty_persistence_domain_count": len(UNCERTAINTY_PERSISTENCE_DOMAINS),

        "uncertainty_objective": (
            "Preserve institutional uncertainty visibility across extended macro horizons, "
            "including policy, sovereign, geopolitical, structural, and cross-asset uncertainty."
        ),

        "uncertainty_contract": {
            "uncertainty_visibility_required": True,
            "premature_certainty_forbidden": True,
            "confidence_decay_tracking_required": True,
            "structural_break_awareness_required": True,
            "human_review_required": True,
        },

        "governance": {
            "uncertainty_persistence_governed": True,
            "false_certainty_blocked": True,
            "overconfidence_penalties_required": True,
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
