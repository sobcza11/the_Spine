from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "fragility_disclosure_layer.json"


FRAGILITY_DOMAINS = [
    "weak_signal_support",
    "thin_source_coverage",
    "unstable_model_behavior",
    "high_uncertainty_regime",
    "unresolved_contradictions",
    "poor_historical_analog_quality",
    "operator_disagreement",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "fragility-disclosure-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "fragility_disclosure_enabled": True,

        "fragility_domains": FRAGILITY_DOMAINS,
        "fragility_domain_count": len(FRAGILITY_DOMAINS),

        "fragility_objective": (
            "Disclose where institutional cognition is weakest across signal support, source coverage, "
            "model stability, uncertainty, contradictions, analog quality, and operator disagreement."
        ),

        "fragility_contract": {
            "fragility_visibility_required": True,
            "weak_signal_disclosure_required": True,
            "source_coverage_disclosure_required": True,
            "contradiction_disclosure_required": True,
            "human_review_required": True,
        },

        "governance": {
            "fragility_disclosure_governed": True,
            "weakness_hiding_forbidden": True,
            "executive_visibility_required": True,
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
