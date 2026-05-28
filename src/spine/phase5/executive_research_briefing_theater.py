from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase5"
OUT_PATH = OUT_DIR / "executive_research_briefing_theater.json"


THEATER_COMPONENTS = [
    "live_research_hypotheses",
    "forecast_competition_panels",
    "confidence_state_panels",
    "causal_validation_panels",
    "historical_analog_panels",
    "contradiction_heatmaps",
    "research_governance_panels",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "executive-research-briefing-theater",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "executive_research_briefing_theater_enabled": True,

        "theater_components": THEATER_COMPONENTS,
        "theater_component_count": len(THEATER_COMPONENTS),

        "theater_objective": (
            "Render a live institutional research environment showing active hypotheses, "
            "forecast competitions, confidence states, causal validation, analog discovery, "
            "contradictions, and governance oversight."
        ),

        "theater_contract": {
            "live_research_visibility_required": True,
            "confidence_visibility_required": True,
            "governance_visibility_required": True,
            "historical_analog_visibility_required": True,
            "human_review_required": True,
        },

        "governance": {
            "research_theater_governed": True,
            "operator_override_visible": True,
            "research_promotion_requires_review": True,
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
