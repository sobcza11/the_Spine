from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "executive_situational_awareness_theater.json"


THEATER_PANELS = [
    "macro_regime_panel",
    "risk_command_panel",
    "liquidity_pressure_panel",
    "sovereign_instability_panel",
    "contradiction_panel",
    "historical_memory_panel",
    "executive_attention_panel",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "executive-situational-awareness-theater",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "situational_awareness_theater_enabled": True,

        "theater_panels": THEATER_PANELS,

        "theater_panel_count": len(THEATER_PANELS),

        "theater_objective": (
            "Render executive situational awareness across macro regime conditions, "
            "risk command, liquidity pressure, sovereign instability, contradictions, "
            "historical memory, and executive attention."
        ),

        "theater_contract": {
            "executive_visibility_required": True,
            "runtime_state_visible": True,
            "contradictions_visible": True,
            "historical_context_visible": True,
            "decision_support_only": True,
        },

        "governance": {
            "situational_awareness_governed": True,
            "llm_writeback_allowed": False,
            "autonomous_execution_allowed": False,
            "human_review_required": True,
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
