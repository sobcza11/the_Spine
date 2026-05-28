from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "tier6"
OUT_PATH = OUT_DIR / "sovereign_contagion_intelligence.json"


TRANSMISSION_CHANNELS = [
    {"channel": "rates_transmission", "risk_level": "medium"},
    {"channel": "fx_pressure", "risk_level": "high"},
    {"channel": "commodity_shock", "risk_level": "medium"},
    {"channel": "capital_flow_stress", "risk_level": "high"},
    {"channel": "regional_policy_divergence", "risk_level": "medium"},
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "GeoScen",
        "module": "sovereign-contagion-intelligence",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "contagion_intelligence_enabled": True,
        "channel_count": len(TRANSMISSION_CHANNELS),
        "transmission_channels": TRANSMISSION_CHANNELS,
        "governance": {
            "sovereign_transmission_requires_sources": True,
            "regional_risk_review_required": True,
            "decision_support_only": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
