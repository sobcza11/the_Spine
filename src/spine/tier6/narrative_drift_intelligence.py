from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "tier6"
OUT_PATH = OUT_DIR / "narrative_drift_intelligence.json"

DRIFT_CHANNELS = [
    "fedspeak_language",
    "earnings_tone",
    "pmi_language",
    "geopolitical_language",
    "risk_sentiment_language",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "OracleChambers",
        "module": "narrative-drift-intelligence",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "narrative_drift_enabled": True,
        "drift_channels": DRIFT_CHANNELS,
        "channel_count": len(DRIFT_CHANNELS),
        "governance": {
            "semantic_drift_requires_sources": True,
            "citation_required": True,
            "human_review_required": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
