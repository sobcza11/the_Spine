from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\oraclechambers")
OUT_PATH = OUT_DIR / "oc_attention_routing_local.json"


def build_payload():
    items = [
        {"area": "Equities", "reason": "Leadership improving but confirmation incomplete", "priority": 82},
        {"area": "Rates", "reason": "Policy/liquidity confirmation required", "priority": 76},
        {"area": "FX", "reason": "Dollar stress routing not yet operational", "priority": 71},
    ]

    return {
        "system": "OracleChambers",
        "module": "oc-attention-routing-local",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "routing_state": "active_watch",
        "executive_attention": sorted(items, key=lambda x: x["priority"], reverse=True),
        "top_priority": max(items, key=lambda x: x["priority"]),
        "governance": {
            "ai_generated": False,
            "writeback_allowed": False,
            "measurement_source": "the_Spine",
            "interpretation_layer": "OracleChambers",
        },
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = build_payload()

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
