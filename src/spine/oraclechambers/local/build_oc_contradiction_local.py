from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\oraclechambers"
)

OUT_PATH = OUT_DIR / "oc_contradiction_local.json"


def build_payload():

    contradictions = [
        {
            "left": "Equities",
            "right": "Liquidity",
            "state": "risk_on_vs_tightening",
            "severity": 72,
        },
        {
            "left": "USD",
            "right": "Commodities",
            "state": "mixed_confirmation",
            "severity": 49,
        },
    ]

    return {
        "system": "OracleChambers",
        "module": "oc-contradiction-local",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "contradictions": contradictions,

        "max_severity": max(
            x["severity"]
            for x in contradictions
        ),
    }


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = build_payload()

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
