from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\oraclechambers"
)

OUT_PATH = OUT_DIR / "oc_historical_memory_local.json"


def build_payload():

    analogs = [
        {
            "regime": "1994 tightening cycle",
            "similarity": 0.71,
        },
        {
            "regime": "2018 liquidity tightening",
            "similarity": 0.66,
        },
    ]

    return {
        "system": "OracleChambers",
        "module": "oc-historical-memory-local",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "historical_analogs": analogs,

        "top_analog": max(
            analogs,
            key=lambda x: x["similarity"]
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
    