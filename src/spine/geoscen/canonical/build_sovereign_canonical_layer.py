from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\geoscen"
)

OUT_PATH = OUT_DIR / "sovereign_canonical_layer.json"


def build_payload():

    countries = [
        {
            "country": "United States",
            "fx_pressure": 64,
            "rates_pressure": 71,
            "inflation_pressure": 59,
        },
        {
            "country": "Japan",
            "fx_pressure": 82,
            "rates_pressure": 47,
            "inflation_pressure": 42,
        },
        {
            "country": "Germany",
            "fx_pressure": 58,
            "rates_pressure": 66,
            "inflation_pressure": 55,
        },
    ]

    return {
        "system": "GeoScen",
        "module": "sovereign_canonical_layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "countries": countries,

        "governance": {
            "deterministic_layer": True,
            "ai_generated": False,
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
