from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\planes"
)

OUT_PATH = OUT_DIR / "cflow_plane.json"


def build_payload():

    return {
        "system": "IsoVector",
        "plane": "cflow",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "state": "commodity_pressure_watch",

        "signals": [
            {
                "signal": "commodity_breadth",
                "state": "mixed",
                "score": 68,
            },
            {
                "signal": "energy_stress",
                "state": "elevated",
                "score": 74,
            },
            {
                "signal": "supply_chain_pressure",
                "state": "moderating",
                "score": 57,
            },
        ],

        "governance": {
            "ai_generated": False,
            "measurement_source": "the_Spine",
            "representation_layer": "IsoVector",
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
