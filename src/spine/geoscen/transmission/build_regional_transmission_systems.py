from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\geoscen"
)

OUT_PATH = OUT_DIR / "regional_transmission_systems.json"


def build_payload():

    transmission = [
        {
            "source": "USD",
            "target": "EM FX",
            "stress_level": 79,
        },
        {
            "source": "China Growth",
            "target": "Commodity Exporters",
            "stress_level": 68,
        },
        {
            "source": "Energy Shock",
            "target": "EU Manufacturing",
            "stress_level": 73,
        },
    ]

    return {
        "system": "GeoScen",
        "module": "regional_transmission_systems",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "transmission_network": transmission,
    }


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = build_payload()

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
