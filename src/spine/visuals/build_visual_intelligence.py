from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\visuals"
)


def write_payload(name: str, payload: dict):

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    path = OUT_DIR / f"{name}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {path}")


def base_payload(module: str, data: dict):

    return {
        "system": "IsoVector",
        "module": module,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        **data,
    }


def main():

    write_payload(
        "contradiction_heatmaps",
        base_payload(
            "contradiction_heatmaps",
            {
                "heatmap": [
                    {"pair": "Equities vs Rates", "severity": 72},
                    {"pair": "USD vs Commodities", "severity": 61},
                ]
            },
        ),
    )

    write_payload(
        "historical_analog_overlays",
        base_payload(
            "historical_analog_overlays",
            {
                "overlays": [
                    {"regime": "1994 Tightening", "similarity": 0.71},
                    {"regime": "2018 QT", "similarity": 0.66},
                ]
            },
        ),
    )

    write_payload(
        "confidence_topology",
        base_payload(
            "confidence_topology",
            {
                "topology": [
                    {"domain": "Equities", "confidence": 74},
                    {"domain": "Rates", "confidence": 69},
                    {"domain": "FX", "confidence": 66},
                ]
            },
        ),
    )


if __name__ == "__main__":
    main()
