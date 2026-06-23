import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_normalization_engine_serving.json"
)

payload = {
    "metric": "GeoScen Normalization Engine",
    "category": "GeoScen",
    "sub_category": "Normalization",
    "source": "the_Spine",
    "frequency": "On Build",
    "meta": {
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "forecasting": False,
        "prediction": False,
        "trade_recommendation": False
    },
    "latest": {
        "target_scale": "0-100",
        "status": "Active"
    },
    "methods": {
        "min_max": {
            "formula":
            "(value-min)/(max-min)*100"
        },
        "percentile": {
            "formula":
            "historical percentile"
        },
        "zscore": {
            "formula":
            "(value-mean)/std"
        }
    },
    "recommended_method":
        "historical percentile"
}

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print(f"Wrote {OUTPUT}")

