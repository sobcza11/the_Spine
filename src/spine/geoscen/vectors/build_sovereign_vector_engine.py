from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\geoscen"
)

OUT_PATH = OUT_DIR / "sovereign_vector_engine.json"


def build_payload():

    vectors = [
        {
            "country": "United States",
            "vector": {
                "pressure": 71,
                "fragility": 48,
                "liquidity": 74,
                "policy_divergence": 66,
            },
        },
        {
            "country": "Japan",
            "vector": {
                "pressure": 82,
                "fragility": 61,
                "liquidity": 52,
                "policy_divergence": 88,
            },
        },
    ]

    return {
        "system": "GeoScen",
        "module": "sovereign_vector_engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "vectors": vectors,
    }


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = build_payload()

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
