from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\oraclechambers"
)

OUT_PATH = OUT_DIR / "oc_final_metric_local.json"


def build_payload():

    confidence = 74
    deployability = 81
    governance = 93

    final_score = (
        confidence * 0.4 +
        deployability * 0.4 +
        governance * 0.2
    )

    return {
        "system": "OracleChambers",
        "module": "oc-final-metric-local",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "scores": {
            "confidence": confidence,
            "deployability": deployability,
            "governance": governance,
            "final_score": round(final_score, 2),
        },

        "state": (
            "deployable"
            if final_score >= 75
            else "watch"
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