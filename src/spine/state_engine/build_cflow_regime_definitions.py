import json
from pathlib import Path
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[3]

OUTPUT = ROOT / "data/serving/cflow/cflow_regime_definitions_serving.json"


REGIMES = {
    "Expansion": {
        "description": "Broad activity support with contained stress.",
        "rules": {
            "P": "low_to_moderate",
            "F": "low",
            "L": "supportive",
            "D": "contained",
            "M": "firm",
            "X": "contained",
            "C": "stable",
            "S": "contained",
        },
    },
    "Softening": {
        "description": "Growth or activity deterioration without systemic stress.",
        "rules": {
            "M": "weakening",
            "econ": "weak",
            "P": "moderate",
            "X": "moderate",
        },
    },
    "Watch": {
        "description": "Mixed diagnostics with enough stress to require monitoring.",
        "rules": {
            "P": "elevated_or_rising",
            "D": "elevated",
            "X": "elevated",
            "C": "weak_or_mixed",
        },
    },
    "Stress": {
        "description": "Pressure, dispersion, and transmission stress are elevated.",
        "rules": {
            "P": "high",
            "D": "high",
            "X": "high",
        },
    },
    "Fragmentation": {
        "description": "High dispersion with weak coherence.",
        "rules": {
            "D": "high",
            "C": "weak",
        },
    },
    "Constraint": {
        "description": "Systemicity is elevated enough to dominate classification.",
        "rules": {
            "S": "high",
        },
    },
}


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "metric": "C•FLOW Regime Definitions",
        "category": "C•FLOW",
        "sub_category": "Regime Definitions",
        "source": "the_Spine deterministic rules",
        "frequency": "On Build",
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "probability": False,
            "trade_recommendation": False,
            "version": "1.0",
        },
        "latest": {
            "regime_count": len(REGIMES),
            "status": "Active",
        },
        "regimes": REGIMES,
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    