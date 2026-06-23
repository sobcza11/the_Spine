import json
from pathlib import Path
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[3]

OUTPUT = ROOT / "data/serving/cflow/cflow_completion_ledger_serving.json"


COMPONENTS = [
    ("Labor Composite", "complete"),
    ("Inflation Composite", "complete"),
    ("Energy Composite", "complete"),
    ("Transport Transmission", "complete"),
    ("Econ Composite", "complete"),
    ("Financial Transmission", "complete"),
    ("Funding Stress", "complete"),
    ("Credit Transmission", "complete"),
    ("Liquidity Constraint", "complete"),
    ("Capital Composite", "complete"),
    ("Fragility Composite", "complete"),
    ("Dispersion Composite", "complete"),
    ("C•FLOW Composite", "complete"),
    ("IV[t] Routing 8/8", "complete"),
    ("State Engine", "complete"),
    ("State History", "complete"),
    ("Regime Definitions", "complete"),
    ("Regime Engine", "complete"),
    ("Oracle C•FLOW Chamber", "complete"),
    ("IsoVector UI Hook", "complete"),
]


def main():
    now = datetime.now(timezone.utc).isoformat()

    complete = sum(1 for _, status in COMPONENTS if status == "complete")
    total = len(COMPONENTS)

    payload = {
        "metric": "C•FLOW Completion Ledger",
        "category": "C•FLOW",
        "sub_category": "Completion Audit",
        "source": "the_Spine build ledger",
        "frequency": "On Build",
        "meta": {
            "generated_at": now,
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "cpmai": True,
            "version": "1.0",
        },
        "latest": {
            "completion_pct": round((complete / total) * 100, 2),
            "complete_components": complete,
            "total_components": total,
            "status": "C•FLOW Operational Complete",
        },
        "components": [
            {
                "name": name,
                "status": status,
            }
            for name, status in COMPONENTS
        ],
        "governance": {
            "diagnostics_only": True,
            "forecasting_prohibited": True,
            "probability_prohibited": True,
            "trade_recommendations_prohibited": True,
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    