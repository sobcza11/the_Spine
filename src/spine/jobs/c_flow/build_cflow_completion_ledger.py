import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[4]

OUT_FILE = (
    ROOT
    / "data/serving/cflow"
    / "cflow_completion_ledger_serving.json"
)

COMPONENTS = [
    {
        "name": "Capital Composite",
        "file": "capital_composite_serving.json",
        "status": "complete",
        "phase": "8A",
    },
    {
        "name": "Credit Transmission Composite",
        "file": "credit_transmission_composite_serving.json",
        "status": "complete",
        "phase": "8B",
    },
    {
        "name": "Liquidity Constraint Composite",
        "file": "liquidity_constraint_composite_serving.json",
        "status": "complete",
        "phase": "8C",
    },
    {
        "name": "Financial Transmission Composite",
        "file": "financial_transmission_composite_serving.json",
        "status": "complete",
        "phase": "8D",
    },
    {
        "name": "Transport Transmission Composite",
        "file": "transport_transmission_composite_serving.json",
        "status": "complete",
        "phase": "8E",
    },
    {
        "name": "Econ Composite",
        "file": "econ_composite_serving.json",
        "status": "complete",
        "phase": "8F",
    },
    {
        "name": "C•FLOW State Engine",
        "file": "cflow_state_engine_serving.json",
        "status": "complete",
        "phase": "9A",
    },
    {
        "name": "C•FLOW Regime Engine",
        "file": "cflow_regime_engine_serving.json",
        "status": "complete",
        "phase": "9B",
    },
    {
        "name": "C•FLOW Chamber v2",
        "file": "../oraclechambers/cflow_chamber_serving.json",
        "status": "complete",
        "phase": "OC",
    },
]


def component_exists(component):
    rel = component["file"]

    if rel.startswith("../oraclechambers/"):
        path = (
            ROOT
            / "data/serving/oraclechambers"
            / rel.replace("../oraclechambers/", "")
        )
    else:
        path = ROOT / "data/serving/cflow" / rel

    return path.exists(), path


def main():
    components = []

    for component in COMPONENTS:
        exists, path = component_exists(component)

        status = (
            "complete"
            if exists and component["status"] == "complete"
            else "missing"
        )

        components.append(
            {
                "name": component["name"],
                "phase": component["phase"],
                "file": str(path),
                "status": status,
            }
        )

    complete = sum(
        1 for c in components
        if c["status"] == "complete"
    )

    total = len(components)

    completion_pct = round(
        (complete / total) * 100,
        2
    )

    payload = {
        "metric": "C•FLOW Completion Ledger",
        "category": "C•FLOW",
        "sub_category": "Completion Audit",
        "source": "the_Spine",
        "frequency": "On Build",

        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "ledger_version": "2.0",
        },

        "latest": {
            "completion_pct": completion_pct,
            "complete_components": complete,
            "total_components": total,
            "status": (
                "C•FLOW Operational"
                if completion_pct == 100
                else "C•FLOW Partial"
            ),
        },

        "components": components,

        "governance": {
            "observe": True,
            "measure": True,
            "diagnose": True,
            "attribute": True,
            "forecast": False,
            "predict": False,
            "recommend_trades": False,
        },
    }

    OUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUT_FILE,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {OUT_FILE}")


if __name__ == "__main__":
    main()
    