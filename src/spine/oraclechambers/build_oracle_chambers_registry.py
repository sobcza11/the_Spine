import json
from pathlib import Path
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[3]

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "oracle_chambers_registry.json"
)


def build_registry() -> dict:
    now = datetime.now(timezone.utc).isoformat()

    chambers = [
        {
            "id": "macro",
            "name": "Macro Chamber",
            "purpose": "Macroeconomic context and state explanation",
            "allowed": [
                "observation",
                "measurement",
                "diagnosis",
                "attribution",
                "source_lineage"
            ],
            "forbidden": [
                "forecasting",
                "price_targets",
                "trade_recommendations"
            ]
        },
        {
            "id": "cflow",
            "name": "C•FLOW Chamber",
            "purpose": "Explain C•FLOW diagnostics and regime state",
            "allowed": [
                "observation",
                "measurement",
                "diagnosis",
                "attribution",
                "state_history_review"
            ],
            "forbidden": [
                "forecasting",
                "probabilities",
                "trade_recommendations"
            ]
        },
        {
            "id": "geoscen",
            "name": "GeoScen Chamber",
            "purpose": "Geography, supply-chain, resource and chokepoint analysis",
            "allowed": [
                "observation",
                "measurement",
                "diagnosis",
                "resource_mapping"
            ],
            "forbidden": [
                "forecasting",
                "resource_price_targets"
            ]
        },
        {
            "id": "credit",
            "name": "Credit Chamber",
            "purpose": "Credit transmission and funding analysis",
            "allowed": [
                "observation",
                "measurement",
                "diagnosis"
            ],
            "forbidden": [
                "security_recommendations",
                "forecasting"
            ]
        },
        {
            "id": "energy",
            "name": "Energy Chamber",
            "purpose": "Energy system diagnostics",
            "allowed": [
                "observation",
                "measurement",
                "diagnosis"
            ],
            "forbidden": [
                "oil_price_forecasts",
                "trade_recommendations"
            ]
        },
        {
            "id": "policy",
            "name": "Policy Chamber",
            "purpose": "Policy interpretation and attribution",
            "allowed": [
                "observation",
                "measurement",
                "diagnosis",
                "policy_context"
            ],
            "forbidden": [
                "forecasting",
                "political_advocacy"
            ]
        },
        {
            "id": "ai_governance",
            "name": "AI Governance Chamber",
            "purpose": "CPMAI governance and model audit support",
            "allowed": [
                "audit",
                "measurement",
                "lineage",
                "explainability"
            ],
            "forbidden": [
                "black_box_reasoning",
                "unauditable_outputs"
            ]
        }
    ]

    return {
        "metric": "Oracle Chambers Registry",
        "category": "Oracle Chambers",
        "sub_category": "Registry",
        "source": "the_Spine",
        "frequency": "On Build",
        "meta": {
            "generated_at": now,
            "version": "1.0",
            "forecasting": "prohibited",
            "cpmai": True,
            "chamber_count": len(chambers)
        },
        "latest": {
            "active_chambers": len(chambers),
            "status": "Registry Active"
        },
        "chambers": chambers,
        "governance": {
            "observe": True,
            "measure": True,
            "diagnose": True,
            "attribute": True,
            "forecast": False,
            "predict": False,
            "recommend_trades": False
        }
    }


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    payload = build_registry()

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    