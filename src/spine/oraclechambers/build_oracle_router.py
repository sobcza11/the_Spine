import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "cflow": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "cflow_chamber_serving.json"
    ),
    "geoscen": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "geoscen_chamber_serving.json"
    ),
    "rates": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "rates_chamber_serving.json"
    ),
    "equities": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "equities_chamber_serving.json"
    ),
    "fx": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "fx_chamber_serving.json"
    ),
    "crossasset": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "crossasset_chamber_serving.json"
    ),
}

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "oracle_router_serving.json"
)


def load_json(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_latest_score(payload):
    latest = payload.get("latest", {})
    return (
        latest.get("score")
        or latest.get("cflow_score")
        or latest.get("diagnostic_score")
        or latest.get("value")
    )


def get_latest_state(payload):
    latest = payload.get("latest", {})
    return latest.get("state") or latest.get("regime") or "Unknown"


def main():

    chambers = {
        name: load_json(path)
        for name, path in INPUTS.items()
    }

    chamber_rows = []

    for name, payload in chambers.items():
        score = get_latest_score(payload)
        state = get_latest_state(payload)

        chamber_rows.append(
            {
                "id": name,
                "metric": payload.get("metric"),
                "category": payload.get("category"),
                "sub_category": payload.get("sub_category"),
                "score": float(score) if score is not None else None,
                "state": state,
                "observation": payload.get("observation"),
                "diagnosis": payload.get("diagnosis"),
                "attribution": payload.get("attribution", {}),
            }
        )

    scored = [
        row for row in chamber_rows
        if row["score"] is not None
    ]

    ranked = sorted(
        scored,
        key=lambda x: x["score"],
        reverse=True
    )

    router_score = round(
        sum(row["score"] for row in scored) / len(scored),
        2
    )

    if router_score >= 75:
        router_state = "Stress"
    elif router_score >= 50:
        router_state = "Watch"
    elif router_score >= 25:
        router_state = "Soft"
    else:
        router_state = "Stable"

    payload = {
        "metric": "Oracle Router",
        "category": "Oracle Chambers",
        "sub_category": "Router",
        "source": "the_Spine",
        "frequency": "Mixed",

        "meta": {
            "generated_at":
                datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "router_version": "1.0",
            "inputs": {
                name: str(path)
                for name, path in INPUTS.items()
            }
        },

        "latest": {
            "score": router_score,
            "state": router_state,
            "active_chambers": len(scored),
            "total_chambers": len(INPUTS),
        },

        "observation":
            f"Oracle Router currently classifies "
            f"the chamber family as {router_state} "
            f"with a diagnostic score of "
            f"{router_score}.",

        "measurement": [
            {
                "chamber": row["id"],
                "metric": row["metric"],
                "score": row["score"],
                "state": row["state"]
            }
            for row in ranked
        ],

        "diagnosis":
            f"{ranked[0]['id']} is the highest "
            f"active chamber contributor.",

        "attribution": {
            "drivers": [
                {
                    "chamber": row["id"],
                    "score": row["score"],
                    "state": row["state"]
                }
                for row in ranked[:2]
            ],
            "offsets": [
                {
                    "chamber": row["id"],
                    "score": row["score"],
                    "state": row["state"]
                }
                for row in ranked[-2:]
            ],
            "chambers": chamber_rows,
        },

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

    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUTPUT,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()