from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


FINSTATE_INPUTS = [
    {
        "name": "macro_fragility",
        "score": 0.42,
        "weight": 0.25,
        "iv_vectors": ["F", "S"],
    },
    {
        "name": "liquidity_stress",
        "score": 0.38,
        "weight": 0.25,
        "iv_vectors": ["L", "C"],
    },
    {
        "name": "margin_pressure",
        "score": 0.34,
        "weight": 0.15,
        "iv_vectors": ["M", "P"],
    },
    {
        "name": "credit_fragility",
        "score": 0.46,
        "weight": 0.20,
        "iv_vectors": ["C", "S"],
    },
    {
        "name": "earnings_drift",
        "score": 0.31,
        "weight": 0.15,
        "iv_vectors": ["P", "D"],
    },
]


def classify_state(score):
    if score >= 0.75:
        return "systemic_finstate_stress"

    if score >= 0.60:
        return "fragile_finstate_stress"

    if score >= 0.40:
        return "elevated_finstate_stress"

    if score >= 0.25:
        return "watch_finstate_stress"

    return "stable_finstate"


def build_finstate_recursive_integration_v1():
    repo_root = Path.cwd()

    out_dir = repo_root / "data" / "finstate" / "recursive"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []

    for item in FINSTATE_INPUTS:
        recursive_pressure = round(
            min(1.0, item["score"] * item["weight"]),
            4,
        )

        rows.append(
            {
                "component": item["name"],
                "raw_score": item["score"],
                "weight": item["weight"],
                "recursive_pressure": recursive_pressure,
                "iv_vectors": item["iv_vectors"],
                "state": classify_state(item["score"]),
            }
        )

    df = pd.DataFrame(rows)

    finstate_recursive_pressure = round(
        float(df["recursive_pressure"].sum()),
        4,
    )

    max_component_score = round(
        float(df["raw_score"].max()),
        4,
    )

    finstate_recursive_state = classify_state(finstate_recursive_pressure)

    summary = {
        "component": "finstate_recursive_integration_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "finstate_recursive_pressure": finstate_recursive_pressure,
        "max_component_score": max_component_score,
        "finstate_recursive_state": finstate_recursive_state,
        "iv_vector_targets": sorted(
            list(
                set(
                    v
                    for vectors in df["iv_vectors"]
                    for v in vectors
                )
            )
        ),
        "active_components": df["component"].tolist(),
        "status": "finstate_recursive_integration_complete",
    }

    parquet_path = out_dir / "finstate_recursive_integration_v1.parquet"
    json_path = out_dir / "finstate_recursive_integration_v1.json"
    summary_path = out_dir / "finstate_recursive_integration_summary_v1.json"

    df.to_parquet(parquet_path, index=False)

    df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("FinState recursive integration complete")
    print("Rows:", len(df))
    print("FinState Recursive Pressure:", finstate_recursive_pressure)
    print("FinState Recursive State:", finstate_recursive_state)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df


if __name__ == "__main__":
    build_finstate_recursive_integration_v1()
