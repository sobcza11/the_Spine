from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


RATES_INPUTS = [
    {
        "name": "curve_inversion_pressure",
        "score": 0.44,
        "weight": 0.25,
        "iv_vectors": ["L", "S"],
    },
    {
        "name": "policy_rate_pressure",
        "score": 0.41,
        "weight": 0.20,
        "iv_vectors": ["C", "L"],
    },
    {
        "name": "long_end_stress",
        "score": 0.36,
        "weight": 0.20,
        "iv_vectors": ["L", "F"],
    },
    {
        "name": "china_policy_liquidity_pressure",
        "score": 0.32,
        "weight": 0.15,
        "iv_vectors": ["C", "S"],
    },
    {
        "name": "sovereign_reflexivity_pressure",
        "score": 0.39,
        "weight": 0.20,
        "iv_vectors": ["L", "X"],
    },
]


def classify_state(score):
    if score >= 0.75:
        return "systemic_rates_stress"

    if score >= 0.60:
        return "fragile_rates_stress"

    if score >= 0.40:
        return "elevated_rates_stress"

    if score >= 0.25:
        return "watch_rates_stress"

    return "stable_rates"


def build_rates_recursive_pressure_engine_v1():
    repo_root = Path.cwd()

    out_dir = repo_root / "data" / "rates" / "recursive"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []

    for item in RATES_INPUTS:
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

    rates_recursive_pressure = round(
        float(df["recursive_pressure"].sum()),
        4,
    )

    max_component_score = round(
        float(df["raw_score"].max()),
        4,
    )

    rates_recursive_state = classify_state(rates_recursive_pressure)

    summary = {
        "component": "rates_recursive_pressure_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "rates_recursive_pressure": rates_recursive_pressure,
        "max_component_score": max_component_score,
        "rates_recursive_state": rates_recursive_state,
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
        "status": "rates_recursive_pressure_complete",
    }

    parquet_path = out_dir / "rates_recursive_pressure_engine_v1.parquet"
    json_path = out_dir / "rates_recursive_pressure_engine_v1.json"
    summary_path = out_dir / "rates_recursive_pressure_summary_v1.json"

    df.to_parquet(parquet_path, index=False)

    df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Rates recursive pressure engine complete")
    print("Rows:", len(df))
    print("Rates Recursive Pressure:", rates_recursive_pressure)
    print("Rates Recursive State:", rates_recursive_state)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df


if __name__ == "__main__":
    build_rates_recursive_pressure_engine_v1()
