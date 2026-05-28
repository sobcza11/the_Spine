from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


FX_INPUTS = [
    {
        "name": "usd_strength_pressure",
        "score": 0.42,
        "weight": 0.22,
        "iv_vectors": ["X", "L"],
    },
    {
        "name": "eur_fragility_pressure",
        "score": 0.37,
        "weight": 0.18,
        "iv_vectors": ["X", "S"],
    },
    {
        "name": "jpy_carry_unwind_pressure",
        "score": 0.40,
        "weight": 0.20,
        "iv_vectors": ["L", "S"],
    },
    {
        "name": "gbp_transition_pressure",
        "score": 0.46,
        "weight": 0.18,
        "iv_vectors": ["X", "S"],
    },
    {
        "name": "commodity_fx_pressure",
        "score": 0.34,
        "weight": 0.12,
        "iv_vectors": ["C", "X"],
    },
    {
        "name": "safe_haven_rotation_pressure",
        "score": 0.39,
        "weight": 0.10,
        "iv_vectors": ["L", "S"],
    },
]


def classify_state(score):
    if score >= 0.75:
        return "systemic_fx_stress"

    if score >= 0.60:
        return "fragile_fx_stress"

    if score >= 0.40:
        return "elevated_fx_stress"

    if score >= 0.25:
        return "watch_fx_stress"

    return "stable_fx"


def build_fx_recursive_stress_engine_v1():
    repo_root = Path.cwd()

    out_dir = repo_root / "data" / "fx" / "recursive"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []

    for item in FX_INPUTS:
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

    fx_recursive_pressure = round(
        float(df["recursive_pressure"].sum()),
        4,
    )

    max_component_score = round(
        float(df["raw_score"].max()),
        4,
    )

    fx_recursive_state = classify_state(fx_recursive_pressure)

    summary = {
        "component": "fx_recursive_stress_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "fx_recursive_pressure": fx_recursive_pressure,
        "max_component_score": max_component_score,
        "fx_recursive_state": fx_recursive_state,
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
        "status": "fx_recursive_stress_complete",
    }

    parquet_path = out_dir / "fx_recursive_stress_engine_v1.parquet"
    json_path = out_dir / "fx_recursive_stress_engine_v1.json"
    summary_path = out_dir / "fx_recursive_stress_summary_v1.json"

    df.to_parquet(parquet_path, index=False)

    df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("FX recursive stress engine complete")
    print("Rows:", len(df))
    print("FX Recursive Pressure:", fx_recursive_pressure)
    print("FX Recursive State:", fx_recursive_state)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df


if __name__ == "__main__":
    build_fx_recursive_stress_engine_v1()
