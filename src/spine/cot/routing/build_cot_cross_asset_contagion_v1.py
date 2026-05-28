from pathlib import Path
import json
import pandas as pd


CONTAGION_RELATIONSHIPS = {
    "ES": ["NQ", "RTY", "SOFR", "EUR", "JPY"],
    "NQ": ["ES", "RTY", "SOFR"],
    "RTY": ["ES", "NQ", "SOFR"],
    "EUR": ["JPY", "ES", "NQ"],
    "JPY": ["EUR", "ES", "NQ", "SOFR"],
    "SOFR": ["ES", "NQ", "RTY", "JPY"],
}


def build_cot_cross_asset_contagion_v1():
    repo_root = Path.cwd()

    input_path = repo_root / "data" / "cot" / "routing" / "cot_iv_vector_map_v1.parquet"

    out_dir = repo_root / "data" / "cot" / "routing"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path).copy()

    required_cols = {
        "date",
        "instrument",
        "geoscen_cot_stress",
        "cot_instability_score",
        "unwind_probability",
        "cot_iv_transition_pressure",
        "cot_iv_state",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()

    stress_lookup = latest.set_index("instrument")[
        [
            "geoscen_cot_stress",
            "cot_instability_score",
            "unwind_probability",
            "cot_iv_transition_pressure",
        ]
    ].to_dict(orient="index")

    rows = []

    for source, targets in CONTAGION_RELATIONSHIPS.items():
        if source not in stress_lookup:
            continue

        source_stress = stress_lookup[source]

        for target in targets:
            if target not in stress_lookup:
                continue

            target_stress = stress_lookup[target]

            contagion_pressure = (
                0.40 * source_stress["cot_iv_transition_pressure"]
                + 0.25 * source_stress["cot_instability_score"]
                + 0.20 * target_stress["cot_iv_transition_pressure"]
                + 0.15 * target_stress["unwind_probability"]
            )

            rows.append(
                {
                    "date": latest_date,
                    "source_instrument": source,
                    "target_instrument": target,
                    "source_iv_pressure": source_stress["cot_iv_transition_pressure"],
                    "target_iv_pressure": target_stress["cot_iv_transition_pressure"],
                    "source_instability": source_stress["cot_instability_score"],
                    "target_unwind_probability": target_stress["unwind_probability"],
                    "contagion_pressure": round(float(min(max(contagion_pressure, 0), 1)), 4),
                }
            )

    df_edges = pd.DataFrame(rows)

    if df_edges.empty:
        raise ValueError("No contagion relationships generated")

    df_edges["contagion_state"] = "normal"

    df_edges.loc[
        df_edges["contagion_pressure"] >= 0.30,
        "contagion_state",
    ] = "watch"

    df_edges.loc[
        df_edges["contagion_pressure"] >= 0.50,
        "contagion_state",
    ] = "elevated"

    df_edges.loc[
        df_edges["contagion_pressure"] >= 0.70,
        "contagion_state",
    ] = "systemic_contagion"

    parquet_path = out_dir / "cot_cross_asset_contagion_v1.parquet"
    json_path = out_dir / "cot_cross_asset_contagion_v1.json"

    df_edges.to_parquet(parquet_path, index=False)

    df_edges.to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    summary = {
        "component": "cot_cross_asset_contagion_v1",
        "date": str(latest_date),
        "relationship_count": int(len(df_edges)),
        "avg_contagion_pressure": round(float(df_edges["contagion_pressure"].mean()), 4),
        "max_contagion_pressure": round(float(df_edges["contagion_pressure"].max()), 4),
        "elevated_edges": df_edges[
            df_edges["contagion_state"].isin(["elevated", "systemic_contagion"])
        ][["source_instrument", "target_instrument"]].to_dict(orient="records"),
        "status": "cross_asset_cot_contagion_mapped",
    }

    summary_path = out_dir / "cot_cross_asset_contagion_summary_v1.json"

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("COT cross-asset contagion mapping complete")
    print("Rows:", len(df_edges))
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY JSON:", summary_path)
    print("Summary:", summary)

    return df_edges


if __name__ == "__main__":
    build_cot_cross_asset_contagion_v1()
