from pathlib import Path
import json
import re
import pandas as pd


def normalize_vectors(vectors):
    if vectors is None:
        return []

    try:
        if pd.isna(vectors):
            return []
    except Exception:
        pass

    text = str(vectors)

    cleaned = (
        text.replace("[", " ")
        .replace("]", " ")
        .replace("'", " ")
        .replace('"', " ")
        .replace(",", " ")
    )

    return [
        x.strip().upper()
        for x in re.split(r"\s+", cleaned)
        if x.strip()
    ]


def build_cot_iv_vector_map_v1():
    repo_root = Path.cwd()

    input_path = repo_root / "data" / "cot" / "routing" / "cot_geoscen_route_v1.parquet"
    out_dir = repo_root / "data" / "cot" / "routing"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path).copy()

    required_cols = {
        "date",
        "instrument",
        "iv_vectors",
        "geoscen_cot_stress",
        "cot_instability_score",
        "unwind_probability",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    df["iv_vectors_clean"] = df["iv_vectors"].apply(normalize_vectors)

    print("VECTOR SAMPLE:")
    print(df["iv_vectors"].head())

    print("CLEAN VECTOR SAMPLE:")
    print(df["iv_vectors_clean"].head())

    df["iv_x_exposure"] = df["iv_vectors_clean"].apply(lambda x: 1.0 if "X" in x else 0.0)
    df["iv_s_exposure"] = df["iv_vectors_clean"].apply(lambda x: 1.0 if "S" in x else 0.0)

    print("X EXPOSURE COUNTS:")
    print(df["iv_x_exposure"].value_counts())

    print("S EXPOSURE COUNTS:")
    print(df["iv_s_exposure"].value_counts())

    df["iv_x_cot_pressure"] = (
        df["iv_x_exposure"]
        * (
            0.50 * df["geoscen_cot_stress"].fillna(0.0)
            + 0.30 * df["cot_instability_score"].fillna(0.0)
            + 0.20 * df["unwind_probability"].fillna(0.0)
        )
    ).clip(0, 1).round(4)

    df["iv_s_cot_pressure"] = (
        df["iv_s_exposure"]
        * (
            0.45 * df["geoscen_cot_stress"].fillna(0.0)
            + 0.35 * df["unwind_probability"].fillna(0.0)
            + 0.20 * df["cot_instability_score"].fillna(0.0)
        )
    ).clip(0, 1).round(4)

    df["cot_iv_transition_pressure"] = (
        0.55 * df["iv_x_cot_pressure"]
        + 0.45 * df["iv_s_cot_pressure"]
    ).clip(0, 1).round(4)

    df["cot_iv_state"] = "normal"
    df.loc[df["cot_iv_transition_pressure"] >= 0.35, "cot_iv_state"] = "watch"
    df.loc[df["cot_iv_transition_pressure"] >= 0.55, "cot_iv_state"] = "elevated"
    df.loc[df["cot_iv_transition_pressure"] >= 0.75, "cot_iv_state"] = "systemic_positioning_transition"

    output_cols = [
        "date",
        "instrument",
        "iv_vectors_clean",
        "geoscen_cot_stress",
        "cot_instability_score",
        "unwind_probability",
        "iv_x_exposure",
        "iv_s_exposure",
        "iv_x_cot_pressure",
        "iv_s_cot_pressure",
        "cot_iv_transition_pressure",
        "cot_iv_state",
    ]

    df_out = df[output_cols].copy()

    parquet_path = out_dir / "cot_iv_vector_map_v1.parquet"
    json_path = out_dir / "cot_iv_vector_map_v1.json"

    df_out.to_parquet(parquet_path, index=False)
    df_out.to_json(json_path, orient="records", indent=2, date_format="iso")

    aggregate = {
        "component": "cot_iv_vector_map_v1",
        "date": str(df_out["date"].max()),
        "iv_x_cot_pressure": round(float(df_out["iv_x_cot_pressure"].mean()), 4),
        "iv_s_cot_pressure": round(float(df_out["iv_s_cot_pressure"].mean()), 4),
        "cot_iv_transition_pressure": round(float(df_out["cot_iv_transition_pressure"].mean()), 4),
        "max_transition_pressure": round(float(df_out["cot_iv_transition_pressure"].max()), 4),
        "transition_instruments": df_out[
            df_out["cot_iv_state"].isin(["elevated", "systemic_positioning_transition"])
        ]["instrument"].tolist(),
        "iv_targets": ["X", "S"],
        "status": "mapped_to_iv_vectors",
    }

    aggregate_json = out_dir / "cot_iv_vector_summary_v1.json"

    with open(aggregate_json, "w", encoding="utf-8") as f:
        json.dump(aggregate, f, indent=2)

    print("COT IV[t] vector mapping complete")
    print("Rows:", len(df_out))
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY JSON:", aggregate_json)
    print("Aggregate:", aggregate)

    return df_out


if __name__ == "__main__":
    build_cot_iv_vector_map_v1()
