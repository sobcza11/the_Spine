from pathlib import Path
import json
import pandas as pd


IV_VECTOR_MAP = {
    "ES": ["X", "S"],
    "NQ": ["X", "S"],
    "RTY": ["X", "S"],

    "USD": ["X", "L", "S"],
    "EUR": ["X", "L"],
    "JPY": ["X", "L"],
    "GBP": ["X", "S"],
    "CAD": ["C", "X"],
    "AUD": ["C", "S"],
    "CHF": ["L", "S"],

    "SOFR": ["L", "C", "S"],
    "US10Y": ["L", "C"],

    "CL": ["C", "X", "S"],
    "NG": ["X", "S"],
    "GC": ["L", "S"],
    "SI": ["X", "S"],
    "HG": ["C", "X"],

    "BTC": ["X", "S"],
    "ETH": ["X", "C"],
}


def build_cot_geoscen_route_v1():
    repo_root = Path.cwd()

    input_path = repo_root / "data" / "cot" / "signals" / "cot_unwind_probability_latest_v1.parquet"
    out_dir = repo_root / "data" / "cot" / "routing"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path).copy()

    conditioning_path = (
        repo_root
        / "data"
        / "cot"
        / "conditioning"
        / "cot_conditioned_panel_v1.parquet"
    )

    if conditioning_path.exists():
        conditioning = pd.read_parquet(conditioning_path).copy()

        conditioning_latest = (
            conditioning
            .sort_values(["instrument", "date"])
            .groupby("instrument")
            .tail(1)
            [
                [
                    "instrument",
                    "sample_size",
                    "signal_quality_flag",
                    "signal_quality_weight",
                    "winsorized",
                    "winsor_lower",
                    "winsor_upper",
                    "scaling_method",
                    "rolling_window",
                ]
            ]
        )

        df = df.merge(
            conditioning_latest,
            on="instrument",
            how="left",
        )

    else:
        df["sample_size"] = None
        df["signal_quality_flag"] = "unknown"
        df["signal_quality_weight"] = 0.50
        df["winsorized"] = False
        df["winsor_lower"] = None
        df["winsor_upper"] = None
        df["scaling_method"] = "unknown"
        df["rolling_window"] = None

    required_cols = {
        "date",
        "instrument",
        "crowding_stress_score",
        "acceleration_stress_score",
        "unwind_probability",
        "cot_instability_score",
        "unwind_risk_state",
        "signal_quality_weight",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    df["iv_vectors"] = df["instrument"].map(IV_VECTOR_MAP)
    df["iv_vectors"] = df["iv_vectors"].apply(
        lambda x: x if isinstance(x, list) else ["X", "S"]
    )

    df["signal_quality_weight"] = pd.to_numeric(
        df["signal_quality_weight"],
        errors="coerce",
    ).fillna(0.50)

    df["raw_geoscen_cot_stress"] = (
        0.35 * df["crowding_stress_score"].fillna(0.0)
        + 0.30 * df["acceleration_stress_score"].fillna(0.0)
        + 0.35 * df["unwind_probability"].fillna(0.0)
    ).clip(0, 1).round(4)

    df["geoscen_cot_stress"] = (
        df["raw_geoscen_cot_stress"]
        * df["signal_quality_weight"]
    ).clip(0, 1).round(4)

    df["geoscen_route_state"] = "normal"
    df.loc[df["geoscen_cot_stress"] >= 0.40, "geoscen_route_state"] = "watch"
    df.loc[df["geoscen_cot_stress"] >= 0.60, "geoscen_route_state"] = "elevated"
    df.loc[df["geoscen_cot_stress"] >= 0.75, "geoscen_route_state"] = "systemic_positioning_risk"

    routed_cols = [
        "date",
        "instrument",
        "iv_vectors",
        "crowding_stress_score",
        "acceleration_stress_score",
        "unwind_probability",
        "cot_instability_score",
        "unwind_risk_state",
        "raw_geoscen_cot_stress",
        "signal_quality_weight",
        "geoscen_cot_stress",
        "geoscen_route_state",
        "sample_size",
        "signal_quality_flag",
        "winsorized",
        "winsor_lower",
        "winsor_upper",
        "scaling_method",
        "rolling_window",
    ]

    df_route = df[routed_cols].copy()

    route_parquet = out_dir / "cot_geoscen_route_v1.parquet"
    route_json = out_dir / "cot_geoscen_route_v1.json"

    df_route.to_parquet(route_parquet, index=False)

    df_route.to_json(
        route_json,
        orient="records",
        indent=2,
        date_format="iso",
    )

    aggregate = {
        "component": "cot_geoscen_route_v1",
        "date": str(df_route["date"].max()),
        "instrument_count": int(df_route["instrument"].nunique()),
        "raw_cot_stress": round(float(df_route["raw_geoscen_cot_stress"].mean()), 4),
        "quality_weighted_cot_stress": round(float(df_route["geoscen_cot_stress"].mean()), 4),
        "max_cot_stress": round(float(df_route["geoscen_cot_stress"].max()), 4),
        "high_risk_instruments": df_route[
            df_route["geoscen_route_state"].isin(
                ["elevated", "systemic_positioning_risk"]
            )
        ]["instrument"].tolist(),
        "quality_counts": df_route["signal_quality_flag"].value_counts().to_dict(),
        "conditioning": {
            "winsorized": bool(df_route["winsorized"].fillna(False).any()),
            "winsor_lower": df_route["winsor_lower"].dropna().iloc[0]
            if df_route["winsor_lower"].notna().any()
            else None,
            "winsor_upper": df_route["winsor_upper"].dropna().iloc[0]
            if df_route["winsor_upper"].notna().any()
            else None,
            "scaling_method": df_route["scaling_method"].dropna().iloc[0]
            if df_route["scaling_method"].notna().any()
            else None,
            "rolling_window": int(df_route["rolling_window"].dropna().iloc[0])
            if df_route["rolling_window"].notna().any()
            else None,
        },
        "iv_vector_targets": sorted(
            set(v for vectors in df_route["iv_vectors"] for v in vectors)
        ),
        "status": "routed_to_geoscen_with_conditioning_metadata",
    }

    aggregate_json = out_dir / "cot_geoscen_route_summary_v1.json"

    with open(aggregate_json, "w", encoding="utf-8") as f:
        json.dump(aggregate, f, indent=2)

    print("COT GeoScen routing complete")
    print("Rows:", len(df_route))
    print("Instruments:", sorted(df_route["instrument"].tolist()))
    print("Quality Counts:", aggregate["quality_counts"])
    print("Route PARQUET:", route_parquet)
    print("Route JSON:", route_json)
    print("Summary JSON:", aggregate_json)
    print("Aggregate:", aggregate)

    return df_route


if __name__ == "__main__":
    build_cot_geoscen_route_v1()
