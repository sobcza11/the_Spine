from pathlib import Path
import json
import pandas as pd


def classify_conditioned_state(score):
    if score >= 0.75:
        return "systemic_positioning_risk"

    if score >= 0.60:
        return "elevated"

    if score >= 0.40:
        return "watch"

    return "normal"


def build_geoscen_conditioned_cot_routing_v1():
    repo_root = Path.cwd()

    registry_path = (
        repo_root
        / "data"
        / "geoscen"
        / "conditioning"
        / "geoscen_signal_conditioning_registry_v1.parquet"
    )

    cot_route_path = (
        repo_root
        / "data"
        / "cot"
        / "routing"
        / "cot_geoscen_route_v1.parquet"
    )

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "conditioning"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    registry = pd.read_parquet(registry_path).copy()
    cot_route = pd.read_parquet(cot_route_path).copy()

    required_registry_cols = {
        "instrument",
        "registry_signal_quality_score",
        "registry_weighted_stress",
        "registry_confidence_state",
    }

    missing_registry = required_registry_cols - set(registry.columns)
    if missing_registry:
        raise KeyError(f"Missing registry columns: {missing_registry}")

    required_route_cols = {
        "date",
        "instrument",
        "iv_vectors",
        "raw_geoscen_cot_stress",
        "geoscen_cot_stress",
        "cot_instability_score",
        "unwind_probability",
    }

    missing_route = required_route_cols - set(cot_route.columns)
    if missing_route:
        raise KeyError(f"Missing COT route columns: {missing_route}")

    merged = cot_route.merge(
        registry[
            [
                "instrument",
                "registry_signal_quality_score",
                "registry_weighted_stress",
                "registry_confidence_state",
            ]
        ],
        on="instrument",
        how="left",
    )

    merged["registry_signal_quality_score"] = pd.to_numeric(
        merged["registry_signal_quality_score"],
        errors="coerce",
    ).fillna(0.50)

    merged["conditioned_geoscen_cot_stress"] = (
        pd.to_numeric(merged["raw_geoscen_cot_stress"], errors="coerce").fillna(0.0)
        * merged["registry_signal_quality_score"]
    ).clip(0, 1).round(4)

    merged["conditioned_route_state"] = (
        merged["conditioned_geoscen_cot_stress"]
        .apply(classify_conditioned_state)
    )

    output_cols = [
        "date",
        "instrument",
        "iv_vectors",
        "raw_geoscen_cot_stress",
        "registry_signal_quality_score",
        "registry_confidence_state",
        "conditioned_geoscen_cot_stress",
        "conditioned_route_state",
        "cot_instability_score",
        "unwind_probability",
    ]

    df_out = merged[output_cols].copy()

    parquet_path = out_dir / "geoscen_conditioned_cot_routing_v1.parquet"
    json_path = out_dir / "geoscen_conditioned_cot_routing_v1.json"
    summary_path = out_dir / "geoscen_conditioned_cot_routing_summary_v1.json"

    df_out.to_parquet(parquet_path, index=False)

    df_out.to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    summary = {
        "component": "geoscen_conditioned_cot_routing_v1",
        "instrument_count": int(df_out["instrument"].nunique()),
        "avg_raw_cot_stress": round(float(df_out["raw_geoscen_cot_stress"].mean()), 4),
        "avg_conditioned_cot_stress": round(float(df_out["conditioned_geoscen_cot_stress"].mean()), 4),
        "max_conditioned_cot_stress": round(float(df_out["conditioned_geoscen_cot_stress"].max()), 4),
        "risk_instruments": df_out[
            df_out["conditioned_route_state"].isin(
                ["elevated", "systemic_positioning_risk"]
            )
        ]["instrument"].tolist(),
        "confidence_counts": df_out["registry_confidence_state"].value_counts().to_dict(),
        "route_state_counts": df_out["conditioned_route_state"].value_counts().to_dict(),
        "status": "geoscen_cot_routing_reweighted_by_signal_quality",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("GeoScen conditioned COT routing complete")
    print("Rows:", len(df_out))
    print("Instruments:", sorted(df_out["instrument"].dropna().unique().tolist()))
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df_out


if __name__ == "__main__":
    build_geoscen_conditioned_cot_routing_v1()
