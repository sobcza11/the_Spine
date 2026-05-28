from pathlib import Path
import json
import pandas as pd


def build_geoscen_signal_conditioning_registry_v1():
    repo_root = Path.cwd()

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

    if not cot_route_path.exists():
        raise FileNotFoundError(f"Missing COT routing file: {cot_route_path}")

    cot = pd.read_parquet(cot_route_path).copy()

    required_cols = {
        "date",
        "instrument",
        "raw_geoscen_cot_stress",
        "signal_quality_weight",
        "geoscen_cot_stress",
        "signal_quality_flag",
        "winsorized",
        "scaling_method",
        "rolling_window",
    }

    missing = required_cols - set(cot.columns)
    if missing:
        raise KeyError(f"Missing required columns from COT routing: {missing}")

    cot["source_component"] = "COT"
    cot["source_signal"] = "positioning_instability"
    cot["conditioning_status"] = "conditioned"

    cot["registry_signal_quality_score"] = (
        pd.to_numeric(cot["signal_quality_weight"], errors="coerce")
        .fillna(0.50)
        .clip(0, 1)
    )

    cot["registry_weighted_stress"] = (
        pd.to_numeric(cot["raw_geoscen_cot_stress"], errors="coerce").fillna(0.0)
        * cot["registry_signal_quality_score"]
    ).clip(0, 1).round(4)

    cot["registry_confidence_state"] = "low"
    cot.loc[cot["registry_signal_quality_score"] >= 0.60, "registry_confidence_state"] = "medium"
    cot.loc[cot["registry_signal_quality_score"] >= 0.85, "registry_confidence_state"] = "high"

    registry_cols = [
        "date",
        "source_component",
        "source_signal",
        "instrument",
        "conditioning_status",
        "signal_quality_flag",
        "registry_signal_quality_score",
        "raw_geoscen_cot_stress",
        "registry_weighted_stress",
        "geoscen_cot_stress",
        "registry_confidence_state",
        "winsorized",
        "scaling_method",
        "rolling_window",
    ]

    registry = cot[registry_cols].copy()

    parquet_path = out_dir / "geoscen_signal_conditioning_registry_v1.parquet"
    json_path = out_dir / "geoscen_signal_conditioning_registry_v1.json"
    summary_path = out_dir / "geoscen_signal_conditioning_registry_summary_v1.json"

    registry.to_parquet(parquet_path, index=False)

    registry.to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    summary = {
        "component": "geoscen_signal_conditioning_registry_v1",
        "source_components": sorted(registry["source_component"].unique().tolist()),
        "source_signals": sorted(registry["source_signal"].unique().tolist()),
        "instrument_count": int(registry["instrument"].nunique()),
        "avg_signal_quality_score": round(float(registry["registry_signal_quality_score"].mean()), 4),
        "avg_raw_stress": round(float(registry["raw_geoscen_cot_stress"].mean()), 4),
        "avg_weighted_stress": round(float(registry["registry_weighted_stress"].mean()), 4),
        "quality_counts": registry["signal_quality_flag"].value_counts().to_dict(),
        "confidence_counts": registry["registry_confidence_state"].value_counts().to_dict(),
        "conditioning_methods": sorted(registry["scaling_method"].dropna().unique().tolist()),
        "status": "geoscen_conditioning_registry_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("GeoScen signal-conditioning registry complete")
    print("Rows:", len(registry))
    print("Instruments:", sorted(registry["instrument"].dropna().unique().tolist()))
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return registry


if __name__ == "__main__":
    build_geoscen_signal_conditioning_registry_v1()
