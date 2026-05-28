from pathlib import Path
import json
import pandas as pd


def classify_regime(row):
    stress = row.get("cot_instability_score", 0.0)
    unwind = row.get("unwind_probability", 0.0)
    crowding = row.get("crowding_stress_score", 0.0)
    acceleration = row.get("acceleration_stress_score", 0.0)

    if stress >= 0.70 or unwind >= 0.75:
        return "positioning_stress_regime"

    if crowding >= 0.60 and acceleration >= 0.50:
        return "crowded_acceleration_regime"

    if crowding >= 0.60:
        return "crowded_positioning_regime"

    if acceleration >= 0.60:
        return "fast_repositioning_regime"

    return "normal_positioning_regime"


def build_cot_regime_conditioned_behavior_v1():
    repo_root = Path.cwd()

    input_path = repo_root / "data" / "cot" / "signals" / "cot_unwind_probability_v1.parquet"
    contagion_path = repo_root / "data" / "cot" / "routing" / "cot_cross_asset_contagion_summary_v1.json"

    out_dir = repo_root / "data" / "cot" / "routing"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path).copy()

    required_cols = {
        "date",
        "instrument",
        "crowding_stress_score",
        "acceleration_stress_score",
        "unwind_probability",
        "cot_instability_score",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    df = df.sort_values(["instrument", "date"]).reset_index(drop=True)

    df["cot_behavior_regime"] = df.apply(classify_regime, axis=1)

    regime_panel = (
        df.groupby(["date", "cot_behavior_regime"])
        .agg(
            instrument_count=("instrument", "nunique"),
            avg_crowding_stress=("crowding_stress_score", "mean"),
            avg_acceleration_stress=("acceleration_stress_score", "mean"),
            avg_unwind_probability=("unwind_probability", "mean"),
            avg_cot_instability=("cot_instability_score", "mean"),
            max_cot_instability=("cot_instability_score", "max"),
        )
        .reset_index()
    )

    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()

    latest_summary = (
        latest.groupby("cot_behavior_regime")
        .agg(
            instrument_count=("instrument", "nunique"),
            avg_cot_instability=("cot_instability_score", "mean"),
            max_cot_instability=("cot_instability_score", "max"),
            avg_unwind_probability=("unwind_probability", "mean"),
        )
        .reset_index()
        .sort_values("avg_cot_instability", ascending=False)
    )

    dominant_regime = (
        latest_summary.iloc[0]["cot_behavior_regime"]
        if not latest_summary.empty
        else "unknown"
    )

    contagion_summary = {}

    if contagion_path.exists():
        with open(contagion_path, "r", encoding="utf-8") as f:
            contagion_summary = json.load(f)

    system_summary = {
        "component": "cot_regime_conditioned_behavior_v1",
        "date": str(latest_date),
        "dominant_cot_regime": dominant_regime,
        "latest_regime_count": int(latest["cot_behavior_regime"].nunique()),
        "avg_latest_instability": round(float(latest["cot_instability_score"].mean()), 4),
        "max_latest_instability": round(float(latest["cot_instability_score"].max()), 4),
        "avg_latest_unwind_probability": round(float(latest["unwind_probability"].mean()), 4),
        "contagion_pressure": contagion_summary.get("avg_contagion_pressure"),
        "max_contagion_pressure": contagion_summary.get("max_contagion_pressure"),
        "status": "regime_conditioned_cot_behavior_mapped",
    }

    panel_path = out_dir / "cot_regime_conditioned_behavior_panel_v1.parquet"
    panel_json = out_dir / "cot_regime_conditioned_behavior_panel_v1.json"

    latest_path = out_dir / "cot_regime_conditioned_behavior_latest_v1.parquet"
    latest_json = out_dir / "cot_regime_conditioned_behavior_latest_v1.json"

    summary_path = out_dir / "cot_regime_conditioned_behavior_summary_v1.json"

    regime_panel.to_parquet(panel_path, index=False)
    regime_panel.to_json(
        panel_json,
        orient="records",
        indent=2,
        date_format="iso",
    )

    latest_summary.to_parquet(latest_path, index=False)
    latest_summary.to_json(
        latest_json,
        orient="records",
        indent=2,
        date_format="iso",
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(system_summary, f, indent=2)

    print("COT regime-conditioned behavior analysis complete")
    print("Panel rows:", len(regime_panel))
    print("Latest regimes:", latest_summary["cot_behavior_regime"].tolist())
    print("PANEL PARQUET:", panel_path)
    print("PANEL JSON:", panel_json)
    print("LATEST PARQUET:", latest_path)
    print("LATEST JSON:", latest_json)
    print("SUMMARY JSON:", summary_path)
    print("Summary:", system_summary)

    return regime_panel


if __name__ == "__main__":
    build_cot_regime_conditioned_behavior_v1()
