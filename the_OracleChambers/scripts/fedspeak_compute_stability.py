from pathlib import Path
import pandas as pd

from scripts.write_metadata import write_metadata


def main():
    repo_root = Path(__file__).resolve().parents[1]
    leaf_path = repo_root / "data" / "processed" / "FedSpeak" / "combined_policy_leaf.parquet"

    if not leaf_path.exists():
        raise FileNotFoundError(f"combined_policy_leaf not found at {leaf_path}")

    df = pd.read_parquet(leaf_path)

    if "event_date" not in df.columns:
        raise KeyError("combined_policy_leaf is missing 'event_date' column")

    # Ensure datetime
    df["event_date"] = pd.to_datetime(df["event_date"])

    # Derive quarter labels like '2025Q1'
    df["quarter"] = df["event_date"].dt.to_period("Q")

    grouped = df.groupby("quarter")

    rows = []
    for quarter, g in grouped:
        rows.append(
            {
                "quarter": str(quarter),
                "quarter_start": g["event_date"].min(),
                "quarter_end": g["event_date"].max(),
                "n_events": int(len(g)),
                "mean_policy_bias": float(g["policy_bias"].mean()),
                "std_policy_bias": float(g["policy_bias"].std(ddof=0)),
                "mean_inflation_risk": float(g["inflation_risk"].mean()),
                "std_inflation_risk": float(g["inflation_risk"].std(ddof=0)),
                "mean_growth_risk": float(g["growth_risk"].mean()),
                "std_growth_risk": float(g["growth_risk"].std(ddof=0)),
            }
        )

    stability_df = pd.DataFrame(rows).sort_values("quarter").reset_index(drop=True)

    out_dir = repo_root / "data" / "processed" / "FedSpeak"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "fedspeak_stability.parquet"
    stability_df.to_parquet(out_path, index=False)

    data_date = (
        str(df["event_date"].max()) if not df["event_date"].isna().all() else ""
    )

    write_metadata(
        artifact_path=out_path,
        artifact_name="fedspeak_stability",
        artifact_type="diagnostic",
        channel="fedspeak",
        data_date=data_date,
        df=stability_df,
        pipeline_config_version="fedspeak_stability_v1.0.0",
        notes=(
            "Quarterly stability summary for FedSpeak policy_bias, "
            "inflation_risk, and growth_risk."
        ),
    )

    print(f"Wrote fedspeak_stability → {out_path}")


if __name__ == "__main__":
    main()
