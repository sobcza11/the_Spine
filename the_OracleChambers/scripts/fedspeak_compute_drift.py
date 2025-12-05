from pathlib import Path
import json
import pandas as pd

from scripts.write_metadata import write_metadata


def compute_z(value, mean, std):
    if std == 0 or pd.isna(std):
        return 0.0
    return (value - mean) / std


def main():
    repo_root = Path(__file__).resolve().parents[1]

    leaf_path = repo_root / "data" / "processed" / "FedSpeak" / "combined_policy_leaf.parquet"
    baseline_path = repo_root / "config" / "fedspeak_feature_baseline.json"

    if not leaf_path.exists():
        raise FileNotFoundError(f"combined_policy_leaf not found at {leaf_path}")
    if not baseline_path.exists():
        raise FileNotFoundError(f"Baseline config not found at {baseline_path}")

    df = pd.read_parquet(leaf_path)

    with baseline_path.open("r", encoding="utf-8") as f:
        baseline = json.load(f)

    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "event_date": row["event_date"],
                "inflation_risk_z": compute_z(
                    row["inflation_risk"],
                    baseline["inflation_risk"]["mean"],
                    baseline["inflation_risk"]["std"],
                ),
                "growth_risk_z": compute_z(
                    row["growth_risk"],
                    baseline["growth_risk"]["mean"],
                    baseline["growth_risk"]["std"],
                ),
                "policy_bias_z": compute_z(
                    row["policy_bias"],
                    baseline["policy_bias"]["mean"],
                    baseline["policy_bias"]["std"],
                ),
            }
        )

    drift_df = pd.DataFrame(rows)

    out_dir = repo_root / "data" / "processed" / "FedSpeak"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "fedspeak_drift_report.parquet"
    drift_df.to_parquet(out_path, index=False)

    # Metadata
    data_date = (
        str(drift_df["event_date"].max())
        if not drift_df["event_date"].isna().all()
        else ""
    )

    write_metadata(
        artifact_path=out_path,
        artifact_name="fedspeak_drift_report",
        artifact_type="diagnostic",
        channel="fedspeak",
        data_date=data_date,
        df=drift_df,
        pipeline_config_version="fedspeak_drift_v1.0.0",
        notes="FedSpeak drift report with Z-scores vs long-run baseline.",
    )

    print(f"Wrote fedspeak_drift_report → {out_path}")


if __name__ == "__main__":
    main()

