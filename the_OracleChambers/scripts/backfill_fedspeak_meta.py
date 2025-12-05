from pathlib import Path
import pandas as pd

from scripts.write_metadata import write_metadata


def main():
    # Project root (the_OracleChambers)
    repo_root = Path(__file__).resolve().parents[1]

    # Path to the existing combined_policy_leaf artifact
    artifact_rel = "data/processed/FedSpeak/combined_policy_leaf.parquet"
    artifact_path = repo_root / artifact_rel

    if not artifact_path.exists():
        raise FileNotFoundError(f"Could not find {artifact_path}")

    # Load the existing leaf
    df = pd.read_parquet(artifact_path)

    # Use the latest event_date as the data_date, if present
    if "event_date" in df.columns:
        data_date = df["event_date"].max()
        try:
            data_date = data_date.strftime("%Y-%m-%d")
        except AttributeError:
            # if it's already a string
            data_date = str(data_date)
    else:
        data_date = ""

    # Write metadata sidecar
    meta_path = write_metadata(
        artifact_path=artifact_path,
        artifact_name="FedSpeak_combined_policy_leaf",
        artifact_type="leaf",
        channel="combined",
        data_date=data_date,
        df=df,
        pipeline_config_version="fedspeak_pipeline_v1.0.0",
        notes="Backfilled metadata for existing combined_policy_leaf artifact.",
    )

    print(f"Wrote metadata to: {meta_path}")


if __name__ == "__main__":
    main()
