from pathlib import Path
import pandas as pd

from scripts.write_metadata import write_metadata


def classify_policy_regime(policy_bias: float, inflation_risk: float) -> str:
    """
    Simple, interpretable regime classification:

    - Hawkish:  policy_bias >= 0.3 and inflation_risk >= 0.2
    - Dovish:   policy_bias <= -0.3 and inflation_risk <= -0.2
    - Neutral:  everything else
    """
    if pd.isna(policy_bias) or pd.isna(inflation_risk):
        return "unknown"

    if policy_bias >= 0.3 and inflation_risk >= 0.2:
        return "hawkish"
    if policy_bias <= -0.3 and inflation_risk <= -0.2:
        return "dovish"
    return "neutral"


def main():
    repo_root = Path(__file__).resolve().parents[1]
    leaf_path = repo_root / "data" / "processed" / "FedSpeak" / "combined_policy_leaf.parquet"

    if not leaf_path.exists():
        raise FileNotFoundError(f"combined_policy_leaf not found at {leaf_path}")

    df = pd.read_parquet(leaf_path)

    # --- Ensure future channel columns exist (even if NaN) ---
    # Minutes columns already exist in your current schema; we leave them as-is.
    new_channel_cols = [
        "statement_inflation_risk",
        "statement_growth_risk",
        "statement_policy_bias",
        "speech_inflation_risk",
        "speech_growth_risk",
        "speech_policy_bias",
    ]

    for col in new_channel_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # --- Add policy_regime_label ---
    df["policy_regime_label"] = df.apply(
        lambda row: classify_policy_regime(
            row.get("policy_bias"), row.get("inflation_risk")
        ),
        axis=1,
    )

    # --- Save back to Parquet ---
    df.to_parquet(leaf_path, index=False)

    # --- Rewrite metadata for combined_policy_leaf ---
    data_date = (
        str(df["event_date"].max())
        if "event_date" in df.columns and not df["event_date"].isna().all()
        else ""
    )

    write_metadata(
        artifact_path=leaf_path,
        artifact_name="combined_policy_leaf",
        artifact_type="policy_leaf",
        channel="fedspeak",
        data_date=data_date,
        df=df,
        pipeline_config_version="combined_policy_leaf_v2.0.0",
        notes=(
            "Added channel placeholder fields (statement_*, speech_*) and "
            "policy_regime_label for simple hawkish/dovish/neutral classification."
        ),
    )

    print(f"Updated combined_policy_leaf → {leaf_path}")


if __name__ == "__main__":
    main()

