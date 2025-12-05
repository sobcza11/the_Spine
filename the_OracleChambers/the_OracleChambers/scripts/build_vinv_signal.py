# scripts/build_vinv_signal.py

import json
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.write_metadata import write_metadata

ROOT = Path(__file__).resolve().parents[1]


def load_paths_config() -> dict:
    """Load artifact path configuration from config/artifacts_paths.json."""
    config_path = ROOT / "config" / "artifacts_paths.json"
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_parquet_from_key(artifact_key: str) -> pd.DataFrame:
    """
    Generic helper:
    - resolve logical key (e.g., 'vinv_canonical') via artifacts_paths.json
    - load Parquet as DataFrame
    """
    paths = load_paths_config()
    if artifact_key not in paths:
        raise KeyError(f"Artifact key '{artifact_key}' not found in artifacts_paths.json")

    parquet_path = ROOT / paths[artifact_key]
    if not parquet_path.exists():
        raise FileNotFoundError(f"Artifact '{artifact_key}' not found at {parquet_path}")

    return pd.read_parquet(parquet_path)


def _bound_to_unit_interval(series: pd.Series, clip_min: float = -3.0, clip_max: float = 3.0) -> pd.Series:
    """
    Map a z-like score into [-1, 1] in a deterministic, explainable way.

    - Clip extreme values to [clip_min, clip_max]
    - Linearly rescale to [-1, 1]

    This keeps VinV firmly within the_Spine's bounded-score contract.
    """
    clipped = series.clip(lower=clip_min, upper=clip_max)
    # Rescale from [clip_min, clip_max] to [-1, 1]
    return (clipped - clip_min) / (clip_max - clip_min) * 2 - 1


def build_vinv_signal(df_canonical: pd.DataFrame) -> pd.DataFrame:
    """
    Build VinV MVP signal from canonical equity inputs.

    Expected (but tweakable) input columns in vinv_canonical:

    - date
    - value_vs_growth_z        # cross-sectional z-score of value factor vs growth
    - dividend_durability_z    # stability of dividends / payout resilience
    - policy_tilt_z            # optional, e.g. from FedSpeak or macro mapping

    If you do not yet have policy_tilt_z, we treat it as 0 for now
    (purely macro/value-driven).
    """

    df = df_canonical.copy()

    if "date" not in df.columns:
        raise ValueError("vinv_canonical must contain a 'date' column")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date", keep="last")

    # ---- Macro-value support ----
    # Macro support from value vs growth and dividend durability.
    value_col = "value_vs_growth_z"
    durable_col = "dividend_durability_z"

    # If columns are missing, default to zeros (transparent & deterministic)
    if value_col not in df.columns:
        df[value_col] = 0.0
    if durable_col not in df.columns:
        df[durable_col] = 0.0

    macro_raw = 0.6 * df[value_col] + 0.4 * df[durable_col]
    df["macro_value_support"] = _bound_to_unit_interval(macro_raw)

    # ---- Policy-value support ----
    # Optional channel: a policy-aware preference for value exposures.
    policy_col = "policy_tilt_z"
    if policy_col not in df.columns:
        df[policy_col] = 0.0

    policy_raw = df[policy_col]
    df["policy_value_support"] = _bound_to_unit_interval(policy_raw)

    # ---- VinV composite score ----
    # Blend macro & policy channels.
    df["vinv_score"] = (
        0.6 * df["macro_value_support"] + 0.4 * df["policy_value_support"]
    )

    # Final bounding in [-1, 1] for safety
    df["vinv_score"] = df["vinv_score"].clip(-1.0, 1.0)

    out = df[["date", "vinv_score", "macro_value_support", "policy_value_support"]].copy()
    out = out.sort_values("date").reset_index(drop=True)

    return out


def main() -> None:
    paths = load_paths_config()

    # 1) Load canonical equity inputs
    canonical_df = load_parquet_from_key("vinv_canonical")

    # 2) Build VinV MVP signal
    vinv_df = build_vinv_signal(canonical_df)

    # 3) Save to declared vinv_signal path
    if "vinv_signal" not in paths:
        raise KeyError("vinv_signal not defined in artifacts_paths.json")

    vinv_path = ROOT / paths["vinv_signal"]
    vinv_path.parent.mkdir(parents=True, exist_ok=True)
    vinv_df.to_parquet(vinv_path, index=False)

    # 4) Write metadata sidecar (CPMAI governance)
    write_metadata(
        artifact_path=vinv_path,
        artifact_name="vinv_signal",
        artifact_type="leaf",
        channel="equity_vinv",
        data_date=str(vinv_df["date"].max().date()),
        df=vinv_df,
        pipeline_config_version="vinv_mvp_v1",
        source_reference="vinv_canonical",
        notes="VinV MVP signal: macro_value_support, policy_value_support, vinv_score ∈ [-1, 1].",
    )

    print(f"Updated vinv_signal → {vinv_path}")


if __name__ == "__main__":
    main()

