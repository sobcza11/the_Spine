"""
Fed multi-channel policy leaf.

Goal:
    Fuse separate Fed channels (BeigeBook, FOMC Minutes, and later
    Statements, SEP, Speeches) into a single, interpretable policy leaf
    that *the*_Spine can consume via p_Sentiment_US.

Current implementation:
    - Uses BeigeBook and FOMC Minutes event-level leaves (if available)
    - Computes a weighted combination of inflation_risk, growth_risk,
      and policy_bias
    - Preserves channel-specific columns for diagnostics

Design choice (CPMAI-friendly):
    - If a channel leaf is missing the expected risk columns, we create
      neutral zeros for them instead of raising. This allows the overall
      architecture to function while you incrementally enrich the leaves.
"""

import logging
from pathlib import Path
from typing import Optional, Dict

import numpy as np
import pandas as pd

from fed_speak.utils.r2_upload import upload_to_r2

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _load_leaf(path: Path, label: str) -> Optional[pd.DataFrame]:
    """
    Load a parquet leaf if it exists, otherwise return None and log a warning.
    """
    if not path.exists():
        logger.warning(f"{label} leaf not found at {path} (skipping channel).")
        return None
    df = pd.read_parquet(path)
    logger.info(f"Loaded {label} leaf from {path} with {len(df)} rows.")
    return df


def _ensure_risk_columns(df: pd.DataFrame, channel_name: str) -> pd.DataFrame:
    """
    Ensure a leaf has the minimum risk columns required for fusion:
        - inflation_risk
        - growth_risk
        - policy_bias

    If they are missing, we add them as zeros and log a warning.

    This keeps the fusion skeleton operational even when legacy leaves
    are not yet enriched with risk metrics.
    """
    risk_cols = ["inflation_risk", "growth_risk", "policy_bias"]
    missing = [c for c in risk_cols if c not in df.columns]

    if missing:
        logger.warning(
            f"{channel_name} leaf missing expected risk columns {missing}. "
            f"Creating neutral 0.0 placeholders. You can upgrade this by "
            f"adding real risk metrics to the {channel_name} leaf later."
        )
        for col in missing:
            df[col] = 0.0

    return df


def _prepare_channel(
    df: pd.DataFrame,
    channel_name: str,
) -> pd.DataFrame:
    """
    Normalize channel leaf schema and prefix metric columns.

    Expected columns (minimum):
        - event_id
        - inflation_risk
        - growth_risk
        - policy_bias
    Optional:
        - event_date
    """
    if "event_id" not in df.columns:
        raise ValueError(f"{channel_name} leaf missing required column: 'event_id'")

    df = _ensure_risk_columns(df, channel_name)

    cols = ["event_id"]
    if "event_date" in df.columns:
        cols.append("event_date")

    metric_cols = ["inflation_risk", "growth_risk", "policy_bias"]
    cols.extend(metric_cols)

    df = df[cols].copy()
    # Prefix metric columns with channel name
    rename_map = {c: f"{channel_name}_{c}" for c in metric_cols}
    df = df.rename(columns=rename_map)

    return df


def _weighted_combine(row: pd.Series, col: str, weights: Dict[str, float]) -> float:
    """
    Combine channel-specific metrics into a single scalar for one row.

    Example:
        col = "inflation_risk"
        weights = {"beige": 0.5, "minutes": 0.5}
    """
    num = 0.0
    den = 0.0
    for channel, w in weights.items():
        channel_col = f"{channel}_{col}"
        if channel_col in row and not pd.isna(row[channel_col]):
            num += w * float(row[channel_col])
            den += w
    if den == 0.0:
        return np.nan
    return num / den


def build_fed_multichannel_leaf(
    beige_leaf_path: Path,
    minutes_leaf_path: Path,
    output_path: Path,
    weights: Optional[Dict[str, float]] = None,
) -> pd.DataFrame:
    """
    Build a Fed multi-channel policy leaf.

    Inputs:
        - BeigeBook leaf: data/processed/BeigeBook/leaf.parquet
        - FOMC Minutes leaf: data/processed/FOMC_Minutes/minutes_leaf.parquet

    Weights (default):
        - beige: 0.5
        - minutes: 0.5

    Behavior:
        - If only Beige is available, produces a Beige-only combined leaf.
        - If Minutes is also available, uses the provided weights.
    """
    weights = weights or {"beige": 0.5, "minutes": 0.5}

    beige_df = _load_leaf(beige_leaf_path, "BeigeBook")
    minutes_df = _load_leaf(minutes_leaf_path, "FOMC Minutes")

    if beige_df is None and minutes_df is None:
        raise RuntimeError("No Fed channels available to build multi-channel leaf.")

    merged: Optional[pd.DataFrame] = None

    if beige_df is not None:
        beige_norm = _prepare_channel(beige_df, "beige")
        merged = beige_norm

    if minutes_df is not None:
        minutes_norm = _prepare_channel(minutes_df, "minutes")
        if merged is None:
            merged = minutes_norm
        else:
            merged = merged.merge(
                minutes_norm,
                on=["event_id"],
                how="outer",
                suffixes=("", "_minutes_extra"),
            )

    # Harmonize event_date if present
    if "event_date" not in merged.columns:
        if "beige_event_date" in merged.columns:
            merged["event_date"] = merged["beige_event_date"]
        elif "minutes_event_date" in merged.columns:
            merged["event_date"] = merged["minutes_event_date"]
        else:
            merged["event_date"] = pd.NaT

    # Compute combined metrics
    combined_rows = []
    for _, row in merged.iterrows():
        inflation = _weighted_combine(row, "inflation_risk", weights)
        growth = _weighted_combine(row, "growth_risk", weights)
        policy = _weighted_combine(row, "policy_bias", weights)

        combined_rows.append(
            {
                "event_id": row["event_id"],
                "event_date": row.get("event_date", pd.NaT),
                "inflation_risk": inflation,
                "growth_risk": growth,
                "policy_bias": policy,
                # Channel-specific metrics for diagnostics
                "beige_inflation_risk": row.get("beige_inflation_risk", np.nan),
                "beige_growth_risk": row.get("beige_growth_risk", np.nan),
                "beige_policy_bias": row.get("beige_policy_bias", np.nan),
                "minutes_inflation_risk": row.get("minutes_inflation_risk", np.nan),
                "minutes_growth_risk": row.get("minutes_growth_risk", np.nan),
                "minutes_policy_bias": row.get("minutes_policy_bias", np.nan),
            }
        )

    out_df = pd.DataFrame(combined_rows)
    out_df = out_df.sort_values("event_date").reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(output_path, index=False)
    logger.info(f"Saved Fed multi-channel policy leaf to {output_path} with {len(out_df)} rows.")

    return out_df


def main() -> None:
    """
    CLI entry point to build the multi-channel Fed policy leaf.

    Default locations:
        BeigeBook leaf:    data/processed/BeigeBook/leaf.parquet
        Minutes leaf:      data/processed/FOMC_Minutes/minutes_leaf.parquet
        Output (fed leaf): data/processed/FedSpeak/combined_policy_leaf.parquet

    R2 export:
        - Uploads the combined policy leaf to thespine-us-hub:
            FedSpeak/combined_policy_leaf.parquet
    """
    base_processed = Path("data/processed")

    beige_leaf_path = base_processed / "BeigeBook" / "leaf.parquet"
    minutes_leaf_path = base_processed / "FOMC_Minutes" / "minutes_leaf.parquet"
    output_path = base_processed / "FedSpeak" / "combined_policy_leaf.parquet"

    # Build the combined Fed policy leaf (Beige-only for now if Minutes missing)
    build_fed_multichannel_leaf(
        beige_leaf_path=beige_leaf_path,
        minutes_leaf_path=minutes_leaf_path,
        output_path=output_path,
        weights={"beige": 0.5, "minutes": 0.5},
    )

    # Upload Parquet to R2 (Power BI reads directly)
    bucket = "thespine-us-hub"
    upload_to_r2(output_path, bucket, "FedSpeak/combined_policy_leaf.parquet")


if __name__ == "__main__":
    main()

