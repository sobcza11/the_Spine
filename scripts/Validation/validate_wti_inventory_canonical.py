"""
validate_wti_inventory_canonical.py
-----------------------------------
Validation gate for WTI inventory canonical leaf.

Intended to run after:
    scripts/WTI/build_wti_inventory_canonical.py

Checks:
    - File exists
    - Required columns present
    - as_of_date is datetime & sorted
    - No all-null inventory_level
    - Reasonable cadence
    - Outlier detection on inventory_change
Exit codes:
    0 = all good (or only warnings)
    1 = hard failure (missing file / critical schema issue)
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
CANONICAL_PATH = BASE_DIR / "data" / "spine_us" / "us_wti_inventory_canonical.parquet"

REQUIRED_COLUMNS = {"as_of_date", "inventory_level"}
OPTIONAL_COLUMNS = {"inventory_change", "source"}


def load_canonical() -> pd.DataFrame:
    if not CANONICAL_PATH.exists():
        print(f"[WTI-VAL] ERROR: Canonical file not found at {CANONICAL_PATH}")
        sys.exit(1)

    df = pd.read_parquet(CANONICAL_PATH)
    return df


def check_columns(df: pd.DataFrame):
    cols = set(df.columns)
    missing = REQUIRED_COLUMNS - cols
    if missing:
        print(f"[WTI-VAL] ERROR: Missing required columns: {missing}")
        sys.exit(1)

    extra = cols - (REQUIRED_COLUMNS | OPTIONAL_COLUMNS)
    if extra:
        print(f"[WTI-VAL] Warning: Extra columns present: {extra}")


def check_types_and_sort(df: pd.DataFrame) -> pd.DataFrame:
    # as_of_date type
    try:
        df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    except Exception as e:
        print(f"[WTI-VAL] ERROR: Failed to convert 'as_of_date' to datetime: {e}")
        sys.exit(1)

    # numeric inventory
    df["inventory_level"] = pd.to_numeric(df["inventory_level"], errors="coerce")

    null_inventory = df["inventory_level"].isna().mean()
    if null_inventory > 0.05:
        print(f"[WTI-VAL] Warning: {null_inventory:.1%} of inventory_level values are NaN.")

    df = df.sort_values("as_of_date").reset_index(drop=True)
    return df


def check_cadence(df: pd.DataFrame):
    if df.shape[0] < 3:
        print("[WTI-VAL] Note: fewer than 3 rows; cadence check skipped.")
        return

    diffs = df["as_of_date"].diff().dt.days.dropna()
    if diffs.empty:
        print("[WTI-VAL] Note: insufficient intervals for cadence check.")
        return

    mode_diff = diffs.mode().iloc[0]
    irregular = diffs[diffs != mode_diff]

    frac_irregular = len(irregular) / len(diffs) if len(diffs) > 0 else 0.0

    print(f"[WTI-VAL] Most common interval: {mode_diff} days.")
    if frac_irregular > 0.25:
        print(f"[WTI-VAL] Warning: {frac_irregular:.1%} of intervals deviate from modal cadence.")


def check_outliers(df: pd.DataFrame):
    if "inventory_change" not in df.columns:
        print("[WTI-VAL] Note: 'inventory_change' column not found; outlier check skipped.")
        return

    changes = pd.to_numeric(df["inventory_change"], errors="coerce").dropna()
    if changes.empty:
        print("[WTI-VAL] Note: no non-NaN inventory_change values; outlier check skipped.")
        return

    mean = changes.mean()
    std = changes.std(ddof=1) if len(changes) > 1 else 0

    if std == 0:
        print("[WTI-VAL] Note: zero std for inventory_change; outlier check not meaningful.")
        return

    z_scores = (changes - mean) / std
    mask = np.abs(z_scores) > 4.0  # 4-sigma threshold

    outlier_count = mask.sum()
    if outlier_count > 0:
        print(f"[WTI-VAL] Warning: {outlier_count} potential outlier(s) in inventory_change (>4σ).")
    else:
        print("[WTI-VAL] No extreme outliers detected in inventory_change.")


def summary(df: pd.DataFrame):
    print(f"[WTI-VAL] Row count: {df.shape[0]}")
    print(f"[WTI-VAL] Date range: {df['as_of_date'].min().date()} → {df['as_of_date'].max().date()}")
    print(f"[WTI-VAL] inventory_level min/max: {df['inventory_level'].min()} / {df['inventory_level'].max()}")


def main():
    print("\n=== Validating WTI Inventory Canonical Leaf ===")

    df = load_canonical()
    check_columns(df)
    df = check_types_and_sort(df)
    summary(df)
    check_cadence(df)
    check_outliers(df)

    print("=== WTI canonical validation completed (no hard failures). ===\n")
    sys.exit(0)


if __name__ == "__main__":
    main()
