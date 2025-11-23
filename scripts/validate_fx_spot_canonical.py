"""
Validate canonical FX spot leaf stored in R2.

Usage (from repo root):

    $env:PYTHONPATH = "$PWD\src"
    python scripts/validate_fx_spot_canonical.py
"""

from datetime import date
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from common.r2_client import read_parquet_from_r2


# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

R2_KEY = "spine_us/us_fx_spot_canonical.parquet"

# Canonical pair universe you discovered in the leaf
EXPECTED_PAIRS: Optional[List[str]] = [
    "AUDCAD",
    "AUDJPY",
    "AUDUSD",
    "EURAUD",
    "EURBRL",
    "EURCAD",
    "EURCHF",
    "EURGBP",
    "EURJPY",
    "EURNOK",
    "EURNZD",
    "EURSEK",
    "EURUSD",
    "EURZAR",
    "GBPUSD",
    "NZDAUD",
    "NZDUSD",
    "USDBRL",
    "USDCAD",
    "USDCHF",
    "USDJPY",
    "USDNOK",
    "USDSEK",
    "USDZAR",
]

# Freshness guardrail in calendar days
MAX_STALENESS_DAYS = 5

# Candidate column names if schema ≠ internal aliases
DATE_CANDIDATES = [
    "as_of_date",
    "fx_date",
    "date",
    "obs_date",
    "time",
    "ref_date",
    "TIME_PERIOD",
]

VALUE_CANDIDATES = [
    "spot_mid",
    "fx_close",
    "spot",
    "rate",
    "fx_rate",
    "value",
    "OBS_VALUE",
    "price",
]


# ----------------------------------------------------------------------
# SCHEMA RESOLUTION
# ----------------------------------------------------------------------

def _guess_date_col(df: pd.DataFrame) -> Optional[str]:
    # 1) by known names
    for c in DATE_CANDIDATES:
        if c in df.columns:
            return c

    # 2) any datetime-like column
    for c in df.columns:
        if np.issubdtype(df[c].dtype, np.datetime64):
            return c

    return None


def _guess_value_col(df: pd.DataFrame) -> Optional[str]:
    # 1) by known names
    for c in VALUE_CANDIDATES:
        if c in df.columns:
            return c

    # 2) first float column
    for c in df.columns:
        if pd.api.types.is_float_dtype(df[c].dtype):
            return c

    # 3) first numeric column
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c].dtype):
            return c

    return None


def resolve_core_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, bool]:
    """
    Map whatever the canonical build produced → internal aliases:
        as_of_date, pair, spot_mid
    """
    print("[FX-RESOLVE] Available columns in canonical leaf:")
    print("            ", list(df.columns))

    if "pair" not in df.columns:
        print(
            "[FX-ERR] Required column 'pair' not found. "
            "Check build_fx_spot_canonical.py output schema."
        )
        return df, False

    date_col = _guess_date_col(df)
    value_col = _guess_value_col(df)

    if date_col is None:
        print(
            "[FX-ERR] Could not infer a date column from candidates "
            f"{DATE_CANDIDATES} or dtype inspection."
        )
        return df, False

    if value_col is None:
        print(
            "[FX-ERR] Could not infer a spot/value column from candidates "
            f"{VALUE_CANDIDATES} or dtype inspection."
        )
        return df, False

    print(f"[FX-RESOLVE] Using '{date_col}' as as_of_date")
    print(f"[FX-RESOLVE] Using '{value_col}' as spot_mid")

    df = df.copy()
    df = df.rename(
        columns={
            date_col: "as_of_date",
            value_col: "spot_mid",
        }
    )

    return df, True


# ----------------------------------------------------------------------
# CHECK HELPERS
# ----------------------------------------------------------------------

def check_columns(df: pd.DataFrame) -> bool:
    print("[FX-CHK] Checking required columns …")
    required = {"as_of_date", "pair", "spot_mid"}
    missing = required - set(df.columns)
    if missing:
        print(f"[FX-ERR] Missing required columns after resolution: {missing}")
        return False

    print(f"[FX-OK ] All required columns present: {required}")
    return True


def check_dtypes(df: pd.DataFrame) -> bool:
    print("[FX-CHK] Checking dtypes …")
    ok = True

    # as_of_date → datetime
    if not np.issubdtype(df["as_of_date"].dtype, np.datetime64):
        try:
            df["as_of_date"] = pd.to_datetime(df["as_of_date"])
            print("[FX-OK ] Coerced 'as_of_date' to datetime")
        except Exception as e:
            print(f"[FX-ERR] Failed to coerce 'as_of_date' to datetime: {e}")
            ok = False

    # pair → string
    if df["pair"].dtype != "O" and not pd.api.types.is_string_dtype(df["pair"].dtype):
        try:
            df["pair"] = df["pair"].astype(str)
            print("[FX-OK ] Coerced 'pair' to string")
        except Exception as e:
            print(f"[FX-ERR] Failed to coerce 'pair' to string: {e}")
            ok = False

    # spot_mid → float
    if not pd.api.types.is_float_dtype(df["spot_mid"].dtype):
        try:
            df["spot_mid"] = df["spot_mid"].astype(float)
            print("[FX-OK ] Coerced 'spot_mid' to float")
        except Exception as e:
            print(f"[FX-ERR] Failed to coerce 'spot_mid' to float: {e}")
            ok = False

    return ok


def check_duplicates(df: pd.DataFrame) -> bool:
    print("[FX-CHK] Checking for duplicate (pair, as_of_date) keys …")
    dupes = (
        df.groupby(["pair", "as_of_date"])
        .size()
        .reset_index(name="n")
        .query("n > 1")
    )

    if dupes.empty:
        print("[FX-OK ] No duplicate (pair, as_of_date) rows")
        return True

    print("[FX-ERR] Found duplicate (pair, as_of_date) rows:")
    print(dupes.head(20))
    return False


def check_values(df: pd.DataFrame) -> bool:
    print("[FX-CHK] Checking for null / non-positive spot_mid values …")
    ok = True

    null_count = df["spot_mid"].isna().sum()
    if null_count > 0:
        print(f"[FX-ERR] Found {null_count} NULL spot_mid values")
        ok = False
    else:
        print("[FX-OK ] No NULL spot_mid values")

    non_pos = df.query("spot_mid <= 0")
    if not non_pos.empty:
        print(f"[FX-ERR] Found {len(non_pos)} non-positive spot_mid values")
        print(non_pos.head(20))
        ok = False
    else:
        print("[FX-OK ] All spot_mid values are positive")

    return ok


def check_freshness(df: pd.DataFrame) -> bool:
    print("[FX-CHK] Checking data freshness …")
    max_date = df["as_of_date"].max().date()
    today = date.today()
    staleness = (today - max_date).days

    print(f"[FX-INFO] Max as_of_date in file: {max_date}")
    print(f"[FX-INFO] Today: {today} → staleness={staleness} days")

    if staleness <= MAX_STALENESS_DAYS:
        print(f"[FX-OK ] Freshness within threshold (<= {MAX_STALENESS_DAYS} days)")
        return True

    print(
        f"[FX-WARN] Data is stale by {staleness} days "
        f"(threshold={MAX_STALENESS_DAYS}). Check ECB feed / scheduler."
    )
    # WARN, not hard FAIL
    return True


def check_pair_universe(df: pd.DataFrame) -> bool:
    print("[FX-CHK] Inspecting pair universe …")
    pairs = sorted(df["pair"].unique())
    print(f"[FX-INFO] Discovered {len(pairs)} unique pairs:")
    print(", ".join(pairs))

    if EXPECTED_PAIRS is None:
        print(
            "[FX-INFO] EXPECTED_PAIRS not set. "
            "Edit this script & fill it in once you freeze the canonical universe."
        )
        return True

    expected_set = set(EXPECTED_PAIRS)
    discovered_set = set(pairs)

    missing = expected_set - discovered_set
    extra = discovered_set - expected_set

    ok = True
    if missing:
        print(f"[FX-ERR] Missing expected pairs: {sorted(missing)}")
        ok = False
    else:
        print("[FX-OK ] All expected pairs present")

    if extra:
        print(f"[FX-WARN] Found unexpected pairs: {sorted(extra)}")
    else:
        print("[FX-OK ] No unexpected pairs")

    return ok


def check_density(df: pd.DataFrame) -> bool:
    """
    Simple continuity / coverage check:
    For each pair, compare #rows to date range & flag very sparse series.
    """
    print("[FX-CHK] Checking series density by pair …")
    ok = True
    results = []

    for pair, g in df.groupby("pair"):
        g = g.sort_values("as_of_date")
        start = g["as_of_date"].iloc[0].date()
        end = g["as_of_date"].iloc[-1].date()
        days = (end - start).days + 1

        density = len(g) / days if days > 0 else 0.0
        results.append((pair, start, end, len(g), days, density))

        if density < 0.5:
            print(
                f"[FX-WARN] Sparse series for {pair}: "
                f"rows={len(g)}, range={start}→{end} ({days} days), "
                f"density={density:.2f}"
            )

    res_df = pd.DataFrame(
        results,
        columns=["pair", "start_date", "end_date", "rows", "days", "density"],
    ).sort_values("pair")

    print("[FX-INFO] Density summary (top 10):")
    print(res_df.head(10).to_string(index=False))

    return ok


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main() -> None:
    print("[FX-VALIDATE] Loading canonical FX spot leaf from R2 …")
    df = read_parquet_from_r2(R2_KEY)
    print(f"[FX-INFO] Loaded {len(df):,} rows from {R2_KEY}")

    # Map external schema → internal aliases
    df, resolved_ok = resolve_core_columns(df)
    if not resolved_ok:
        print("\n[FX-RESULT] ❌ FX spot canonical leaf FAILED schema resolution")
        return

    checks = [
        ("columns", check_columns),
        ("dtypes", check_dtypes),
        ("duplicates", check_duplicates),
        ("values", check_values),
        ("freshness", check_freshness),
        ("pair_universe", check_pair_universe),
        ("density", check_density),
    ]

    all_ok = True
    for name, fn in checks:
        print(f"\n[FX-STEP] Running check: {name}")
        ok = fn(df)
        all_ok = all_ok and ok

    print("\n" + "=" * 72)
    if all_ok:
        print("[FX-RESULT] ✅ FX spot canonical leaf PASSED all hard checks")
    else:
        print("[FX-RESULT] ❌ FX spot canonical leaf FAILED one or more checks")
    print("=" * 72)


if __name__ == "__main__":
    main()


