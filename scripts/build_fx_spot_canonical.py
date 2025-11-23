"""
Validate canonical FX spot leaf stored in R2.

Usage (from repo root):

    $env:PYTHONPATH = "$PWD\src"
    python scripts/validate_fx_spot_canonical.py
"""

from datetime import date
from typing import List, Optional

import numpy as np
import pandas as pd

from common.r2_client import read_parquet_from_r2


# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

R2_KEY = "spine_us/us_fx_spot_canonical.parquet"

# Optional: once you’re happy with the canonical universe,
# fill in EXPECTED_PAIRS with your 24 canonical pairs.
# Leave as None for discovery mode (it will just print what it finds).
EXPECTED_PAIRS: Optional[List[str]] = None
# Example:
# EXPECTED_PAIRS = [
#     "EURUSD", "USDJPY", "GBPUSD", "USDCHF", "AUDUSD", "NZDUSD",
#     "USDCAD", "USDNOK", "USDSEK", "USDZAR", "USDBRL",
#     "EURJPY", "EURGBP", "EURCHF", "EURAUD", "EURNZD",
#     "EURCAD", "EURNOK", "EURSEK", "EURZAR", "EURBRL",
#     "GBPJPY", "AUDJPY", "CADJPY"
# ]

# Freshness guardrail in calendar days
MAX_STALENESS_DAYS = 5


# ----------------------------------------------------------------------
# CHECK HELPERS
# ----------------------------------------------------------------------

def check_columns(df: pd.DataFrame) -> bool:
    print("[FX-CHK] Checking required columns …")
    required = {"as_of_date", "pair", "spot_mid"}
    missing = required - set(df.columns)
    if missing:
        print(f"[FX-ERR] Missing required columns: {missing}")
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
    # I treat this as a WARN, not hard FAIL, so return True.
    return True


def check_pair_universe(df: pd.DataFrame) -> bool:
    print("[FX-CHK] Inspecting pair universe …")
    pairs = sorted(df["pair"].unique())
    print(f"[FX-INFO] Discovered {len(pairs)} unique pairs:")
    print(", ".join(pairs))

    if EXPECTED_PAIRS is None:
        print("[FX-INFO] EXPECTED_PAIRS not set. "
              "Edit this script & fill it in once you freeze the canonical universe.")
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

        # Naive density: rows / total days in range (calendar days)
        density = len(g) / days if days > 0 else 0.0
        results.append((pair, start, end, len(g), days, density))

        if density < 0.5:
            print(
                f"[FX-WARN] Sparse series for {pair}: "
                f"rows={len(g)}, range={start}→{end} ({days} days), "
                f"density={density:.2f}"
            )
            # Not a hard fail — weekends/holidays & ECB gaps are expected.
            # Keep it as a WARN.

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


