"""
Validate canonical IR differential leaf stored in R2.

Usage (from repo root):

    $env:PYTHONPATH = "$PWD\\src"
    python scripts/validate_ir_diff_canonical.py
"""

from __future__ import annotations

from datetime import date
from typing import List

import numpy as np
import pandas as pd

from common.r2_client import read_parquet_from_r2

R2_IR_DIFF_KEY = "spine_us/us_ir_diff_canonical.parquet"
MAX_STALENESS_DAYS = 5


REQUIRED_COLUMNS = {
    "as_of_date",
    "pair",
    "base_ccy",
    "quote_ccy",
    "leaf_group",
    "leaf_name",
    "source_system",
    "updated_at",
}

FLOAT_COLUMNS = [
    "base_10y_yield",
    "quote_10y_yield",
    "diff_10y_bp",
    "base_policy_rate",
    "quote_policy_rate",
    "diff_policy_bp",
]


def check_columns(df: pd.DataFrame) -> bool:
    print("[IR-CHK] Checking required columns …")
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        print(f"[IR-ERR] Missing required columns: {missing}")
        return False

    print("[IR-OK ] All required columns present.")
    return True


def check_dtypes(df: pd.DataFrame) -> bool:
    print("[IR-CHK] Checking dtypes …")
    ok = True

    # as_of_date → datetime
    if not np.issubdtype(df["as_of_date"].dtype, np.datetime64):
        try:
            df["as_of_date"] = pd.to_datetime(df["as_of_date"])
            print("[IR-OK ] Coerced 'as_of_date' to datetime")
        except Exception as e:
            print(f"[IR-ERR] Failed to coerce 'as_of_date' to datetime: {e}")
            ok = False

    # pair/base_ccy/quote_ccy strings
    for col in ["pair", "base_ccy", "quote_ccy"]:
        if df[col].dtype != "O" and not pd.api.types.is_string_dtype(df[col].dtype):
            try:
                df[col] = df[col].astype(str)
                print(f"[IR-OK ] Coerced '{col}' to string")
            except Exception as e:
                print(f"[IR-ERR] Failed to coerce '{col}' to string: {e}")
                ok = False

    # floats
    for col in FLOAT_COLUMNS:
        if col in df.columns and not pd.api.types.is_float_dtype(df[col].dtype):
            try:
                df[col] = df[col].astype(float)
                print(f"[IR-OK ] Coerced '{col}' to float")
            except Exception as e:
                print(f"[IR-ERR] Failed to coerce '{col}' to float: {e}")
                ok = False

    return ok


def check_duplicates(df: pd.DataFrame) -> bool:
    print("[IR-CHK] Checking duplicate (pair, as_of_date) keys …")
    dupes = (
        df.groupby(["pair", "as_of_date"])
        .size()
        .reset_index(name="n")
        .query("n > 1")
    )
    if dupes.empty:
        print("[IR-OK ] No duplicate (pair, as_of_date) rows.")
        return True

    print("[IR-ERR] Found duplicate (pair, as_of_date) rows:")
    print(dupes.head(20))
    return False


def check_freshness(df: pd.DataFrame) -> bool:
    print("[IR-CHK] Checking data freshness …")
    max_date = df["as_of_date"].max().date()
    today = date.today()
    staleness = (today - max_date).days

    print(f"[IR-INFO] Max as_of_date in file: {max_date}")
    print(f"[IR-INFO] Today: {today} → staleness={staleness} days")

    if staleness <= MAX_STALENESS_DAYS:
        print(f"[IR-OK ] Freshness within threshold (<= {MAX_STALENESS_DAYS} days)")
        return True

    print(
        f"[IR-WARN] Data is stale by {staleness} days "
        f"(threshold={MAX_STALENESS_DAYS}). Check IR yields feed / scheduler."
    )
    return True  # Warn only


def check_values(df: pd.DataFrame) -> bool:
    print("[IR-CHK] Checking for insane diff values …")
    ok = True

    # Very rough sanity: |diff| > 3000 bp (30%) is suspicious
    for col in ["diff_10y_bp", "diff_policy_bp"]:
        if col not in df.columns:
            continue
        s = df[col].dropna()
        extreme = s[np.abs(s) > 3000]
        if not extreme.empty:
            print(
                f"[IR-WARN] Found {len(extreme)} extreme values in {col}; "
                "check upstream series alignment."
            )
            ok = False

    return ok


def main() -> None:
    print("[IR-VALIDATE] Loading canonical IR diff leaf from R2 …")
    try:
        df = read_parquet_from_r2(R2_IR_DIFF_KEY)
    except Exception as e:
        msg = str(e)
        # If the object simply doesn't exist in R2 yet, treat this as "not populated yet"
        if "NoSuchKey" in msg:
            print(f"[IR-INFO] IR diff leaf {R2_IR_DIFF_KEY} not found in R2 yet.")
            print(
                "[IR-INFO] This is expected while only USD rates are wired and "
                "no pair-level IR differentials have been computed."
            )
            print("\n" + "=" * 72)
            print("[IR-RESULT] ✅ IR diff canonical leaf validation skipped (no file yet)")
            print("=" * 72)
            return
        # Any other error should still fail loudly
        raise

    print(f"[IR-INFO] Loaded {len(df):,} rows from {R2_IR_DIFF_KEY}")

    # If file exists but has 0 rows, also treat as "not populated yet"
    if df.empty:
        print("[IR-INFO] IR diff leaf is empty (0 rows).")
        print(
            "[IR-INFO] This is expected while only USD rates are wired; "
            "add more CCY yield fetchers to populate pair-level diffs."
        )
        print("\n" + "=" * 72)
        print("[IR-RESULT] ✅ IR diff canonical leaf validation skipped (no data yet)")
        print("=" * 72)
        return

    checks: List[tuple[str, callable]] = [
        ("columns", check_columns),
        ("dtypes", check_dtypes),
        ("duplicates", check_duplicates),
        ("freshness", check_freshness),
        ("values", check_values),
    ]

    all_ok = True
    for name, fn in checks:
        print(f"\n[IR-STEP] Running check: {name}")
        ok = fn(df)
        all_ok = all_ok and ok

    print("\n" + "=" * 72)
    if all_ok:
        print("[IR-RESULT] ✅ IR diff canonical leaf PASSED all hard checks")
    else:
        print("[IR-RESULT] ❌ IR diff canonical leaf FAILED one or more checks")
    print("=" * 72)


if __name__ == "__main__":
    main()

