"""
build_wti_inventory_canonical.py
--------------------------------
Canonical builder for WTI inventory & storage data.

Reads raw inventory history from:
    data/us/wti_inv_stor/

Produces canonical parquet:
    data/spine_us/us_wti_inventory_canonical.parquet

Schema:
    as_of_date (datetime64)
    inventory_level (float)
    inventory_change (float, optional)
    source (str)

Uploads to Cloudflare R2 (non-blocking) if .env is configured.
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime

# Optional R2 uploader
try:
    from src.utils.storage_r2 import upload_file_to_r2
    R2_AVAILABLE = True
except ImportError:
    R2_AVAILABLE = False


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "us" / "wti_inv_stor"
CANONICAL_DIR = BASE_DIR / "data" / "spine_us"
CANONICAL_PATH = CANONICAL_DIR / "us_wti_inventory_canonical.parquet"

R2_TARGET_KEY = "spine_us/us_wti_inventory_canonical.parquet"


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def load_raw_wti() -> pd.DataFrame:
    """
    Loads the most reasonable raw file from data/us/wti_inv_stor/.
    Accepts .csv, .parquet, or .xlsx.
    Returns DataFrame with at least:
        date | inventory
    """

    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Raw WTI directory not found: {RAW_DIR}")

    files = list(RAW_DIR.glob("*"))
    if len(files) == 0:
        raise FileNotFoundError(f"No raw WTI files found in: {RAW_DIR}")

    # Pick the most recent file by modified time
    raw_path = max(files, key=lambda f: f.stat().st_mtime)
    print(f"[WTI] Loading raw file: {raw_path.name}")

    if raw_path.suffix == ".csv":
        df = pd.read_csv(raw_path)
    elif raw_path.suffix == ".parquet":
        df = pd.read_parquet(raw_path)
    elif raw_path.suffix in [".xls", ".xlsx"]:
        df = pd.read_excel(raw_path)
    else:
        raise ValueError(f"Unsupported file format: {raw_path.suffix}")

    return df


def infer_inventory_column(df_raw: pd.DataFrame) -> str:
    """
    Try to infer the inventory column from common patterns & numeric heuristics.
    """

    # 1) If any of these known names exist, use them
    inv_candidates = [
        "inventory",
        "inventory_level",
        "crude_inv",
        "crude_inventory",
        "crude_stocks",
        "wti_inventory",
        "WTI_Inv",
        "inv_level",
        "stocks",
    ]
    for c in inv_candidates:
        if c in df_raw.columns:
            print(f"[WTI] Using explicit inventory column: {c}")
            return c

    # 2) Any column whose name contains an inventory-ish token
    inventory_tokens = ["inv", "stock", "inventory", "crude", "wti"]
    for col in df_raw.columns:
        lower = col.lower()
        if any(tok in lower for tok in inventory_tokens):
            print(f"[WTI] Using heuristic inventory column (name match): {col}")
            return col

    # 3) Fallback: pick the first numeric column that is NOT obviously a date
    numeric_cols = []
    for col in df_raw.columns:
        # skip obvious date-like columns
        if "date" in col.lower() or "week" in col.lower():
            continue
        s = pd.to_numeric(df_raw[col], errors="coerce")
        if s.notna().mean() > 0.8:  # at least 80% numeric-ish
            numeric_cols.append((col, s))

    if numeric_cols:
        # pick the one with the largest variance
        col, s = max(numeric_cols, key=lambda t: t[1].var())
        print(f"[WTI] Using numeric heuristic inventory column: {col}")
        return col

    # If we still haven't found anything, bail out with a helpful message
    raise KeyError(
        f"No valid inventory column found in raw WTI data. "
        f"Available columns: {list(df_raw.columns)}"
    )


def normalize_wti(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Converts raw WTI inventory history into canonical format:
        as_of_date | inventory_level | inventory_change | source
    """

    # Try common date names + fuzzy search
    date_cols = ["date", "Date", "as_of_date", "week_ending", "week", "Week"]
    date_col = next((c for c in date_cols if c in df_raw.columns), None)

    if date_col is None:
        # fuzzy heuristic: first column containing 'date' or 'week'
        for col in df_raw.columns:
            lower = col.lower()
            if "date" in lower or "week" in lower:
                date_col = col
                print(f"[WTI] Using heuristic date column: {col}")
                break

    if date_col is None:
        raise KeyError(
            f"No valid date column found in raw WTI data. "
            f"Available columns: {list(df_raw.columns)}"
        )

    inv_col = infer_inventory_column(df_raw)

    df = df_raw[[date_col, inv_col]].copy()
    df.columns = ["as_of_date", "inventory_level"]

    # Normalize types
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df["inventory_level"] = pd.to_numeric(df["inventory_level"], errors="coerce")

    # Sort
    df = df.sort_values("as_of_date").reset_index(drop=True)

    # Add inventory change
    df["inventory_change"] = df["inventory_level"].diff()

    # Add source label
    df["source"] = "EIA"  # adjust if needed

    return df


def write_canonical(df: pd.DataFrame):
    CANONICAL_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(CANONICAL_PATH, index=False)
    print(f"[WTI] Wrote canonical leaf → {CANONICAL_PATH}")


def try_r2_upload():
    if not R2_AVAILABLE:
        print("[WTI] R2 upload skipped: storage_r2 not available.")
        return

    try:
        upload_file_to_r2(CANONICAL_PATH, R2_TARGET_KEY)
        print("[WTI] Uploaded canonical leaf to R2.")
    except Exception as e:
        print(f"[WTI] Warning: R2 upload failed: {e}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
def main():
    print("\n=== Building WTI Inventory Canonical Leaf ===")

    df_raw = load_raw_wti()
    df_canon = normalize_wti(df_raw)
    write_canonical(df_canon)
    try_r2_upload()

    print("=== Done: WTI canonical build ===\n")


if __name__ == "__main__":
    main()

