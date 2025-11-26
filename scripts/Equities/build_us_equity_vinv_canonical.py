"""
build_us_equity_vinv_canonical.py
---------------------------------
Canonical builder for the VinV (Value in Vogue) equity factor leaf.

This script takes a pre-computed VinV time series (from a notebook, WRDS
query, or another upstream pipeline) and converts it into a canonical
Spine-US leaf:

    data/spine_us/us_equity_vinv_canonical.parquet

Canonical schema:
    as_of_date          datetime64[ns]
    vinv_spread_val     float
    vinv_spread_ret_12m float
    vinv_breadth        float (0–1)
    vinv_score          float (approx [-1, +1])
    vinv_regime         str  ("out_of_favor", "transition", "in_vogue")

Upstream raw input (current expectation):
    data/spine_us/raw/us_equity_vinv_raw.parquet

You can initially export this raw file from your existing VinV notebook,
and later replace that step with a WRDS-based query.
"""

from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "spine_us" / "raw"
RAW_PATH = RAW_DIR / "us_equity_vinv_raw.parquet"

CANONICAL_DIR = BASE_DIR / "data" / "spine_us"
CANONICAL_PATH = CANONICAL_DIR / "us_equity_vinv_canonical.parquet"
VINV_PATH = BASE_DIR / "data" / "spine_us" / "us_equity_vinv_canonical.parquet"


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def load_vinv() -> pd.DataFrame:
    """
    Loads VinV canonical leaf.

    Returns empty DataFrame if the file is not present so the macro
    panel build remains non-blocking.
    """
    if not VINV_PATH.exists():
        print(f"[MACRO] VinV canonical leaf not found at {VINV_PATH}, skipping.")
        return pd.DataFrame()

    df = pd.read_parquet(VINV_PATH)

    if "as_of_date" not in df.columns:
        print("[MACRO] VinV leaf missing 'as_of_date'; skipping VinV merge.")
        return pd.DataFrame()

    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df = df.sort_values("as_of_date").reset_index(drop=True)
    print(f"[MACRO] Loaded VinV leaf with {df.shape[0]} rows.")
    return df

def load_raw_vinv() -> pd.DataFrame:
    """
    Loads the raw VinV time series.

    For now, this expects a parquet file exported from your notebook:
        data/spine_us/raw/us_equity_vinv_raw.parquet

    Expected to contain:
        - as_of_date
        - some valuation spread column
        - some return spread column
        - some breadth column

    Column names are mapped heuristically below.
    """

    if not RAW_PATH.exists():
        raise FileNotFoundError(
            f"Raw VinV file not found at {RAW_PATH}. "
            "Export a raw VinV panel from your notebook or upstream pipeline first."
        )

    print(f"[VinV] Loading raw VinV panel from: {RAW_PATH}")
    df = pd.read_parquet(RAW_PATH)
    return df


def infer_column(df: pd.DataFrame, candidates, fallback_msg: str) -> str:
    """
    Given a DataFrame and a list of candidate column names, return the
    first one found. If none found, raise a helpful error.
    """
    for c in candidates:
        if c in df.columns:
            print(f"[VinV] Using column '{c}' for {fallback_msg}")
            return c

    raise KeyError(
        f"No suitable column found for {fallback_msg}. "
        f"Expected one of: {candidates}. "
        f"Available columns: {list(df.columns)}"
    )


def normalize_vinv(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Converts raw VinV time series into canonical format:
        as_of_date | vinv_spread_val | vinv_spread_ret_12m |
        vinv_breadth | vinv_score | vinv_regime
    """

    # --- date column ---
    date_candidates = ["as_of_date", "date", "Date", "month_end"]
    date_col = infer_column(df_raw, date_candidates, "as_of_date (VinV date index)")

    # --- valuation spread column ---
    val_candidates = [
        "valuation_spread",
        "bm_spread",
        "value_spread",
        "vinv_spread_val",
    ]
    val_col = infer_column(df_raw, val_candidates, "valuation spread (value vs market/growth)")

    # --- return spread column (12m) ---
    ret_candidates = [
        "ret_12m_spread",
        "excess_ret_12m",
        "vinv_spread_ret_12m",
        "perf_spread_12m",
    ]
    ret_col = infer_column(df_raw, ret_candidates, "12m return spread (value vs market/growth)")

    # --- breadth column ---
    breadth_candidates = [
        "breadth",
        "pct_outperform",
        "vinv_breadth",
    ]
    breadth_col = infer_column(
        df_raw,
        breadth_candidates,
        "VinV breadth (fraction of cohort outperforming benchmark)",
    )

    df = df_raw[[date_col, val_col, ret_col, breadth_col]].copy()
    df.columns = ["as_of_date", "vinv_spread_val", "vinv_spread_ret_12m", "vinv_breadth"]

    # Types
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df["vinv_spread_val"] = pd.to_numeric(df["vinv_spread_val"], errors="coerce")
    df["vinv_spread_ret_12m"] = pd.to_numeric(df["vinv_spread_ret_12m"], errors="coerce")
    df["vinv_breadth"] = pd.to_numeric(df["vinv_breadth"], errors="coerce")

    # If breadth is in percent (e.g. 0–100), scale to 0–1
    if df["vinv_breadth"].max() > 1.5:
        print("[VinV] Detected breadth likely in percent; scaling by 100.")
        df["vinv_breadth"] = df["vinv_breadth"] / 100.0

    # Sort & drop duplicates
    df = df.sort_values("as_of_date").drop_duplicates("as_of_date").reset_index(drop=True)

    # Compute VinV score & regime label
    df["vinv_score"], df["vinv_regime"] = compute_vinv_score_and_regime(
        df["vinv_spread_val"], df["vinv_spread_ret_12m"], df["vinv_breadth"]
    )

    return df


def compute_vinv_score_and_regime(
    val_spread: pd.Series,
    ret_spread: pd.Series,
    breadth: pd.Series,
) -> Tuple[pd.Series, pd.Series]:
    """
    Computes a composite VinV score and regime label.

    This is intentionally simple & transparent so you can refine later.

    Approach:
        - Standardize each component (z-scores, clipped)
        - Average the three z-scores to get vinv_score
        - Map vinv_score into discrete regimes
    """

    def zclip(s: pd.Series) -> pd.Series:
        z = (s - s.mean()) / (s.std(ddof=1) if s.std(ddof=1) != 0 else 1.0)
        return z.clip(-3, 3)

    z_val = zclip(val_spread.fillna(val_spread.median()))
    z_ret = zclip(ret_spread.fillna(ret_spread.median()))
    z_breadth = zclip(breadth.fillna(breadth.median()))

    vinv_score = (z_val + z_ret + z_breadth) / 3.0
    # Normalize roughly to [-1, 1] for interpretability
    vinv_score = (vinv_score / vinv_score.abs().max()).fillna(0.0)

    # Regime thresholds (tune later as needed)
    def label_regime(x: float) -> str:
        if x <= -0.33:
            return "out_of_favor"
        elif x >= 0.33:
            return "in_vogue"
        else:
            return "transition"

    vinv_regime = vinv_score.apply(label_regime)

    return vinv_score, vinv_regime


def write_canonical(df: pd.DataFrame) -> None:
    CANONICAL_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(CANONICAL_PATH, index=False)
    print(f"[VinV] Wrote canonical VinV leaf → {CANONICAL_PATH}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
def main():
    print("\n=== Building VinV Canonical Leaf (us_equity_vinv_canonical) ===")

    df_raw = load_raw_vinv()
    df_canon = normalize_vinv(df_raw)
    write_canonical(df_canon)

    print("=== Done: VinV canonical build ===\n")

if __name__ == "__main__":
    main()