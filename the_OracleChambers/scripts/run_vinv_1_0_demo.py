"""
VinV_1.0 demo runner

Step 1: build monthly panels & portfolio
Step 2: compute yearly stats & compare VinV vs ETF benchmarks (DVY, IVV, IWD, etc.)
         with comparisons displayed from 1990 onward.

This version is flexible to 'date' or 'month' as the time column.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

# -----------------------------
# Make src/ importable
# -----------------------------
THIS_DIR = Path(__file__).resolve().parent      # .../the_OracleChambers/scripts
REPO_ROOT = THIS_DIR.parent                     # .../the_OracleChambers
SRC_DIR = REPO_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from vinv import vinv_monthly  # src/vinv/vinv_monthly.py


# -----------------------------
# Helpers to locate VinV_1_0
# -----------------------------

def find_vinv_root() -> Path:
    """
    Locate the VinV_1_0 root directory relative to this repo.

    We avoid hard-coding paths by searching for a folder named 'VinV_1_0'
    that contains the expected parquet artifacts.
    """
    repo_root = REPO_ROOT

    candidates = list(repo_root.rglob("VinV_1_0"))
    for cand in candidates:
        panel = cand / "vinv_monthly_panel.parquet"
        port = cand / "vinv_portfolio_monthly.parquet"
        bench = cand / "vinv_benchmarks_monthly.parquet"
        if panel.exists() and port.exists() and bench.exists():
            return cand

    raise FileNotFoundError(
        "Could not find VinV_1_0 root containing vinv_monthly_panel.parquet, "
        "vinv_portfolio_monthly.parquet & vinv_benchmarks_monthly.parquet."
    )


# -----------------------------
# Yearly VinV computation
# -----------------------------

def _get_time_col(df: pd.DataFrame, path: Path) -> str:
    """
    Pick the time column: prefer 'date', fallback to 'month'.
    """
    cols = set(df.columns)
    for cand in ("date", "month"):
        if cand in cols:
            return cand
    raise KeyError(
        f"No 'date' or 'month' column found in {path}. Columns: {df.columns}"
    )


def load_vinv_annual(root: Path) -> Tuple[pd.DataFrame, float]:
    """
    Load vinv_portfolio_monthly.parquet & compute:
      - yearly VinV returns (DataFrame with 'VinV_annual_%')
      - monthly VinV Sharpe (rf = 0, annualized)

    Assumes there is one numeric return column (e.g., 'vinv_monthly_ret').
    """
    path = root / "vinv_portfolio_monthly.parquet"
    df = pd.read_parquet(path)

    # Normalize column names
    df.columns = [str(c).lower() for c in df.columns]

    # Time column: 'date' or 'month'
    time_col = _get_time_col(df, path)
    df[time_col] = pd.to_datetime(df[time_col])

    # Use first numeric column as portfolio return
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        raise ValueError(f"No numeric return columns found in {path}.")
    ret_col = num_cols[0]

    r = df[ret_col].astype(float)

    # Monthly Sharpe (rf = 0), annualized
    mu = r.mean()
    sigma = r.std()
    sharpe_monthly_rf0 = float("nan") if sigma == 0 else (mu / sigma) * math.sqrt(12.0)

    # Yearly aggregation
    df["year"] = df[time_col].dt.year
    grouped = (1.0 + r).groupby(df["year"]).prod() - 1.0
    vinv_annual = pd.DataFrame({"VinV_annual_%": grouped * 100.0})
    vinv_annual.index.name = "year"

    return vinv_annual, sharpe_monthly_rf0


# -----------------------------
# Yearly benchmark computation
# -----------------------------

def load_benchmarks_yearly(root: Path) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Load vinv_benchmarks_monthly.parquet & compute:

      - summary table:
            ticker | annual_ret_% (full sample) | sharpe_monthly_rf0

      - per-ticker yearly returns (dict of DataFrames indexed by year)
    """
    path = root / "vinv_benchmarks_monthly.parquet"
    df = pd.read_parquet(path)
    df.columns = [str(c).lower() for c in df.columns]

    if "ticker" not in df.columns:
        raise KeyError(f"'ticker' missing in {path}. Columns: {df.columns}")

    # Time column: 'date' or 'month'
    time_col = _get_time_col(df, path)
    df[time_col] = pd.to_datetime(df[time_col])

    # Numeric return column
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        raise ValueError(f"No numeric return columns found in {path}.")
    ret_col = num_cols[0]

    df["year"] = df[time_col].dt.year

    summary_rows = []
    yearly_by_ticker: Dict[str, pd.DataFrame] = {}

    for ticker, grp in df.groupby("ticker"):
        r = grp[ret_col].astype(float)

        mu = r.mean()
        sigma = r.std()
        sharpe = float("nan") if sigma == 0 else (mu / sigma) * math.sqrt(12.0)

        ann_ret_by_year = (1.0 + r).groupby(grp["year"]).prod() - 1.0
        full_ret_pct = ((1.0 + ann_ret_by_year).prod() - 1.0) * 100.0

        summary_rows.append(
            {
                "ticker": ticker,
                "annual_ret_% (full sample)": full_ret_pct,
                "sharpe_monthly_rf0": sharpe,
            }
        )

        yearly_df = pd.DataFrame({ticker: ann_ret_by_year * 100.0})
        yearly_df.index.name = "year"
        yearly_by_ticker[ticker] = yearly_df

    summary = pd.DataFrame(summary_rows)
    summary = summary.sort_values("ticker").reset_index(drop=True)

    return summary, yearly_by_ticker


# -----------------------------
# Combine VinV & benchmark
# -----------------------------

def combine_vinv_and_bench(
    vinv_annual: pd.DataFrame, bench_annual: pd.DataFrame
) -> pd.DataFrame:
    """
    Combine VinV annual returns & one benchmark's annual returns on overlapping years.
    Returns DataFrame with columns: VinV, <ticker>, excess
    """
    ticker = bench_annual.columns[0]

    merged = vinv_annual.join(bench_annual, how="inner")
    merged = merged.rename(columns={"VinV_annual_%": "VinV"})
    merged["excess"] = merged["VinV"] - merged[ticker]

    return merged[["VinV", ticker, "excess"]]


# -----------------------------
# Main demo runner
# -----------------------------

def main() -> None:
    # Step 1: (re)build monthly panel & portfolio
    print("[VINV_1_0 DEMO] Step 1/2: monthly panel & portfolio build")
    vinv_monthly.main()

    # Step 2: yearly stats & benchmark comparison
    print("\n[VINV_1_0 DEMO] Step 2/2: yearly stats & benchmark comparison")

    root = find_vinv_root()

    # VinV yearly
    vinv_annual, vinv_sharpe_m = load_vinv_annual(root)

    print("========== VinV_1.0 Yearly Summary ==========")
    print("VinV annual returns:")
    print(vinv_annual)
    print(f"\nVinV monthly Sharpe (rf=0): {vinv_sharpe_m:.2f}\n")

    # Benchmarks
    bench_summary, yearly_by_ticker = load_benchmarks_yearly(root)

    print("========== Benchmark Comparison ==========")
    print(bench_summary.to_string(index=False))

    print("\n========== VinV vs Benchmarks (Overlapping Years, 1990+) ==========\n")

    # Filter VinV to 1990+ for presentation
    vinv_1990 = vinv_annual.loc[vinv_annual.index >= 1990]

    for ticker, df_bench in yearly_by_ticker.items():
        merged = combine_vinv_and_bench(vinv_1990, df_bench)
        merged = merged.loc[merged.index >= 1990]

        print(f"--- {ticker} (1990+) ---")
        print(merged)
        print()  # blank line between tables


if __name__ == "__main__":
    main()



