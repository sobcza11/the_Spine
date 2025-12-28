"""
VinV A–LEVI Demo Driver

This script wires together the VinV components for the partial universe
(A through LEVI) to produce:

    - Monthly total returns panel
    - VinV equal-weight basket vs SPY
    - Basic outperformance summary
    - Per-ticker outperformance ranking
    - Simple COVID crisis window stats
"""

from pathlib import Path

import pandas as pd

from src.vinv.vinv_monthly import build_monthly_total_returns
from src.vinv.vinv_basket import build_vinv_equal_weight_series, align_with_benchmark
from src.vinv.vinv_basic_stats import basic_outperformance_summary
from src.vinv.vinv_per_ticker_stats import per_ticker_outperformance_stats
from src.vinv.vinv_crisis_windows import crisis_window_stats
from src.vinv.vinv_universe import load_vinv_eligible_tickers, intersect_with_panel_tickers

import sys

# Ensure project root (the_Spine) is on sys.path so `src` can be imported
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# -----------------------------
# CONFIG
# -----------------------------

DAILY_PANEL_PATH = "data/vinv_panel_A_LEVI_daily.parquet"
MONTHLY_PANEL_PATH = "data/vinv_panel_A_LEVI_monthly.parquet"

VINV_ELIGIBLE_CSV = "data/vinv_eligible_A_LEVI.csv"

BENCHMARK_TICKER = "SPY"

COVID_START = "2020-02-01"
COVID_END = "2021-03-31"


def main() -> None:
    print(">>> Step 1: Build monthly panel...")
    build_monthly_total_returns(
        daily_parquet_path=DAILY_PANEL_PATH,
        output_parquet_path=MONTHLY_PANEL_PATH,
        universe_tickers=None,
    )

    # Load VinV eligible tickers from CSV
    vinv_all = load_vinv_eligible_tickers(VINV_ELIGIBLE_CSV)

    # Intersect with panel
    vinv_tickers = intersect_with_panel_tickers(vinv_all, MONTHLY_PANEL_PATH)
    print(f">>> VinV tickers found in A–LEVI panel: {vinv_tickers}")

    if not vinv_tickers:
        raise ValueError("No VinV-eligible tickers found in A–LEVI panel.")

    print(">>> Step 2: Construct VinV equal-weight factor...")
    vinv_ew = build_vinv_equal_weight_series(
        monthly_parquet_path=MONTHLY_PANEL_PATH,
        vinv_tickers=vinv_tickers,
    )

    print(f">>> Step 3: Align VinV vs benchmark {BENCHMARK_TICKER}...")
    df_vinv_bench, bench = align_with_benchmark(
        vinv_ew, MONTHLY_PANEL_PATH, BENCHMARK_TICKER
    )

    print(">>> Step 4: Summary metrics...")
    summary = basic_outperformance_summary(df_vinv_bench)
    print(summary)

    print(">>> Step 5: Per-ticker stats...")
    df_stats = per_ticker_outperformance_stats(
        monthly_parquet_path=MONTHLY_PANEL_PATH,
        vinv_tickers=vinv_tickers,
        benchmark_ticker=BENCHMARK_TICKER,
    )
    print(df_stats.head(15))

    print(">>> Step 6: COVID crisis slice...")
    crisis = crisis_window_stats(
        df_vinv_bench, COVID_START, COVID_END, "COVID"
    )
    print(crisis)

    print(">>> VinV A–LEVI demo: COMPLETE.")


if __name__ == "__main__":
    main()
