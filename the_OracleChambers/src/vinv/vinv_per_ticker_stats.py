from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


def per_ticker_outperformance_stats(
    monthly_parquet_path: str,
    vinv_tickers: Iterable[str],
    benchmark_ticker: str,
) -> pd.DataFrame:
    """
    Compute simple per-ticker outperformance stats vs a benchmark.

    Input parquet schema (same as vinv_monthly.build_monthly_total_returns output):
        - date
        - ticker
        - total_return

    Parameters
    ----------
    monthly_parquet_path : str
        Path to the monthly total-returns parquet.
    vinv_tickers : Iterable[str]
        Tickers you consider VinV-eligible (A–LEVI subset).
    benchmark_ticker : str
        Ticker of the benchmark (e.g. 'SPY' or 'IWD') present in the same panel.

    Returns
    -------
    pd.DataFrame
        Columns:
            - ticker
            - n_months
            - cum_return
            - bench_cum_return
            - pct_months_outperform
    """
    vinv_tickers = {t.upper() for t in vinv_tickers}
    bench = benchmark_ticker.upper()

    df = pd.read_parquet(Path(monthly_parquet_path))
    df["ticker"] = df["ticker"].str.upper()
    df["date"] = pd.to_datetime(df["date"])

    # Split benchmark vs universe
    df_bench = df[df["ticker"] == bench].copy()
    if df_bench.empty:
        raise ValueError(f"Benchmark {bench} not found in monthly panel.")

    df_bench = df_bench.set_index("date").sort_index()
    df_bench = df_bench[["total_return"]].rename(columns={"total_return": "benchmark_ret"})

    results = []

    for t in sorted(vinv_tickers):
        df_t = df[df["ticker"] == t].copy()
        if df_t.empty:
            # Skip if we don't have data for this ticker yet
            continue

        df_t = df_t.set_index("date").sort_index()
        df_join = df_t.join(df_bench, how="inner")
        df_join = df_join.dropna(subset=["total_return", "benchmark_ret"])

        if df_join.empty:
            continue

        # Cumulative returns
        growth_t = (1.0 + df_join["total_return"]).cumprod()
        growth_b = (1.0 + df_join["benchmark_ret"]).cumprod()

        cum_t = float(growth_t.iloc[-1] - 1.0)
        cum_b = float(growth_b.iloc[-1] - 1.0)

        # Frequency of monthly outperformance
        out_mask = df_join["total_return"] > df_join["benchmark_ret"]
        pct_out = float(out_mask.mean()) * 100.0

        results.append(
            {
                "ticker": t,
                "n_months": int(len(df_join)),
                "cum_return": cum_t,
                "bench_cum_return": cum_b,
                "pct_months_outperform": pct_out,
            }
        )

    if not results:
        return pd.DataFrame(
            columns=[
                "ticker",
                "n_months",
                "cum_return",
                "bench_cum_return",
                "pct_months_outperform",
            ]
        )

    df_stats = pd.DataFrame(results).sort_values(
        by=["pct_months_outperform", "cum_return"], ascending=[False, False]
    )
    return df_stats.reset_index(drop=True)
