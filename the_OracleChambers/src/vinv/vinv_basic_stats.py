from __future__ import annotations

import numpy as np
import pandas as pd


def basic_outperformance_summary(df: pd.DataFrame) -> dict:
    """
    Compute simple outperformance metrics for VinV vs a benchmark.

    Expects:
        df with columns:
            - vinv_ew_ret
            - benchmark_ret

    Returns:
        dict with:
            - n_months
            - vinv_cum_return
            - benchmark_cum_return
            - pct_months_vinv_outperforms
            - vinv_ann_return
            - bench_ann_return
            - vinv_ann_vol
            - bench_ann_vol
            - vinv_sharpe
            - excess_mean
            - excess_t_stat
    """
    required = {"vinv_ew_ret", "benchmark_ret"}
    if not required <= set(df.columns):
        raise ValueError("DataFrame must contain vinv_ew_ret and benchmark_ret columns.")

    df = df.dropna(subset=["vinv_ew_ret", "benchmark_ret"]).copy()
    if df.empty:
        raise ValueError("No overlapping monthly observations for VinV and benchmark.")

    vinv = df["vinv_ew_ret"]
    bench = df["benchmark_ret"]

    # Cumulative growth: (1+r).cumprod() - 1
    vinv_growth = (1.0 + vinv).cumprod()
    bench_growth = (1.0 + bench).cumprod()

    vinv_cum = float(vinv_growth.iloc[-1] - 1.0)
    bench_cum = float(bench_growth.iloc[-1] - 1.0)

    # Frequency of monthly outperformance
    outperf_mask = vinv > bench
    pct_outperf = float(outperf_mask.mean()) * 100.0

    # Annualized stats (assuming monthly data)
    n_months = len(df)
    periods_per_year = 12.0

    vinv_mean_m = float(vinv.mean())
    vinv_std_m = float(vinv.std(ddof=1))

    bench_mean_m = float(bench.mean())
    bench_std_m = float(bench.std(ddof=1))

    vinv_ann_ret = (1.0 + vinv_mean_m) ** periods_per_year - 1.0
    bench_ann_ret = (1.0 + bench_mean_m) ** periods_per_year - 1.0

    vinv_ann_vol = vinv_std_m * np.sqrt(periods_per_year)
    bench_ann_vol = bench_std_m * np.sqrt(periods_per_year)

    # Simple Sharpe-style metric (no risk-free rate)
    vinv_sharpe = vinv_ann_ret / vinv_ann_vol if vinv_ann_vol > 0 else np.nan

    # Excess return series and t-stat for mean > 0
    excess = vinv - bench
    excess_mean = float(excess.mean())
    excess_std = float(excess.std(ddof=1))
    if excess_std > 0 and n_months > 1:
        excess_t = excess_mean / (excess_std / np.sqrt(n_months))
    else:
        excess_t = np.nan

    return {
        "n_months": int(n_months),
        "vinv_cum_return": vinv_cum,
        "benchmark_cum_return": bench_cum,
        "pct_months_vinv_outperforms": pct_outperf,
        "vinv_ann_return": vinv_ann_ret,
        "bench_ann_return": bench_ann_ret,
        "vinv_ann_vol": vinv_ann_vol,
        "bench_ann_vol": bench_ann_vol,
        "vinv_sharpe": vinv_sharpe,
        "excess_mean": excess_mean,
        "excess_t_stat": excess_t,
    }
