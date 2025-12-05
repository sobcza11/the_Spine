from __future__ import annotations

import numpy as np
import pandas as pd


def basic_series_stats(ret: pd.Series) -> dict:
    """
    Basic stats for a single return series (e.g. VinV basket).

    Assumes monthly frequency.
    """
    ret = ret.dropna()
    if ret.empty:
        raise ValueError("Return series is empty.")

    n = len(ret)
    periods_per_year = 12.0

    mean_m = float(ret.mean())
    std_m = float(ret.std(ddof=1))

    ann_ret = (1.0 + mean_m) ** periods_per_year - 1.0
    ann_vol = std_m * np.sqrt(periods_per_year) if std_m > 0 else np.nan
    sharpe = ann_ret / ann_vol if ann_vol and ann_vol > 0 else np.nan

    growth = (1.0 + ret).cumprod()
    cum_ret = float(growth.iloc[-1] - 1.0)

    return {
        "n_months": int(n),
        "cum_return": cum_ret,
        "ann_return": ann_ret,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
    }


def basic_pair_summary(vinv: pd.Series, bench: pd.Series) -> dict:
    """
    Simple VinV vs benchmark summary, assuming aligned monthly data.
    """
    df = pd.concat([vinv.rename("vinv"), bench.rename("bench")], axis=1).dropna()
    if df.empty:
        raise ValueError("No overlapping data for VinV and benchmark.")

    vinv = df["vinv"]
    bench = df["bench"]

    vinv_stats = basic_series_stats(vinv)
    bench_stats = basic_series_stats(bench)

    excess = vinv - bench
    n = len(excess)
    mean_excess = float(excess.mean())
    std_excess = float(excess.std(ddof=1))
    t_stat = (
        mean_excess / (std_excess / np.sqrt(n))
        if std_excess > 0 and n > 1
        else np.nan
    )

    pct_outperf = float((vinv > bench).mean()) * 100.0

    return {
        "vinv": vinv_stats,
        "bench": bench_stats,
        "pct_months_outperf": pct_outperf,
        "mean_excess": mean_excess,
        "excess_t_stat": t_stat,
    }
