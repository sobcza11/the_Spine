from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class CrisisWindowResult:
    name: str
    start: pd.Timestamp
    end: pd.Timestamp
    vinv_cum_return: float
    bench_cum_return: float
    vinv_max_dd: float
    bench_max_dd: float
    n_months: int


def _cum_and_max_dd(returns: pd.Series) -> tuple[float, float]:
    """
    Helper: cumulative return and max drawdown from a monthly return series.
    """
    if returns.empty:
        return float("nan"), float("nan")

    growth = (1.0 + returns).cumprod()
    # Cumulative return
    cum_ret = float(growth.iloc[-1] - 1.0)
    # Max drawdown
    running_max = growth.cummax()
    drawdowns = growth / running_max - 1.0
    max_dd = float(drawdowns.min())
    return cum_ret, max_dd


def crisis_window_stats(
    df_vinv_bench: pd.DataFrame,
    start: str,
    end: str,
    window_name: Optional[str] = None,
) -> CrisisWindowResult:
    """
    Compute simple crisis-window stats for VinV vs benchmark.

    Expects df_vinv_bench with:
        - index as date (or a 'date' column)
        - columns: vinv_ew_ret, benchmark_ret

    Parameters
    ----------
    df_vinv_bench : pd.DataFrame
        Monthly VinV vs benchmark returns.
    start : str
        Start date (e.g. '2020-02-01').
    end : str
        End date (e.g. '2021-03-31').
    window_name : Optional[str]
        Human-friendly label (e.g. 'COVID', 'GFC').

    Returns
    -------
    CrisisWindowResult
    """
    if "date" in df_vinv_bench.columns:
        df = df_vinv_bench.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
    else:
        df = df_vinv_bench.copy()
        df.index = pd.to_datetime(df.index)

    start_ts = pd.to_datetime(start)
    end_ts = pd.to_datetime(end)

    df_w = df.loc[(df.index >= start_ts) & (df.index <= end_ts)].copy()
    df_w = df_w.dropna(subset=["vinv_ew_ret", "benchmark_ret"])

    name = window_name or f"{start_ts.date()} to {end_ts.date()}"

    vinv_cum, vinv_dd = _cum_and_max_dd(df_w["vinv_ew_ret"])
    bench_cum, bench_dd = _cum_and_max_dd(df_w["benchmark_ret"])

    return CrisisWindowResult(
        name=name,
        start=start_ts,
        end=end_ts,
        vinv_cum_return=vinv_cum,
        bench_cum_return=bench_cum,
        vinv_max_dd=vinv_dd,
        bench_max_dd=bench_dd,
        n_months=int(len(df_w)),
    )
