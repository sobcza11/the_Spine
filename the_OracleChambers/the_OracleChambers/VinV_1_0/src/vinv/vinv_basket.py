from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


def build_vinv_equal_weight_series(
    monthly_parquet_path: str,
    vinv_tickers: Iterable[str],
) -> pd.Series:
    """
    Construct an equal-weight VinV return series from a monthly panel.

    Input parquet schema (from vinv_monthly.build_monthly_total_returns):
        - date
        - ticker
        - total_return

    Returns:
        pandas Series indexed by date with the equal-weight VinV monthly returns.
    """
    vinv_tickers = {t.upper() for t in vinv_tickers}

    df = pd.read_parquet(Path(monthly_parquet_path))
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"])

    # Filter to VinV tickers only
    df = df[df["ticker"].isin(vinv_tickers)]
    if df.empty:
        raise ValueError("No rows for provided VinV tickers in monthly panel.")

    # Pivot: rows = date, columns = ticker, values = total_return
    pivot = df.pivot(index="date", columns="ticker", values="total_return").sort_index()

    # Equal-weight across available tickers each month
    vinv_ew = pivot.mean(axis=1, skipna=True)
    vinv_ew.name = "vinv_ew_ret"

    return vinv_ew


def try_get_benchmark_series(
    monthly_parquet_path: str,
    benchmark_ticker: str,
) -> pd.Series | None:
    """
    Try to extract a benchmark return series (e.g. SPY) from the same monthly panel.

    Returns a pandas Series indexed by date with name 'benchmark_ret',
    or None if the benchmark ticker is not present.
    """
    df = pd.read_parquet(Path(monthly_parquet_path))
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"])

    bench = benchmark_ticker.upper()
    df_bench = df[df["ticker"] == bench].copy()

    if df_bench.empty:
        return None

    df_bench = df_bench.sort_values("date").set_index("date")
    s = df_bench["total_return"].copy()
    s.name = "benchmark_ret"
    return s


