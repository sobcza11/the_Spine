from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class RegimeConfig:
    """
    Placeholder configuration for macro regimes.

    In the future, this can be extended with:
        - inflation thresholds
        - liquidity indicators
        - yield curve signals
        - credit spreads, etc.
    """
    name: str
    # For now, just a start/end date range definition
    start: Optional[str] = None
    end: Optional[str] = None


def tag_regimes_by_date_ranges(
    df_vinv_bench: pd.DataFrame,
    regimes: list[RegimeConfig],
    date_col: str = "date",
) -> pd.DataFrame:
    """
    Tag VinV vs benchmark data with simple date-based regime labels.

    Expects df_vinv_bench to have a datetime-like 'date' column or index,
    plus at least vinv_ew_ret and benchmark_ret.

    Returns a copy of df_vinv_bench with an added 'regime' column.
    """
    if date_col in df_vinv_bench.columns:
        df = df_vinv_bench.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col)
    else:
        df = df_vinv_bench.copy()
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df[date_col] = df.index

    df["regime"] = "UNLABELED"

    for reg in regimes:
        if reg.start is None and reg.end is None:
            continue

        start = pd.to_datetime(reg.start) if reg.start is not None else df[date_col].min()
        end = pd.to_datetime(reg.end) if reg.end is not None else df[date_col].max()

        mask = (df[date_col] >= start) & (df[date_col] <= end)
        df.loc[mask, "regime"] = reg.name

    return df

