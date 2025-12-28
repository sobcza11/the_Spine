"""
Utility helpers for working with ETF benchmarks inside VinV_1_0.
"""

import pandas as pd
from pathlib import Path


def load_benchmarks(root: Path) -> pd.DataFrame:
    path = root / "vinv_benchmarks_monthly.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["ticker"] = df["ticker"].str.upper()
    return df


def list_available_benchmarks(root: Path):
    df = load_benchmarks(root)
    if df.empty:
        return []
    return sorted(df["ticker"].unique())

