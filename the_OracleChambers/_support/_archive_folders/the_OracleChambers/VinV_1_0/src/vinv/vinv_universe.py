from __future__ import annotations

from pathlib import Path
from typing import Iterable, Set

import pandas as pd


def load_vinv_eligible_tickers(csv_path: str) -> list[str]:
    """
    Load VinV-eligible tickers from a CSV file.

    Flexible behavior:
        - If CSV has 'ticker' and 'vinv_eligible':
              use rows where vinv_eligible is True/1.
        - If CSV has only 'ticker' (no vinv_eligible):
              treat all listed tickers as eligible.
        - If CSV has 'symbol' but not 'ticker':
              rename 'symbol' -> 'ticker'.

    This is designed to work with the existing vinv_universe.csv
    without forcing a specific schema upfront.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"VinV eligible CSV not found at: {path}")

    df = pd.read_csv(path)

    # Normalize ticker column name
    if "ticker" not in df.columns:
        if "symbol" in df.columns:
            df = df.rename(columns={"symbol": "ticker"})
        elif "Ticker" in df.columns:
            df = df.rename(columns={"Ticker": "ticker"})
        else:
            raise ValueError(
                "CSV must contain a 'ticker' (or 'symbol' / 'Ticker') column. "
                f"Found columns: {list(df.columns)}"
            )

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    # If vinv_eligible column exists, use it
    if "vinv_eligible" in df.columns:
        df["vinv_eligible"] = df["vinv_eligible"].astype(bool)
        eligible = df.loc[df["vinv_eligible"], "ticker"].dropna().unique().tolist()
    else:
        # Otherwise, treat all tickers as eligible
        eligible = df["ticker"].dropna().unique().tolist()

    return sorted(eligible)


def intersect_with_panel_tickers(
    vinv_tickers: Iterable[str],
    panel_parquet_path: str,
) -> list[str]:
    """
    Intersect VinV-eligible tickers with those actually present in a returns panel.
    """
    vinv_set: Set[str] = {t.upper() for t in vinv_tickers}

    panel_path = Path(panel_parquet_path)
    df = pd.read_parquet(panel_path, columns=["ticker"])
    panel_tickers = {t.upper() for t in df["ticker"].astype(str).unique()}

    intersected = sorted(vinv_set & panel_tickers)
    return intersected

