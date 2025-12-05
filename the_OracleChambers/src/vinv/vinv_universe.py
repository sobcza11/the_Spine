from __future__ import annotations

from pathlib import Path
from typing import Iterable, Set

import pandas as pd


def load_vinv_eligible_tickers(csv_path: str) -> list[str]:
    """
    Load VinV-eligible tickers from a CSV file.

    Expected CSV columns (minimal):
        - ticker
        - vinv_eligible (boolean or 0/1)

    Any rows with vinv_eligible == True (or 1) are returned.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"VinV eligible CSV not found at: {path}")

    df = pd.read_csv(path)
    if "ticker" not in df.columns or "vinv_eligible" not in df.columns:
        raise ValueError("CSV must contain 'ticker' and 'vinv_eligible' columns.")

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["vinv_eligible"] = df["vinv_eligible"].astype(bool)

    eligible = df.loc[df["vinv_eligible"], "ticker"].dropna().unique().tolist()
    return sorted(eligible)


def intersect_with_panel_tickers(
    vinv_tickers: Iterable[str],
    panel_parquet_path: str,
) -> list[str]:
    """
    Intersect VinV-eligible tickers with those actually present in a returns panel.

    Useful when the universe is partial (e.g. A–LEVI only).
    """
    vinv_set: Set[str] = {t.upper() for t in vinv_tickers}

    panel_path = Path(panel_parquet_path)
    df = pd.read_parquet(panel_path, columns=["ticker"])
    panel_tickers = {t.upper() for t in df["ticker"].astype(str).unique()}

    intersected = sorted(vinv_set & panel_tickers)
    return intersected
