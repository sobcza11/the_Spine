"""
Data processing utilities for COT Engine Lite.

These match the function names used in your Streamlit app, but keep the logic simple
so the engine can run even before you plug in richer calculations.
"""

from __future__ import annotations

import pandas as pd


def calculate_net_positions(positions: pd.DataFrame | None) -> pd.DataFrame | None:
    """
    Given a positions DataFrame, compute net positions by category.

    Expected input structure (from your real parser):
        - 'Category'
        - 'Long'
        - 'Short'

    Minimal implementation:
        - If positions is None, return None.
        - If present, add 'Net' = Long - Short.
    """
    if positions is None or positions.empty:
        return None

    df = positions.copy()
    if "Long" in df.columns and "Short" in df.columns:
        df["Net"] = df["Long"].astype(float) - df["Short"].astype(float)
    return df


def calculate_market_composition(
    positions: pd.DataFrame | None, debug: bool = False
) -> pd.DataFrame | None:
    """
    Compute long/short/net for each trader category & their shares of open interest.

    Expected input:
        positions with columns:
            - 'Category'
            - 'Open Interest'
            - 'Long'
            - 'Short'

    Minimal implementation:
        - Returns positions + 'Net' (if possible).
        - Does not compute percentages unless 'Open Interest' is available.
    """
    if positions is None or positions.empty:
        if debug:
            print("[calculate_market_composition] No positions provided.")
        return None

    df = positions.copy()

    # Net
    if "Long" in df.columns and "Short" in df.columns:
        df["Net"] = df["Long"].astype(float) - df["Short"].astype(float)

    # Percent of open interest (if possible)
    if "Open Interest" in df.columns:
        oi = df["Open Interest"].astype(float)
        with pd.option_context("mode.use_inf_as_na", True):
            df["Long %"] = (df["Long"].astype(float) / oi) * 100.0
            df["Short %"] = (df["Short"].astype(float) / oi) * 100.0

    if debug:
        print("[calculate_market_composition] Returning", len(df), "rows")

    return df

