"""
Time-window utilities for building rolling macro / narrative snapshots.
"""

import pandas as pd
from typing import Optional


def latest_row(df: Optional[pd.DataFrame], date_col: str = "date") -> Optional[pd.Series]:
    if df is None or df.empty or date_col not in df.columns:
        return None
    latest_date = df[date_col].max()
    latest_df = df[df[date_col] == latest_date]
    # if multiple rows, just take the last
    return latest_df.sort_index().iloc[-1]
