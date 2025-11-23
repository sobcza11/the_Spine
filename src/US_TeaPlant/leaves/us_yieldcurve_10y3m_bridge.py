from __future__ import annotations

import pandas as pd

from US_TeaPlant.branches.us_yieldcurve_10y3m_br import get_yc_10y3m_hist


def build_us_yieldcurve_10y3m_bridge() -> pd.DataFrame:
    df = get_yc_10y3m_hist().copy()
    df = df.sort_values("Date").reset_index(drop=True)

    core_cols = [
        "Date",
        "YC_10Y3M",
        "YC_10Y3M_Z",
        "YC_10Y3M_Stress_Flag",
    ]
    return df[core_cols]
