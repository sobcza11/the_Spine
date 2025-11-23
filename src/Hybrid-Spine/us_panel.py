from __future__ import annotations
from typing import Optional
import pandas as pd
from Spine.us_uscore_loader import load_us_core_wti_leaf
from Spine.us_uscore_loader import (
    load_us_core_wti_leaf,
    load_us_core_yc_10y3m_leaf,
)


def build_spine_us_panel(as_of: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Build the U.S. leaf panel for the Spine.

    For now:
      - Includes WTI_Inv_STOR leaf
    Later:
      - Add rates, CPI, curve, growth, etc.

    Index:
      Date (release date, e.g. EIA Wednesday)
    """
    # Start with WTI inventory & storage leaf
    df_wti = load_us_core_wti_leaf(as_of=as_of)

    # For now, this *is* the U.S. panel
    df_panel = df_wti.copy()

    # When you add more U.S. leaves later:
    # df_rates = load_us_core_rates_leaf(as_of=as_of)
    # df_panel = df_panel.join(df_rates, how="outer")

    return df_panel


def build_spine_us_panel(as_of: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    df_wti = load_us_core_wti_leaf(as_of=as_of)

    # new yield curve leaf
    df_yc = load_us_core_yc_10y3m_leaf(as_of=as_of)
    
    # start with WTI
    df_panel = df_wti
    
    # join yield curve on Date index
    df_panel = df_panel.join(df_yc, how="outer", rsuffix="_YC")
    return df_panel


