from __future__ import annotations

from typing import Optional
import pandas as pd

# Import from the US_TeaPlant leaf bridge
from US_TeaPlant.trunk.us_core_lvs_bridge import build_us_core_lvs_bridge
from US_TeaPlant.leaves.us_yieldcurve_10y3m_bridge import ( build_us_yieldcurve_10y3m_bridge, )

def load_us_core_wti_leaf(as_of: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Load the core U.S. WTI leaf in a Spine-ready format.
    
    - Pulls from US_TeaPlant leaf bridge
    - Sorts by release date
    - Applies optional point-in-time filtering
    - Adds Spine-facing metadata tags
    """
    df = build_us_core_lvs_bridge().copy()

    # Ensure datetime & sorting
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date")

    # Optional point-in-time (PIT) filter
    if as_of is not None:
        df = df[df["Date"] <= as_of]

    # Spine prefers Date as index
    df = df.set_index("Date")

    # Spine metadata tags
    df["leaf_group"] = "US_CORE"
    df["leaf_name"] = "WTI_INV"

    return df

def load_us_core_yc_10y3m_leaf(as_of: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    df = build_us_yieldcurve_10y3m_bridge().copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date")

    if as_of is not None:
        df = df[df["Date"] <= as_of]

    df = df.set_index("Date")
    df["leaf_group"] = "US_CORE"
    df["leaf_name"] = "YC_10Y3M"
    return df

