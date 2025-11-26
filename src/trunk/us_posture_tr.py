# the_Spine/src/_Trunk/us_posture_tr.py

from __future__ import annotations

import pathlib
from datetime import datetime

import pandas as pd

from US_TeaPlant.trunk.us_core_lvs_bridge import build_us_core_lvs_bridge

DATA_DIR = pathlib.Path("data/us/posture")
HIST_PATH = DATA_DIR / "df_us_posture_hist.parquet"  # df_ prefix


def _classify_posture_row(row: pd.Series) -> str:
    """
    First simple posture rule based only on WTI storage.

    Uses:
      WTI_STOR_Stress_Flag : tight / neutral / loose
      WTI_STOR_Sprd_Idx    : 0–100
    """
    flag = row.get("WTI_STOR_Stress_Flag", "neutral")
    sprd = float(row.get("WTI_STOR_Sprd_Idx", 50.0))

    if flag == "tight" and sprd >= 60:
        return "US_POSTURE_CAUTION_ENERGY_TIGHT"

    if flag == "loose" and sprd >= 60:
        return "US_POSTURE_EASING_ENERGY_LOOSE"

    return "US_POSTURE_NEUTRAL"


def build_us_posture_hist_tr() -> pd.DataFrame:
    """
    Build (or rebuild) full US posture history from US core leaves.

    Input:
      - us_core_lvs_bridge (WTI_Inv_STOR leaves)

    Output parquet:
      data/us/posture/df_us_posture_hist.parquet
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    df_lvs = build_us_core_lvs_bridge().copy()
    df_lvs = df_lvs.sort_values("Date").reset_index(drop=True)

    df_lvs["US_Posture"] = df_lvs.apply(_classify_posture_row, axis=1)

    df_post = df_lvs[
        [
            "Date",
            "US_Posture",
            "WTI_STOR_Sprd_Idx",
            "WTI_STOR_Stress_Flag",
            "WTI_INV_Surplus",
            "WTI_INV_Std_Dev_Position",
            "WTI_Seas_INV_Idx",
        ]
    ].copy()

    df_post.to_parquet(HIST_PATH, index=False)
    return df_post


def get_us_posture_hist_tr() -> pd.DataFrame:
    """
    Accessor for _Trunk or dashboards.
    """
    if not HIST_PATH.exists():
        return build_us_posture_hist_tr()
    return pd.read_parquet(HIST_PATH)


def get_latest_us_posture_tr() -> dict:
    """
    Convenience: latest posture as a small dict.
    """
    df_post = get_us_posture_hist_tr().sort_values("Date")
    last = df_post.iloc[-1].to_dict()
    last["as_of_utc"] = datetime.utcnow().isoformat()
    return last


