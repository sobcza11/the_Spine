import pandas as pd
from dataclasses import dataclass
from datetime import datetime


@dataclass
class YCConfig:
    min_year: int = 1990
    max_year: int = 2100
    col_date: str = "Date"
    col_10y: str = "y10"
    col_3m: str = "y3m"
    col_2y: str = "y2y"


def normalize_yc_schema(df: pd.DataFrame, cfg: YCConfig) -> pd.DataFrame:
    """
    Normalizes raw YC data.

    Expect raw columns like:
        Date, y10, y3m, y2y
    """
    df = df.copy()
    df[cfg.col_date] = pd.to_datetime(df[cfg.col_date])
    df["year"] = df[cfg.col_date].dt.year
    df["W#"] = df[cfg.col_date].dt.isocalendar().week.astype(int)

    df["yc_10y3m_spread"] = df[cfg.col_10y] - df[cfg.col_3m]
    df["yc_10y2y_spread"] = df[cfg.col_10y] - df[cfg.col_2y]

    return df


def build_yc_spread_leaf(
    df_hist: pd.DataFrame,
    cfg: YCConfig,
    target_year: int,
) -> pd.DataFrame:
    """
    Build YC leaf aligned to weekly Spine calendar:

    index  = yc_date (canonical Wednesday per ISO week)
    cols   = yc_week_num, yc_10y3m_spread, yc_10y2y_spread
    """
    df = normalize_yc_schema(df_hist, cfg)

    df = df[(df["year"] >= cfg.min_year) & (df["year"] <= cfg.max_year)]
    df_year = df[df["year"] == target_year].copy()

    # weekly avg for each spread
    df_week = (
        df_year
        .groupby("W#")[["yc_10y3m_spread", "yc_10y2y_spread"]]
        .mean()
        .reset_index()
        .sort_values("W#")
    )

    # canonical Wednesday for each week
    df_week["yc_date"] = df_week["W#"].apply(
        lambda w: datetime.fromisocalendar(int(target_year), int(w), 3)
    )
    df_week["yc_week_num"] = df_week["W#"].astype(int)

    df_week = df_week.set_index("yc_date").sort_index()

    return df_week[["yc_week_num", "yc_10y3m_spread", "yc_10y2y_spread"]]

