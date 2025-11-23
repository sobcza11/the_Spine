from __future__ import annotations

import pathlib
from typing import Optional

import numpy as np
import pandas as pd

THIS_FILE = pathlib.Path(__file__).resolve()
ROOT_DIR = THIS_FILE.parents[3]  # the_Spine/
DATA_DIR = ROOT_DIR / "data" / "us" / "yieldcurve_10y3m"
DATA_DIR.mkdir(parents=True, exist_ok=True)
HIST_PATH = DATA_DIR / "df_yieldcurve_10y3m_hist.parquet"

# local raw CSV for now (FRED export)
LOCAL_CSV = ROOT_DIR / "_support" / "data" / "T10Y3M.csv"


def fetch_yc_10y3m_raw(start_date: Optional[str] = None) -> pd.DataFrame:
    if not LOCAL_CSV.exists():
        raise FileNotFoundError(f"Missing raw curve CSV: {LOCAL_CSV}")

    df = pd.read_csv(LOCAL_CSV)

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Accept both FRED standard & your version
    if "DATE" in df.columns:
        date_col = "DATE"
    elif "observation_date" in df.columns:
        date_col = "observation_date"
    else:
        raise ValueError(
            "Expected a 'DATE' or 'observation_date' column in T10Y3M CSV"
        )

    # First non-date column is the value
    value_cols = [c for c in df.columns if c != date_col]
    if not value_cols:
        raise ValueError("No yield column found in T10Y3M CSV")
    value_col = value_cols[0]

    df = df[[date_col, value_col]].rename(
        columns={date_col: "Date", value_col: "YC_10Y3M"}
    )

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["YC_10Y3M"] = pd.to_numeric(df["YC_10Y3M"], errors="coerce")

    df = df.dropna(subset=["Date", "YC_10Y3M"]).sort_values("Date").reset_index(drop=True)

    if start_date is not None:
        df = df[df["Date"] >= pd.to_datetime(start_date)]

    return df


def _calc_yc_10y3m_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values("Date")
    df["YC_10Y3M"] = df["YC_10Y3M"].astype(float)

    # rolling z-score to see how extreme the curve is vs history
    roll_win = 252  # ~1Y of trading days
    roll_mean = df["YC_10Y3M"].rolling(roll_win, min_periods=60).mean()
    roll_std = df["YC_10Y3M"].rolling(roll_win, min_periods=60).std()

    df["YC_10Y3M_Z"] = (df["YC_10Y3M"] - roll_mean) / roll_std

    def _stress(z: float) -> str:
        if np.isnan(z):
            return "neutral"
        if z <= -1.0:
            return "inverted"
        if z >= 1.0:
            return "steep"
        return "neutral"

    df["YC_10Y3M_Stress_Flag"] = df["YC_10Y3M_Z"].apply(_stress)

    return df[["Date", "YC_10Y3M", "YC_10Y3M_Z", "YC_10Y3M_Stress_Flag"]]


def build_yc_10y3m_hist(start_date: str = "1990-01-01") -> pd.DataFrame:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_df = fetch_yc_10y3m_raw(start_date=start_date)
    if raw_df.empty:
        raise ValueError("fetch_yc_10y3m_raw returned empty DataFrame")

    hist_df = _calc_yc_10y3m_features(raw_df)
    hist_df.to_parquet(HIST_PATH, index=False)
    return hist_df


def get_yc_10y3m_hist() -> pd.DataFrame:
    if not HIST_PATH.exists():
        build_yc_10y3m_hist()
    return pd.read_parquet(HIST_PATH)
