import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import datetime


@dataclass
class WTIInventoryConfig:
    min_year: int = 1990
    max_year: int = 2100
    lookback_years: int = 5
    excluded_years: tuple = (1997, 2003, 2008, 2014)


def build_wti_index_envelope(
    df_hist: pd.DataFrame,
    cfg: WTIInventoryConfig,
    target_year: int,
) -> pd.DataFrame:
    df = df_hist.copy()

    start_year = max(cfg.min_year, target_year - cfg.lookback_years)
    end_year = min(cfg.max_year, target_year)
    df = df[(df["year"] >= start_year) & (df["year"] <= end_year)]

    if cfg.excluded_years:
        df = df[~df["year"].isin(cfg.excluded_years)]

    df = df[df["W#"].notna()].copy()
    df["W#"] = df["W#"].astype(int)

    df_year_start = (
        df[df["W#"] == 1][["year", "wkly_inv"]]
        .rename(columns={"wkly_inv": "Inv_Yr_Start"})
        .drop_duplicates(subset=["year"])
        .set_index("year")
    )

    df["Inv_Yr_Start"] = df["year"].map(df_year_start["Inv_Yr_Start"])
    df = df[df["Inv_Yr_Start"].notna()].copy()

    df["Index"] = 100.0 * df["wkly_inv"] / df["Inv_Yr_Start"]
    df["Index"] = df["Index"].round(2)

    hist = df[df["year"] < target_year].copy()

    df_env = (
        hist.groupby("W#")["Index"]
        .agg(min="min", avg="mean", max="max")
        .reset_index()
        .sort_values("W#")
    )

    if df_env["W#"].max() == 53:
        df_env = df_env[df_env["W#"] < 53]

    cur = (
        df[df["year"] == target_year][["W#", "Index"]]
        .drop_duplicates(subset=["W#"])
        .sort_values("W#")
        .rename(columns={"Index": "current"})
    )

    if cur["W#"].max() == 53:
        cur = cur[cur["W#"] < 53]

    df_index = df_env.merge(cur, on="W#", how="left")

    df_index = df_index.rename(
        columns={
            "min": "wti_index_min",
            "avg": "wti_index_avg",
            "max": "wti_index_max",
            "current": "wti_index_current",
        }
    )

    df_index["W#"] = df_index["W#"].astype(int)

    return df_index


def build_wti_leaf(
    df_hist: pd.DataFrame,
    cfg: WTIInventoryConfig,
    target_year: int,
) -> pd.DataFrame:
    df_index = build_wti_index_envelope(df_hist, cfg, target_year=target_year)

    df_leaf = df_index.copy()
    df_leaf["wti_date"] = df_leaf["W#"].apply(
        lambda w: datetime.fromisocalendar(int(target_year), int(w), 3)
    )

    df_leaf["wti_week_num"] = df_leaf["W#"].astype(int)
    df_leaf = df_leaf.set_index("wti_date").sort_index()

    return df_leaf[
        [
            "wti_week_num",
            "wti_index_min",
            "wti_index_avg",
            "wti_index_max",
            "wti_index_current",
        ]
    ]

