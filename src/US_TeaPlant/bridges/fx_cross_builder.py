from __future__ import annotations

"""
fx_cross_builder.py

Builds cross FX pairs from the Fed-based majors in us_fx_fed_raw.parquet.

Input schema (from fx_fed_bridge.py):

    fx_date        datetime64[ns]
    pair           str      # e.g., "EURUSD", "USDJPY"
    base_ccy       str
    quote_ccy      str
    fx_close       float
    leaf_group     str
    leaf_name      str
    source_system  str
    updated_at     datetime64[ns]

Output schema (for derived crosses):

    fx_date        datetime64[ns]
    pair           str
    base_ccy       str
    quote_ccy      str
    fx_close       float
    leaf_group     = "fx_stream"
    leaf_name      = "fx_cross_from_fed"
    source_system  = "fed_cross"
    updated_at     datetime64[ns]
"""

import pandas as pd


def build_cross_pairs_from_fed(df_fed: pd.DataFrame) -> pd.DataFrame:
    # Ensure datetime
    df = df_fed.copy()
    df["fx_date"] = pd.to_datetime(df["fx_date"])

    # Pivot to wide: one column per pair
    df_wide = df.pivot_table(
        index="fx_date",
        columns="pair",
        values="fx_close",
        aggfunc="last",
    )

    # We will derive crosses only where source pairs exist
    cols = df_wide.columns

    def has(*pairs: str) -> bool:
        return all(p in cols for p in pairs)

    # Container for which crosses we successfully create
    crosses_created = []

    # --- EUR crosses (all per 1 EUR) ---

    # EURGBP = EURUSD / GBPUSD
    if has("EURUSD", "GBPUSD"):
        df_wide["EURGBP"] = df_wide["EURUSD"] / df_wide["GBPUSD"]
        crosses_created.append("EURGBP")

    # EURJPY = EURUSD * USDJPY
    if has("EURUSD", "USDJPY"):
        df_wide["EURJPY"] = df_wide["EURUSD"] * df_wide["USDJPY"]
        crosses_created.append("EURJPY")

    # EURAUD = EURUSD / AUDUSD
    if has("EURUSD", "AUDUSD"):
        df_wide["EURAUD"] = df_wide["EURUSD"] / df_wide["AUDUSD"]
        crosses_created.append("EURAUD")

    # EURNZD = EURUSD / NZDUSD
    if has("EURUSD", "NZDUSD"):
        df_wide["EURNZD"] = df_wide["EURUSD"] / df_wide["NZDUSD"]
        crosses_created.append("EURNZD")

    # EURCAD = EURUSD * USDCAD
    if has("EURUSD", "USDCAD"):
        df_wide["EURCAD"] = df_wide["EURUSD"] * df_wide["USDCAD"]
        crosses_created.append("EURCAD")

    # EURCHF = EURUSD * USDCHF
    if has("EURUSD", "USDCHF"):
        df_wide["EURCHF"] = df_wide["EURUSD"] * df_wide["USDCHF"]
        crosses_created.append("EURCHF")

    # EURNOK = EURUSD * USDNOK
    if has("EURUSD", "USDNOK"):
        df_wide["EURNOK"] = df_wide["EURUSD"] * df_wide["USDNOK"]
        crosses_created.append("EURNOK")

    # EURSEK = EURUSD * USDSEK
    if has("EURUSD", "USDSEK"):
        df_wide["EURSEK"] = df_wide["EURUSD"] * df_wide["USDSEK"]
        crosses_created.append("EURSEK")

    # EURZAR = EURUSD * USDZAR
    if has("EURUSD", "USDZAR"):
        df_wide["EURZAR"] = df_wide["EURUSD"] * df_wide["USDZAR"]
        crosses_created.append("EURZAR")

    # EURBRL = EURUSD * USDBRL
    if has("EURUSD", "USDBRL"):
        df_wide["EURBRL"] = df_wide["EURUSD"] * df_wide["USDBRL"]
        crosses_created.append("EURBRL")

    # --- Non-EUR crosses of interest ---

    # NZDAUD = NZDUSD / AUDUSD
    if has("NZDUSD", "AUDUSD"):
        df_wide["NZDAUD"] = df_wide["NZDUSD"] / df_wide["AUDUSD"]
        crosses_created.append("NZDAUD")

    # AUDCAD = AUDUSD * USDCAD
    if has("AUDUSD", "USDCAD"):
        df_wide["AUDCAD"] = df_wide["AUDUSD"] * df_wide["USDCAD"]
        crosses_created.append("AUDCAD")

    # AUDJPY = AUDUSD * USDJPY
    if has("AUDUSD", "USDJPY"):
        df_wide["AUDJPY"] = df_wide["AUDUSD"] * df_wide["USDJPY"]
        crosses_created.append("AUDJPY")

    if not crosses_created:
        print("[WARN] No crosses created from Fed FX majors.")
        return pd.DataFrame()

    # Stack back to long form for the derived pairs only
    df_cross = df_wide[crosses_created].reset_index().melt(
        id_vars="fx_date", value_name="fx_close", var_name="pair"
    )
    df_cross = df_cross.dropna(subset=["fx_close"])

    # Infer base and quote from pair name (all 3-letter ISO codes)
    df_cross["base_ccy"] = df_cross["pair"].str.slice(0, 3)
    df_cross["quote_ccy"] = df_cross["pair"].str.slice(3, 6)

    df_cross["leaf_group"] = "fx_stream"
    df_cross["leaf_name"] = "fx_cross_from_fed"
    df_cross["source_system"] = "fed_cross"
    df_cross["updated_at"] = pd.Timestamp.utcnow()

    return df_cross

