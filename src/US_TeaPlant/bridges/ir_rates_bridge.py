"""
US_TeaPlant.bridges.ir_rates_bridge

Builds a canonical interest-rate yields leaf across currencies,
using only free, official public sources (FRED, ECB, BoE, BoC, etc.).

Schema (canonical yields leaf):

    as_of_date      datetime64[ns]
    ccy             str          # USD, EUR, JPY, GBP, ...
    tenor           str          # e.g. 'POLICY', '2Y', '10Y'
    rate_value      float        # in percent or yield, consistent per series
    source_system   str
    updated_at      datetime64[ns]

R2 key:

    spine_us/us_ir_yields_canonical.parquet
"""

from __future__ import annotations

import datetime as dt
from typing import Iterable, List

import pandas as pd

from common.r2_client import write_parquet_to_r2

R2_IR_YIELDS_KEY = "spine_us/us_ir_yields_canonical.parquet"


# ---------------------------------------------------------------------------
# PER-CCY FETCHERS (STUBS TO BE IMPLEMENTED)
# ---------------------------------------------------------------------------

def _date_range_str(start_date: dt.date, end_date: dt.date) -> str:
    return f"{start_date.isoformat()}→{end_date.isoformat()}"


def fetch_usd_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch USD yields and policy rates.

    Recommended free source:
        - FRED API (requires free key via env var FRED_API_KEY)
            DGS10      10-year Treasury
            DGS2       2-year Treasury
            TB3MS      3-month T-Bill
            FEDFUNDS   Effective Fed Funds Rate
    """
    # TODO: Implement actual FRED API calls here.
    # For now, return empty DataFrame with correct columns.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_eur_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch EUR yields and policy rates.

    Recommended free source:
        - ECB SDW REST API
          Example 10Y benchmark:
            FM.M.U2.EUR.RT.BB10.SV_YM.E
          Example MRO policy rate:
            FM.M.U2.EUR.4F.KR.MRR_FR.LEV
    """
    # TODO: Implement ECB SDW calls here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_gbp_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch GBP yields and policy rates.

    Recommended free source:
        - Bank of England (BoE) public API
          Bank Rate, 10Y benchmark yields.
    """
    # TODO: Implement BoE calls here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_jpy_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch JPY yields and policy rates.

    Recommended free source:
        - Bank of Japan data portal
          10Y JGB yields + policy rate.
    """
    # TODO: Implement BoJ calls here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_chf_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch CHF yields and policy rates.

    Recommended free source:
        - Swiss National Bank (SNB) statistics.
    """
    # TODO: Implement SNB calls here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_cad_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch CAD yields and policy rates.

    Recommended free source:
        - Bank of Canada JSON API.
    """
    # TODO: Implement BoC calls here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_aud_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch AUD yields and policy rates.

    Recommended free source:
        - Reserve Bank of Australia (RBA).
    """
    # TODO: Implement RBA calls here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_nzd_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch NZD yields and policy rates.

    Recommended free source:
        - Reserve Bank of New Zealand (RBNZ).
    """
    # TODO: Implement RBNZ API calls here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_nok_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch NOK yields and policy rates.

    Recommended free source:
        - Norges Bank open API.
    """
    # TODO: Implement Norges Bank calls here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_sek_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch SEK yields and policy rates.

    Recommended free source:
        - Sveriges Riksbank API.
    """
    # TODO: Implement Riksbank calls here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_zar_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch ZAR yields and policy rates.

    Recommended free source:
        - DBnomics (aggregates SARB data).
    """
    # TODO: Implement DBnomics or SARB CSV fetch here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


def fetch_brl_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Fetch BRL yields and policy rates.

    Recommended free source:
        - Banco Central do Brasil SGS API.
    """
    # TODO: Implement BCB SGS API calls here.
    return pd.DataFrame(columns=["as_of_date", "ccy", "tenor", "rate_value"])


# ---------------------------------------------------------------------------
# CANONICAL BUILDER
# ---------------------------------------------------------------------------

def _concat_nonempty(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(
            columns=["as_of_date", "ccy", "tenor", "rate_value",
                     "source_system", "updated_at"]
        )
    return pd.concat(frames, ignore_index=True)


def build_ir_yields_canonical(
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Build the canonical IR yields leaf across currencies and write to R2.

    Returns the canonical DataFrame.
    """
    print(
        f"[IR-RATES] Building canonical IR yields for "
        f"{_date_range_str(start_date, end_date)} …"
    )

    dfs: List[pd.DataFrame] = []

    dfs.append(fetch_usd_yields(start_date, end_date))
    dfs.append(fetch_eur_yields(start_date, end_date))
    dfs.append(fetch_gbp_yields(start_date, end_date))
    dfs.append(fetch_jpy_yields(start_date, end_date))
    dfs.append(fetch_chf_yields(start_date, end_date))
    dfs.append(fetch_cad_yields(start_date, end_date))
    dfs.append(fetch_aud_yields(start_date, end_date))
    dfs.append(fetch_nzd_yields(start_date, end_date))
    dfs.append(fetch_nok_yields(start_date, end_date))
    dfs.append(fetch_sek_yields(start_date, end_date))
    dfs.append(fetch_zar_yields(start_date, end_date))
    dfs.append(fetch_brl_yields(start_date, end_date))

    df_all = _concat_nonempty(dfs)

    if df_all.empty:
        print("[IR-RATES] WARNING: No IR yields data fetched. "
              "Check per-CCY fetchers / API wiring.")
        return df_all

    # Canonical housekeeping
    df_all["as_of_date"] = pd.to_datetime(df_all["as_of_date"])
    df_all["source_system"] = df_all.get("source_system", "PUBLIC_CENTRAL_BANK_API")
    df_all["updated_at"] = pd.Timestamp.utcnow().normalize()

    df_all = df_all.sort_values(["as_of_date", "ccy", "tenor"]).reset_index(drop=True)

    print(
        "[IR-RATES] Built canonical IR yields leaf: "
        f"rows={len(df_all):,}, ccy={df_all['ccy'].nunique()}, "
        f"tenors={sorted(df_all['tenor'].unique().tolist())}"
    )

    write_parquet_to_r2(df_all, R2_IR_YIELDS_KEY, index=False)
    print(
        f"[IR-RATES] Wrote canonical IR yields leaf to R2 at {R2_IR_YIELDS_KEY} "
        f"(rows={len(df_all):,})"
    )

    return df_all

