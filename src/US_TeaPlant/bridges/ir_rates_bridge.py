"""
US_TeaPlant.bridges.ir_rates_bridge

Canonical interest-rate *yields* leaf builder for the_Spine.

- Source: FRED API (St. Louis Fed).
- CCYs currently wired: USD, EUR, CAD, GBP
- Tenors: "10Y" (long yield), "POLICY" (central-bank policy rate)

Schema written to R2 at spine_us/us_ir_yields_canonical.parquet:

    as_of_date  : datetime64[ns]  (daily or monthly dates from FRED)
    ccy         : string          (e.g., "USD", "EUR")
    tenor       : string          ("10Y", "POLICY")
    rate_value  : float64         (percent per annum)
    leaf_group  : string          ("IR_YIELDS")
    leaf_name   : string          ("us_ir_yields_canonical")
    source_system : string        ("FRED")
    updated_at  : datetime64[ns]  (UTC timestamp of build)
"""

from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from typing import List, Dict

import pandas as pd
import requests

from common.r2_client import write_parquet_to_r2

# R2 location for canonical yields
R2_IR_YIELDS_KEY = "spine_us/us_ir_yields_canonical.parquet"

# FRED config
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_TIMEOUT_SEC = 30


class FredError(RuntimeError):
    """Custom error for FRED-related issues."""


@dataclass(frozen=True)
class FredSeriesConfig:
    ccy: str
    tenor: str
    series_id: str
    description: str


def _get_fred_api_key() -> str:
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        raise FredError(
            "FRED_API_KEY env var is missing or empty – "
            "set it locally and in GitHub Secrets for the IR workflow."
        )
    return api_key


def _fetch_fred_series(
    series_id: str,
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Fetch a single FRED series into a DataFrame with columns:
        DATE, value  (both strings initially)
    """
    api_key = _get_fred_api_key()

    params: Dict[str, str] = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date.isoformat(),
        "observation_end": end_date.isoformat(),
    }

    resp = requests.get(FRED_BASE_URL, params=params, timeout=FRED_TIMEOUT_SEC)
    resp.raise_for_status()

    data = resp.json()
    obs = data.get("observations", [])
    if not obs:
        # Empty series is allowed but should be visible in logs
        print(f"[IR-RATES] WARNING: FRED series {series_id} returned 0 observations.")
        return pd.DataFrame(columns=["DATE", "value"])

    df = pd.DataFrame(obs)[["date", "value"]]
    df = df.rename(columns={"date": "DATE"})
    return df


def _normalize_fred_observations(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Convert FRED's DATE/value strings into:
        as_of_date: datetime64[ns]
        rate_value: float64
    Drop rows with missing ('.') or non-convertible values.
    """
    if df_raw.empty:
        return pd.DataFrame(columns=["as_of_date", "rate_value"])

    df = df_raw.copy()

    # Drop obvious missing values
    df = df[df["value"].notna() & (df["value"] != ".")]

    # Convert
    df["as_of_date"] = pd.to_datetime(df["DATE"], errors="coerce", utc=False)
    df["rate_value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["as_of_date", "rate_value"])
    df = df[["as_of_date", "rate_value"]].sort_values("as_of_date")

    return df


def _build_ccy_yields(
    ccy: str,
    tenors: List[str],
    series_ids: List[str],
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Generic helper for a single CCY.

    Parameters
    ----------
    ccy : "USD", "EUR", "CAD", "GBP", ...
    tenors : list like ["10Y", "POLICY"]
    series_ids : matching FRED series IDs for these tenors
    """
    frames: List[pd.DataFrame] = []
    updated_at = pd.Timestamp.utcnow()

    for tenor, series_id in zip(tenors, series_ids):
        print(
            f"[IR-RATES] Fetching {ccy} {tenor} from FRED "
            f"(series_id={series_id}) …"
        )
        raw = _fetch_fred_series(series_id, start_date, end_date)
        norm = _normalize_fred_observations(raw)

        if norm.empty:
            print(
                f"[IR-RATES] WARNING: No data for {ccy} {tenor} "
                f"(series_id={series_id})."
            )
            continue

        norm["ccy"] = ccy
        norm["tenor"] = tenor
        norm["leaf_group"] = "IR_YIELDS"
        norm["leaf_name"] = "us_ir_yields_canonical"
        norm["source_system"] = "FRED"
        norm["updated_at"] = updated_at

        frames.append(norm)

    if not frames:
        return pd.DataFrame(
            columns=[
                "as_of_date",
                "ccy",
                "tenor",
                "rate_value",
                "leaf_group",
                "leaf_name",
                "source_system",
                "updated_at",
            ]
        )

    df_ccy = pd.concat(frames, ignore_index=True)
    df_ccy = df_ccy.sort_values(["as_of_date", "ccy", "tenor"])
    return df_ccy


# ---------- Per-CCY builders -------------------------------------------------


def _build_usd_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    USD:
      10Y     -> DGS10 (10-Year Treasury Constant Maturity Rate)
      POLICY  -> DFF   (Effective Federal Funds Rate)
    """
    tenors = ["10Y", "POLICY"]
    series_ids = [
        "DGS10",  # 10-year Treasury
        "DFF",    # Effective Federal Funds Rate
    ]
    return _build_ccy_yields("USD", tenors, series_ids, start_date, end_date)


def _build_eur_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    EUR (Euro area):
      10Y     -> IRLTLT01EZM156N (10-year government bond yield, euro area) :contentReference[oaicite:2]{index=2}
      POLICY  -> ECBDFR (ECB Deposit Facility Rate – used as policy proxy)
    """
    tenors = ["10Y", "POLICY"]
    series_ids = [
        "IRLTLT01EZM156N",  # 10Y gov bond yield, euro area
        "ECBDFR",           # ECB Deposit Facility Rate
    ]
    return _build_ccy_yields("EUR", tenors, series_ids, start_date, end_date)


def _build_cad_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    CAD (Canada):
      10Y     -> IRLTLT01CAM156N (10-year government bond yield, Canada) :contentReference[oaicite:3]{index=3}
      POLICY  -> IRSTCI01CAM156N (Short-term central bank policy rate proxy)
    """
    tenors = ["10Y", "POLICY"]
    series_ids = [
        "IRLTLT01CAM156N",  # 10Y gov bond yield, Canada
        "IRSTCI01CAM156N",  # Policy / short-term rate proxy
    ]
    return _build_ccy_yields("CAD", tenors, series_ids, start_date, end_date)


def _build_gbp_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    GBP (United Kingdom):

      10Y     -> IRLTLT01GBM156N (10-year gov bond yield, UK) :contentReference[oaicite:4]{index=4}
      POLICY  -> BOERUKM         (Bank of England policy rate) :contentReference[oaicite:5]{index=5}
    """
    tenors = ["10Y", "POLICY"]
    series_ids = [
        "IRLTLT01GBM156N",  # 10Y gov bond yield, UK (monthly)
        "BOERUKM",          # Bank of England policy rate (monthly)
    ]
    return _build_ccy_yields("GBP", tenors, series_ids, start_date, end_date)

def _build_aud_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    AUD (Australia):

      10Y     -> IRLTLT01AUM156N (10-year government bond yield, Australia)
      POLICY  -> IRSTCI01AUM156N (Immediate interbank/call rate – policy proxy)
    """
    tenors = ["10Y", "POLICY"]
    series_ids = [
        "IRLTLT01AUM156N",  # 10Y gov bond yield, Australia (monthly)
        "IRSTCI01AUM156N",  # Immediate interbank/call rate, Australia (monthly)
    ]
    return _build_ccy_yields("AUD", tenors, series_ids, start_date, end_date)


def _build_nzd_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    NZD (New Zealand):

      10Y     -> IRLTLT01NZM156N (10-year government bond yield, New Zealand)
      POLICY  -> IRSTCI01NZM156N (Immediate interbank/call rate – policy proxy)
    """
    tenors = ["10Y", "POLICY"]
    series_ids = [
        "IRLTLT01NZM156N",  # 10Y gov bond yield, New Zealand (monthly)
        "IRSTCI01NZM156N",  # Immediate interbank/call rate, New Zealand (monthly)
    ]
    return _build_ccy_yields("NZD", tenors, series_ids, start_date, end_date)

def _build_chf_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    CHF (Switzerland):

      10Y     -> IRLTLT01CHM156N (10-year government bond yield, Switzerland)
      POLICY  -> IRSTCI01CHM156N (Short-term policy rate proxy, Switzerland)
    """
    tenors = ["10Y", "POLICY"]
    series_ids = [
        "IRLTLT01CHM156N",  # 10Y gov bond yield, Switzerland (monthly)
        "IRSTCI01CHM156N",  # Short-term policy rate proxy, Switzerland
    ]
    return _build_ccy_yields("CHF", tenors, series_ids, start_date, end_date)


def _build_sek_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    SEK (Sweden):

      10Y     -> IRLTLT01SEM156N (10-year government bond yield, Sweden)
      POLICY  -> IRSTCI01SEM156N (Short-term policy rate proxy, Sweden)
    """
    tenors = ["10Y", "POLICY"]
    series_ids = [
        "IRLTLT01SEM156N",  # 10Y gov bond yield, Sweden (monthly)
        "IRSTCI01SEM156N",  # Short-term policy rate proxy, Sweden
    ]
    return _build_ccy_yields("SEK", tenors, series_ids, start_date, end_date)


def _build_nok_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    NOK (Norway):

      10Y     -> IRLTLT01NOM156N (10-year government bond yield, Norway)
      POLICY  -> IRSTCI01NOM156N (Short-term policy rate proxy, Norway)
    """
    tenors = ["10Y", "POLICY"]
    series_ids = [
        "IRLTLT01NOM156N",  # 10Y gov bond yield, Norway (monthly)
        "IRSTCI01NOM156N",  # Short-term policy rate proxy, Norway
    ]
    return _build_ccy_yields("NOK", tenors, series_ids, start_date, end_date)


# ---------- Public builder ---------------------------------------------------


def build_ir_yields_canonical(
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Build the canonical IR yields leaf across all wired CCYs & tenors,
    then write it to R2 and return the DataFrame.

    This is what scripts/build_ir_yields_canonical.py orchestrates.

    Currently wired:
      - USD: 10Y, POLICY
      - EUR: 10Y, POLICY
      - CAD: 10Y, POLICY
      - GBP: 10Y, POLICY
    """
    print(
        f"[IR-RATES] Building canonical IR yields for "
        f"{start_date}→{end_date} …"
    )

    frames: List[pd.DataFrame] = []

    # Order here controls logging but not semantics
    frames.append(_build_usd_yields(start_date, end_date))
    frames.append(_build_eur_yields(start_date, end_date))
    frames.append(_build_cad_yields(start_date, end_date))
    frames.append(_build_gbp_yields(start_date, end_date))
    frames.append(_build_jpy_yields(start_date, end_date))
    frames.append(_build_aud_yields(start_date, end_date))
    frames.append(_build_nzd_yields(start_date, end_date))
    frames.append(_build_chf_yields(start_date, end_date))
    frames.append(_build_sek_yields(start_date, end_date))
    frames.append(_build_nok_yields(start_date, end_date))

    # Filter out any empty CCYs (e.g., if FRED series temporarily missing)
    frames = [f for f in frames if not f.empty]

    if not frames:
        raise RuntimeError(
            "[IR-RATES] No IR yields built for any CCY – check FRED connectivity"
        )

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.sort_values(["as_of_date", "ccy", "tenor"])

    ccy_count = df_all["ccy"].nunique()
    tenors = sorted(df_all["tenor"].unique().tolist())
    print(
        f"[IR-RATES] Built canonical IR yields leaf: "
        f"rows={len(df_all):,}, ccy={ccy_count}, tenors={tenors}"
    )

    write_parquet_to_r2(df_all, R2_IR_YIELDS_KEY)
    print(
        f"[IR-RATES] Wrote canonical IR yields leaf to R2 at "
        f"{R2_IR_YIELDS_KEY} (rows={len(df_all):,})"
    )

    return df_all

def _build_jpy_yields(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    JPY (Japan):

      10Y     -> IRLTLT01JPM156N (10-year government bond yield, Japan)
      POLICY  -> IRSTCI01JPM156N (Short-term policy rate proxy)
    """
    tenors = ["10Y", "POLICY"]
    series_ids = [
        "IRLTLT01JPM156N",  # 10Y gov bond yield, Japan (monthly)
        "IRSTCI01JPM156N",  # Policy rate proxy (monthly)
    ]
    return _build_ccy_yields("JPY", tenors, series_ids, start_date, end_date)



