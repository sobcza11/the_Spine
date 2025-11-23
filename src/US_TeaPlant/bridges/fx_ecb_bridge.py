from __future__ import annotations

import datetime as dt
import io
from dataclasses import dataclass
from typing import List

import pandas as pd
import requests

from common.r2_client import write_parquet_to_r2

"""
fx_ecb_bridge.py

Fetches daily FX reference rates from the ECB Data Portal API (EXR dataflow).

We pull EUR-reference series like:

  EXR.D.USD.EUR.SP00.A  -> USD per 1 EUR
  EXR.D.JPY.EUR.SP00.A  -> JPY per 1 EUR
  ...

and write a long table:

    fx_date            datetime64[ns]
    ccy                str           # "USD", "JPY", ...
    rate_ccy_per_eur   float         # e.g. 1.09 USD per 1 EUR
    leaf_group         "fx_stream"
    leaf_name          "fx_ecb_eur_raw"
    source_system      "ecb"
    updated_at         datetime64[ns]

R2 key:
    spine_us/us_fx_ecb_eur_raw.parquet
"""

R2_ECB_FX_KEY = "spine_us/us_fx_ecb_eur_raw.parquet"


@dataclass
class EcbFXConfig:
    ccy: str  # ISO code, e.g. "USD"

    @property
    def series_key(self) -> str:
        # EXR key structure: FREQ.CURRENCY.CURRENCY_DENOM.EXR_TYPE.EXR_SUFFIX
        # We want: Daily (D), CURRENCY, EUR, SP00 (spot), A (avg)
        return f"D.{self.ccy}.EUR.SP00.A"

    @property
    def url(self) -> str:
        # New ECB Data Portal API (csvdata format)
        # Docs: https://data-api.ecb.europa.eu/service/data/EXR
        return f"https://data-api.ecb.europa.eu/service/data/EXR/{self.series_key}"


def get_ecb_fx_configs() -> List[EcbFXConfig]:
    """
    FX universe we want from ECB, all quoted vs EUR.

    These currencies give us enough to re-construct the existing Spine FX universe
    (majors, key USD crosses, NZD/AUD cross, BRL, ZAR).
    """
    return [
        EcbFXConfig("USD"),
        EcbFXConfig("JPY"),
        EcbFXConfig("GBP"),
        EcbFXConfig("CHF"),
        EcbFXConfig("AUD"),
        EcbFXConfig("NZD"),
        EcbFXConfig("CAD"),
        EcbFXConfig("NOK"),
        EcbFXConfig("SEK"),
        EcbFXConfig("ZAR"),
        EcbFXConfig("BRL"),
    ]


def fetch_ecb_fx_series(
    cfg: EcbFXConfig,
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Fetch a single ECB FX series in CSV form and normalize to Spine schema.

    ECB csvdata format includes columns like:
      KEY, FREQ, CURRENCY, CURRENCY_DENOM, EXR_TYPE, EXR_SUFFIX,
      TIME_PERIOD, OBS_VALUE, ...
    """
    params = {
        "startPeriod": start_date.isoformat(),
        "endPeriod": end_date.isoformat(),
        "format": "csvdata",
    }

    try:
        resp = requests.get(cfg.url, params=params, timeout=30)
    except requests.RequestException as exc:
        print(f"[ECB-FX] Request failed for {cfg.ccy}: {exc}")
        return pd.DataFrame()

    if resp.status_code != 200:
        print(
            f"[ECB-FX] HTTP {resp.status_code} for {cfg.ccy} "
            f"({cfg.series_key}): {resp.reason}"
        )
        return pd.DataFrame()

    # Parse CSV into DataFrame
    df_raw = pd.read_csv(io.StringIO(resp.text))

    if "TIME_PERIOD" not in df_raw.columns or "OBS_VALUE" not in df_raw.columns:
        print(f"[ECB-FX] Unexpected CSV schema for {cfg.ccy}, columns={df_raw.columns}")
        return pd.DataFrame()

    df = df_raw.rename(
        columns={
            "TIME_PERIOD": "fx_date",
            "OBS_VALUE": "rate_ccy_per_eur",
        }
    )

    df["fx_date"] = pd.to_datetime(df["fx_date"], errors="coerce")
    df["rate_ccy_per_eur"] = pd.to_numeric(df["rate_ccy_per_eur"], errors="coerce")

    df = df.dropna(subset=["fx_date", "rate_ccy_per_eur"])

    df["ccy"] = cfg.ccy
    df["leaf_group"] = "fx_stream"
    df["leaf_name"] = "fx_ecb_eur_raw"
    df["source_system"] = "ecb"
    df["updated_at"] = pd.Timestamp.utcnow()

    return df[["fx_date", "ccy", "rate_ccy_per_eur",
               "leaf_group", "leaf_name", "source_system", "updated_at"]]


def build_ecb_eur_fx_history(
    start_date: dt.date = dt.date(2000, 1, 1),
    end_date: dt.date | None = None,
) -> pd.DataFrame:
    """
    Pull all configured currencies vs EUR from ECB and write long EUR-reference table.
    """
    if end_date is None:
        end_date = dt.date.today()

    frames: list[pd.DataFrame] = []
    cfgs = get_ecb_fx_configs()

    for cfg in cfgs:
        print(f"[ECB-FX] Fetching {cfg.ccy} (series={cfg.series_key}) …")
        df = fetch_ecb_fx_series(cfg, start_date=start_date, end_date=end_date)
        if df.empty:
            print(f"[ECB-FX] No data returned for {cfg.ccy}")
            continue
        frames.append(df)

    if not frames:
        raise RuntimeError(
            f"No ECB FX data fetched for any currency in range {start_date} → {end_date}"
        )

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["fx_date", "ccy"])
    df_all = df_all.sort_values(["fx_date", "ccy"])

    write_parquet_to_r2(df_all, R2_ECB_FX_KEY, index=False)

    print(
        f"[ECB-FX] Wrote ECB EUR FX history to R2 at {R2_ECB_FX_KEY} "
        f"(rows={len(df_all)}, ccy={df_all['ccy'].nunique()})"
    )

    return df_all


def main() -> int:
    build_ecb_eur_fx_history()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


