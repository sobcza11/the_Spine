from __future__ import annotations
import datetime as dt
import io
from dataclasses import dataclass
from typing import Dict, List

import pandas as pd
import requests

from common.r2_client import write_parquet_to_r2

"""
fx_fed_bridge.py

Fetches FX series from the Federal Reserve (FRED) CSV endpoints.
These are USD-based spot exchange rates (H.10/G.5) provided by the Fed.

Output schema (long):

    fx_date        datetime64[ns]
    pair           str            # e.g., "USDJPY"  (USD per 1 JPY? or invert)
    base_ccy       str
    quote_ccy      str
    fx_close       float
    leaf_group     str            # "fx_stream"
    leaf_name      str            # "fx_fed_raw"
    source_system  str            # "fred"
    updated_at     datetime64[ns]

Writes to R2 key:
    spine_us/us_fx_fed_raw.parquet
"""


R2_FED_FX_KEY = "spine_us/us_fx_fed_raw.parquet"


@dataclass
class FredFXConfig:
    fred_id: str      # FRED series ID, e.g., "DEXJPUS"
    pair: str         # e.g., "USDJPY"
    base_ccy: str     # e.g., "USD"
    quote_ccy: str    # e.g., "JPY"

    @property
    def url(self) -> str:
        # CSV download endpoint from FRED
        return f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={self.fred_id}"


def get_fred_fx_configs() -> List[FredFXConfig]:
    """
    Pair universe coming from Fed H.10 and G.5 datasets.

    IMPORTANT: We align to *market conventions*:

      - EURUSD, GBPUSD, AUDUSD, NZDUSD  (foreign base, USD quote)
      - USDJPY, USDCHF, USDCAD, USDNOK, USDSEK, USDZAR, USDBRL

    We do NOT invert DEXUSEU / DEXUSUK / DEXUSAL / DEXUSNZ values;
    we simply name the pair from the foreign currency's perspective.
    """

    cfgs = [
        # JPY per 1 USD -> USDJPY (market standard)
        FredFXConfig("DEXJPUS", "USDJPY", "USD", "JPY"),

        # USD per 1 EUR -> EURUSD
        FredFXConfig("DEXUSEU", "EURUSD", "EUR", "USD"),

        # USD per 1 GBP -> GBPUSD
        FredFXConfig("DEXUSUK", "GBPUSD", "GBP", "USD"),

        # USD per 1 AUD -> AUDUSD
        FredFXConfig("DEXUSAL", "AUDUSD", "AUD", "USD"),

        # USD per 1 NZD -> NZDUSD
        FredFXConfig("DEXUSNZ", "NZDUSD", "NZD", "USD"),

        # CAD per 1 USD -> USDCAD
        FredFXConfig("DEXCAUS", "USDCAD", "USD", "CAD"),

        # CHF per 1 USD -> USDCHF
        FredFXConfig("DEXSZUS", "USDCHF", "USD", "CHF"),

        # NOK per 1 USD -> USDNOK
        FredFXConfig("DEXNOUS", "USDNOK", "USD", "NOK"),

        # SEK per 1 USD -> USDSEK
        FredFXConfig("DEXSDUS", "USDSEK", "USD", "SEK"),

        # ZAR per 1 USD -> USDZAR
        FredFXConfig("DEXSFUS", "USDZAR", "USD", "ZAR"),

        # BRL per 1 USD -> USDBRL
        FredFXConfig("DEXBZUS", "USDBRL", "USD", "BRL"),
    ]
    return cfgs



def fetch_fred_series(cfg: FredFXConfig) -> pd.DataFrame:
    """
    Fetch a single FRED FX series as CSV and convert to Spine format.
    FRED CSV usually has columns like: DATE or observation_date, plus <FRED_ID>.
    """
    try:
        resp = requests.get(cfg.url, timeout=30)
    except requests.RequestException as exc:
        print(f"[WARN] FRED request failed for {cfg.pair}: {exc}")
        return pd.DataFrame()

    if resp.status_code != 200:
        print(f"[WARN] FRED {cfg.fred_id} returned {resp.status_code} {resp.reason}")
        return pd.DataFrame()

    text = resp.text.strip()

    # Guard against HTML / weird responses
    if not text or "<html" in text.lower() or "doctype html" in text.lower():
        print(f"[WARN] FRED returned HTML instead of CSV for {cfg.fred_id}. Skipping.")
        return pd.DataFrame()

    df_raw = pd.read_csv(io.StringIO(text))

    # --- Detect date column (DATE vs observation_date) ---
    if "DATE" in df_raw.columns:
        date_col = "DATE"
    elif "observation_date" in df_raw.columns:
        date_col = "observation_date"
    else:
        print(f"[WARN] FRED CSV for {cfg.fred_id} missing DATE/observation_date. Head:")
        print(df_raw.head())
        return pd.DataFrame()

    # --- Detect FX numeric column ---
    if cfg.fred_id not in df_raw.columns:
        print(f"[WARN] FRED CSV for {cfg.fred_id} missing FX column '{cfg.fred_id}'. Head:")
        print(df_raw.head())
        return pd.DataFrame()

    # Rename into standardized schema
    df_raw = df_raw.rename(columns={date_col: "fx_date", cfg.fred_id: "fx_close"})
    df_raw["fx_date"] = pd.to_datetime(df_raw["fx_date"], errors="coerce")
    df_raw["fx_close"] = pd.to_numeric(df_raw["fx_close"], errors="coerce")

    df_raw = df_raw.dropna(subset=["fx_date", "fx_close"])
    if df_raw.empty:
        print(f"[WARN] FRED CSV for {cfg.fred_id} contains no valid numeric FX data.")
        return pd.DataFrame()

    # Attach Spine metadata
    df_raw["pair"] = cfg.pair
    df_raw["base_ccy"] = cfg.base_ccy
    df_raw["quote_ccy"] = cfg.quote_ccy
    df_raw["leaf_group"] = "fx_stream"
    df_raw["leaf_name"] = "fx_fed_raw"
    df_raw["source_system"] = "fred"
    df_raw["updated_at"] = pd.Timestamp.utcnow()

    return df_raw


def build_fed_fx_history(start_date: dt.date = dt.date(2000, 1, 1),
                         end_date: dt.date | None = None) -> pd.DataFrame:
    if end_date is None:
        end_date = dt.date.today()

    frames = []
    cfgs = get_fred_fx_configs()

    for cfg in cfgs:
        print(f"[INFO] Fetching FRED FX for {cfg.pair} ({cfg.fred_id}) ...")
        df = fetch_fred_series(cfg)
        if df.empty:
            print(f"[WARN] No data returned for {cfg.pair}")
            continue

        # Filter by our canonical window
        df = df[
            (df["fx_date"].dt.date >= start_date)
            & (df["fx_date"].dt.date <= end_date)
        ]
        if not df.empty:
            frames.append(df)

    if not frames:
        raise RuntimeError("No FRED FX data fetched for any configured pair.")

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["pair", "fx_date"])
    df_all = df_all.sort_values(["fx_date", "pair"])

    write_parquet_to_r2(df_all, R2_FED_FX_KEY, index=False)

    print(
        f"[INFO] Wrote FRED FX history to R2 at {R2_FED_FX_KEY} "
        f"(rows={len(df_all)}, pairs={df_all['pair'].nunique()})"
    )

    return df_all


def main() -> int:
    build_fed_fx_history()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

def main() -> int:
    # Run up to "yesterday" so a 1 AM job always pulls the prior day
    today = dt.date.today()
    end_date = today - dt.timedelta(days=1)

    build_fed_fx_history(end_date=end_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


