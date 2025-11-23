from __future__ import annotations

import datetime as dt
import io
from dataclasses import dataclass
from typing import Dict, List, Sequence

import pandas as pd
import requests

from common.r2_client import write_parquet_to_r2, read_parquet_from_r2

"""
fx_ir_yield_bridge.py

10Y government bond yield leaf & yield-differential leaf
for the FX Stream, sourced from FRED's OECD long-term
government bond yield series (10-year, monthly).

Leaf 1: 10Y yield per currency (monthly)
----------------------------------------
Schema (long):

    as_of_date     datetime64[ns]  # FRED observation_date
    ccy            str             # e.g., "NZD"
    country        str             # e.g., "New Zealand"
    yld_10y        float           # percent
    leaf_group     str             # 'fx_stream'
    leaf_name      str             # 'fx_10y_yields'
    source_system  str             # 'fred_mei'
    updated_at     datetime64[ns]

R2 key:
    spine_us/us_fx_10y_yields.parquet

Leaf 2: 10Y yield differentials per FX pair (monthly)
-----------------------------------------------------
For each FX pair (e.g., NZDAUD), we compute:

    yld_10y_diff =  yld_10y_base - yld_10y_quote

Schema:

    as_of_date     datetime64[ns]
    pair           str
    base_ccy       str
    quote_ccy      str
    yld_10y_base   float
    yld_10y_quote  float
    yld_10y_diff   float
    leaf_group     str    # 'fx_stream'
    leaf_name      str    # 'fx_10y_spreads'
    source_system  str    # 'fred_mei'
    updated_at     datetime64[ns]

R2 key:
    spine_us/us_fx_10y_spreads.parquet
"""

R2_YIELD_KEY = "spine_us/us_fx_10y_yields.parquet"
R2_SPREAD_KEY = "spine_us/us_fx_10y_spreads.parquet"


@dataclass
class TenYearYieldConfig:
    fred_id: str
    ccy: str
    country: str

    @property
    def url(self) -> str:
        # Standard FRED CSV endpoint
        return f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={self.fred_id}"


def get_10y_yield_configs() -> List[TenYearYieldConfig]:
    """
    10Y gov't bond yield universe (monthly % yields) from FRED/OECD.

    Series are "Interest Rates: Long-Term Government Bond Yields:
    10-Year: Main (Including Benchmark)" for each country/area.
    """
    return [
        # USD – United States
        TenYearYieldConfig("IRLTLT01USM156N", "USD", "United States"),
        # EUR – Euro Area (19 Countries)
        TenYearYieldConfig("IRLTLT01EZM156N", "EUR", "Euro Area (19)"),
        # GBP – United Kingdom
        TenYearYieldConfig("IRLTLT01GBM156N", "GBP", "United Kingdom"),
        # JPY – Japan
        TenYearYieldConfig("IRLTLT01JPM156N", "JPY", "Japan"),
        # CAD – Canada
        TenYearYieldConfig("IRLTLT01CAM156N", "CAD", "Canada"),
        # AUD – Australia
        TenYearYieldConfig("IRLTLT01AUM156N", "AUD", "Australia"),
        # NZD – New Zealand
        TenYearYieldConfig("IRLTLT01NZM156N", "NZD", "New Zealand"),
        # CHF – Switzerland
        TenYearYieldConfig("IRLTLT01CHM156N", "CHF", "Switzerland"),
        # NOK – Norway
        TenYearYieldConfig("IRLTLT01NOM156N", "NOK", "Norway"),
        # SEK – Sweden
        TenYearYieldConfig("IRLTLT01SEM156N", "SEK", "Sweden"),
        # ZAR – South Africa
        TenYearYieldConfig("IRLTLT01ZAM156N", "ZAR", "South Africa"),
    ]


def fetch_10y_yield_series(cfg: TenYearYieldConfig) -> pd.DataFrame:
    """
    Fetch a single 10Y yield series as CSV from FRED and convert to Spine format.

    FRED CSV pattern:
        observation_date,<FRED_ID>
    """
    try:
        resp = requests.get(cfg.url, timeout=30)
    except requests.RequestException as exc:
        print(f"[YLD10] FRED request failed for {cfg.ccy}: {exc}")
        return pd.DataFrame()

    if resp.status_code != 200:
        print(
            f"[YLD10] FRED {cfg.fred_id} returned "
            f"{resp.status_code} {resp.reason}"
        )
        return pd.DataFrame()

    df_raw = pd.read_csv(io.StringIO(resp.text))

    # Normalise column names: observation_date -> as_of_date
    if "observation_date" not in df_raw.columns:
        # Defensive: some FRED downloads might use DATE
        if "DATE" in df_raw.columns:
            df_raw = df_raw.rename(columns={"DATE": "observation_date"})
        else:
            print(
                f"[YLD10] Unexpected columns for {cfg.ccy}: "
                f"{list(df_raw.columns)}"
            )
            return pd.DataFrame()

    df_raw = df_raw.rename(
        columns={"observation_date": "as_of_date", cfg.fred_id: "yld_10y"}
    )
    df_raw["as_of_date"] = pd.to_datetime(
        df_raw["as_of_date"], errors="coerce"
    )
    df_raw["yld_10y"] = pd.to_numeric(df_raw["yld_10y"], errors="coerce")

    df_raw = df_raw.dropna(subset=["as_of_date", "yld_10y"])

    df_raw["ccy"] = cfg.ccy
    df_raw["country"] = cfg.country

    # Spine metadata
    df_raw["leaf_group"] = "fx_stream"
    df_raw["leaf_name"] = "fx_10y_yields"
    df_raw["source_system"] = "fred_mei"
    df_raw["updated_at"] = pd.Timestamp.utcnow()

    return df_raw


def build_10y_yield_leaf(
    start_date: dt.date = dt.date(2000, 1, 1),
    end_date: dt.date | None = None,
) -> pd.DataFrame:
    """
    Build the 10Y yield leaf for all configured currencies and
    write to R2 as spine_us/us_fx_10y_yields.parquet.
    """
    if end_date is None:
        end_date = dt.date.today()

    frames: List[pd.DataFrame] = []
    cfgs = get_10y_yield_configs()

    for cfg in cfgs:
        print(f"[YLD10] Fetching 10Y yield for {cfg.ccy} ({cfg.fred_id}) …")
        df = fetch_10y_yield_series(cfg)
        if df.empty:
            print(f"[YLD10] No data returned for {cfg.ccy}")
            continue

        # Filter by canonical window
        df = df[
            (df["as_of_date"].dt.date >= start_date)
            & (df["as_of_date"].dt.date <= end_date)
        ]

        if df.empty:
            print(f"[YLD10] {cfg.ccy} has no data in requested window.")
            continue

        frames.append(df)

    if not frames:
        raise RuntimeError("No 10Y yield data fetched for any currency.")

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["ccy", "as_of_date"])
    df_all = df_all.sort_values(["as_of_date", "ccy"])

    write_parquet_to_r2(df_all, R2_YIELD_KEY, index=False)

    print(
        f"[YLD10] Wrote 10Y yield leaf to R2 at {R2_YIELD_KEY} "
        f"(rows={len(df_all)}, ccy={df_all['ccy'].nunique()})"
    )

    return df_all


# FX pairs for which we want 10Y yield differentials
DEFAULT_IRD_PAIRS: Sequence[str] = [
    # Major USD crosses
    "EURUSD",
    "GBPUSD",
    "AUDUSD",
    "NZDUSD",
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "USDNOK",
    "USDSEK",
    "USDZAR",
    # Euro crosses
    "EURGBP",
    "EURAUD",
    "EURNZD",
    "EURCAD",
    "EURCHF",
    "EURNOK",
    "EURSEK",
    "EURZAR",
    # Commodity / regional of interest
    "NZDAUD",
]


def build_10y_yield_spreads(
    pairs: Sequence[str] | None = None,
    start_date: dt.date = dt.date(2000, 1, 1),
    end_date: dt.date | None = None,
) -> pd.DataFrame:
    """
    Build 10Y yield differentials per FX pair and persist to R2.

    For each pair XYZWV (e.g., NZDAUD), we parse:
        base_ccy  = pair[0:3]
        quote_ccy = pair[3:6]

    Then compute, for each as_of_date where both are available:
        yld_10y_diff = yld_10y_base - yld_10y_quote
    """
    if pairs is None:
        pairs = DEFAULT_IRD_PAIRS

    if end_date is None:
        end_date = dt.date.today()

    # Load yield leaf from R2
    print("[YLD10-SPR] Loading 10Y yield leaf from R2 …")
    df_yld = read_parquet_from_r2(R2_YIELD_KEY)

    # Ensure types
    df_yld["as_of_date"] = pd.to_datetime(df_yld["as_of_date"])
    df_yld["yld_10y"] = pd.to_numeric(df_yld["yld_10y"], errors="coerce")

    df_yld = df_yld.dropna(subset=["as_of_date", "yld_10y"])

    # Filter by requested window
    df_yld = df_yld[
        (df_yld["as_of_date"].dt.date >= start_date)
        & (df_yld["as_of_date"].dt.date <= end_date)
    ]

    if df_yld.empty:
        raise RuntimeError("10Y yield leaf is empty after date filtering.")

    # Pivot: monthly time index × currency columns
    yld_pivot = df_yld.pivot(
        index="as_of_date", columns="ccy", values="yld_10y"
    )

    frames: List[pd.DataFrame] = []
    now_ts = pd.Timestamp.utcnow()

    for pair in pairs:
        if len(pair) != 6:
            print(f"[YLD10-SPR] Skipping malformed pair '{pair}'")
            continue

        base_ccy = pair[0:3]
        quote_ccy = pair[3:6]

        if base_ccy not in yld_pivot.columns or quote_ccy not in yld_pivot.columns:
            print(
                f"[YLD10-SPR] Skipping {pair}: "
                f"missing yields for {base_ccy} or {quote_ccy}"
            )
            continue

        df_pair = yld_pivot[[base_ccy, quote_ccy]].copy()
        df_pair = df_pair.rename(
            columns={base_ccy: "yld_10y_base", quote_ccy: "yld_10y_quote"}
        )
        df_pair["yld_10y_diff"] = (
            df_pair["yld_10y_base"] - df_pair["yld_10y_quote"]
        )

        df_pair = df_pair.dropna(subset=["yld_10y_base", "yld_10y_quote"])

        if df_pair.empty:
            print(f"[YLD10-SPR] No overlapping yield data for pair {pair}")
            continue

        df_pair = df_pair.reset_index()
        df_pair["pair"] = pair
        df_pair["base_ccy"] = base_ccy
        df_pair["quote_ccy"] = quote_ccy

        # Metadata
        df_pair["leaf_group"] = "fx_stream"
        df_pair["leaf_name"] = "fx_10y_spreads"
        df_pair["source_system"] = "fred_mei"
        df_pair["updated_at"] = now_ts

        frames.append(df_pair)

    if not frames:
        raise RuntimeError("No 10Y yield spreads built for any FX pair.")

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.sort_values(["as_of_date", "pair"])

    write_parquet_to_r2(df_all, R2_SPREAD_KEY, index=False)

    print(
        f"[YLD10-SPR] Wrote 10Y yield spreads leaf to R2 at {R2_SPREAD_KEY} "
        f"(rows={len(df_all)}, pairs={df_all['pair'].nunique()})"
    )

    return df_all


def main() -> int:
    # Full build: yields + spreads
    build_10y_yield_leaf()
    build_10y_yield_spreads()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

