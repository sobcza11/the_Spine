from __future__ import annotations

import datetime as dt
from typing import List, Tuple

import pandas as pd

from common.r2_client import read_parquet_from_r2, write_parquet_to_r2

"""
fx_spot_bridge.py

Builds the canonical FX spot leaf for Spine-US using ECB EUR-reference FX data.

Input (from R2):
    spine_us/us_fx_ecb_eur_raw.parquet

    fx_date            datetime64[ns]
    ccy                str           # "USD", "JPY", ...
    rate_ccy_per_eur   float         # e.g. 1.09 USD per 1 EUR
    leaf_group         "fx_stream"
    leaf_name          "fx_ecb_eur_raw"
    source_system      "ecb"
    updated_at         datetime64[ns]

Output (to R2):
    spine_us/us_fx_spot_canonical.parquet

    fx_date     datetime64[ns]
    fx_close    float          # market-style rate
    pair        str           # "EURUSD", "USDJPY", "AUDCAD", ...
    base_ccy    str
    quote_ccy   str
    leaf_group  "fx_stream"
    leaf_name   "fx_spot_canonical"
    source_system "ecb"
    updated_at  datetime64[ns]
"""

ECB_RAW_KEY = "spine_us/us_fx_ecb_eur_raw.parquet"
FX_SPOT_CANONICAL_KEY = "spine_us/us_fx_spot_canonical.parquet"


def get_canonical_pair_specs() -> List[Tuple[str, str]]:
    """
    Base/quote pairs we want in the Spine canonical FX leaf.
    All rates are "quote_ccy per 1 base_ccy".
    """
    return [
        # EUR majors & regionals
        ("EUR", "USD"),
        ("EUR", "GBP"),
        ("EUR", "JPY"),
        ("EUR", "CHF"),
        ("EUR", "AUD"),
        ("EUR", "NZD"),
        ("EUR", "CAD"),
        ("EUR", "NOK"),
        ("EUR", "SEK"),
        ("EUR", "ZAR"),
        ("EUR", "BRL"),

        # USD majors & EM
        ("USD", "JPY"),
        ("USD", "CHF"),
        ("USD", "CAD"),
        ("USD", "NOK"),
        ("USD", "SEK"),
        ("USD", "ZAR"),
        ("USD", "BRL"),

        # Other crosses
        ("GBP", "USD"),
        ("AUD", "USD"),
        ("NZD", "USD"),
        ("AUD", "CAD"),
        ("AUD", "JPY"),
        ("NZD", "AUD"),
    ]


def _build_pairs_from_eur_rates(df_eur: pd.DataFrame) -> pd.DataFrame:
    """
    Given EUR-reference FX history, compute all desired pairs.

    Input df_eur:
        fx_date, ccy, rate_ccy_per_eur

    where rate_ccy_per_eur = (ccy per 1 EUR).

    Logic:
      - EURQ:    Q per 1 EUR is directly rate_ccy_per_eur (base=EUR, quote=Q).
      - B/Q:     quote_ccy per base_ccy = r_Q / r_B
                 (using cross formula: (Q/EUR)/(B/EUR)).
    """
    # Pivot to wide form: index=fx_date, cols=ccy -> rate_ccy_per_eur
    wide = df_eur.pivot(index="fx_date", columns="ccy", values="rate_ccy_per_eur")
    wide = wide.sort_index()

    pair_specs = get_canonical_pair_specs()
    frames: list[pd.DataFrame] = []

    for base, quote in pair_specs:
        if base == "EUR":
            # Direct from EUR to quote_ccy
            if quote not in wide.columns:
                print(f"[FX-SPOT] Missing EUR reference for {quote}, skipping {base}{quote}")
                continue
            rate = wide[quote]
        else:
            # Cross from EUR-based rates:
            # (quote_ccy per EUR) / (base_ccy per EUR)
            if base not in wide.columns or quote not in wide.columns:
                print(
                    f"[FX-SPOT] Missing EUR reference for base={base} or quote={quote}, "
                    f"skipping {base}{quote}"
                )
                continue
            rate = wide[quote] / wide[base]

        rate = rate.dropna()
        if rate.empty:
            print(f"[FX-SPOT] No non-null rates for {base}{quote}, skipping.")
            continue

        df_pair = pd.DataFrame(
            {
                "fx_date": rate.index,
                "fx_close": rate.values,
                "pair": f"{base}{quote}",
                "base_ccy": base,
                "quote_ccy": quote,
            }
        )
        frames.append(df_pair)

    if not frames:
        raise RuntimeError("[FX-SPOT] No FX pairs constructed from ECB EUR rates.")

    df_all = pd.concat(frames, ignore_index=True)
    df_all["leaf_group"] = "fx_stream"
    df_all["leaf_name"] = "fx_spot_canonical"
    df_all["source_system"] = "ecb"
    df_all["updated_at"] = pd.Timestamp.utcnow()

    # De-duplicate & sort for nice downstream behavior
    df_all = df_all.dropna(subset=["fx_date", "fx_close"])
    df_all = df_all.drop_duplicates(subset=["fx_date", "pair"])
    df_all = df_all.sort_values(["fx_date", "pair"])

    return df_all


def build_fx_spot_canonical_from_ecb(
    start_date: dt.date = dt.date(2000, 1, 1),
    end_date: dt.date | None = None,
) -> pd.DataFrame:
    """
    Main builder: read ECB EUR-reference FX from R2, build canonical FX spot leaf,
    and write to R2.
    """
    if end_date is None:
        end_date = dt.date.today()

    print(f"[FX-SPOT] Loading ECB EUR FX from R2 key={ECB_RAW_KEY} …")
    df_eur = read_parquet_from_r2(ECB_RAW_KEY)

    if df_eur.empty:
        raise RuntimeError("[FX-SPOT] ECB EUR FX leaf is empty; run fx_ecb_bridge first.")

    # Ensure fx_date is datetime
    df_eur["fx_date"] = pd.to_datetime(df_eur["fx_date"], errors="coerce")
    df_eur = df_eur.dropna(subset=["fx_date"])

    df_eur = df_eur[
        (df_eur["fx_date"].dt.date >= start_date)
        & (df_eur["fx_date"].dt.date <= end_date)
    ]

    if df_eur.empty:
        raise RuntimeError(
            f"[FX-SPOT] No ECB EUR FX rows in window {start_date} → {end_date}"
        )

    print(
        f"[FX-SPOT] Building canonical pairs from ECB EUR FX "
        f"(rows={len(df_eur)}, ccy={df_eur['ccy'].nunique()}) …"
    )
    df_spot = _build_pairs_from_eur_rates(df_eur)

    write_parquet_to_r2(df_spot, FX_SPOT_CANONICAL_KEY, index=False)

    print(
        f"[FX-SPOT] Wrote canonical FX spot leaf to R2 at {FX_SPOT_CANONICAL_KEY} "
        f"(rows={len(df_spot)}, pairs={df_spot['pair'].nunique()})"
    )

    return df_spot


# Backwards-compatible alias if other scripts import build_fx_spot_canonical
def build_fx_spot_canonical(
    start_date: dt.date = dt.date(2000, 1, 1),
    end_date: dt.date | None = None,
) -> pd.DataFrame:
    return build_fx_spot_canonical_from_ecb(start_date=start_date, end_date=end_date)

