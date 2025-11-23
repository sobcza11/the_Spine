"""
US_TeaPlant.bridges.ir_diff_bridge

Builds the canonical Interest Rate Differential leaf from the canonical
IR yields leaf + FX canonical universe.

Schema (canonical IR diff leaf):

    as_of_date              datetime64[ns]
    pair                    str         # e.g. 'USDJPY'
    base_ccy                str
    quote_ccy               str

    base_10y_yield          float
    quote_10y_yield         float
    diff_10y_bp             float

    base_policy_rate        float
    quote_policy_rate       float
    diff_policy_bp          float

    leaf_group              str   # 'IR'
    leaf_name               str   # 'IR_DIFF_CANONICAL'
    source_system           str
    updated_at              datetime64[ns]

R2 key:

    spine_us/us_ir_diff_canonical.parquet
"""

from __future__ import annotations

import datetime as dt
from typing import List, Tuple

import numpy as np
import pandas as pd

from common.r2_client import read_parquet_from_r2, write_parquet_to_r2

R2_IR_YIELDS_KEY = "spine_us/us_ir_yields_canonical.parquet"
R2_FX_SPOT_KEY = "spine_us/us_fx_spot_canonical.parquet"
R2_IR_DIFF_KEY = "spine_us/us_ir_diff_canonical.parquet"


TENOR_POLICY = "POLICY"
TENOR_10Y = "10Y"


def _get_fx_pair_universe() -> List[Tuple[str, str, str]]:
    """
    Read canonical FX spot leaf and return a unique universe of pairs:
        [(pair, base_ccy, quote_ccy), ...]
    """
    df_fx = read_parquet_from_r2(R2_FX_SPOT_KEY, columns=["pair", "base_ccy", "quote_ccy"])
    df_fx = df_fx.drop_duplicates(subset=["pair"]).sort_values("pair")
    return list(df_fx[["pair", "base_ccy", "quote_ccy"]].itertuples(index=False, name=None))


def _pivot_yields(df_yield: pd.DataFrame, tenor: str) -> pd.DataFrame:
    """
    Filter to one tenor & pivot to wide form:

        as_of_date, ccy -> rate_value

    Used for joining base vs quote curves.
    """
    df = df_yield[df_yield["tenor"] == tenor].copy()
    if df.empty:
        return pd.DataFrame()

    df = df[["as_of_date", "ccy", "rate_value"]]
    df = df.dropna(subset=["rate_value"])
    df = df.pivot(index="as_of_date", columns="ccy", values="rate_value")
    df.columns.name = None
    return df


def build_ir_diff_canonical(
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Build the canonical IR differential leaf for all FX pairs over [start_date, end_date]
    and write to R2.
    """
    print(
        f"[IR-DIFF] Building IR differentials for "
        f"{start_date.isoformat()}→{end_date.isoformat()} …"
    )

    df_y = read_parquet_from_r2(R2_IR_YIELDS_KEY)
    if df_y.empty:
        print("[IR-DIFF] WARNING: IR yields leaf is empty; nothing to build.")
        return df_y

    df_y["as_of_date"] = pd.to_datetime(df_y["as_of_date"])
    mask = (df_y["as_of_date"].dt.date >= start_date) & (
        df_y["as_of_date"].dt.date <= end_date
    )
    df_y = df_y.loc[mask].copy()

    if df_y.empty:
        print("[IR-DIFF] WARNING: No IR yields in requested date window.")
        return df_y

    # Wide matrices for policy & 10Y
    m_policy = _pivot_yields(df_y, TENOR_POLICY)
    m_10y = _pivot_yields(df_y, TENOR_10Y)

    pairs = _get_fx_pair_universe()
    rows: List[dict] = []

    for pair, base_ccy, quote_ccy in pairs:
        # Ensure both ccy exist in matrices
        missing_policy = (
            base_ccy not in m_policy.columns or quote_ccy not in m_policy.columns
        )
        missing_10y = base_ccy not in m_10y.columns or quote_ccy not in m_10y.columns

        if missing_policy and missing_10y:
            # No usable data for this pair at all
            continue

        # Align indices
        idx = m_policy.index if not m_policy.empty else m_10y.index
        idx = idx.sort_values()

        for as_of_date in idx:
            row = {
                "as_of_date": as_of_date,
                "pair": pair,
                "base_ccy": base_ccy,
                "quote_ccy": quote_ccy,
            }

            # 10Y differential
            if not m_10y.empty and base_ccy in m_10y.columns and quote_ccy in m_10y.columns:
                b10 = m_10y.at[as_of_date, base_ccy]
                q10 = m_10y.at[as_of_date, quote_ccy]
                if not (np.isnan(b10) or np.isnan(q10)):
                    row["base_10y_yield"] = float(b10)
                    row["quote_10y_yield"] = float(q10)
                    row["diff_10y_bp"] = float((b10 - q10) * 100.0)
            # Policy differential
            if (
                not m_policy.empty
                and base_ccy in m_policy.columns
                and quote_ccy in m_policy.columns
            ):
                bp = m_policy.at[as_of_date, base_ccy]
                qp = m_policy.at[as_of_date, quote_ccy]
                if not (np.isnan(bp) or np.isnan(qp)):
                    row["base_policy_rate"] = float(bp)
                    row["quote_policy_rate"] = float(qp)
                    row["diff_policy_bp"] = float((bp - qp) * 100.0)

            # Only keep rows with at least one diff populated
            if any(
                k in row
                for k in (
                    "diff_10y_bp",
                    "diff_policy_bp",
                )
            ):
                rows.append(row)

    if not rows:
        print("[IR-DIFF] WARNING: No IR differentials computed; "
              "check tenor coverage in IR yields leaf.")
        return pd.DataFrame()

    df_diff = pd.DataFrame(rows)
    df_diff["as_of_date"] = pd.to_datetime(df_diff["as_of_date"])
    df_diff["leaf_group"] = "IR"
    df_diff["leaf_name"] = "IR_DIFF_CANONICAL"
    df_diff["source_system"] = "PUBLIC_CENTRAL_BANK_API"
    df_diff["updated_at"] = pd.Timestamp.utcnow().normalize()

    df_diff = df_diff.sort_values(["as_of_date", "pair"]).reset_index(drop=True)

    print(
        "[IR-DIFF] Built canonical IR diff leaf: "
        f"rows={len(df_diff):,}, pairs={df_diff['pair'].nunique()}"
    )

    write_parquet_to_r2(df_diff, R2_IR_DIFF_KEY, index=False)
    print(
        f"[IR-DIFF] Wrote canonical IR diff leaf to R2 at {R2_IR_DIFF_KEY} "
        f"(rows={len(df_diff):,})"
    )

    return df_diff

