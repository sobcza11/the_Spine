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


def _pivot_yields(df_y: pd.DataFrame, tenor: str) -> pd.DataFrame:
    """
    Take the long IR yields DF, filter to a tenor (e.g. '10Y' or 'POLICY'),
    and return a wide table indexed by as_of_date with one column per ccy.

    It is tolerant of schema changes:
    - prefers 'rate_value' if present (legacy)
    - falls back to 'rate'
    - then to 'yield'
    """
    df = df_y[df_y["tenor"] == tenor].copy()

    # Resolve which column holds the numeric rate
    if "rate_value" in df.columns:
        value_col = "rate_value"
    elif "rate" in df.columns:
        value_col = "rate"
    elif "yield" in df.columns:
        value_col = "yield"
    else:
        raise KeyError(
            "No suitable rate column found; expected one of "
            "['rate_value', 'rate', 'yield'] in IR yields leaf."
        )

    df = df[["as_of_date", "ccy", value_col]].copy()
    df = df.pivot(index="as_of_date", columns="ccy", values=value_col)
    df = df.sort_index()

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

        # Align indices: only use dates that exist in the matrices we will read from
        if not m_policy.empty and not m_10y.empty:
            # Dates that exist in BOTH policy & 10Y
            idx = m_policy.index.intersection(m_10y.index)
        elif not m_policy.empty:
            idx = m_policy.index
        else:
            idx = m_10y.index

        idx = idx.sort_values()

        for as_of_date in idx:
            row = {
                "as_of_date": as_of_date,
                "pair": pair,
                "base_ccy": base_ccy,
                "quote_ccy": quote_ccy,
            }

            # 10Y differential
            if (
                not m_10y.empty
                and base_ccy in m_10y.columns
                and quote_ccy in m_10y.columns
            ):
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
        print(
            "[IR-DIFF] WARNING: No IR differentials computed; "
            "check tenor coverage in IR yields leaf."
        )

        df_empty = pd.DataFrame(
            columns=[
                "as_of_date",
                "pair",
                "base_ccy",
                "quote_ccy",
                "base_10y_yield",
                "quote_10y_yield",
                "diff_10y_bp",
                "base_policy_rate",
                "quote_policy_rate",
                "diff_policy_bp",
                "leaf_group",
                "leaf_name",
                "source_system",
                "updated_at",
            ]
        )

        # Fill static metadata (helps validator & downstream)
        df_empty["leaf_group"] = pd.Series(dtype="object")
        df_empty["leaf_name"] = pd.Series(dtype="object")
        df_empty["source_system"] = pd.Series(dtype="object")
        df_empty["updated_at"] = pd.Series(dtype="datetime64[ns]")

        write_parquet_to_r2(df_empty, R2_IR_DIFF_KEY, index=False)
        print(
            f"[IR-DIFF] Wrote EMPTY IR diff leaf to R2 at {R2_IR_DIFF_KEY} "
            "(rows=0)"
        )
        return df_empty

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


def main() -> None:
    """
    CLI entrypoint used by spine.jobs.build_ir_diff_canonical.
    Builds IR diff canonical over the default governed window.

    Window rule:
      - Start: 2000-01-01
      - End:   today (UTC date)
    """
    start_date = dt.date(2000, 1, 1)
    end_date = dt.datetime.utcnow().date()
    build_ir_diff_canonical(start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    main()

