"""
QuickStart fundamentals generator for VinV_2.0

Goal:
- Take the existing vinv_returns_monthly.parquet
- Create a fully populated fundamentals panel for ALL symbols & dates
- Save to:
    data/vinv/vinv_fundamentals_monthly.parquet
    data/vinv/vinv_fundamentals_monthly_template.csv

This is *synthetic but structured* data so VinV_2.0 can run end-to-end
until real WRDS/Compustat fundamentals are plugged in.
"""

from pathlib import Path

import numpy as np
import pandas as pd


def main() -> None:
    # Resolve the_Spine root from this file (no hard-coded absolute paths)
    # __file__ = ...\the_Spine\the_OracleChambers\src\oracles\build_vinv_fundamentals_quickstart.py
    # parents[0] = ...\oracles
    # parents[1] = ...\src
    # parents[2] = ...\the_OracleChambers
    # parents[3] = ...\the_Spine   ← this is the project root we want
    spine_root = Path(__file__).resolve().parents[3]

    data_dir = spine_root / "data" / "vinv"

    returns_path = data_dir / "vinv_returns_monthly.parquet"
    fundamentals_parquet = data_dir / "vinv_fundamentals_monthly.parquet"
    fundamentals_csv = data_dir / "vinv_fundamentals_monthly_template.csv"

    print(f"[QuickStart] Loading returns from: {returns_path}")
    df_ret = pd.read_parquet(returns_path)

    # Build symbol/date grid
    df = (
        df_ret[["symbol", "date"]]
        .drop_duplicates()
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )
    df["date"] = pd.to_datetime(df["date"])

    symbols = sorted(df["symbol"].unique())
    print(f"[QuickStart] Found {len(symbols)} symbols in returns: {symbols}")

    # Stable, deterministic base values by symbol
    base_pe_map = {sym: 12.0 + i for i, sym in enumerate(symbols)}          # 12,13,14,...
    base_eps_map = {sym: 1.0 + 0.1 * i for i, sym in enumerate(symbols)}    # 1.0,1.1,1.2,...
    base_sales_map = {
        sym: 1_000.0 * (1.0 + i) for i, sym in enumerate(symbols)
    }  # 1k,2k,3k,...

    def map_symbol(map_dict: dict[str, float], col: str) -> pd.Series:
        s = df["symbol"].map(map_dict).astype(float)
        if s.isna().any():
            missing = df.loc[s.isna(), "symbol"].unique()
            raise ValueError(f"Missing mapping for symbols in {col}: {missing}")
        return s

    year = df["date"].dt.year

    # Tranche-style multipliers over time
    #   1970–1990: early value regime
    #   1990–2010: baseline
    #   2010–2025: growthier
    #   2025+    : frothier
    conds = [
        year < 1990,
        (year >= 1990) & (year < 2010),
        (year >= 2010) & (year < 2025),
        year >= 2025,
    ]

    pe_mult = np.select(conds, [0.9, 1.0, 1.1, 1.2], default=1.0)
    eps_mult = np.select(conds, [0.6, 1.0, 1.5, 2.0], default=1.0)
    sales_mult = np.select(conds, [0.5, 1.0, 2.0, 3.0], default=1.0)

    # Core fundamentals
    df["pe"] = map_symbol(base_pe_map, "pe") * pe_mult
    df["eps"] = map_symbol(base_eps_map, "eps") * eps_mult
    df["sales"] = map_symbol(base_sales_map, "sales") * sales_mult

    # Simple derived ratios
    df["peg"] = 1.0  # placeholder
    df["sales_multiple"] = df["pe"] / 10.0

    # Growth metrics (month-over-month % change * 100)
    df["eps_growth_pct"] = (
        df.groupby("symbol")["eps"].pct_change() * 100.0
    )
    df["sales_growth_pct"] = (
        df.groupby("symbol")["sales"].pct_change() * 100.0
    )

    # Net income: EPS * notional 1,000 "shares" (scale not important for z-scores)
    df["net_income"] = df["eps"] * 1_000.0

    # Column order expected by vinv_phase2
    cols = [
        "symbol",
        "date",
        "pe",
        "peg",
        "sales_multiple",
        "eps",
        "eps_growth_pct",
        "net_income",
        "sales",
        "sales_growth_pct",
    ]
    df = df[cols].sort_values(["symbol", "date"]).reset_index(drop=True)

    print("\n[QuickStart] Sample of generated fundamentals:")
    print(df.head(10))

    # Save parquet + CSV (CSV stays editable by hand if you ever want to override)
    fundamentals_parquet.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(fundamentals_parquet, index=False)
    df.to_csv(fundamentals_csv, index=False)

    print(f"\n[QuickStart] Saved fundamentals parquet → {fundamentals_parquet}")
    print(f"[QuickStart] Saved editable CSV template → {fundamentals_csv}")
    print(f"[QuickStart] Shape: {df.shape}")


if __name__ == "__main__":
    main()

    