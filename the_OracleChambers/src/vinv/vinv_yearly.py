# src/vinv/vinv_yearly.py

from typing import Dict

import numpy as np
import pandas as pd

from pathlib import Path

def get_vinv_root() -> Path:
    here = Path(__file__).resolve().parents[2]  # the_OracleChambers

    for p in here.rglob("data_yahoo_combined"):
        if p.is_dir():
            return p.parent

    raise FileNotFoundError(
        f"Could not find a VinV root containing 'data_yahoo_combined' under {here}. "
        "Ensure VinV_1_0 has that folder."
    )


def load_monthly_data() -> Dict[str, pd.DataFrame]:
    root = get_vinv_root()
    panel_path = root / "vinv_monthly_panel.parquet"
    vinv_path = root / "vinv_portfolio_monthly.parquet"
    bench_path = root / "vinv_benchmarks_monthly.parquet"

    if not panel_path.exists() or not vinv_path.exists():
        raise FileNotFoundError(
            "Monthly panel or VinV portfolio missing. "
            "Run vinv_monthly.py first."
        )

    monthly = pd.read_parquet(panel_path)
    vinv = pd.read_parquet(vinv_path)

    bench = None
    if bench_path.exists():
        bench = pd.read_parquet(bench_path)

    return {"monthly": monthly, "vinv": vinv, "bench": bench}


def compute_annual_returns_from_monthly(
    monthly_ret: pd.Series,
) -> pd.Series:
    """
    Given a monthly return series (indexed by month), compute annual returns.

    annual_ret = product(1 + monthly_ret_in_year) - 1
    """
    monthly_ret = monthly_ret.copy()
    monthly_ret.index = pd.to_datetime(monthly_ret.index)

    annual = (1.0 + monthly_ret).groupby(monthly_ret.index.year).prod() - 1.0
    annual.index.name = "year"
    return annual


def compute_sharpe_from_monthly(
    monthly_ret: pd.Series,
    risk_free_monthly: float = 0.0,
) -> float:
    """
    Simple Sharpe ratio based on monthly returns:
    Sharpe = mean(excess) / std(excess) * sqrt(12)
    """
    excess = monthly_ret - risk_free_monthly
    if excess.std(ddof=1) == 0:
        return np.nan
    return (excess.mean() / excess.std(ddof=1)) * np.sqrt(12.0)


def main():
    data = load_monthly_data()
    monthly = data["monthly"]
    vinv = data["vinv"]
    bench = data["bench"]

    # Prepare VinV monthly series
    vinv_series = vinv.set_index("month")["vinv_monthly_ret"]
    vinv_series.index = pd.to_datetime(vinv_series.index)

    vinv_annual = compute_annual_returns_from_monthly(vinv_series)
    vinv_sharpe = compute_sharpe_from_monthly(vinv_series)

    print("========== VinV_1.0 Yearly Summary ==========")
    print("VinV annual returns:")
    print((vinv_annual * 100).round(2).to_frame("VinV_annual_%"))
    print()
    print(f"VinV monthly Sharpe (rf=0): {vinv_sharpe:.2f}")
    print()

    if bench is not None and not bench.empty:
        # Benchmarks: one row per ticker per month
        bench = bench.copy()
        bench["month"] = pd.to_datetime(bench["month"])
        bench["ticker"] = bench["ticker"].str.upper()

        tickers = sorted(bench["ticker"].unique())
        print("========== Benchmark Comparison ==========")
        rows = []
        for t in tickers:
            s = (
                bench[bench["ticker"] == t]
                .set_index("month")["monthly_ret"]
                .sort_index()
            )
            annual = compute_annual_returns_from_monthly(s)
            sharpe = compute_sharpe_from_monthly(s)

            rows.append(
                {
                    "ticker": t,
                    "annual_ret_% (full sample)": (1.0 + annual).prod() ** (1 / len(annual)) * 100 - 100
                    if len(annual) > 0 else np.nan,
                    "sharpe_monthly_rf0": sharpe,
                }
            )

        bench_summary = pd.DataFrame(rows)
        print(bench_summary.round(2))
        print()

        # Simple VinV vs benchmark annual excess
        # (based on overlapping years only)
        common_years = vinv_annual.index
        print("========== VinV vs Benchmarks (Overlapping Years) ==========")
        for t in tickers:
            s = (
                bench[bench["ticker"] == t]
                .set_index("month")["monthly_ret"]
                .sort_index()
            )
            annual = compute_annual_returns_from_monthly(s)
            aligned = pd.concat(
                [vinv_annual, annual.reindex(vinv_annual.index)],
                axis=1,
                keys=["VinV", t],
            ).dropna()

            if aligned.empty:
                continue

            aligned["excess"] = aligned["VinV"] - aligned[t]
            print(f"\n--- {t} ---")
            print((aligned[["VinV", t, "excess"]] * 100).round(2))
    else:
        print("No benchmark file found. Only VinV_1.0 stats printed.")


if __name__ == "__main__":
    main()

