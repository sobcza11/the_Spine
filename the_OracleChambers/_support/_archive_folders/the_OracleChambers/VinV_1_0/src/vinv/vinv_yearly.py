from pathlib import Path
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------
def get_vinv_root() -> Path:
    here = Path(__file__).resolve().parents[2]
    for p in here.rglob("data_yahoo_combined"):
        if p.is_dir():
            return p.parent
    raise FileNotFoundError(
        f"Cannot locate VinV_1_0 root under {here}; missing data_yahoo_combined/"
    )


# ---------------------------------------------------------------------
def load_monthly_data():
    root = get_vinv_root()

    monthly_path = root / "vinv_monthly_panel.parquet"
    vinv_path = root / "vinv_portfolio_monthly.parquet"
    bench_path = root / "vinv_benchmarks_monthly.parquet"

    if not monthly_path.exists() or not vinv_path.exists():
        raise FileNotFoundError(
            "Monthly panel or VinV portfolio missing — run vinv_monthly.py first."
        )

    monthly = pd.read_parquet(monthly_path)
    vinv = pd.read_parquet(vinv_path)
    bench = pd.read_parquet(bench_path) if bench_path.exists() else None

    return {"monthly": monthly, "vinv": vinv, "bench": bench}


# ---------------------------------------------------------------------
def compute_annual_returns_from_monthly(monthly_series):
    monthly_series.index = pd.to_datetime(monthly_series.index)
    annual = (1 + monthly_series).groupby(monthly_series.index.year).prod() - 1
    annual.index.name = "year"
    return annual


def compute_sharpe_from_monthly(monthly_series, risk_free=0.0):
    excess = monthly_series - risk_free
    if excess.std(ddof=1) == 0:
        return np.nan
    return (excess.mean() / excess.std(ddof=1)) * np.sqrt(12)


# ---------------------------------------------------------------------
def main():
    data = load_monthly_data()
    monthly = data["monthly"]
    vinv = data["vinv"]
    bench = data["bench"]

    vinv_series = vinv.set_index("month")["vinv_monthly_ret"]
    vinv_series.index = pd.to_datetime(vinv_series.index)

    vinv_annual = compute_annual_returns_from_monthly(vinv_series)
    vinv_sharpe = compute_sharpe_from_monthly(vinv_series)

    print("========== VinV_1.0 Yearly Summary ==========")
    print((vinv_annual * 100).round(2).to_frame("VinV_annual_%"))
    print()
    print(f"VinV monthly Sharpe (rf=0): {vinv_sharpe:.2f}\n")

    if bench is None or bench.empty:
        print("No benchmark file found. Only VinV stats printed.")
        return

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
                "annual_ret_% (full sample)": (
                    (1 + annual).prod() ** (1 / len(annual)) * 100 - 100
                    if len(annual) > 0 else np.nan
                ),
                "sharpe_monthly_rf0": sharpe,
            }
        )

    print(pd.DataFrame(rows).round(2))
    print()

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
        print((aligned * 100).round(2))


if __name__ == "__main__":
    main()


