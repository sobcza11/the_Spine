from pathlib import Path
from typing import Optional, List

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------
# ROOT DISCOVERY (dynamic)
# ---------------------------------------------------------------------
def get_vinv_root() -> Path:
    """
    Find VinV_1_0 root dynamically by searching for data_yahoo_combined/.
    """
    here = Path(__file__).resolve().parents[2]

    for p in here.rglob("data_yahoo_combined"):
        if p.is_dir():
            return p.parent

    raise FileNotFoundError(
        f"Could not find a VinV root containing 'data_yahoo_combined' under {here}."
    )


# ---------------------------------------------------------------------
# SCHEMA NORMALIZER
# ---------------------------------------------------------------------
def normalize_price_dividend_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure columns: date, ticker, adj_close, dividends.
    Supports real-world Yahoo variations.
    """
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # --- Date ---
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # --- Adjusted Close / Price ---
    adj_candidates = [
        "adj_close", "adj close", "adj close*", "adjclose",
        "price",    # your scrape output
        "close",
    ]

    adj_src = next((c for c in adj_candidates if c in df.columns), None)
    if adj_src is None:
        raise KeyError(
            f"Could not find adjusted-close-like column among {df.columns}."
        )
    df["adj_close"] = df[adj_src].astype(float)

    # --- Dividends ---
    div_candidates = ["dividends", "dividend", "dividends*", "div", "dist"]
    div_src = next((c for c in div_candidates if c in df.columns), None)

    if div_src is not None:
        df["dividends"] = df[div_src].fillna(0.0).astype(float)
    else:
        df["dividends"] = 0.0

    # --- Ticker ---
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.upper()

    return df


# ---------------------------------------------------------------------
# LOAD DAILY PANEL (build from CSV if parquet missing)
# ---------------------------------------------------------------------
def load_daily_panel(panel_path: Optional[Path] = None) -> pd.DataFrame:
    root = get_vinv_root()

    if panel_path is None:
        panel_path = root / "panel_prices_dividends.parquet"

    if panel_path.exists():
        df = pd.read_parquet(panel_path)
        return normalize_price_dividend_columns(df)

    combined_dir = root / "data_yahoo_combined"
    combined_files = sorted(combined_dir.glob("*.csv"))
    if not combined_files:
        raise FileNotFoundError(
            f"No parquet at {panel_path} & no CSVs in {combined_dir}."
        )

    frames = []
    for f in combined_files:
        ticker = f.stem.replace("_combined", "").upper()
        raw = pd.read_csv(f)

        # Normalize early fields
        raw.columns = [c.lower() for c in raw.columns]
        raw["ticker"] = ticker

        # rename possible variants
        rename_map = {}
        if "adj close" in raw.columns:
            rename_map["adj close"] = "adj_close"
        if "adj close*" in raw.columns:
            rename_map["adj close*"] = "adj_close"
        if "price" in raw.columns:
            rename_map["price"] = "adj_close"
        if "dividend" in raw.columns:
            rename_map["dividend"] = "dividends"

        raw = raw.rename(columns=rename_map)

        # minimal subset
        keep_cols = [c for c in ["date", "adj_close", "dividends", "ticker"] if c in raw.columns]
        df = raw[keep_cols].copy()

        frames.append(df)

    out = pd.concat(frames, ignore_index=True)
    out = normalize_price_dividend_columns(out)
    out.sort_values(["ticker", "date"], inplace=True)

    panel_path.parent.mkdir(exist_ok=True, parents=True)
    out.to_parquet(panel_path, index=False)

    return out


# ---------------------------------------------------------------------
# DAILY TOTAL RETURNS
# ---------------------------------------------------------------------
def compute_daily_total_return(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.sort_values(["ticker", "date"], inplace=True)

    def _calc(group):
        group = group.copy()
        group["adj_close"] = group["adj_close"].astype(float)
        group["dividends"] = group["dividends"].astype(float).fillna(0.0)

        prev = group["adj_close"].shift(1)
        price_ret = group["adj_close"].pct_change(fill_method=None)
        div_ret = group["dividends"] / prev.replace(0, np.nan)

        group["daily_total_ret"] = price_ret.fillna(0.0) + div_ret.fillna(0.0)
        return group

    return df.groupby("ticker", group_keys=False).apply(_calc)


# ---------------------------------------------------------------------
# MONTHLY RETURNS
# ---------------------------------------------------------------------
def compute_monthly_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.set_index("date", inplace=True)

    def _resample(g):
        out = (1.0 + g["daily_total_ret"]).resample("ME").prod() - 1.0
        return out.to_frame("monthly_ret")

    monthly = df.groupby("ticker", group_keys=True).apply(_resample)
    monthly = monthly.reset_index().rename(columns={"date": "month"})
    monthly.sort_values(["ticker", "month"], inplace=True)
    return monthly


# ---------------------------------------------------------------------
# LOAD UNIVERSE
# ---------------------------------------------------------------------
def load_vinv_universe(root: Optional[Path] = None) -> pd.DataFrame:
    if root is None:
        root = get_vinv_root()

    uni = root / "vinv_universe.csv"
    if not uni.exists():
        return pd.DataFrame(columns=["ticker"])

    df = pd.read_csv(uni)
    df.columns = [c.lower() for c in df.columns]
    df["ticker"] = df["ticker"].str.upper()
    return df


# ---------------------------------------------------------------------
# VINV PORTFOLIO
# ---------------------------------------------------------------------
def compute_vinv_portfolio(monthly, vinv_universe) -> pd.DataFrame:
    monthly = monthly.copy()
    monthly["ticker"] = monthly["ticker"].str.upper()

    if not vinv_universe.empty:
        eligible = set(vinv_universe["ticker"].unique())
        monthly = monthly[monthly["ticker"].isin(eligible)]

    if monthly.empty:
        raise ValueError("No eligible tickers for VinV_1_0 portfolio.")

    vinv = (
        monthly.groupby("month")["monthly_ret"]
        .mean()
        .to_frame("vinv_monthly_ret")
        .reset_index()
    )
    return vinv


# ---------------------------------------------------------------------
# SAVE OUTPUTS
# ---------------------------------------------------------------------
def save_outputs(monthly, vinv_monthly, benchmark_tickers=None):
    root = get_vinv_root()

    (root / "vinv_monthly_panel.parquet").write_bytes(
        monthly.to_parquet(index=False)
    )
    vinv_monthly.to_parquet(root / "vinv_portfolio_monthly.parquet", index=False)

    if benchmark_tickers:
        m = monthly.copy()
        m["ticker"] = m["ticker"].str.upper()
        bench = m[m["ticker"].isin([t.upper() for t in benchmark_tickers])]
        bench.to_parquet(root / "vinv_benchmarks_monthly.parquet", index=False)


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------
def main():
    root = get_vinv_root()
    print(f"[INFO] VinV_1_0 root: {root}")

    print("[STEP] Loading daily panel...")
    df_daily = load_daily_panel()

    print("[STEP] Computing daily total returns...")
    df_daily = compute_daily_total_return(df_daily)

    print("[STEP] Aggregating to monthly returns...")
    monthly = compute_monthly_returns(df_daily)

    print("[STEP] Loading VinV universe...")
    uni = load_vinv_universe(root=root)

    print("[STEP] Computing VinV_1_0 portfolio...")
    vinv_monthly = compute_vinv_portfolio(monthly, uni)

    print("[STEP] Saving outputs...")
    benchmark_tickers = [
        "IVV",  # S&P 500 (iShares) - institutional anchor
        "VOO",  # S&P 500 (Vanguard) - retail/FinTech anchor
        "IWD",  # Russell 1000 Value
        "VTV",  # Vanguard Value (CRSP)
        "IVE",  # S&P 500 Value (iShares)
        "VIG",  # Dividend Appreciation (quality-dividend)
        "DVY",  # High dividend yield
        "SCHD", # Dividend/value hybrid, inflation-resilient
    ]
    save_outputs(monthly, vinv_monthly, benchmark_tickers)

    print(
        "[DONE] vinv_monthly_panel.parquet, vinv_portfolio_monthly.parquet "
        "& vinv_benchmarks_monthly.parquet created."
    )


if __name__ == "__main__":
    main()

