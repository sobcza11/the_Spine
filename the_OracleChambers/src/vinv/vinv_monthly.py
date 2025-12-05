# src/vinv/vinv_monthly.py

from typing import Optional, List

import numpy as np
import pandas as pd

from pathlib import Path

def normalize_price_dividend_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names so we always have:
      - 'date'
      - 'ticker'
      - 'adj_close'
      - 'dividends'

    Handles common Yahoo variations like 'Adj Close', 'Adj Close*', etc.
    """
    df = df.copy()
    # Lowercase all column names for matching
    df.columns = [c.lower() for c in df.columns]

    # Date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    else:
        # Try common variants
        for cand in ["timestamp", "dt"]:
            if cand in df.columns:
                df["date"] = pd.to_datetime(df[cand])
                break

    # Adj close candidate list
    adj_candidates = [
        "adj_close",
        "adj close",
        "adj close*",
        "adjclose",
        "price",          # your current schema
        "close",          # generic fallback
    ]

    adj_src = None
    for c in adj_candidates:
        if c in df.columns:
            adj_src = c
            break

    if adj_src is None:
        raise KeyError(
            f"Could not find any adjusted-close like column in {df.columns}."
        )

    df["adj_close"] = df[adj_src].astype(float)

    # Dividends
    div_candidates = [
        "dividends",
        "dividend",
        "dividends*",
        "div",           # just in case
    ]

    div_src = None
    for c in div_candidates:
        if c in df.columns:
            div_src = c
            break

    if div_src is not None:
        df["dividends"] = df[div_src].fillna(0.0).astype(float)
    else:
        df["dividends"] = 0.0

    # Ticker
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.upper()

    return df

def get_vinv_root() -> Path:
    """
    Discover the VinV_1_0 root dynamically.

    Strategy:
    - Start from two levels up (the_OracleChambers)
    - Walk all subdirectories recursively
    - Return the parent directory of the first 'data_yahoo_combined' folder
    """
    here = Path(__file__).resolve().parents[2]  # the_OracleChambers

    for p in here.rglob("data_yahoo_combined"):
        if p.is_dir():
            return p.parent

    raise FileNotFoundError(
        f"Could not find a VinV root containing 'data_yahoo_combined' under {here}. "
        "Ensure your scrape output folder is named exactly 'data_yahoo_combined' & "
        "lives inside VinV_1_0 (or whatever the VinV folder is called)."
    )


def load_daily_panel(panel_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Load the daily price & dividend panel.

    If panel_prices_dividends.parquet does not exist yet, this function
    will build it from data_yahoo_combined/*.csv with a simple schema
    assumption: each combined file has Date, Adj Close, Dividends columns.
    """
    root = get_vinv_root()

    if panel_path is None:
        panel_path = root / "panel_prices_dividends.parquet"

    if panel_path.exists():
        df = pd.read_parquet(panel_path)
        df = normalize_price_dividend_columns(df)
        return df

    # Build from CSVs if parquet is missing
    combined_dir = root / "data_yahoo_combined"
    combined_files = sorted(combined_dir.glob("*.csv"))
    if not combined_files:
        raise FileNotFoundError(
            f"No parquet at {panel_path} & no CSVs in {combined_dir}. "
            "Run the scrape step first."
        )

    records = []
    for f in combined_files:
        ticker = f.stem.replace("_combined", "").upper()
        tmp = pd.read_csv(f)
        # Try to be robust to Yahoo column names
        cols = {c.lower(): c for c in tmp.columns}

        date_col = cols.get("date")
        adj_col = cols.get("adj close") or cols.get("adj_close")
        div_col = cols.get("dividends") or cols.get("dividend")

        if date_col is None or adj_col is None:
            raise ValueError(
                f"File {f} missing required columns. "
                f"Found: {list(tmp.columns)}"
            )

        tmp = tmp[[date_col, adj_col] + ([div_col] if div_col is not None else [])]
        tmp.columns = ["date", "adj_close"] + (["dividends"] if div_col is not None else [])
        tmp["ticker"] = ticker
        if "dividends" not in tmp.columns:
            tmp["dividends"] = 0.0

        records.append(tmp)

    df = pd.concat(records, ignore_index=True)
    df = normalize_price_dividend_columns(df)
    df.sort_values(["ticker", "date"], inplace=True)

    panel_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(panel_path, index=False)

    return df


def compute_daily_total_return(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily total return per ticker using price + cash dividends.

    total_return = adj_close.pct_change() + dividends / adj_close.shift(1)

    Assumes:
    - df has columns: ticker, date, adj_close, dividends
    """
    df = df.copy()
    df.sort_values(["ticker", "date"], inplace=True)

    def _per_ticker(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        group["adj_close"] = group["adj_close"].astype(float)
        group["dividends"] = group["dividends"].fillna(0.0).astype(float)

        prev_price = group["adj_close"].shift(1)
        price_ret = group["adj_close"].pct_change()
        div_ret = group["dividends"] / prev_price.replace(0, np.nan)

        group["daily_total_ret"] = price_ret.fillna(0.0) + div_ret.fillna(0.0)
        return group

    df = df.groupby("ticker", group_keys=False).apply(_per_ticker)
    return df


def compute_monthly_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resample daily total returns to monthly total returns per ticker.

    monthly_ret = product(1 + daily_total_ret) - 1
    """
    df = df.copy()
    df.set_index("date", inplace=True)

    def _per_ticker(group: pd.DataFrame) -> pd.DataFrame:
        out = (1.0 + group["daily_total_ret"]).resample("M").prod() - 1.0
        return out.to_frame("monthly_ret")

    monthly = df.groupby("ticker", group_keys=True).apply(_per_ticker)
    monthly = monthly.reset_index().rename(columns={"date": "month"})
    monthly.sort_values(["ticker", "month"], inplace=True)
    return monthly


def load_vinv_universe(root: Optional[Path] = None) -> pd.DataFrame:
    """
    Load vinv_universe.csv if present.

    We assume at minimum a 'ticker' column.
    Optionally a flag column like 'vinv_flag' or 'include_in_vinv'.
    If no flag is present, we include all tickers that appear in the file.
    """
    if root is None:
        root = get_vinv_root()

    uni_path = root / "vinv_universe.csv"
    if not uni_path.exists():
        # Fallback: return empty -> caller will use all tickers
        return pd.DataFrame(columns=["ticker"])

    uni = pd.read_csv(uni_path)
    uni.columns = [c.lower() for c in uni.columns]
    if "ticker" not in uni.columns:
        raise ValueError("vinv_universe.csv must contain a 'ticker' column.")
    uni["ticker"] = uni["ticker"].str.upper()
    return uni


def compute_vinv_portfolio(
    monthly: pd.DataFrame,
    vinv_universe: pd.DataFrame,
    flag_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Compute the VinV_1.0 equal-weight portfolio return by month.

    Logic:
    - Determine eligible tickers using vinv_universe & optional flag columns.
    - For each month, take the simple average of eligible ticker monthly_ret.
    """
    monthly = monthly.copy()
    monthly["ticker"] = monthly["ticker"].str.upper()

    # Determine eligible tickers
    if flag_columns is None:
        flag_columns = ["vinv_flag", "include_in_vinv", "is_in_vinv"]

    if not vinv_universe.empty:
        eligible = vinv_universe.copy()
        eligible["ticker"] = eligible["ticker"].str.upper()

        # If there is a flag column, filter on it.
        flag_col = None
        for c in flag_columns:
            if c in eligible.columns:
                flag_col = c
                break

        if flag_col is not None:
            eligible = eligible[eligible[flag_col].astype(bool)]

        eligible_tickers = set(eligible["ticker"])
        monthly = monthly[monthly["ticker"].isin(eligible_tickers)]

    # If vinv_universe is empty or no eligible tickers found,
    # VinV uses the full universe as a simple default.
    if monthly.empty:
        raise ValueError(
            "No eligible tickers for VinV portfolio. "
            "Check vinv_universe.csv or flag columns."
        )

    vinv = (
        monthly.groupby("month")["monthly_ret"]
        .mean()
        .to_frame("vinv_monthly_ret")
        .reset_index()
    )

    return vinv


def save_outputs(
    monthly: pd.DataFrame,
    vinv_monthly: pd.DataFrame,
    benchmark_tickers: Optional[List[str]] = None,
) -> None:
    """
    Save:
    - full monthly panel: vinv_monthly_panel.parquet
    - VinV portfolio only: vinv_portfolio_monthly.parquet
    - benchmark subset: vinv_benchmarks_monthly.parquet (if tickers provided)
    """
    root = get_vinv_root()

    monthly_path = root / "vinv_monthly_panel.parquet"
    vinv_path = root / "vinv_portfolio_monthly.parquet"
    bench_path = root / "vinv_benchmarks_monthly.parquet"

    monthly.to_parquet(monthly_path, index=False)
    vinv_monthly.to_parquet(vinv_path, index=False)

    if benchmark_tickers:
        m = monthly.copy()
        m["ticker"] = m["ticker"].str.upper()
        bench = m[m["ticker"].isin([t.upper() for t in benchmark_tickers])]
        bench.to_parquet(bench_path, index=False)


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
    vinv_universe = load_vinv_universe(root=root)

    print("[STEP] Computing VinV_1_0 portfolio...")
    vinv_monthly = compute_vinv_portfolio(monthly, vinv_universe)

    print("[STEP] Saving outputs...")
    benchmark_tickers = ["IVV", "IWD", "DVY", "VOO", "VTV", "IVE", "VIG", "SCHD"]  # extend as needed
    save_outputs(monthly, vinv_monthly, benchmark_tickers=benchmark_tickers)

    print("[DONE] vinv_monthly_panel.parquet, vinv_portfolio_monthly.parquet "
          "& vinv_benchmarks_monthly.parquet created.")


if __name__ == "__main__":
    main()

