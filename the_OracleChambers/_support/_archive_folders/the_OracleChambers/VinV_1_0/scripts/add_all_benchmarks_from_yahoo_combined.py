"""
add_all_benchmarks_from_yahoo_combined.py

Goal:
  - For a set of benchmark ETFs (SPY, IVV, VOO, RSP, IWD, IVE, VTV, DVY, VIG, SCHD),
    try to load <TICKER>_combined.csv from data_yahoo_combined/.
  - Normalize each to ['date', 'ticker', 'price', 'dividend'].
  - Append into panel_prices_dividends.parquet (if not already present).

Any missing tickers or odd files are just logged and skipped.

Usage (from ViV_t root):

    python scripts/add_all_benchmarks_from_yahoo_combined.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PANEL_PATH = PROJECT_ROOT / "panel_prices_dividends.parquet"
COMBINED_DIR = PROJECT_ROOT / "data_yahoo_combined"

BENCH_TICKERS = [
    # Broad market
    "SPY",
    "IVV",
    "VOO",
    "RSP",
    # Value
    "IWD",
    "IVE",
    "VTV",
    # Dividend / quality
    "DVY",
    "VIG",
    "SCHD",
]


def load_ticker_from_combined(ticker: str) -> pd.DataFrame | None:
    """
    Try to load <TICKER>_combined.csv from data_yahoo_combined/ and normalize
    to ['date', 'ticker', 'price', 'dividend'].

    If the file is missing or malformed, return None and print a message.
    """
    fname = f"{ticker}_combined.csv"
    path = COMBINED_DIR / fname
    if not path.exists():
        print(f"    {ticker}: {path} not found; skipping.")
        return None

    print(f"    Loading {path} ...")
    df = pd.read_csv(path)

    if df.empty:
        print(f"    {ticker}: file is empty; skipping.")
        return None

    # Lowercased lookup
    col_map = {c.lower(): c for c in df.columns}

    # Date column
    date_col = None
    for cand in ["date", "datetime", "timestamp"]:
        if cand in col_map:
            date_col = col_map[cand]
            break
    if date_col is None:
        print(
            f"    {ticker}: no date-like column found. "
            f"Columns = {list(df.columns)}; skipping."
        )
        return None

    # Price column
    price_col = None
    for cand in ["price", "adj close", "adj_close", "close"]:
        if cand in col_map:
            price_col = col_map[cand]
            break
    if price_col is None:
        print(
            f"    {ticker}: no price-like column found. "
            "Looked for ['price', 'Adj Close', 'adj_close', 'Close']. "
            f"Columns = {list(df.columns)}; skipping."
        )
        return None

    # Dividend column (optional)
    div_col_lower = None
    for cand in ["dividend", "dividends", "cash_dividend"]:
        if cand in col_map:
            div_col_lower = cand
            break

    if div_col_lower is None:
        print(f"    {ticker}: no dividend column found; assuming zero dividends.")
        df["dividend"] = 0.0
        div_col_name = "dividend"
    else:
        div_col_name = col_map[div_col_lower]

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col]),
            "ticker": ticker,
            "price": df[price_col].astype(float),
            "dividend": df[div_col_name].fillna(0.0).astype(float),
        }
    ).sort_values("date").reset_index(drop=True)

    print(f"    {ticker}: {len(out)} daily rows loaded.")
    return out


def append_benchmarks_to_panel() -> None:
    """
    Append any missing BENCH_TICKERS into panel_prices_dividends.parquet.
    """
    if not PANEL_PATH.exists():
        raise FileNotFoundError(
            f"Daily panel not found at {PANEL_PATH}. "
            "Make sure panel_prices_dividends.parquet exists in ViV_t root."
        )

    print(f">>> Loading existing panel from {PANEL_PATH}...")
    df_panel = pd.read_parquet(PANEL_PATH)

    if not {"date", "ticker", "price", "dividend"}.issubset(df_panel.columns):
        raise ValueError(
            "panel_prices_dividends.parquet must contain "
            "['date', 'ticker', 'price', 'dividend'].\n"
            f"Found columns: {list(df_panel.columns)}"
        )

    df_panel["date"] = pd.to_datetime(df_panel["date"])
    df_panel["ticker"] = df_panel["ticker"].astype(str).str.upper()

    existing = set(df_panel["ticker"].unique())
    print(f"    Tickers already in panel (sample): {list(sorted(existing))[:10]} ...")

    frames = [df_panel]

    for t in BENCH_TICKERS:
        if t in existing:
            print(f">>> {t}: already present in panel; skipping append.")
            continue

        print(f">>> Attempting to add benchmark {t} from data_yahoo_combined...")
        df_t = load_ticker_from_combined(t)
        if df_t is None:
            print(f">>> {t}: could not be loaded; leaving it out.")
            continue

        frames.append(df_t)

    df_all = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["date", "ticker"])
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )

    df_all.to_parquet(PANEL_PATH, index=False)
    print(f">>> Updated panel saved to {PANEL_PATH} with {len(df_all)} rows.")


def main() -> None:
    append_benchmarks_to_panel()
    print(">>> Done. Now run: python scripts/run_vinv_a_levi_demo.py")


if __name__ == "__main__":
    main()

