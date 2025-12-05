"""
VinV 2.0: Free Data Downloader for Core Macro / Factor Signals

Signals included (10):
1) Commitment of Traders (COT) - S&P 500 futures leveraged funds positioning
2) 10Y / 2Y yield curve
3) ISM Manufacturing PMI
4) ISM Manufacturing New Orders
5) Federal Reserve Balance Sheet
6) Corporate Profits
7) U. of Michigan Consumer Sentiment
8) Real M2
9) Wholesale Inventories
10) Durable Goods Orders

Requirements:
    pip install pandas pandas-datareader requests

Notes:
- All macro series use FRED via pandas-datareader.
- COT uses the CFTC weekly financial futures file.
- Adjust market_name in get_cot_spx_lev_funds() if you want a different contract.
"""

import sys
import datetime as dt
from typing import Dict

import pandas as pd
import requests
from pandas_datareader import data as web


# -----------------------------
#  FRED SERIES CONFIG
# -----------------------------

FRED_SERIES = {
    # Key: our label, Value: FRED series ID
    "10y_2y_curve": "T10Y2Y",                # 10Y minus 2Y Treasury
    "fed_balance_sheet": "WALCL",            # Federal Reserve Total Assets
    "corp_profits": "CP",                    # Corporate Profits After Tax
    "umich_sentiment": "UMCSENT",            # U. Michigan Consumer Sentiment
    "real_m2": "M2REAL",                     # Real M2 Money Stock
    "wholesale_inventories": "WHLSLRIMSA",   # Wholesaler Inventories
    "durable_goods_orders": "DGORDER",       # Durable Goods Orders

    # ISM Manufacturing PMI & New Orders
    "ism_manu_pmi": "NAPM",                  # ISM Manufacturing PMI
    "ism_manu_new_orders": "NAPMNOI",        # ISM Manufacturing New Orders
}


def fetch_fred_series(
    series_map: Dict[str, str],
    start: str = "1990-01-01",
    end: str | None = None,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch multiple FRED series using pandas-datareader.

    Parameters
    ----------
    series_map : dict
        Mapping from our label -> FRED series ID.
    start : str
        Start date (YYYY-MM-DD).
    end : str or None
        End date. If None, today's date.

    Returns
    -------
    dict
        label -> DataFrame with a single column (the FRED series).
    """
    if end is None:
        end = dt.date.today().strftime("%Y-%m-%d")

    out: Dict[str, pd.DataFrame] = {}
    for label, fred_id in series_map.items():
        print(f"[FRED] Fetching {label} ({fred_id})...")
        s = web.DataReader(fred_id, "fred", start, end)
        s = s.rename(columns={fred_id: label})
        out[label] = s
    return out


# -----------------------------
#  COT (Commitment of Traders)
# -----------------------------

def get_cot_spx_lev_funds(
    url: str = "https://www.cftc.gov/dea/newcot/FinFutWk.txt",
    market_name: str = "S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
) -> pd.DataFrame:
    """
    Fetch weekly COT data for a specific financial futures market (e.g. S&P 500)
    from the CFTC FinFutWk.txt file.

    This parser is intentionally simple:
    - Reads the fixed-width text file
    - Filters rows matching the given market_name
    - Extracts report date & leveraged funds net positions (where available)

    Parameters
    ----------
    url : str
        URL of the CFTC weekly financial futures report.
    market_name : str
        Exact market name as appears in the file.

    Returns
    -------
    DataFrame
        Indexed by date, with columns for leveraged funds long, short, and net.
    """
    print(f"[COT] Downloading financial futures file from {url} ...")
    r = requests.get(url)
    r.raise_for_status()

    raw_text = r.text.splitlines()
    records: list[dict] = []

    for line in raw_text:
        if market_name in line:
            # First 10 chars usually date (MM/DD/YY)
            date_str = line[0:10].strip()
            try:
                report_date = dt.datetime.strptime(date_str, "%m/%d/%y").date()
            except ValueError:
                continue

            # Approximate leveraged money manager long/short location.
            # These column slices may need adjustment if CFTC layout changes.
            try:
                lev_long = int(line[110:123].strip())
                lev_short = int(line[123:136].strip())
            except ValueError:
                continue

            records.append(
                {
                    "date": report_date,
                    "lev_long": lev_long,
                    "lev_short": lev_short,
                    "lev_net": lev_long - lev_short,
                }
            )

    if not records:
        raise ValueError(
            f"No records found for market_name='{market_name}'. "
            f"Open the COT file in a text editor to confirm the exact label/format."
        )

    df = pd.DataFrame(records).sort_values("date")
    df = df.set_index("date")
    df.index = pd.to_datetime(df.index)
    df.columns = ["cot_lev_long", "cot_lev_short", "cot_lev_net"]

    print(f"[COT] Parsed {len(df)} weekly rows for {market_name}")
    return df


# -----------------------------
#  MAIN AGGREGATOR
# -----------------------------

def get_vinv2_signals(
    start: str = "1990-01-01",
    end: str | None = None,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch all 10 VinV 2.0 macro/factor signals as a dict of DataFrames.

    Returns
    -------
    dict
        Keys:
            - "cot_spx_lev_funds"
            - all keys from FRED_SERIES
    """
    if end is None:
        end = dt.date.today().strftime("%Y-%m-%d")

    signals: Dict[str, pd.DataFrame] = {}

    # 1) COT (S&P 500 stock index futures, leveraged funds)
    try:
        cot_df = get_cot_spx_lev_funds()
        cot_df = cot_df.loc[(cot_df.index >= start) & (cot_df.index <= end)]
        signals["cot_spx_lev_funds"] = cot_df
    except Exception as e:
        print(f"[WARN] Failed to fetch/parse COT data: {e}", file=sys.stderr)

    # 2–10) FRED series
    fred_signals = fetch_fred_series(FRED_SERIES, start=start, end=end)
    signals.update(fred_signals)

    return signals


def save_signals_to_csv(
    signals: Dict[str, pd.DataFrame],
    out_dir: str = "vinv2_data",
) -> None:
    """
    Save each signal DataFrame to a separate CSV file.

    Parameters
    ----------
    signals : dict
        label -> DataFrame
    out_dir : str
        Directory to save CSV files to.
    """
    import os

    os.makedirs(out_dir, exist_ok=True)
    for label, df in signals.items():
        path = os.path.join(out_dir, f"{label}.csv")
        print(f"[SAVE] {label} -> {path}")
        df.to_csv(path)


if __name__ == "__main__":
    # Usage examples:
    #   python vinv2_signals_download.py
    #   python vinv2_signals_download.py 1995-01-01 2025-12-31

    if len(sys.argv) >= 2:
        start_date = sys.argv[1]
    else:
        start_date = "1990-01-01"

    if len(sys.argv) >= 3:
        end_date = sys.argv[2]
    else:
        end_date = None

    print(f"Fetching VinV 2.0 signals from {start_date} to {end_date or 'today'}...")
    sigs = get_vinv2_signals(start=start_date, end=end_date)
    save_signals_to_csv(sigs)
    print("Done.")

