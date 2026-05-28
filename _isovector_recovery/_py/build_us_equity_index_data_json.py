from __future__ import annotations

import json
import os
from datetime import date, timedelta
from pathlib import Path

import requests


TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "").strip()
TICKERS = ["SPY", "QQQ", "DIA", "ITOT", "MDY", "IWM"]

OUTPUT_PATH = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\serving\equities\us_equity_index_data.json"
)

LOOKBACK_DAYS = 120  # enough to give the site real history


def fetch_tiingo_daily_prices(ticker: str, start_date: str, end_date: str) -> list[dict]:
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    headers = {"Content-Type": "application/json"}
    params = {
        "token": TIINGO_API_KEY,
        "startDate": start_date,
        "endDate": end_date,
        "resampleFreq": "daily",
    }

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    rows: list[dict] = []
    for row in payload:
        date_str = str(row.get("date", ""))[:10]
        close = row.get("adjClose", row.get("close"))
        if not date_str or close is None:
            continue

        rows.append(
            {
                "symbol": ticker,
                "date": date_str,
                "close": float(close),
            }
        )

    return rows


def main() -> None:
    if not TIINGO_API_KEY:
        raise EnvironmentError("Missing TIINGO_API_KEY environment variable.")

    end_dt = date.today()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)

    all_rows: list[dict] = []
    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        rows = fetch_tiingo_daily_prices(
            ticker=ticker,
            start_date=start_dt.isoformat(),
            end_date=end_dt.isoformat(),
        )
        all_rows.extend(rows)

    all_rows.sort(key=lambda x: (x["symbol"], x["date"]))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(all_rows, f, indent=2)

    print(f"Wrote {len(all_rows)} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

    