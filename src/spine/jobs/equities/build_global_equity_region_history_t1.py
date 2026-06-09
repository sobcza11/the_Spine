from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[4]

OUT_DIR = ROOT / "data" / "serving" / "equities"
OUT_JSON = OUT_DIR / "global_equity_region_history.json"

TIINGO_TOKEN = os.getenv("TIINGO_API_KEY") or os.getenv("TIINGO_TOKEN")

ETF_REGION_MAP = {
    "EXI": "Europe+",
    "EUFN": "Europe+",
    "VGK": "Europe+",

    "EWJ": "Japan",
    "AAXJ": "Asia-Pacific",
    "EWH": "Hong Kong",
    "EWA": "Australia",
    "FXI": "China Gateway",
}

HORIZONS = {
    "5D": 5,
    "15D": 15,
    "30D": 30,
    "45D": 45,
}


def fetch_tiingo_daily(symbol: str) -> pd.DataFrame:
    if not TIINGO_TOKEN:
        raise RuntimeError("Missing TIINGO_API_KEY or TIINGO_TOKEN environment variable.")

    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"

    params = {
        "token": TIINGO_TOKEN,
        "startDate": "2025-01-01",
        "format": "json",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    rows = response.json()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df["symbol"] = symbol
    df["region"] = ETF_REGION_MAP[symbol]
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df["close"] = pd.to_numeric(df["adjClose"].fillna(df["close"]), errors="coerce")

    return df[["date", "symbol", "region", "close"]].dropna()


def add_scaled_indexes(df: pd.DataFrame) -> pd.DataFrame:
    out = []

    for symbol, g in df.groupby("symbol"):
        g = g.sort_values("date").copy()

        for horizon_label, n in HORIZONS.items():
            window = g.tail(n).copy()

            if len(window) < 2:
                continue

            base = float(window.iloc[0]["close"])
            if base == 0:
                continue

            window["horizon"] = horizon_label
            window["scaled_index"] = (window["close"] / base * 100).round(4)

            out.append(window)

    if not out:
        return pd.DataFrame(
            columns=["date", "symbol", "region", "close", "horizon", "scaled_index"]
        )

    return pd.concat(out, ignore_index=True)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    frames = []

    for symbol in ETF_REGION_MAP:
        print(f"Pulling {symbol}...")
        frames.append(fetch_tiingo_daily(symbol))
        time.sleep(0.25)

    raw = pd.concat(frames, ignore_index=True)
    scaled = add_scaled_indexes(raw)

    rows = scaled.sort_values(
        ["region", "symbol", "horizon", "date"]
    ).to_dict(orient="records")

    payload = {
        "meta": {
            "name": "global_equity_region_history",
            "source": "Tiingo",
            "method": "ETF daily close scaled to 100 from View Horizon start point.",
            "rows": len(rows),
            "unique_dates": int(scaled["date"].nunique()) if len(scaled) else 0,
            "symbols": sorted(ETF_REGION_MAP.keys()),
            "horizons": list(HORIZONS.keys()),
        },
        "rows": rows,
    }

    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    tail = scaled.sort_values(["region", "symbol", "horizon", "date"]).tail(5)

    print("OK | Global Equity ETF indexes scaled from View Horizon start points")
    print(tail[["date", "symbol", "region", "horizon", "close", "scaled_index"]].to_string(index=False))
    print(f"rows: {len(rows)} | unique dates: {payload['meta']['unique_dates']}")
    print(f"wrote: {OUT_JSON}")


if __name__ == "__main__":
    main()