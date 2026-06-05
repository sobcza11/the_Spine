from pathlib import Path
import os
import requests
import pandas as pd

REPO_ROOT = Path.cwd()
RAW_DIR = REPO_ROOT / "data" / "fx" / "fx_depth" / "raw"

TIINGO_API_KEY = os.environ["TIINGO_API_KEY"]

CONFIG = [
    {
        "ticker": "EWU",
        "out": RAW_DIR / "ftse_proxy.parquet",
        "label": "FTSE proxy | EWU",
    },
    {
        "ticker": "SPY",
        "out": RAW_DIR / "spx_proxy.parquet",
        "label": "S&P 500 proxy | SPY",
    },
]


def pull_tiingo_daily(ticker: str) -> pd.DataFrame:
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"

    params = {
        "startDate": "2015-01-01",
        "format": "json",
        "token": TIINGO_API_KEY,
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    df = pd.DataFrame(r.json())

    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["value"] = pd.to_numeric(df["adjClose"], errors="coerce")

    return (
        df[["date", "value"]]
        .dropna()
        .sort_values("date")
    )


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    for cfg in CONFIG:
        df = pull_tiingo_daily(cfg["ticker"])
        df.to_parquet(cfg["out"], index=False)

        print(
            f"SAVED: {cfg['label']} | "
            f"{cfg['out']} | rows={len(df)} | as_of={df['date'].max().date()}"
        )


if __name__ == "__main__":
    main()

    