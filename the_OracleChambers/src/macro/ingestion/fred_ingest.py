# src/macro/fred_ingest.py

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# FRED series config
try:
    # if run as: python -m macro.fred_ingest
    from macro.fred_series_config import FRED_SERIES  # type: ignore
except ModuleNotFoundError:
    # if run as: python src/macro/fred_ingest.py
    from fred_series_config import FRED_SERIES

# FRED API key from ENV (NOT hard-coded)
FRED_API_KEY = os.environ.get("FRED_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("FRED_API_KEY environment variable is not set")




# Try to use the existing R2 writer if available
try:
    from spine_io import write_spine_parquet  # adjust import if your helper lives elsewhere
except ImportError:
    def write_spine_parquet(df: pd.DataFrame, rel_path: str) -> None:
        """
        Local fallback: write under ./data for dev / testing when R2 helper is unavailable.
        """
        out_path = Path("data") / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False)
        print(f"[Local] Wrote parquet to {out_path}")


FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def fetch_fred_series(series_id: str,
                      start: str = "1970-01-01",
                      end: str | None = None,
                      api_key: str | None = None) -> pd.DataFrame:
    """
    Fetch a single FRED series as a DataFrame: [date, value].
    """
    if api_key is None:
        api_key = os.getenv("FRED_API_KEY")

    if not api_key:
        raise RuntimeError("FRED_API_KEY environment variable is not set.")

    if end is None:
        end = datetime.today().strftime("%Y-%m-%d")

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
        "observation_end": end,
    }

    r = requests.get(FRED_BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    obs = data.get("observations", [])
    if not obs:
        print(f"⚠️ No observations for {series_id}")
        return pd.DataFrame(columns=["date", "value"])

    df = pd.DataFrame(obs)[["date", "value"]]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def main():
    START_DATE = "1970-01-01"
    END_DATE = datetime.today().strftime("%Y-%m-%d")

    all_rows = []

    print(f"[FRED] Fetching {len(FRED_SERIES)} series from {START_DATE} to {END_DATE}")

    for cfg in FRED_SERIES:
        sid = cfg["series_id"]
        name = cfg["name"]
        group = cfg["group"]

        print(f"  → Fetching {sid} ({name})...")
        df_raw = fetch_fred_series(sid, start=START_DATE, end=END_DATE)

        if df_raw.empty:
            print(f"    ⚠️ Empty series: {sid}")
            continue

        # Convert to monthly (last observation per month)
        df_m = (
            df_raw.set_index("date")
            .resample("M")
            .last()
            .reset_index()
        )
        df_m["series_id"] = sid
        df_m["series_name"] = name
        df_m["group"] = group

        all_rows.append(df_m)

    if not all_rows:
        raise RuntimeError("No FRED series returned any data. Check API key / config.")

    df_all = pd.concat(all_rows, ignore_index=True).sort_values(["series_id", "date"])

    print("[FRED] Combined monthly DataFrame:", df_all.shape)
    print(df_all.head())

    # Write raw monthly panel
    write_spine_parquet(
        df_all,
        "macro/fred_raw_monthly.parquet",  # object key / relative path (root handled outside)
    )
    print("[FRED] Wrote macro/fred_raw_monthly.parquet")


if __name__ == "__main__":
    main()
