import os
from pathlib import Path

import pandas as pd
import requests


FRED_API_KEY = os.getenv("FRED_API_KEY")

if not FRED_API_KEY:
    raise RuntimeError("Missing FRED_API_KEY environment variable.")


SERIES = {
    "y2": "DGS2",
    "y5": "DGS5",
    "y10": "DGS10",
    "y30": "DGS30",
    "effr": "EFFR",
    "real_y10": "DFII10",
    "term_premium_10y": "THREEFYTP10",

    "eu_y10": "IRLTLT01EZM156N",
    "jp_y10": "IRLTLT01JPM156N",
    "uk_y10": "IRLTLT01GBM156N",
    "cn_bis_proxy": "RBCNBIS",
    "cn_policy_pressure": "INTDSRCNM193N",
}


OUT_DIR = Path("data/rates/tier1")
SERVE_DIR = Path("data/rates/serving")
OUT_DIR.mkdir(parents=True, exist_ok=True)
SERVE_DIR.mkdir(parents=True, exist_ok=True)


def fetch_fred_series(series_id: str) -> pd.DataFrame:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()

    rows = r.json().get("observations", [])
    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=["date", series_id])

    df = df[["date", "value"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")
    df = df.rename(columns={"value": series_id})
    return df


def main() -> None:
    frames = []

    for label, series_id in SERIES.items():
        print(f"Fetching {label}: {series_id}")
        df = fetch_fred_series(series_id)
        df = df.rename(columns={series_id: label})
        frames.append(df)

    panel = frames[0]

    for df in frames[1:]:
        panel = panel.merge(df, on="date", how="outer")

    panel = panel.sort_values("date").reset_index(drop=True)

    value_cols = [c for c in panel.columns if c != "date"]
    panel[value_cols] = panel[value_cols].ffill()

    panel["spread_10_2"] = panel["y10"] - panel["y2"]
    panel["spread_30_5"] = panel["y30"] - panel["y5"]
    panel["policy_gap_2y_effr"] = panel["y2"] - panel["effr"]

    panel["us_eu_10y_spread"] = panel["y10"] - panel["eu_y10"]
    panel["us_jp_10y_spread"] = panel["y10"] - panel["jp_y10"]
    panel["us_uk_10y_spread"] = panel["y10"] - panel["uk_y10"]

    panel = panel.dropna(subset=["y2", "y5", "y10", "y30"]).reset_index(drop=True)

    latest = panel.tail(1).to_dict(orient="records")[0]
    latest["date"] = latest["date"].strftime("%Y-%m-%d")

    panel_out = OUT_DIR / "rates_tier1_panel.parquet"
    latest_out = SERVE_DIR / "rates_tier1_latest.json"
    json_out = SERVE_DIR / "rates_tier1_panel.json"

    panel.to_parquet(panel_out, index=False)
    panel.tail(750).assign(date=lambda x: x["date"].dt.strftime("%Y-%m-%d")).to_json(
        json_out,
        orient="records",
        indent=2,
    )

    pd.Series(latest).to_json(latest_out, indent=2)

    print("PASS")
    print(f"Panel: {panel_out}")
    print(f"Serving JSON: {json_out}")
    print(f"Latest JSON: {latest_out}")
    print(latest)


if __name__ == "__main__":
    main()