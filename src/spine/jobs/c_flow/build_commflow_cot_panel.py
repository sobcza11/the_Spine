import io
import zipfile
from pathlib import Path

import pandas as pd
import requests


OUT_DIR = Path("data/processed/commflow")
SERVING_DIR = Path("data/serving/commflow")

YEARS = range(2020, 2027)

MAP_PATH = "config/cftc_market_map.csv"  # adjust if needed

MARKET_KEYWORDS = {
    # Commodities
    "WTI": "CRUDE OIL, LIGHT SWEET",
    "NATGAS": "NATURAL GAS",
    "GOLD": "GOLD",
    "SILVER": "SILVER",
    "COPPER": "COPPER",
    "CORN": "CORN",
    "WHEAT": "WHEAT",
    "SOYBEANS": "SOYBEANS",

    # Equity index futures
    "SPX_FUTURES": "E-MINI S&P 500",
    "NASDAQ_FUTURES": "NASDAQ-100",
    "DOW_FUTURES": "DJIA",
    "RUSSELL_FUTURES": "RUSSELL 2000",

    # Crypto futures
    "BITCOIN": "BITCOIN",
}


def fetch_year(year: int) -> pd.DataFrame:
    url = f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"
    r = requests.get(url, timeout=45)
    r.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        name = z.namelist()[0]
        return pd.read_csv(z.open(name), low_memory=False)


def map_asset(market_name: str) -> str | None:
    name = str(market_name).upper()
    for asset, keyword in MARKET_KEYWORDS.items():
        if keyword in name:
            return asset
    return None


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    SERVING_DIR.mkdir(parents=True, exist_ok=True)

    frames = []
    for year in YEARS:
        try:
            frames.append(fetch_year(year))
            print(f"OK | COT {year}")
        except Exception as exc:
            print(f"SKIP | COT {year} | {exc}")

    if not frames:
        raise RuntimeError("No COT files loaded.")

    df = pd.concat(frames, ignore_index=True)

    map_df = pd.read_csv(MAP_PATH)

    active_map = map_df[map_df["active"] == 1].copy()

    # Merge on exact market name
    df = df.merge(
        active_map[["cftc_market_name", "spine_symbol"]],
        left_on="Market_and_Exchange_Names",
        right_on="cftc_market_name",
        how="inner"
    )

    df["asset"] = df["spine_symbol"]

    df["date"] = pd.to_datetime(df["Report_Date_as_YYYY-MM-DD"])

    df["managed_money_net"] = (
        df["M_Money_Positions_Long_All"] - df["M_Money_Positions_Short_All"]
    )
    df["producer_net"] = (
        df["Prod_Merc_Positions_Long_All"] - df["Prod_Merc_Positions_Short_All"]
    )

    df["managed_money_pct_oi"] = df["managed_money_net"] / df["Open_Interest_All"]
    df["producer_pct_oi"] = df["producer_net"] / df["Open_Interest_All"]

    df = df.sort_values(["asset", "date"]).reset_index(drop=True)

    df["managed_money_pct_oi_delta"] = df.groupby("asset")["managed_money_pct_oi"].diff()

    roll = df.groupby("asset")["managed_money_pct_oi"]
    df["managed_money_z"] = (
        (df["managed_money_pct_oi"] - roll.transform(lambda x: x.rolling(156, min_periods=52).mean()))
        / roll.transform(lambda x: x.rolling(156, min_periods=52).std())
    )

    df["crowding_index"] = df["managed_money_z"].abs()

    keep = [
        "date",
        "asset",
        "Market_and_Exchange_Names",
        "Open_Interest_All",
        "managed_money_net",
        "producer_net",
        "managed_money_pct_oi",
        "producer_pct_oi",
        "managed_money_pct_oi_delta",
        "managed_money_z",
        "crowding_index",
    ]

    panel = df[keep].copy()

    panel.to_parquet(OUT_DIR / "commflow_cot_panel.parquet", index=False)
    panel.to_json(SERVING_DIR / "commflow_panel.json", orient="records", date_format="iso")

    latest = (
        panel.sort_values(["asset", "date"])
        .groupby("asset", as_index=False)
        .tail(1)
        .sort_values("asset")
    )
    latest.to_json(SERVING_DIR / "commflow_latest.json", orient="records", date_format="iso")

    print("PASS | Built CommFlow COT panel")


if __name__ == "__main__":
    main()

