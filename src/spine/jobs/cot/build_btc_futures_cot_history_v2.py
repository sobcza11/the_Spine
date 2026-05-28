from pathlib import Path
from io import BytesIO
import zipfile
import urllib.request

import pandas as pd


BASE_URL = "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"
BTC_CODE = "133741"


def fetch_zip_df(year: int) -> pd.DataFrame:
    url = BASE_URL.format(year=year)

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0"
        },
    )

    with urllib.request.urlopen(req, timeout=60) as response:
        raw = response.read()

    with zipfile.ZipFile(BytesIO(raw)) as z:
        names = z.namelist()
        data_files = [n for n in names if n.lower().endswith((".txt", ".csv"))]

        if not data_files:
            raise ValueError(f"No txt/csv file inside zip for {year}: {names}")

        with z.open(data_files[0]) as f:
            return pd.read_csv(f)


def find_col(df: pd.DataFrame, candidates):
    cols = list(df.columns)

    for cand in candidates:
        cand_l = cand.lower()
        for col in cols:
            if cand_l in col.lower():
                return col

    return None


def main():
    repo_root = Path.cwd()

    out_hist = repo_root / "data" / "cot" / "btc_futures_cot_history_v1.csv"
    out_raw = repo_root / "data" / "cot" / "btc_futures_cot_raw.csv"

    frames = []

    for year in range(2018, 2027):
        print(f"reading year: {year}")

        try:
            df = fetch_zip_df(year)
        except Exception as e:
            print(f"WARN | failed year {year}: {e}")
            continue

        code_col = find_col(df, ["cftc_contract_market_code", "cftc market code", "market code"])
        date_col = find_col(df, ["report_date_as_yyyy-mm-dd", "report date as yyyy-mm-dd", "report_date"])
        oi_col = find_col(df, ["open_interest_all", "open interest all"])

        long_col = find_col(df, [
            "Lev_Money_Positions_Long_All",
            "lev_money_positions_long_all",
            "leveraged_funds_positions_long_all",
            "noncomm_positions_long_all",
        ])

        short_col = find_col(df, [
            "Lev_Money_Positions_Short_All",
            "lev_money_positions_short_all",
            "leveraged_funds_positions_short_all",
            "noncomm_positions_short_all",
        ])

        if not all([code_col, date_col, oi_col, long_col, short_col]):
            print(f"WARN | missing expected columns for {year}")
            print(df.columns.tolist())
            continue

        btc = df[df[code_col].astype(str).str.strip() == BTC_CODE].copy()

        if btc.empty:
            print(f"WARN | no BTC rows for {year}")
            continue

        btc_out = btc[[date_col, long_col, short_col, oi_col]].copy()
        btc_out.columns = ["date", "leveraged_long", "leveraged_short", "open_interest"]

        frames.append(btc_out)

    if not frames:
        raise ValueError("No historical BTC COT rows found.")

    hist = pd.concat(frames, ignore_index=True)
    hist["date"] = pd.to_datetime(hist["date"])
    hist = hist.sort_values("date").drop_duplicates("date").reset_index(drop=True)

    hist.to_csv(out_hist, index=False)
    hist.to_csv(out_raw, index=False)

    print("OK | BTC futures COT history v2")
    print(f"history: {out_hist}")
    print(f"raw_for_pipeline: {out_raw}")
    print(f"rows: {len(hist)}")
    print(hist.tail(10).to_string(index=False))


if __name__ == "__main__":
    main()
