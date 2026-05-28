from pathlib import Path
import re
import urllib.request

import pandas as pd


BASE_URL = "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"
BTC_CODE = "133741"


def fetch_year(year: int) -> pd.DataFrame:
    url = BASE_URL.format(year=year)

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
            )
        },
    )

    try:
        df = pd.read_csv(url)
    except Exception:
        try:
            df = pd.read_csv(req)
        except Exception as e:
            print(f"WARN | failed year {year}: {e}")
            return pd.DataFrame()

    return df


def find_col(df, candidates):
    normalized = {c.lower().strip(): c for c in df.columns}

    for cand in candidates:
        cand_l = cand.lower().strip()
        for key, original in normalized.items():
            if cand_l in key:
                return original

    return None


def main():
    repo_root = Path.cwd()

    out_raw = repo_root / "data" / "cot" / "btc_futures_cot_raw.csv"
    out_hist = repo_root / "data" / "cot" / "btc_futures_cot_history_v1.csv"

    out_raw.parent.mkdir(parents=True, exist_ok=True)

    frames = []

    for year in range(2018, 2027):
        print(f"reading year: {year}")
        df = fetch_year(year)

        if df.empty:
            continue

        code_col = find_col(df, ["cftc_contract_market_code", "cftc_market_code", "market_code"])
        date_col = find_col(df, ["report_date_as_yyyy-mm-dd", "report_date", "as_of_date"])
        oi_col = find_col(df, ["open_interest_all", "open interest"])

        long_col = find_col(df, ["noncomm_positions_long_all", "non-commercial long", "noncomm long"])
        short_col = find_col(df, ["noncomm_positions_short_all", "non-commercial short", "noncomm short"])

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

    print("OK | BTC futures COT history v1")
    print(f"history: {out_hist}")
    print(f"raw_for_pipeline: {out_raw}")
    print(f"rows: {len(hist)}")
    print(hist.tail(10).to_string(index=False))


if __name__ == "__main__":
    main()

