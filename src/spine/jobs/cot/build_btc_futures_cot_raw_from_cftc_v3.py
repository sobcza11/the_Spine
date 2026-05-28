from pathlib import Path
import re
import urllib.request

import pandas as pd


CFTC_FINANCIAL_SHORT_URL = "https://www.cftc.gov/dea/futures/financial_lf.htm"


def fetch_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
            )
        },
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode("utf-8", errors="ignore")


def parse_int(x):
    return int(str(x).replace(",", "").strip())


def main():
    repo_root = Path.cwd()
    out_path = repo_root / "data" / "cot" / "btc_futures_cot_raw.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    text = fetch_text(CFTC_FINANCIAL_SHORT_URL)

    date_match = re.search(
        r"Positions as of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        text,
        re.IGNORECASE,
    )
    if not date_match:
        raise ValueError("Could not find report date.")

    report_date = pd.to_datetime(date_match.group(1))

    block_match = re.search(
        r"BITCOIN - CHICAGO MERCANTILE EXCHANGE.*?"
        r"CFTC Code #133741\s+Open Interest is\s+([\d,]+).*?"
        r"Positions\s+(.*?)(?:\n\s*\n|Changes from:|Percent of Open Interest)",
        text,
        re.DOTALL | re.IGNORECASE,
    )

    if not block_match:
        raise ValueError("Could not find BTC COT block.")

    open_interest = parse_int(block_match.group(1))
    nums = re.findall(r"\d[\d,]*", block_match.group(2))

    if len(nums) < 6:
        raise ValueError(f"Not enough BTC position numbers found: {nums}")

    leveraged_long = parse_int(nums[4])
    leveraged_short = parse_int(nums[5])

    out = pd.DataFrame(
        [
            {
                "date": report_date,
                "leveraged_long": leveraged_long,
                "leveraged_short": leveraged_short,
                "open_interest": open_interest,
                "source": CFTC_FINANCIAL_SHORT_URL,
                "market_code": "133741",
                "market_name": "BITCOIN - CHICAGO MERCANTILE EXCHANGE",
            }
        ]
    )

    out.to_csv(out_path, index=False)

    print("OK | BTC futures COT raw from CFTC v3")
    print(f"output: {out_path}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()

