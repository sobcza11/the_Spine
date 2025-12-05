"""
Fetch Block 4 raw files from CFTC (Legacy Futures Only) for 1986–1989.

Design:
- Pull yearly "historical compressed" legacy futures-only files for requested years.
- Avoid downloading the massive *_1986_2016.zip bundles unless explicitly enabled.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from urllib.parse import urljoin
from urllib.request import Request, urlopen


def _http_get(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req) as resp:
        return resp.read()


def _is_bundle(fname: str) -> bool:
    # Matches CFTC multi-year bundle naming like deacot1986_2016.zip, deafut_xls_1986_2016.zip
    return bool(re.search(r"1986_2016\.(zip|txt|csv)$", fname, re.IGNORECASE))


def _matches_year(fname: str, years: list[int]) -> bool:
    # Only accept if the filename contains a requested year and does NOT look like a multi-year bundle.
    return any(re.search(rf"(?<!\d){y}(?!\d)", fname) for y in years)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_dir", type=str, required=True)
    ap.add_argument(
        "--base_url",
        type=str,
        default="https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm",
    )
    ap.add_argument("--years", type=str, default="1986,1987,1988,1989")
    ap.add_argument(
        "--include_bundles",
        action="store_true",
        help="If set, also download large multi-year bundle files (e.g., *_1986_2016.zip).",
    )
    args = ap.parse_args()

    years = [int(y.strip()) for y in args.years.split(",") if y.strip()]
    os.makedirs(args.out_dir, exist_ok=True)

    html = _http_get(args.base_url).decode("utf-8", errors="ignore")

    # Extract href links from the page
    hrefs = re.findall(r'href="([^"]+)"', html)
    abs_links = [urljoin(args.base_url, href) for href in hrefs]

    # Candidate download links: zip/txt/csv only
    file_links = [u for u in abs_links if re.search(r"\.(zip|txt|csv)$", u, re.IGNORECASE)]

    # Filter: must match requested years, and avoid bundles unless enabled
    candidates: list[str] = []
    for u in file_links:
        fname = os.path.basename(u)
        if not _matches_year(fname, years):
            continue
        if _is_bundle(fname) and not args.include_bundles:
            continue
        candidates.append(u)

    if not candidates:
        print("No downloadable year links found after filtering.")
        print("Try opening the base_url and verifying filenames, then adjust the matching rules if needed.")
        sys.exit(1)

    # Download candidates, dedupe by filename
    downloaded: set[str] = set()
    for u in sorted(candidates):
        fname = os.path.basename(u)
        if fname in downloaded:
            continue
        downloaded.add(fname)

        out_path = os.path.join(args.out_dir, fname)
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            print(f"[skip] {fname} already exists")
            continue

        print(f"[get]  {u}")
        blob = _http_get(u)
        with open(out_path, "wb") as f:
            f.write(blob)
        print(f"[ok]   {out_path} ({len(blob):,} bytes)")

    print("\nDone.")


if __name__ == "__main__":
    main()



