"""
Ingest historical Beige Book archive documents into GeoScen canonical text format.

Scope:
1996–2010 only.

Run:
python -m spine.jobs.geoscen.beige_book.historical.ingest_beige_book_historical_t1
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from spine.jobs.geoscen.beige_book.beige_book_constants import (
    BEIGE_BOOK_ARCHIVE_URL,
    DOCUMENT_FAMILY,
    FED_BASE_URL,
    SCHEMA_VERSION,
    SOURCE,
)


START_YEAR = 1996
END_YEAR = 2010
LOCAL_OUTPUT_PATH = "data/geoscen/beige_book/beige_book_historical_canonical_t1.parquet"


def _get_html(url: str) -> str:
    import time

    for attempt in range(3):
        try:
            response = requests.get(
                url,
                timeout=30,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                },
            )
            response.raise_for_status()

            # throttle (critical)
            time.sleep(0.5)

            return response.text

        except Exception as e:
            print(f"[GeoScen:BeigeBook:Historical] retry {attempt+1} failed for {url}: {e}")
            time.sleep(2)

    return ""


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _discover_year_pages() -> dict[int, str]:
    soup = BeautifulSoup(_get_html(BEIGE_BOOK_ARCHIVE_URL), "html.parser")
    year_pages: dict[int, str] = {}

    for a in soup.find_all("a", href=True):
        label = _clean_text(a.get_text(" "))
        href = a["href"]

        if not re.fullmatch(r"\d{4}", label):
            continue

        year = int(label)
        if START_YEAR <= year <= END_YEAR:
            year_pages[year] = urljoin(FED_BASE_URL, href)

    return dict(sorted(year_pages.items()))


def _parse_date_from_url_or_text(url: str, text: str, year: int) -> str | None:
    url_l = url.lower()

    match = re.search(r"/fomc/beigebook/(\d{4})/(\d{4})(\d{2})(\d{2})/", url_l)
    if match:
        return f"{match.group(2)}-{match.group(3)}-{match.group(4)}"
    
    patterns = [
        r"beigebook[_-]?(\d{4})(\d{2})(\d{2})",
        r"beige[_-]?book[_-]?(\d{4})(\d{2})(\d{2})",
        r"(\d{4})(\d{2})(\d{2})",
    ]

    for pattern in patterns:
        match = re.search(pattern, url_l)
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    text_match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+(\d{1,2}),\s+(\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    if text_match:
        parsed = pd.to_datetime(" ".join(text_match.groups()), errors="coerce")
        if not pd.isna(parsed):
            return parsed.strftime("%Y-%m-%d")

    return None


def _extract_text_from_html(url: str) -> tuple[str, str]:
    soup = BeautifulSoup(_get_html(url), "html.parser")
    title = _clean_text(soup.find("title").get_text(" ")) if soup.find("title") else ""
    text = _clean_text(soup.get_text(" "))
    return title, text


def _extract_document_links(year_url: str) -> list[str]:
    soup = BeautifulSoup(_get_html(year_url), "html.parser")
    links: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        url = urljoin(FED_BASE_URL, href)
        url_l = url.lower()

        is_historical_release = (
            "/fomc/beigebook/" in url_l
            and url_l.endswith("/default.htm")
        )

        if not is_historical_release:
            continue

        links.add(url)

    return sorted(links)


def main() -> None:
    ingested_at_utc = datetime.now(timezone.utc).isoformat()

    year_pages = _discover_year_pages()
    if not year_pages:
        raise RuntimeError("No historical Beige Book year pages discovered.")

    rows: list[dict[str, object]] = []

    for year, year_url in year_pages.items():
        links = _extract_document_links(year_url)
        print(f"[GeoScen:BeigeBook:Historical] year={year} links={len(links)}")

        for url in links:
            title, text = _extract_text_from_html(url)
            date = _parse_date_from_url_or_text(url, f"{title} {text}", year)

            if not date:
                continue

            parsed_year = pd.to_datetime(date).year
            if parsed_year < START_YEAR or parsed_year > END_YEAR:
                continue

            text_hash = _sha256(text)
            document_id = f"beige_book_hist_{date}_{text_hash[:12]}"

            rows.append(
                {
                    "document_id": document_id,
                    "date": date,
                    "year": parsed_year,
                    "source": SOURCE,
                    "document_family": DOCUMENT_FAMILY,
                    "title": title,
                    "url": url,
                    "file_format": "html",
                    "text": text,
                    "text_sha256": text_hash,
                    "text_chars": len(text),
                    "ingested_at_utc": ingested_at_utc,
                    "version": "beige_book_historical_canonical_t1",
                }
            )

    if not rows:
        raise RuntimeError("No historical Beige Book rows parsed.")

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()
    df = df.sort_values(["date", "url"]).drop_duplicates(["date", "url"]).reset_index(drop=True)

    output_path = Path(LOCAL_OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"[GeoScen:BeigeBook:Historical] rows={len(df)}")
    print(f"[GeoScen:BeigeBook:Historical] min_date={df['date'].min().date()}")
    print(f"[GeoScen:BeigeBook:Historical] max_date={df['date'].max().date()}")
    print(f"[GeoScen:BeigeBook:Historical] wrote={output_path}")


if __name__ == "__main__":
    main()

