"""
Ingest FOMC Minutes into GeoScen canonical text format — v1.1.

Run:
python -m spine.jobs.geoscen.fomc.ingest_fomc_minutes_t1
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

from spine.jobs.geoscen.fomc.fomc_constants import (
    CURRENT_CALENDAR_URL,
    DOCUMENT_TYPE,
    FED_BASE_URL,
    HISTORICAL_YEAR_URL,
    LOCAL_OUTPUT_PATH,
    SCHEMA_VERSION,
    SOURCE,
)


def _get_html(url: str) -> str:
    response = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": "the_Spine GeoScen FOMC v1.1"},
    )
    response.raise_for_status()
    return response.text


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _extract_links(index_url: str) -> tuple[set[str], set[str]]:
    soup = BeautifulSoup(_get_html(index_url), "html.parser")

    minutes_links: set[str] = set()
    index_links: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        href_l = href.lower()
        label_l = _clean_text(a.get_text(" ")).lower()
        full_url = urljoin(FED_BASE_URL, href)

        if "fomcminutes" in href_l:
            minutes_links.add(full_url)

        if "fomc_historical" in href_l or "fomccalendars" in href_l:
            index_links.add(full_url)

        if "minutes" in label_l and href_l.endswith((".htm", ".html")):
            minutes_links.add(full_url)

    return minutes_links, index_links


def _discover_minutes_links() -> list[str]:
    seed_pages = {CURRENT_CALENDAR_URL, HISTORICAL_YEAR_URL}
    visited: set[str] = set()
    minutes_links: set[str] = set()

    while seed_pages:
        url = seed_pages.pop()
        if url in visited:
            continue

        visited.add(url)
        found_minutes, found_indexes = _extract_links(url)

        minutes_links.update(found_minutes)

        for next_url in found_indexes:
            if next_url not in visited:
                seed_pages.add(next_url)

    return sorted(minutes_links)


def _parse_date_from_url(url: str) -> str | None:
    match = re.search(r"fomcminutes(\d{4})(\d{2})(\d{2})", url.lower())
    if not match:
        return None

    return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"


def _parse_minutes_page(url: str, ingested_at_utc: str) -> dict[str, str] | None:
    soup = BeautifulSoup(_get_html(url), "html.parser")

    title = _clean_text(soup.find("title").get_text(" ")) if soup.find("title") else ""
    page_text = _clean_text(soup.get_text(" "))

    date = _parse_date_from_url(url)
    if not date:
        return None

    text_hash = _sha256(page_text)
    document_id = f"fomc_minutes_{date}_{text_hash[:12]}"

    return {
        "document_id": document_id,
        "date": date,
        "source": SOURCE,
        "document_type": DOCUMENT_TYPE,
        "title": title,
        "url": url,
        "text": page_text,
        "text_sha256": text_hash,
        "ingested_at_utc": ingested_at_utc,
        "version": SCHEMA_VERSION,
    }


def main() -> None:
    ingested_at_utc = datetime.now(timezone.utc).isoformat()

    links = _discover_minutes_links()
    print(f"[GeoScen:FOMC] discovered_links={len(links)}")

    rows = []
    for url in links:
        row = _parse_minutes_page(url, ingested_at_utc)
        if row:
            rows.append(row)

    if not rows:
        raise RuntimeError("No FOMC Minutes rows parsed.")

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("document_id").reset_index(drop=True)

    output_path = Path(LOCAL_OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"[GeoScen:FOMC] rows={len(df)}")
    print(f"[GeoScen:FOMC] min_date={df['date'].min().date()}")
    print(f"[GeoScen:FOMC] max_date={df['date'].max().date()}")
    print(f"[GeoScen:FOMC] wrote={output_path}")


if __name__ == "__main__":
    main()

