"""
Ingest FOMC historical materials metadata from 1990 onward.

Metadata-first backend:
- captures PDF / HTML links
- classifies document_type from URL/title
- does NOT extract PDF text yet

Run:
python -m spine.jobs.geoscen.fomc.historical.ingest_fomc_historical_materials_t1
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
from sqlalchemy import label

from spine.jobs.geoscen.fomc.historical.fomc_historical_constants import (
    DOCUMENT_FAMILY,
    FED_BASE_URL,
    HISTORICAL_INDEX_URL,
    LOCAL_OUTPUT_PATH,
    SCHEMA_VERSION,
    SOURCE,
    START_YEAR,
)


def _get_html(url: str) -> str:
    response = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": "the_Spine GeoScen FOMC historical v1"},
    )
    response.raise_for_status()
    return response.text


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _discover_year_pages() -> dict[int, str]:
    soup = BeautifulSoup(_get_html(HISTORICAL_INDEX_URL), "html.parser")
    year_pages: dict[int, str] = {}

    for a in soup.find_all("a", href=True):
        label = _clean_text(a.get_text(" "))
        href = a["href"]

        if not re.fullmatch(r"\d{4}", label):
            continue

        year = int(label)
        if year < START_YEAR:
            continue

        year_pages[year] = urljoin(FED_BASE_URL, href)

    return dict(sorted(year_pages.items()))


def _parse_date_from_url_or_text(url: str, text: str, year: int) -> str:
    url_l = url.lower()

    match = re.search(r"(?:fomc|fomcminutes)(\d{4})(\d{2})(\d{2})", url_l)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    match = re.search(r"(\d{4})(\d{2})(\d{2})", url_l)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    text_match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+(\d{1,2})(?:[-–]\d{1,2})?,\s+(\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    if text_match:
        parsed = pd.to_datetime(" ".join(text_match.groups()), errors="coerce")
        if not pd.isna(parsed):
            return parsed.strftime("%Y-%m-%d")

    return f"{year}-01-01"


def _classify_document_type(url: str, title: str) -> str:
    value = f"{url} {title}".lower()

    if "confcall" in value or "conference call" in value:
        return "conference_call"
    if "transcript" in value:
        return "transcript"
    if "agenda" in value:
        return "agenda"

    if "tealbook" in value:
        return "tealbook"
    if "bluebook" in value:
        return "bluebook"
    if "greenbook" in value or "gbpt" in value:
        return "greenbook"

    if "beige" in value:
        return "beige_book_reference"

    if "memos" in value or "memo" in value:
        return "memo"
    if "material" in value or "presentation" in value:
        return "presentation_material"
    if "staff" in value:
        return "staff_material"
    if "chart" in value or "exhibit" in value:
        return "chart_or_exhibit"

    if "minutes of actions" in value or "moa" in value:
        return "minutes_of_actions"
    if "minutes" in value:
        return "minutes"

    if "record" in value and "policy" in value:
        return "record_of_policy_actions"

    if "meeting" in value:
        return "meeting_material"

    return "other_historical_material"


def _file_format(url: str) -> str:
    url_l = url.lower()
    if url_l.endswith(".pdf"):
        return "pdf"
    if url_l.endswith((".htm", ".html")):
        return "html"
    return "unknown"


def _extract_material_rows(year: int, year_url: str, ingested_at_utc: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(_get_html(year_url), "html.parser")
    rows: list[dict[str, str]] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        url = urljoin(FED_BASE_URL, href)
        url_l = url.lower()

        if "/monetarypolicy/files/" not in url_l:
            continue

        if not url_l.endswith(".pdf"):
            continue

        label = _clean_text(a.get_text(" "))
        if not label:
            label = Path(url_l).name

        if "size" in label.lower():
            continue

        label = _clean_text(a.get_text(" "))
        if not label:
            label = Path(url_l).name

        date = _parse_date_from_url_or_text(url, label, year)
        document_type = _classify_document_type(url, label)
        file_format = _file_format(url)

        stable_key = f"{date}|{document_type}|{url}"
        document_id = f"fomc_hist_{date}_{_sha256(stable_key)[:12]}"

        rows.append(
            {
                "document_id": document_id,
                "date": date,
                "year": year,
                "source": SOURCE,
                "document_family": DOCUMENT_FAMILY,
                "document_type": document_type,
                "title": label,
                "url": url,
                "file_format": file_format,
                "text": "",
                "text_sha256": "",
                "ingested_at_utc": ingested_at_utc,
                "version": SCHEMA_VERSION,
            }
        )

    return rows


def main() -> None:
    ingested_at_utc = datetime.now(timezone.utc).isoformat()

    year_pages = _discover_year_pages()
    if not year_pages:
        raise RuntimeError("No FOMC historical year pages discovered.")

    rows: list[dict[str, str]] = []
    for year, year_url in year_pages.items():
        year_rows = _extract_material_rows(year, year_url, ingested_at_utc)
        print(f"[GeoScen:FOMC:Historical] year={year} rows={len(year_rows)}")
        rows.extend(year_rows)

    if not rows:
        raise RuntimeError("No FOMC historical materials rows parsed.")

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()
    df = df.sort_values(["date", "document_type", "url"])
    df = df.drop_duplicates(["date", "document_type", "url"]).reset_index(drop=True)

    output_path = Path(LOCAL_OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"[GeoScen:FOMC:Historical] rows={len(df)}")
    print(f"[GeoScen:FOMC:Historical] min_date={df['date'].min().date()}")
    print(f"[GeoScen:FOMC:Historical] max_date={df['date'].max().date()}")
    print(f"[GeoScen:FOMC:Historical] wrote={output_path}")


if __name__ == "__main__":
    main()
