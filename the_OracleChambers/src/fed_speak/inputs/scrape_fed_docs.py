"""
Scrape and persist raw Fed documents (Beige Book, Statements, Speeches).

Tranche 1 scope:
- Beige Book
- FOMC Statements
- Fed Speeches (per speaker)

Outputs are simple text payloads + metadata saved under data/raw/.
"""

from dataclasses import dataclass, asdict
from typing import List
import requests
from bs4 import BeautifulSoup
import pandas as pd

from fed_speak.config import (
    RAW_BEIGE_DIR,
    RAW_STATEMENT_DIR,
    RAW_SPEECHES_DIR,
    CPMAI_METADATA,
)

@dataclass
class RawFedDoc:
    event_id: str
    category: str
    url: str
    date: str
    title: str
    raw_text: str


def _fetch_html(url: str) -> str:
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.text


def _extract_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # Very simple default: remove scripts/styles, return text
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def scrape_beige_book(index_url: str) -> pd.DataFrame:
    """
    Scrape Beige Book links from the index page and download each report.
    """
    RAW_BEIGE_DIR.mkdir(parents=True, exist_ok=True)

    html = _fetch_html(index_url)
    soup = BeautifulSoup(html, "html.parser")
    records: List[RawFedDoc] = []

    # Placeholder logic: user will refine based on actual Fed HTML structure
    for link in soup.find_all("a", href=True):
        if "beigebook" in link["href"].lower():
            url = link["href"]
            if not url.startswith("http"):
                url = "https://www.federalreserve.gov" + url
            event_id = url.split("/")[-1].replace(".htm", "")
            date = ""  # can be parsed from surrounding HTML or event_id
            title = link.get_text(strip=True)

            doc_html = _fetch_html(url)
            raw_text = _extract_text_from_html(doc_html)

            records.append(
                RawFedDoc(
                    event_id=event_id,
                    category="BeigeBook",
                    url=url,
                    date=date,
                    title=title,
                    raw_text=raw_text,
                )
            )

    df = pd.DataFrame([asdict(r) for r in records])
    df.to_parquet(RAW_BEIGE_DIR / "beige_raw.parquet", index=False)
    return df


def scrape_statements(index_url: str) -> pd.DataFrame:
    """
    Scrape FOMC Statements metadata and text.
    """
    RAW_STATEMENT_DIR.mkdir(parents=True, exist_ok=True)
    # Similar pattern to Beige Book; refine link filters for statements.
    # Placeholder structure:
    # - find links containing "pressreleases/monetary" or similar.
    raise NotImplementedError("Implement FOMC Statement scraping logic.")


def scrape_speeches(index_url: str) -> pd.DataFrame:
    """
    Scrape Fed speeches (Powell, Williams, etc.).
    """
    RAW_SPEECHES_DIR.mkdir(parents=True, exist_ok=True)
    # Placeholder: filter for 'newsevents/speech' links, parse speaker names.
    raise NotImplementedError("Implement Fed speeches scraping logic.")


