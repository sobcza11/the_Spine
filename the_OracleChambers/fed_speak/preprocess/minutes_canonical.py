"""
Build canonical sentences for FOMC Minutes.

Reads HTML files from:
    data/raw/FOMC_Minutes/html/*.html

Outputs:
    data/processed/FOMC_Minutes/canonical_sentences.parquet

Columns:
    event_id
    sentence_id
    sentence_text
"""

import logging
import re
from pathlib import Path
from typing import List

import pandas as pd
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


SENT_SPLIT_REGEX = re.compile(r"(?<=[.!?])\s+")


def html_to_plain_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # Simple approach: strip scripts/styles, get visible text
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(" ", strip=True)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)
    return text


def split_into_sentences(text: str) -> List[str]:
    # Naive but robust sentence splitter for policy prose
    parts = SENT_SPLIT_REGEX.split(text)
    sentences = [p.strip() for p in parts if p.strip()]
    return sentences


def process_html_file(html_path: Path) -> pd.DataFrame:
    event_id = html_path.stem  # e.g., "2025-09-17_FOMC_Minutes"
    logger.info(f"Processing FOMC Minutes HTML: {html_path} (event_id={event_id})")

    html = html_path.read_text(encoding="utf-8", errors="ignore")
    plain_text = html_to_plain_text(html)
    sentences = split_into_sentences(plain_text)

    rows = []
    for i, sent in enumerate(sentences):
        rows.append(
            {
                "event_id": event_id,
                "sentence_id": i,
                "sentence_text": sent,
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    raw_html_dir = Path("data/raw/FOMC_Minutes/html")
    out_dir = Path("data/processed/FOMC_Minutes")
    out_path = out_dir / "canonical_sentences.parquet"

    if not raw_html_dir.exists():
        raise FileNotFoundError(f"No FOMC Minutes HTML directory found at {raw_html_dir}")

    all_rows = []
    for html_path in sorted(raw_html_dir.glob("*.html")):
        try:
            df = process_html_file(html_path)
            all_rows.append(df)
        except Exception as e:
            logger.error(f"Failed to process {html_path}: {e}")

    if not all_rows:
        raise ValueError("No FOMC Minutes HTML files were successfully processed.")

    out_df = pd.concat(all_rows, ignore_index=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_path, index=False)

    logger.info(f"Saved FOMC Minutes canonical sentences to {out_path} with {len(out_df)} rows.")


if __name__ == "__main__":
    main()

