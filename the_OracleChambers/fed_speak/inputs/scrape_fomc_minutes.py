"""
FOMC Minutes ingestion (index-driven, CPMAI-friendly).

Assumes an index CSV at:
    data/raw/FOMC_Minutes/fomc_minutes_index.csv

with at least:
    event_id, url

Example row:
    2025-09-17_FOMC_Minutes,https://www.federalreserve.gov/monetarypolicy/fomcminutes20250917.htm
"""

import csv
import logging
from pathlib import Path
from typing import Dict

import requests


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def load_index(index_path: Path) -> Dict[str, str]:
    if not index_path.exists():
        raise FileNotFoundError(f"Index CSV not found at {index_path}")

    mapping: Dict[str, str] = {}
    with index_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"event_id", "url"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Index CSV missing required columns: {missing}")

        for row in reader:
            event_id = row["event_id"].strip()
            url = row["url"].strip()
            if event_id and url:
                mapping[event_id] = url

    if not mapping:
        raise ValueError("No rows found in FOMC minutes index CSV.")
    return mapping


def fetch_and_save_html(event_id: str, url: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{event_id}.html"

    logger.info(f"Fetching FOMC Minutes for {event_id} from {url} ...")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    out_path.write_text(resp.text, encoding="utf-8")
    logger.info(f"Saved HTML to {out_path}")


def main() -> None:
    base_raw = Path("data/raw/FOMC_Minutes")
    index_path = base_raw / "fomc_minutes_index.csv"
    html_dir = base_raw / "html"

    logger.info(f"Loading FOMC Minutes index from {index_path}...")
    index = load_index(index_path)

    for event_id, url in index.items():
        try:
            fetch_and_save_html(event_id, url, html_dir)
        except Exception as e:
            logger.error(f"Failed to fetch {event_id} from {url}: {e}")


if __name__ == "__main__":
    main()

