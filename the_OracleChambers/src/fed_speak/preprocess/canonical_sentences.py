"""
Canonical sentence layer for FedSpeak.

Reads raw Fed documents (per category), applies spaCy sentence
segmentation, and outputs canonical_sentences.parquet for each.
"""

from pathlib import Path
from typing import List
import pandas as pd
import spacy

from fed_speak.config import (
    RAW_BEIGE_DIR,
    RAW_STATEMENT_DIR,
    RAW_SPEECHES_DIR,
    CANONICAL_SUFFIX,
    PROCESSED_DIR,
)

nlp = spacy.load("en_core_web_sm")
nlp.max_length = 10_000_000   # fully safe for very long Fed docs


def _sentences_from_text(event_id: str, category: str, text: str) -> List[dict]:
    doc = nlp(text)
    rows = []
    for i, sent in enumerate(doc.sents):
        rows.append(
            {
                "event_id": event_id,
                "category": category,
                "sentence_id": i,
                "sentence_text": sent.text,
                "position_in_doc": sent.start,
            }
        )
    return rows


def build_canonical_from_raw(raw_parquet: Path, category: str) -> pd.DataFrame:
    """
    Given a raw parquet file for a category, create canonical sentences parquet.
    """
    df_raw = pd.read_parquet(raw_parquet)
    records: List[dict] = []
    for _, row in df_raw.iterrows():
        records.extend(
            _sentences_from_text(
                event_id=row["event_id"],
                category=category,
                text=row["raw_text"],
            )
        )
    df_sent = pd.DataFrame(records)
    out_dir = PROCESSED_DIR / category
    out_dir.mkdir(parents=True, exist_ok=True)
    df_sent.to_parquet(out_dir / CANONICAL_SUFFIX, index=False)
    return df_sent


def build_all_canonical():
    """
    Orchestrator for Tranche 1 categories.
    """
    build_canonical_from_raw(RAW_BEIGE_DIR / "beige_raw.parquet", "BeigeBook")
    # Add statements & speeches as their raw scrapers are completed.
    # build_canonical_from_raw(RAW_STATEMENT_DIR / "statement_raw.parquet", "FOMC_Statement")
    # build_canonical_from_raw(RAW_SPEECHES_DIR / "speeches_raw.parquet", "Fed_Speeches")

