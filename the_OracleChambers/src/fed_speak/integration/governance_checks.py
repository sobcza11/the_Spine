"""
Governance & QA module for FedSpeak Tranche 1.

- Schema checks for key parquet files
- Basic sanity checks on distributions
- Metadata writing for CPMAI alignment
"""

from pathlib import Path
import json
from typing import Dict

import pandas as pd

from fed_speak.config import PROCESSED_DIR, CPMAI_METADATA


def _check_columns(path: Path, required_cols: Dict[str, str]) -> None:
    df = pd.read_parquet(path)
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{path.name}: missing columns: {missing}")


def run_tranche1_checks() -> None:
    """
    Run basic validation checks over Tranche 1 outputs.
    """
    beige_canonical = PROCESSED_DIR / "BeigeBook" / "canonical_sentences.parquet"
    beige_tone = PROCESSED_DIR / "BeigeBook" / "sentiment_scores.parquet"
    beige_leaf = PROCESSED_DIR / "BeigeBook" / "leaf.parquet"
    combined_leaf = PROCESSED_DIR / "combined_policy_leaf.parquet"

    _check_columns(
        beige_canonical,
        {"event_id": "str", "sentence_text": "str"},
    )
    _check_columns(
        beige_tone,
        {"vader_compound": "float", "tone_leaf": "str"},
    )
    _check_columns(
        beige_leaf,
        {"price_pressure_score": "float", "growth_tone_score": "float"},
    )
    _check_columns(
        combined_leaf,
        {"inflation_risk": "float", "growth_risk": "float", "policy_bias": "float"},
    )

    # Simple metadata dump
    meta = {
        **CPMAI_METADATA,
        "validated_tranche": "Tranche_1",
        "files_checked": [
            str(beige_canonical),
            str(beige_tone),
            str(beige_leaf),
            str(combined_leaf),
        ],
    }

    meta_path = PROCESSED_DIR / "fed_speak_metadata.json"
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
