"""
Fed Speak configuration module.

Centralizes paths, filenames, and basic options so all FedSpeak
pipes stay aligned & auditable.
"""

from pathlib import Path

# Root of the OracleChambers project (resolved dynamically)
ROOT_DIR = Path(__file__).resolve().parents[2]

# Data directories (processed & raw)
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# Raw Fed doc storage
RAW_BEIGE_DIR = RAW_DIR / "BeigeBook"
RAW_STATEMENT_DIR = RAW_DIR / "FOMC_Statement"
RAW_SPEECHES_DIR = RAW_DIR / "Fed_Speeches"

# Canonical & tone parquet outputs (per category)
CANONICAL_SUFFIX = "canonical_sentences.parquet"
TONE_SUFFIX = "sentiment_scores.parquet"
LEAF_SUFFIX = "leaf.parquet"

# Fed categories used in Tranche 1
FED_CATEGORIES = ["BeigeBook", "FOMC_Statement", "Fed_Speeches"]

# Basic CPMAI metadata keys
CPMAI_METADATA = {
    "project": "FedSpeak",
    "owner": "Rand_Sobczak",
    "phase": "Tranche_1_Foundations",
}


