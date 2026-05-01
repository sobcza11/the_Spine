from pathlib import Path

SOURCE_NAME = "PMI_QUAL"
LANGUAGE = "en"

LOCAL_DATA_DIR = Path("data/geoscen/pmi")

RAW_OUTPUT = LOCAL_DATA_DIR / "raw/pmi_qual_raw_v1.parquet"
CANONICAL_OUTPUT = LOCAL_DATA_DIR / "canonical/pmi_commentary_canonical_v1.parquet"
TAGS_OUTPUT = LOCAL_DATA_DIR / "signals/pmi_commentary_tags_v1.parquet"
INDUSTRY_SIGNAL_OUTPUT = LOCAL_DATA_DIR / "signals/pmi_industry_signal_v1.parquet"

NUMERIC_PANEL_INPUT = Path("data/serving/equities/industry_panel_serving.json")

COMMENTARY_NUMERIC_OVERLAY_OUTPUT = (
    LOCAL_DATA_DIR / "signals/pmi_commentary_numeric_overlay_v1.parquet"
)

GEOSCEN_ZT_INPUT_OUTPUT = (
    LOCAL_DATA_DIR / "signals/pmi_geoscen_zt_input_v1.parquet"
)

QUAL_SHEET_NAME = "QUAL"

REQUIRED_CANONICAL_COLUMNS = [
    "date",
    "source",
    "release_type",
    "sector",
    "industry",
    "pmi",
    "new_orders",
    "commentary_text",
    "language",
    "ingested_at_utc",
]

