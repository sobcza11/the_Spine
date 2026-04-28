"""
FOMC historical materials constants — GeoScen v1.
"""

from __future__ import annotations

START_YEAR = 1990

FED_BASE_URL = "https://www.federalreserve.gov"
HISTORICAL_INDEX_URL = "https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm"

LOCAL_OUTPUT_PATH = "data/geoscen/fomc/fomc_historical_materials_canonical.parquet"
R2_KEY = "spine_us/geoscen/canonical/fomc_historical_materials_canonical.parquet"

SOURCE = "federal_reserve"
DOCUMENT_FAMILY = "fomc_historical_material"
SCHEMA_VERSION = "fomc_historical_materials_canonical_v1"

REQUIRED_COLUMNS = [
    "document_id",
    "date",
    "year",
    "source",
    "document_family",
    "document_type",
    "title",
    "url",
    "file_format",
    "text",
    "text_sha256",
    "ingested_at_utc",
    "version",
]