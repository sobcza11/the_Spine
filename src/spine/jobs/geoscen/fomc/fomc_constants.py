"""
FOMC Minutes GeoScen constants — v1.
"""

from __future__ import annotations

LOCAL_OUTPUT_PATH = "data/geoscen/fomc/fomc_minutes_canonical.parquet"
R2_KEY = "spine_us/geoscen/canonical/fomc_minutes_canonical.parquet"

SOURCE = "federal_reserve"
DOCUMENT_TYPE = "fomc_minutes"
SCHEMA_VERSION = "fomc_minutes_canonical_v1"

FED_BASE_URL = "https://www.federalreserve.gov"
CURRENT_CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
HISTORICAL_YEAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm"

REQUIRED_COLUMNS = [
    "document_id",
    "date",
    "source",
    "document_type",
    "title",
    "url",
    "text",
    "text_sha256",
    "ingested_at_utc",
    "version",
]

