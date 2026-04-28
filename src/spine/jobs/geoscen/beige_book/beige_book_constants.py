"""
Beige Book GeoScen constants — v1.
"""

from __future__ import annotations

START_YEAR = 1996

FED_BASE_URL = "https://www.federalreserve.gov"
BEIGE_BOOK_ARCHIVE_URL = "https://www.federalreserve.gov/monetarypolicy/beige-book-archive.htm"
BEIGE_BOOK_DEFAULT_URL = "https://www.federalreserve.gov/monetarypolicy/publications/beige-book-default.htm"

LOCAL_OUTPUT_PATH = "data/geoscen/beige_book/beige_book_canonical_v1.parquet"
R2_KEY = "spine_us/geoscen/canonical/beige_book_canonical_v1.parquet"

SOURCE = "federal_reserve"
DOCUMENT_FAMILY = "beige_book"
SCHEMA_VERSION = "beige_book_canonical_v1"

REQUIRED_COLUMNS = [
    "document_id",
    "date",
    "year",
    "source",
    "document_family",
    "title",
    "url",
    "file_format",
    "text",
    "text_sha256",
    "text_chars",
    "ingested_at_utc",
    "version",
]
