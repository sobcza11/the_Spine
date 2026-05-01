from pathlib import Path

BANK_CODE = "BOC"
BANK_NAME = "Bank of Canada"
CURRENCY = "CAD"
LANGUAGE = "en"

BOC_BASE_URL = "https://www.bankofcanada.ca"

BOC_START_YEAR = 2000
BOC_END_YEAR = 2026

BOC_MONTH_NUMBERS = [
    "01", "02", "03", "04", "05", "06",
    "07", "08", "09", "10", "11", "12",
]

BOC_RATE_URL_PATTERNS = [
    "fad-press-release-{year}-{month}-",
]

BOC_MPR_URL_PATTERNS = [
    "monetary-policy-report-{month_name}-{year}",
]

BOC_MONTH_NAMES = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]

LOCAL_DATA_DIR = Path("data/geoscen/cb/boc")

RATE_ANNOUNCEMENTS_OUTPUT = LOCAL_DATA_DIR / "boc_rate_announcements_t1.parquet"
MPR_OUTPUT = LOCAL_DATA_DIR / "boc_mpr_t1.parquet"
COMBINED_CANONICAL_OUTPUT = LOCAL_DATA_DIR / "boc_combined_canonical_v1.parquet"

R2_PREFIX = "spine_global/leaves/geoscen/cb/boc"

