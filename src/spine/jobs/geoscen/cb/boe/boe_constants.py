from pathlib import Path

# =========================
# CORE IDENTIFIERS
# =========================
BANK_CODE = "BOE"
BANK_NAME = "Bank of England"
CURRENCY = "GBP"
LANGUAGE = "en"

# =========================
# BASE URL
# =========================
BOE_BASE_URL = "https://www.bankofengland.co.uk"

# =========================
# DOCUMENT PATH ROOTS
# =========================
BOE_POLICY_MINUTES_PATH = "monetary-policy-summary-and-minutes"
BOE_MPR_PATH = "monetary-policy-report"

# =========================
# HISTORICAL RANGE
# =========================
BOE_POLICY_MINUTES_START_YEAR = 1997
BOE_MPR_START_YEAR = 1993
BOE_END_YEAR = 2026

# =========================
# MONTH SLUGS
# =========================
BOE_MONTH_SLUGS = [
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

# =========================
# LOCAL DATA PATHS
# =========================
LOCAL_DATA_DIR = Path("data/geoscen/cb/boe")

POLICY_MINUTES_OUTPUT = LOCAL_DATA_DIR / "boe_policy_minutes_t1.parquet"
MPR_OUTPUT = LOCAL_DATA_DIR / "boe_mpr_t1.parquet"
COMBINED_CANONICAL_OUTPUT = LOCAL_DATA_DIR / "boe_combined_canonical_v1.parquet"

# =========================
# R2 STORAGE
# =========================
R2_PREFIX = "spine_global/leaves/geoscen/cb/boe"