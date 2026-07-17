from __future__ import annotations

from datetime import UTC, datetime

APPROVED_PRODUCTS = {"acs5", "acs5/profile", "acs5/subject"}
MIN_ACS_YEAR = 2009
MAX_VARIABLES = 25
MAX_PREDICATE_VALUES = 25
MAX_GROUP_LEN = 40
MAX_LABEL_LEN = 500
SENTINEL_VALUES = {"-666666666", "-888888888", "-999999999", "-222222222"}


def current_year() -> int:
    return datetime.now(UTC).year


def normalize_year(value: object) -> str:
    text = str(value).strip()
    if not text.isdigit() or len(text) != 4:
        raise ValueError("year malformed")
    year = int(text)
    if year < MIN_ACS_YEAR:
        raise ValueError("year below supported range")
    if year > current_year():
        raise ValueError("future year rejected")
    return text


def normalize_product(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("product must be a string")
    text = value.strip().lower()
    if text not in APPROVED_PRODUCTS or "?" in text or "\\" in text or ".." in text or "://" in text:
        raise ValueError("product unsupported")
    return text
