from __future__ import annotations

import re
from datetime import UTC, datetime

STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
    "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY", "PR", "VI", "GU", "AS", "MP",
}

STATE_ID_RE = re.compile(r"^[A-Z]{2}$")
ENTITY_RE = re.compile(r"^[A-Za-z0-9_-]{1,40}$")
YEAR_RE = re.compile(r"^\d{4}$")
MIN_HUD_YEAR = 2007


def normalize_state_code(value: object) -> str:
    text = str(value or "").strip().upper()
    if not STATE_ID_RE.fullmatch(text) or text not in STATE_CODES:
        raise ValueError("state_code invalid")
    return text


def normalize_state_id(value: object) -> str:
    text = str(value or "").strip().upper()
    if not STATE_ID_RE.fullmatch(text) or text not in STATE_CODES:
        raise ValueError("state_id invalid")
    return text


def normalize_entity_id(value: object) -> str:
    text = str(value or "").strip()
    if not ENTITY_RE.fullmatch(text) or any(ch in text for ch in "/\\?&#.:"):
        raise ValueError("entity_id invalid")
    return text


def normalize_year(value: object, name: str = "year") -> str:
    text = str(value or "").strip()
    if not YEAR_RE.fullmatch(text):
        raise ValueError(f"{name} malformed")
    year = int(text)
    if year < MIN_HUD_YEAR or year > datetime.now(UTC).year + 1:
        raise ValueError(f"{name} outside supported range")
    return text
