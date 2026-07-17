from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from spine.jobs.geoscen.eis.exceptions import ResponseValidationError

SERIES_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MIN_DATE = "1776-07-04"
MAX_DATE = "9999-12-31"
MAX_LIMIT = 100000
MAX_OFFSET = 1000000
FREQUENCY_ALLOWLIST = {
    "d",
    "w",
    "bw",
    "m",
    "q",
    "sa",
    "a",
    "wef",
    "weth",
    "wew",
    "wetu",
    "wem",
    "wesa",
    "wesu",
}
AGGREGATION_ALLOWLIST = {"avg", "sum", "eop"}
UNITS_ALLOWLIST = {"lin", "chg", "ch1", "pch", "pc1", "pca", "cch", "cca", "log"}
SORT_ORDER_ALLOWLIST = {"asc", "desc"}


def normalize_series_id(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("series_id must be a string")
    series_id = value.strip()
    if not series_id:
        raise ValueError("series_id is required")
    if "://" in series_id or "/" in series_id or "?" in series_id or "#" in series_id:
        raise ValueError("series_id contains unsafe URL characters")
    if not SERIES_ID_RE.fullmatch(series_id):
        raise ValueError("series_id contains invalid characters")
    return series_id


def normalize_date(value: object, name: str) -> str:
    if not isinstance(value, str) or not DATE_RE.fullmatch(value.strip()):
        raise ValueError(f"{name} must be ISO YYYY-MM-DD")
    date = value.strip()
    if date < MIN_DATE or date > MAX_DATE:
        raise ValueError(f"{name} outside allowed range")
    return date


def normalize_int(value: object, name: str, *, minimum: int, maximum: int) -> int:
    try:
        text = str(value).strip()
        if not re.fullmatch(r"\d+", text):
            raise ValueError
        parsed = int(text)
    except Exception as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{name} outside allowed range")
    return parsed


def normalize_allowlist(value: object, name: str, allowed: set[str]) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    text = value.strip().lower()
    if text not in allowed:
        raise ValueError(f"{name} is not allowed")
    return text


def parse_numeric_value(value: object) -> tuple[int | float | None, str, str | None]:
    if value is None:
        return None, "unavailable", None
    raw = str(value)
    text = raw.strip()
    if text in {"", "."}:
        return None, "unavailable", None
    try:
        decimal_value = Decimal(text.replace(",", ""))
    except InvalidOperation:
        return None, "malformed", "malformed_numeric_value"
    if decimal_value == decimal_value.to_integral_value():
        return int(decimal_value), "available", None
    return float(decimal_value), "available", None


def error_payload(payload: Mapping[str, Any]) -> tuple[str, str] | None:
    if "error_code" not in payload and "error_message" not in payload:
        return None
    return (str(payload.get("error_code") or "fred_error")[:40], str(payload.get("error_message") or "FRED API error")[:240])


def observations_payload(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    observations = payload.get("observations")
    if not isinstance(observations, list):
        raise ResponseValidationError("Missing FRED observations list.", provider="fred", operation="series_observations")
    return [row for row in observations if isinstance(row, Mapping)]


def seriess_payload(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    seriess = payload.get("seriess")
    if not isinstance(seriess, list):
        raise ResponseValidationError("Missing FRED seriess list.", provider="fred", operation="series_metadata")
    return [row for row in seriess if isinstance(row, Mapping)]
