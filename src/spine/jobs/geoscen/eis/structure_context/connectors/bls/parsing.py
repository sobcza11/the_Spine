from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from spine.jobs.geoscen.eis.exceptions import ResponseValidationError

BLS_SUCCESS_STATUSES = {"REQUEST_SUCCEEDED", "REQUEST_SUCCEEDED_WITH_WARNINGS"}
SERIES_ID_RE = re.compile(r"^[A-Z0-9_#-]+$")
MAX_SERIES_IDS = 50
MIN_YEAR = 1900
MAX_YEAR = 2100


def normalize_series_ids(value: object) -> list[str]:
    if not isinstance(value, (list, tuple)):
        raise ValueError("series_ids must be a list or tuple")
    if not value:
        raise ValueError("series_ids must not be empty")
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ValueError("series_ids must contain strings only")
        series_id = item.strip()
        if not series_id:
            raise ValueError("series_id must not be empty")
        if not SERIES_ID_RE.fullmatch(series_id):
            raise ValueError(f"malformed_series_id:{series_id}")
        if series_id not in seen:
            normalized.append(series_id)
            seen.add(series_id)
    if len(normalized) > MAX_SERIES_IDS:
        raise ValueError("series_ids exceeds BLS API maximum")
    return normalized


def normalize_year(value: object, name: str) -> str:
    text = str(value).strip()
    if not re.fullmatch(r"\d{4}", text):
        raise ValueError(f"{name} must be a four-digit year")
    year = int(text)
    if year < MIN_YEAR or year > MAX_YEAR:
        raise ValueError(f"{name} outside allowed range")
    return text


def normalize_bool(value: object, name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{name} must be boolean")


def normalize_footnotes(value: object) -> tuple[list[str], list[str]]:
    codes: list[str] = []
    texts: list[str] = []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return codes, texts
    for item in value:
        if not isinstance(item, Mapping):
            continue
        code = str(item.get("code") or "").strip()
        text = str(item.get("text") or "").strip()
        if code:
            codes.append(code)
        if text:
            texts.append(text)
    return codes, texts


def parse_numeric_value(value: object) -> float | int | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        decimal_value = Decimal(text)
    except InvalidOperation:
        return None
    if decimal_value == decimal_value.to_integral_value():
        return int(decimal_value)
    return float(decimal_value)


def normalize_period(year: str, period: str, period_name: str | None = None) -> dict[str, str | None]:
    if re.fullmatch(r"M(0[1-9]|1[0-2])", period):
        month = period[1:]
        return {
            "period": period,
            "period_name": period_name,
            "period_kind": "monthly",
            "measurement_period": f"{year}-{month}",
            "measurement_date": f"{year}-{month}-01",
            "date_convention": "first_day_of_month",
        }
    if period == "M13":
        return {
            "period": period,
            "period_name": period_name or "Annual Average",
            "period_kind": "annual_average",
            "measurement_period": f"{year}-M13",
            "measurement_date": None,
            "date_convention": "annual_average_no_specific_day",
        }
    if re.fullmatch(r"Q0[1-4]", period):
        return {
            "period": period,
            "period_name": period_name,
            "period_kind": "quarterly",
            "measurement_period": f"{year}-{period}",
            "measurement_date": None,
            "date_convention": "quarter_period_no_specific_day",
        }
    if re.fullmatch(r"S0[1-2]", period):
        return {
            "period": period,
            "period_name": period_name,
            "period_kind": "semiannual",
            "measurement_period": f"{year}-{period}",
            "measurement_date": None,
            "date_convention": "semiannual_period_no_specific_day",
        }
    if period == "A01":
        return {
            "period": period,
            "period_name": period_name or "Annual",
            "period_kind": "annual",
            "measurement_period": f"{year}-A01",
            "measurement_date": None,
            "date_convention": "annual_period_no_specific_day",
        }
    raise ResponseValidationError("Unsupported BLS period code.", provider="bls", operation="timeseries")


def results_series(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    results = payload.get("Results")
    if isinstance(results, list):
        if not results or not isinstance(results[0], Mapping):
            raise ResponseValidationError("Missing BLS Results object.", provider="bls", operation="timeseries")
        series = results[0].get("series")
    elif isinstance(results, Mapping):
        series = results.get("series")
    else:
        raise ResponseValidationError("Missing BLS Results object.", provider="bls", operation="timeseries")
    if not isinstance(series, list):
        raise ResponseValidationError("Missing BLS series list.", provider="bls", operation="timeseries")
    return [item for item in series if isinstance(item, Mapping)]
