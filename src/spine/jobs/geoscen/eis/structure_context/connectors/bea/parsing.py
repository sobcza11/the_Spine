from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from spine.jobs.geoscen.eis.exceptions import ResponseValidationError, UpstreamResponseError
from spine.jobs.geoscen.eis.structure_context.connectors.bea.datasets import (
    MAX_IDENTIFIER_LEN,
    MAX_LIST_VALUES,
    MAX_STRING_LEN,
    NIPA_FREQUENCIES,
)

IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
GEOFIPS_RE = re.compile(r"^[A-Za-z0-9*_-]{1,12}$")
YEAR_RE = re.compile(r"^\d{4}$")
PERIOD_RE = re.compile(r"^(\d{4})(Q[1-4]|M\d{2})?$")
POLLUTION_KEYS = {"__proto__", "constructor", "prototype"}


def normalize_identifier(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    text = value.strip()
    if not text or len(text) > MAX_IDENTIFIER_LEN or not IDENTIFIER_RE.fullmatch(text):
        raise ValueError(f"{name} is invalid")
    return text


def normalize_dataset_name(value: object) -> str:
    text = normalize_identifier(value, "dataset_name")
    return {"nipa": "NIPA", "regional": "Regional"}.get(text.lower(), text)


def normalize_scalar_list(value: object, name: str, *, allow_star: bool = False) -> list[str]:
    raw = value if isinstance(value, (list, tuple)) else [value]
    if not raw or len(raw) > MAX_LIST_VALUES:
        raise ValueError(f"{name} count invalid")
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, (str, int)):
            raise ValueError(f"{name} value invalid")
        text = str(item).strip()
        if not text or len(text) > MAX_STRING_LEN:
            raise ValueError(f"{name} value invalid")
        if text.upper() in {"ALL", "X"}:
            raise ValueError(f"{name} unrestricted value rejected")
        if text not in seen:
            out.append(text)
            seen.add(text)
    return out


def normalize_years(value: object, name: str = "Year") -> list[str]:
    years = normalize_scalar_list(value, name)
    for year in years:
        if not YEAR_RE.fullmatch(year):
            raise ValueError(f"{name} malformed year")
    return sorted(years)


def normalize_frequencies(value: object) -> list[str]:
    values = normalize_scalar_list(value, "Frequency")
    out: list[str] = []
    for item in values:
        freq = item.upper()
        if freq not in NIPA_FREQUENCIES:
            raise ValueError("Frequency invalid")
        if freq not in out:
            out.append(freq)
    return out


def normalize_line_code(value: object) -> str:
    text = str(value).strip()
    if not re.fullmatch(r"\d{1,8}", text) or int(text) <= 0:
        raise ValueError("LineCode invalid")
    return text


def normalize_geofips(value: object) -> list[str]:
    values = normalize_scalar_list(value, "GeoFips", allow_star=True)
    for item in values:
        if item.upper() in {"ALL", "X"} or not GEOFIPS_RE.fullmatch(item):
            raise ValueError("GeoFips invalid")
    return values


def normalize_filters(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError("filters must be a mapping")
    out: dict[str, str] = {}
    for key, raw in value.items():
        if str(key) in POLLUTION_KEYS:
            raise ValueError(f"unsafe filter key:{key}")
        safe_key = normalize_identifier(str(key), "filter")
        values = normalize_scalar_list(raw, safe_key)
        out[safe_key] = ",".join(values)
    return out


def bea_results(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    beaapi = payload.get("BEAAPI")
    if not isinstance(beaapi, Mapping):
        raise ResponseValidationError("Missing BEAAPI object.", provider="bea")
    error = beaapi.get("Error")
    if isinstance(error, Mapping):
        code = str(error.get("APIErrorCode") or "bea_error")[:40]
        description = str(error.get("APIErrorDescription") or "BEA API error")[:240]
        details = error.get("ErrorDetail")
        if isinstance(details, list) and details:
            description = f"{description} | {str(details[0].get('Description', ''))[:160]}"
        raise UpstreamResponseError("BEA API returned an error payload.", provider="bea", context={"bea_error_code": code, "bea_error_message": description})
    results = beaapi.get("Results")
    if not isinstance(results, Mapping):
        raise ResponseValidationError("Missing BEA Results object.", provider="bea")
    return results


def as_list(value: object) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    if isinstance(value, Mapping):
        return [value]
    return []


def parse_bool_like(value: object) -> tuple[bool | None, str | None]:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True, None
    if text in {"0", "false", "no", "n"}:
        return False, None
    if text == "":
        return None, None
    return None, f"unknown_boolean_flag:{value}"


def parse_numeric_value(value: object) -> tuple[int | float | None, str]:
    if value is None:
        return None, "unavailable"
    raw = str(value).strip()
    if not raw or raw in {"--", "(NA)", "NA", "..."}:
        return None, "unavailable"
    try:
        decimal_value = Decimal(raw.replace(",", ""))
    except InvalidOperation:
        return None, "malformed"
    if decimal_value == decimal_value.to_integral_value():
        return int(decimal_value), "available"
    return float(decimal_value), "available"


def period_kind(period: object) -> str:
    text = str(period or "")
    if re.fullmatch(r"\d{4}", text):
        return "annual"
    if re.fullmatch(r"\d{4}Q[1-4]", text):
        return "quarterly"
    if re.fullmatch(r"\d{4}M\d{2}", text):
        return "monthly"
    return "unknown"
