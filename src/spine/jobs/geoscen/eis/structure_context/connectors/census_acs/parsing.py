from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from spine.jobs.geoscen.eis.exceptions import ResponseValidationError, UpstreamResponseError
from spine.jobs.geoscen.eis.structure_context.connectors.census_acs.products import MAX_GROUP_LEN, MAX_LABEL_LEN, MAX_PREDICATE_VALUES, MAX_VARIABLES, SENTINEL_VALUES

VARIABLE_RE = re.compile(r"^(NAME|[A-Z][A-Z0-9]{0,5}_\d{3,}[A-Z]*|[A-Z]{2,4}\d{2,}[A-Z_0-9]*)$")
GROUP_RE = re.compile(r"^[A-Za-z0-9_]{1,40}$")
PREDICATE_KEY_RE = re.compile(r"^[A-Za-z0-9_]{1,80}$")
RESERVED_PREDICATES = {"key", "get", "for", "in", "endpoint", "url"}
POLLUTION_KEYS = {"__proto__", "constructor", "prototype"}


def load_census_json(response_text: str) -> Any:
    try:
        payload = json.loads(response_text)
    except Exception as exc:
        text = response_text.strip()
        if text:
            raise UpstreamResponseError("Malformed Census JSON response.", provider="census_acs", context={"message": text[:240]}) from exc
        raise UpstreamResponseError("Malformed Census JSON response.", provider="census_acs") from exc
    if isinstance(payload, Mapping) and "error" in payload:
        raise UpstreamResponseError("Census API returned an error payload.", provider="census_acs", context={"message": str(payload.get("error"))[:240]})
    return payload


def bounded_text(value: object, limit: int = MAX_LABEL_LEN) -> str | None:
    if value is None:
        return None
    return str(value)[:limit]


def normalize_group(value: object) -> str:
    text = str(value or "").strip()
    if not GROUP_RE.fullmatch(text) or "/" in text or "\\" in text or ".." in text or "?" in text:
        raise ValueError("group invalid")
    if len(text) > MAX_GROUP_LEN:
        raise ValueError("group too long")
    return text


def normalize_variables(value: object) -> list[str]:
    if not isinstance(value, (list, tuple)):
        raise ValueError("variables must be a list")
    if not value or len(value) > MAX_VARIABLES:
        raise ValueError("variables count invalid")
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item).strip()
        if not VARIABLE_RE.fullmatch(text):
            raise ValueError("variable invalid")
        if text not in seen:
            out.append(text)
            seen.add(text)
    return out


def normalize_predicates(value: object | None) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("predicates must be a mapping")
    out: dict[str, str] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key).strip()
        if key in POLLUTION_KEYS or key.lower() in RESERVED_PREDICATES or not PREDICATE_KEY_RE.fullmatch(key):
            raise ValueError(f"predicate rejected:{key}")
        values = raw_value if isinstance(raw_value, (list, tuple)) else [raw_value]
        if not values or len(values) > MAX_PREDICATE_VALUES:
            raise ValueError(f"predicate count invalid:{key}")
        normalized: list[str] = []
        for item in values:
            if not isinstance(item, (str, int, float)):
                raise ValueError(f"predicate value invalid:{key}")
            text = str(item).strip()
            if not text or len(text) > MAX_LABEL_LEN or any(ch in text for ch in ["&", "?", "#"]):
                raise ValueError(f"predicate value invalid:{key}")
            normalized.append(text)
        out[key] = ",".join(normalized)
    return out


def parse_value(value: object) -> tuple[str | None, int | float | None, str]:
    if value is None:
        return None, None, "unavailable"
    raw = str(value)
    text = raw.strip()
    if text == "" or text in SENTINEL_VALUES:
        return raw, None, "unavailable"
    try:
        number = Decimal(text.replace(",", ""))
    except InvalidOperation:
        return raw, None, "text"
    if number == number.to_integral_value():
        return raw, int(number), "available"
    return raw, float(number), "available"


def ensure_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ResponseValidationError(f"Census {label} payload must be an object.", provider="census_acs")
    return value
