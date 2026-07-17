from __future__ import annotations

import re
from typing import Any, Mapping

GEO_TYPES = {"us", "region", "division", "state", "county", "place", "tract", "block group"}
PARENT_TYPES = {"state", "county", "tract"}
FIPS_RE = {
    "us": re.compile(r"^1$"),
    "region": re.compile(r"^(\*|\d)$"),
    "division": re.compile(r"^(\*|\d)$"),
    "state": re.compile(r"^(\*|\d{2})$"),
    "county": re.compile(r"^(\*|\d{3})$"),
    "place": re.compile(r"^(\*|\d{5})$"),
    "tract": re.compile(r"^(\*|\d{6})$"),
    "block group": re.compile(r"^(\*|\d)$"),
}


def normalize_geography(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("geography must be a mapping")
    if set(value) - {"for", "in"}:
        raise ValueError("unexpected geography field")
    target = _normalize_geo_part(value.get("for"), "for")
    parents = [_normalize_geo_part(item, "in") for item in value.get("in", [])]
    _validate_hierarchy(target, parents)
    return {"for": target, "in": parents}


def for_expression(geography: Mapping[str, Any]) -> str:
    target = geography["for"]
    return f"{target['type']}:{target['value']}"


def in_expression(geography: Mapping[str, Any]) -> str | None:
    parents = geography.get("in") or []
    if not parents:
        return None
    return " ".join(f"{item['type']}:{item['value']}" for item in parents)


def geography_metadata(geography: Mapping[str, Any]) -> dict[str, Any]:
    parents = geography.get("in") or []
    return {
        "geography_type": geography["for"]["type"],
        "parent_geography_types": [item["type"] for item in parents],
    }


def _normalize_geo_part(value: object, field: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError(f"geography {field} must be a mapping")
    if set(value) != {"type", "value"}:
        raise ValueError(f"geography {field} fields invalid")
    kind = str(value["type"]).strip().lower()
    raw = str(value["value"]).strip()
    if kind not in GEO_TYPES:
        raise ValueError(f"unsupported geography:{kind}")
    if not FIPS_RE[kind].fullmatch(raw):
        raise ValueError(f"malformed geography:{kind}")
    return {"type": kind, "value": raw}


def _validate_hierarchy(target: Mapping[str, str], parents: list[Mapping[str, str]]) -> None:
    parent_types = [item["type"] for item in parents]
    if len(parent_types) != len(set(parent_types)):
        raise ValueError("duplicate parent geography")
    if any(kind not in PARENT_TYPES for kind in parent_types):
        raise ValueError("unsupported parent geography")
    kind = target["type"]
    value = target["value"]
    if kind in {"us", "region", "division", "state"}:
        if parents:
            raise ValueError("unsupported hierarchy")
        if kind == "us" and value != "1":
            raise ValueError("malformed geography:us")
        return
    if kind in {"county", "place"}:
        _require_exact(parents, ["state"])
        return
    if kind == "tract":
        _require_exact(parents, ["state", "county"])
        return
    if kind == "block group":
        _require_exact(parents, ["state", "county", "tract"])
        return


def _require_exact(parents: list[Mapping[str, str]], required: list[str]) -> None:
    types = [item["type"] for item in parents]
    if types != required:
        raise ValueError("missing required parent geography")
    for item in parents:
        if item["value"] == "*":
            raise ValueError("parent wildcard rejected")
