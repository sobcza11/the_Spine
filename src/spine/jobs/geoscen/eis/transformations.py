from __future__ import annotations

from typing import Any, Mapping, Sequence

TRANSFORMATION_FUNCTIONS = {
    "identity": "identity",
    "parse_numeric": "parse_numeric",
    "percent_change": "percent_change",
    "absolute_change": "absolute_change",
    "basis_point_change": "basis_point_change",
    "index_change": "index_change",
    "latest_valid": "latest_valid",
    "prior_period_valid": "prior_period_valid",
    "scale_by_unit_multiplier": "scale_by_unit_multiplier",
    "wide_to_long": "wide_to_long",
    "select_household_size": "select_household_size",
    "select_bedroom_count": "select_bedroom_count",
}


def identity(value: Any) -> dict[str, Any]:
    return {"value": value, "raw_value": value, "status": "measured" if value is not None else "unavailable", "warnings": [] if value is not None else ["value_missing"]}


def prior_period_valid(rows: Sequence[Mapping[str, Any]], *, value_field: str, period_field: str = "period") -> dict[str, Any]:
    latest = []
    for row in rows:
        value, status, warnings = parse_numeric(_lookup(row, value_field))
        if status == "measured":
            latest.append({"row": row, "value": value, "period_key": str(_lookup(row, period_field) or "")})
    latest = sorted(latest, key=lambda item: item["period_key"])
    if len(latest) < 2:
        return {"value": None, "raw_value": None, "status": "unavailable", "row": None, "warnings": ["prior_period_missing"]}
    chosen = latest[-2]
    return {"value": chosen["value"], "raw_value": _lookup(chosen["row"], value_field), "status": "measured", "row": chosen["row"], "warnings": []}


def parse_numeric(value: Any) -> tuple[float | None, str, list[str]]:
    if value is None or value == "":
        return None, "unavailable", ["value_missing"]
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None, "unavailable", ["value_not_numeric"]
    if number <= -666666000:
        return None, "unavailable", ["provider_suppression_code"]
    return number, "measured", []


def latest_valid(rows: Sequence[Mapping[str, Any]], *, value_field: str, period_field: str = "period") -> dict[str, Any]:
    valid: list[dict[str, Any]] = []
    warnings: list[str] = []
    for row in rows:
        value, status, row_warnings = parse_numeric(_lookup(row, value_field))
        warnings.extend(row_warnings)
        if status == "measured":
            valid.append({"row": row, "value": value, "period_key": str(_lookup(row, period_field) or row.get("measurement_period") or row.get("observation_date") or row.get("year") or "")})
    if not valid:
        return {"value": None, "raw_value": None, "status": "unavailable", "row": None, "warnings": sorted(set(warnings or ["no_valid_value"]))}
    chosen = sorted(valid, key=lambda item: item["period_key"])[-1]
    return {"value": chosen["value"], "raw_value": _lookup(chosen["row"], value_field), "status": "measured", "row": chosen["row"], "warnings": sorted(set(warnings))}


def percent_change(rows: Sequence[Mapping[str, Any]], *, value_field: str, period_field: str = "period") -> dict[str, Any]:
    valid: list[dict[str, Any]] = []
    warnings: list[str] = []
    for row in rows:
        value, status, row_warnings = parse_numeric(_lookup(row, value_field))
        warnings.extend(row_warnings)
        if status == "measured":
            valid.append({"row": row, "value": value, "period_key": str(_lookup(row, period_field) or row.get("measurement_period") or row.get("observation_date") or row.get("year") or "")})
    valid = sorted(valid, key=lambda item: item["period_key"])
    if len(valid) < 2:
        return {"value": None, "raw_value": None, "status": "unavailable", "row": valid[-1]["row"] if valid else None, "warnings": sorted(set(warnings + ["insufficient_history"]))}
    prior, current = valid[-2], valid[-1]
    if prior["value"] == 0:
        return {"value": None, "raw_value": None, "status": "unavailable", "row": current["row"], "warnings": sorted(set(warnings + ["prior_value_zero"]))}
    value = ((current["value"] - prior["value"]) / prior["value"]) * 100
    return {"value": round(value, 6), "raw_value": {"current": current["value"], "prior": prior["value"]}, "status": "derived", "row": current["row"], "warnings": sorted(set(warnings))}


def absolute_change(current: float | int | None, prior: float | int | None) -> dict[str, Any]:
    if current is None or prior is None:
        return {"value": None, "status": "unavailable", "warnings": ["missing_comparison_value"]}
    return {"value": current - prior, "status": "derived", "warnings": []}


def basis_point_change(current: float | int | None, prior: float | int | None) -> dict[str, Any]:
    result = absolute_change(current, prior)
    if result["value"] is not None:
        result["value"] = result["value"] * 100
    return result


def index_change(current: float | int | None, prior: float | int | None) -> dict[str, Any]:
    return absolute_change(current, prior)


def scale_by_unit_multiplier(value: float | int | None, multiplier: float | int) -> float | None:
    return None if value is None else value * multiplier


def wide_to_long(row: Mapping[str, Any], fields: Sequence[str]) -> list[dict[str, Any]]:
    return [{"field": field, "value": _lookup(row, field)} for field in fields]


def select_household_size(rows: Sequence[Mapping[str, Any]], household_size: int) -> list[Mapping[str, Any]]:
    return [row for row in rows if int(row.get("household_size", -1)) == household_size]


def select_bedroom_count(rows: Sequence[Mapping[str, Any]], bedroom_count: int) -> list[Mapping[str, Any]]:
    return [row for row in rows if int(row.get("bedrooms", -1)) == bedroom_count]


def _lookup(row: Mapping[str, Any], dotted: str) -> Any:
    current: Any = row
    for part in dotted.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current
