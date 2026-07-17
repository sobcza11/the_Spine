from __future__ import annotations

from calendar import monthrange
from datetime import date
from typing import Any, Mapping


def normalize_period(row: Mapping[str, Any], *, frequency: str, source_retrieved_at: str | None = None) -> dict[str, Any]:
    candidate = row.get("measurement_period") or row.get("period") or row.get("observation_date") or row.get("time_period") or row.get("year")
    text = str(candidate or "").strip()
    if frequency == "monthly":
        return _monthly(text, row)
    if frequency == "quarterly":
        return _quarterly(text, row)
    if frequency == "annual":
        return _annual(text, row)
    if frequency == "five_year_estimate":
        year = str(row.get("year") or row.get("acs_year") or _year_from_retrieved_at(source_retrieved_at) or "")
        return _period(f"{year}-acs5" if year else "unknown-acs5", frequency, None, None, row, source_retrieved_at)
    return _period(text or "metadata", frequency, None, None, row, source_retrieved_at)


def _monthly(text: str, row: Mapping[str, Any]) -> dict[str, Any]:
    if len(text) == 7 and text[4] == "-":
        year, month = int(text[:4]), int(text[5:7])
    else:
        year = int(row.get("year") or text[:4] or 0)
        month_text = str(row.get("month") or row.get("period") or "")
        month = int(month_text[1:]) if month_text.startswith("M") else _safe_month(month_text)
    if year <= 0:
        return _period("unknown-monthly", "monthly", None, None, row, None)
    last = monthrange(year, month)[1]
    return _period(f"{year:04d}-{month:02d}", "monthly", date(year, month, 1).isoformat(), date(year, month, last).isoformat(), row, None)


def _safe_month(value: str) -> int:
    try:
        parsed = int(value or 1)
    except ValueError:
        return 1
    return parsed if 1 <= parsed <= 12 else 1


def _quarterly(text: str, row: Mapping[str, Any]) -> dict[str, Any]:
    year = str(row.get("year") or text[:4] or "")
    quarter = str(row.get("quarter") or text[-1:] or "")
    if str(row.get("period") or "").startswith("Q"):
        quarter = str(row["period"])[1:]
    return _period(f"{year}Q{quarter}" if year and quarter else text, "quarterly", None, None, row, None)


def _annual(text: str, row: Mapping[str, Any]) -> dict[str, Any]:
    year = str(row.get("year") or text[:4] or "")
    return _period(year or text, "annual", None, None, row, None)


def _year_from_retrieved_at(value: str | None) -> str | None:
    if isinstance(value, str) and len(value) >= 4 and value[:4].isdigit():
        return value[:4]
    return None


def _period(period: str, frequency: str, start: str | None, end: str | None, row: Mapping[str, Any], retrieved_at: str | None) -> dict[str, Any]:
    return {
        "period": period,
        "period_start": start,
        "period_end": end,
        "frequency": frequency,
        "measurement_as_of": row.get("measurement_as_of") or row.get("measurement_date"),
        "publication_date": row.get("publication_date"),
        "retrieval_timestamp": row.get("retrieval_timestamp") or retrieved_at,
    }
