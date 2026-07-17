from __future__ import annotations

import csv
import re
from decimal import Decimal, InvalidOperation
from io import StringIO
from typing import Any

from spine.jobs.geoscen.eis.exceptions import ResponseValidationError
from spine.jobs.geoscen.eis.structure_context.connectors.fhfa.datasets import FHFADataset

MAX_ROWS = 250_000
QUARTER_RE = re.compile(r"^[1-4]$")
YEAR_RE = re.compile(r"^\d{4}$")


def parse_csv_bytes(content: bytes, dataset: FHFADataset) -> tuple[list[dict[str, str]], list[str]]:
    if not content:
        raise ResponseValidationError("FHFA file is empty.", provider="fhfa")
    text = content.decode("utf-8-sig", errors="replace")
    if text.lstrip().lower().startswith("<!doctype html") or text.lstrip().lower().startswith("<html"):
        raise ResponseValidationError("FHFA response appears to be HTML, not CSV.", provider="fhfa")
    reader = csv.DictReader(StringIO(text))
    headers = reader.fieldnames or []
    headers = [header.strip("\ufeff") for header in headers]
    if not headers or any(not header.strip() for header in headers):
        raise ResponseValidationError("FHFA CSV header malformed.", provider="fhfa")
    if len(headers) != len(set(headers)):
        raise ResponseValidationError("FHFA CSV duplicate header.", provider="fhfa")
    missing = [column for column in dataset.expected_columns if column not in headers]
    if missing:
        raise ResponseValidationError("FHFA CSV missing required columns.", provider="fhfa", context={"missing": ",".join(missing)})
    rows: list[dict[str, str]] = []
    warnings: list[str] = []
    for idx, row in enumerate(reader, start=2):
        if idx > MAX_ROWS:
            raise ResponseValidationError("FHFA CSV row count exceeded.", provider="fhfa")
        if row.get(None):
            warnings.append("row_width_mismatch")
            continue
        rows.append({str(key).strip("\ufeff"): str(value or "").strip() for key, value in row.items() if key is not None})
    if not rows:
        warnings.append("no_data_rows")
    return rows, warnings


def parse_decimal(value: object) -> tuple[float | int | None, str]:
    text = str(value or "").strip()
    if text in {"", "-", "--", "NA", "N/A", "."}:
        return None, "unavailable"
    try:
        number = Decimal(text.replace(",", ""))
    except InvalidOperation:
        return None, "malformed"
    if number == number.to_integral_value():
        return int(number), "available"
    return float(number), "available"


def normalize_period(row: dict[str, str], dataset: FHFADataset) -> tuple[str | None, int | None, int | None, int | None, str | None]:
    year_raw = row.get("yr") or row.get("year")
    year = int(year_raw) if year_raw and YEAR_RE.fullmatch(year_raw) else None
    if dataset.frequency == "monthly":
        period_raw = row.get("period") or row.get("month")
        month = int(period_raw) if period_raw and period_raw.isdigit() and 1 <= int(period_raw) <= 12 else None
        period = f"{year:04d}-{month:02d}" if year and month else None
        return period, year, None, month, period_raw
    if dataset.frequency == "quarterly":
        quarter_raw = row.get("qtr") or row.get("period")
        quarter = int(quarter_raw) if quarter_raw and QUARTER_RE.fullmatch(quarter_raw) else None
        period = f"{year:04d}Q{quarter}" if year and quarter else None
        return period, year, quarter, None, quarter_raw
    period = f"{year:04d}" if year else None
    return period, year, None, None, year_raw


def normalize_hpi_rows(raw_rows: list[dict[str, str]], dataset: FHFADataset, retrieved_at: str) -> tuple[list[dict[str, Any]], list[str], str | None]:
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    for row in raw_rows:
        normalized = _base_row(row, dataset, retrieved_at)
        if normalized is None:
            warnings.append("invalid_required_row")
            continue
        rows.append(normalized)
    rows.sort(key=lambda item: (str(item.get("geography_code") or ""), str(item.get("period") or "")))
    measurement = max((str(row.get("period") or "") for row in rows), default=None)
    return rows, warnings, measurement


def _base_row(row: dict[str, str], dataset: FHFADataset, retrieved_at: str) -> dict[str, Any] | None:
    period, year, quarter, month, raw_period = normalize_period(row, dataset)
    value_source = row.get("index_nsa") or row.get("hpi") or row.get("index")
    index_value, value_status = parse_decimal(value_source)
    annual, _ = parse_decimal(row.get("annual_change"))
    quarterly, _ = parse_decimal(row.get("quarterly_change"))
    monthly, _ = parse_decimal(row.get("monthly_change"))
    geo = _geography(row, dataset)
    if not period or not geo:
        return None
    return {
        "provider": "fhfa",
        "dataset_id": dataset.dataset_id,
        "geography_level": dataset.geography_level,
        "geography_code": geo.get("geography_code"),
        "geography_name": geo.get("geography_name"),
        "state_code": geo.get("state_code"),
        "state_name": geo.get("state_name"),
        "metro_code": geo.get("metro_code"),
        "county_fips": geo.get("county_fips"),
        "year": year,
        "quarter": quarter,
        "month": month,
        "period": period,
        "raw_period": raw_period,
        "frequency": dataset.frequency,
        "index_type": dataset.index_type,
        "index_value": index_value,
        "raw_index_value": value_source,
        "value_status": value_status,
        "annual_change": annual,
        "quarterly_change": quarterly,
        "monthly_change": monthly,
        "seasonal_adjustment": dataset.seasonal_adjustment,
        "unit": "index",
        "source": dataset.attribution,
        "retrieval_timestamp": retrieved_at,
    }


def _geography(row: dict[str, str], dataset: FHFADataset) -> dict[str, str] | None:
    if dataset.geography_level == "national":
        return {"geography_code": "US", "geography_name": row.get("place_name") or "United States"}
    if dataset.geography_level == "state":
        state = row.get("state") or row.get("state_code")
        if not state:
            return None
        return {"geography_code": state, "geography_name": row.get("state_name"), "state_code": state, "state_name": row.get("state_name")}
    if dataset.geography_level == "metro":
        code = row.get("cbsa") or row.get("metro_code")
        if not code:
            return None
        return {"geography_code": code, "geography_name": row.get("metro_name"), "metro_code": code}
    if dataset.geography_level == "county":
        fips = row.get("county_fips")
        if not fips or not re.fullmatch(r"\d{5}", fips):
            return None
        return {"geography_code": fips, "geography_name": row.get("county_name"), "county_fips": fips}
    return None
