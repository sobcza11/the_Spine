from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from urllib.parse import urlsplit

from spine.jobs.geoscen.eis.contracts import POLLUTION_KEYS, SCHEMA_VERSION, ValidationResult

TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$", re.I)


def fail(errors: Sequence[str], warnings: Sequence[str] = (), required_fields: Sequence[str] = (), row_count: int = 0) -> ValidationResult:
    return ValidationResult(False, tuple(errors), tuple(warnings), tuple(required_fields), row_count, SCHEMA_VERSION)


def pass_result(warnings: Sequence[str] = (), required_fields: Sequence[str] = (), row_count: int = 0) -> ValidationResult:
    return ValidationResult(True, (), tuple(warnings), tuple(required_fields), row_count, SCHEMA_VERSION)


def validate_required_keys(mapping: Mapping[str, object], required: Sequence[str]) -> ValidationResult:
    errors = _pollution_errors(mapping)
    missing = [key for key in required if key not in mapping]
    errors.extend(f"missing_key:{key}" for key in missing)
    return fail(errors, required_fields=required) if errors else pass_result(required_fields=required)


def validate_required_row_fields(rows: Sequence[Mapping[str, object]], required: Sequence[str]) -> ValidationResult:
    base = validate_normalized_rows(rows)
    errors = list(base.errors)
    for index, row in enumerate(rows):
        for key in required:
            if key not in row:
                errors.append(f"row:{index}:missing_field:{key}")
    return fail(errors, required_fields=required, row_count=len(rows)) if errors else pass_result(required_fields=required, row_count=len(rows))


def validate_non_empty_sequence(value: object, name: str = "sequence") -> ValidationResult:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return fail([f"{name}:not_sequence"])
    if not value:
        return fail([f"{name}:empty"])
    return pass_result(row_count=len(value))


def validate_timestamp(value: object) -> ValidationResult:
    if not isinstance(value, str) or not TIMESTAMP_RE.match(value):
        return fail(["invalid_timestamp"])
    return pass_result()


def validate_numeric(value: object, name: str = "value") -> ValidationResult:
    if isinstance(value, bool):
        return fail([f"{name}:not_numeric"])
    try:
        float(value)
    except (TypeError, ValueError):
        return fail([f"{name}:not_numeric"])
    return pass_result()


def validate_allowlist(value: str, allowed: Sequence[str], name: str = "value") -> ValidationResult:
    return pass_result() if value in set(allowed) else fail([f"{name}:not_allowed"])


def validate_row_count(rows: Sequence[object], min_rows: int = 0, max_rows: int | None = None) -> ValidationResult:
    count = len(rows)
    errors = []
    if count < min_rows:
        errors.append("row_count:too_low")
    if max_rows is not None and count > max_rows:
        errors.append("row_count:too_high")
    return fail(errors, row_count=count) if errors else pass_result(row_count=count)


def validate_content_type(content_type: str | None, allowed: Sequence[str]) -> ValidationResult:
    value = (content_type or "").split(";", 1)[0].strip().lower()
    allowed_l = {item.lower() for item in allowed}
    return pass_result() if value in allowed_l else fail(["content_type:not_allowed"])


def validate_schema_version(value: str) -> ValidationResult:
    return pass_result() if value == SCHEMA_VERSION else fail(["schema_version:invalid"])


def validate_provider_id(value: str) -> ValidationResult:
    return pass_result() if IDENTIFIER_RE.match(value or "") else fail(["provider_id:invalid"])


def validate_operation_id(value: str) -> ValidationResult:
    return pass_result() if IDENTIFIER_RE.match(value or "") else fail(["operation_id:invalid"])


def validate_endpoint_safety(url: str, allowed_hosts: Sequence[str] | None = None) -> ValidationResult:
    parts = urlsplit(url)
    errors = []
    if parts.scheme != "https":
        errors.append("endpoint:scheme_not_https")
    if not parts.netloc:
        errors.append("endpoint:missing_host")
    if allowed_hosts is not None and parts.netloc.lower() not in {host.lower() for host in allowed_hosts}:
        errors.append("endpoint:host_not_allowed")
    return fail(errors) if errors else pass_result()


def validate_normalized_rows(rows: object) -> ValidationResult:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return fail(["normalized_rows:not_sequence"])
    errors = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            errors.append(f"row:{index}:not_mapping")
            continue
        errors.extend(f"row:{index}:{error}" for error in _pollution_errors(row))
    return fail(errors, row_count=len(rows)) if errors else pass_result(row_count=len(rows))


def _pollution_errors(mapping: Mapping[str, object]) -> list[str]:
    errors: list[str] = []
    for key, value in mapping.items():
        if str(key) in POLLUTION_KEYS:
            errors.append(f"unsafe_key:{key}")
        if isinstance(value, Mapping):
            errors.extend(_pollution_errors(value))
    return errors
