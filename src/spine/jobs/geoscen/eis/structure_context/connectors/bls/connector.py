from __future__ import annotations

import json
from typing import Any, Mapping

from spine.jobs.geoscen.eis.contracts import ConnectorRequest, SourceMetadata, UpstreamResponse, ValidationResult
from spine.jobs.geoscen.eis.credentials import Credential
from spine.jobs.geoscen.eis.exceptions import (
    RequestValidationError,
    ResponseValidationError,
    UnsupportedOperationError,
    UpstreamResponseError,
)
from spine.jobs.geoscen.eis.validation import validate_normalized_rows
from spine.jobs.geoscen.eis.structure_context.connectors.bls.parsing import (
    BLS_SUCCESS_STATUSES,
    normalize_bool,
    normalize_footnotes,
    normalize_period,
    normalize_series_ids,
    normalize_year,
    parse_numeric_value,
    results_series,
)

BLS_ENDPOINT = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_TRANSFORMATION_VERSION = "bls-timeseries-v1"
OPTIONAL_FLAG_MAP = {
    "catalog": "catalog",
    "calculations": "calculations",
    "annual_average": "annualaverage",
    "aspects": "aspects",
}
ALLOWED_PARAMETERS = {"series_ids", "start_year", "end_year", *OPTIONAL_FLAG_MAP}
REQUIRED_ROW_FIELDS = (
    "provider",
    "series_id",
    "year",
    "period",
    "period_name",
    "value_text",
    "value",
    "measurement_period",
    "retrieval_timestamp",
)


class BLSConnector:
    provider = "bls"
    supported_operations = ("timeseries",)
    credential_specification = {"BLS_API_KEY": True}
    endpoint = BLS_ENDPOINT

    def validate_request(self, request: ConnectorRequest) -> ValidationResult:
        errors: list[str] = []
        warnings: list[str] = []
        if request.provider != self.provider:
            errors.append("provider:not_bls")
        if request.operation not in self.supported_operations:
            errors.append("operation:unsupported")
        unexpected = sorted(set(request.parameters) - ALLOWED_PARAMETERS)
        errors.extend(f"unexpected_parameter:{key}" for key in unexpected)
        try:
            original = request.parameters.get("series_ids", [])
            if isinstance(original, (list, tuple)) and len(original) > 50:
                errors.append("series_ids exceeds BLS API maximum")
            series_ids = normalize_series_ids(request.parameters.get("series_ids"))
            if isinstance(original, (list, tuple)) and len(series_ids) < len(original):
                warnings.append("duplicate_series_ids_removed")
        except Exception as exc:
            errors.append(str(exc))
        try:
            start_year = normalize_year(request.parameters.get("start_year"), "start_year")
            end_year = normalize_year(request.parameters.get("end_year"), "end_year")
            if int(start_year) > int(end_year):
                errors.append("year_range:reversed")
            if int(end_year) - int(start_year) > 20:
                errors.append("year_range:exceeds_bls_window")
        except Exception as exc:
            errors.append(str(exc))
        for input_key in OPTIONAL_FLAG_MAP:
            if input_key in request.parameters:
                try:
                    normalize_bool(request.parameters[input_key], input_key)
                except Exception as exc:
                    errors.append(str(exc))
        return ValidationResult(
            valid=not errors,
            errors=tuple(errors),
            warnings=tuple(warnings),
            required_fields=("series_ids", "start_year", "end_year"),
        )

    def normalized_request(self, request: ConnectorRequest) -> dict[str, Any]:
        validation = self.validate_request(request)
        if not validation.valid:
            raise RequestValidationError(
                "Invalid BLS timeseries request.",
                provider=self.provider,
                operation=request.operation,
                context={"errors": ",".join(validation.errors[:5])},
            )
        series_ids = normalize_series_ids(request.parameters["series_ids"])
        start_year = normalize_year(request.parameters["start_year"], "start_year")
        end_year = normalize_year(request.parameters["end_year"], "end_year")
        flags = {
            output_key: normalize_bool(request.parameters[input_key], input_key)
            for input_key, output_key in OPTIONAL_FLAG_MAP.items()
            if input_key in request.parameters
        }
        return {"series_ids": series_ids, "start_year": start_year, "end_year": end_year, "flags": flags}

    def build_request(self, request: ConnectorRequest, credentials: Mapping[str, Credential]) -> Mapping[str, Any]:
        if request.operation not in self.supported_operations:
            raise UnsupportedOperationError(self.provider, request.operation)
        normalized = self.normalized_request(request)
        credential = credentials.get("BLS_API_KEY")
        if credential is None:
            raise RequestValidationError("BLS credential unavailable.", provider=self.provider, operation=request.operation)
        body: dict[str, Any] = {
            "seriesid": normalized["series_ids"],
            "startyear": normalized["start_year"],
            "endyear": normalized["end_year"],
            "registrationkey": credential.value,
            **normalized["flags"],
        }
        safe_metadata = {
            "provider": self.provider,
            "operation": request.operation,
            "endpoint_id": "bls_public_data_api_v2_timeseries",
            "method": "POST",
            "series_count": len(normalized["series_ids"]),
            "series_ids": normalized["series_ids"],
            "start_year": normalized["start_year"],
            "end_year": normalized["end_year"],
            "flags": normalized["flags"],
            "correlation_id": request.correlation_id,
        }
        return {"method": "POST", "url": self.endpoint, "headers": {"Content-Type": "application/json"}, "json": body, "safe_metadata": safe_metadata}

    def fetch(self, request: ConnectorRequest, http_client: Any, credentials: Mapping[str, Credential]) -> UpstreamResponse:
        built = self.build_request(request, credentials)
        return http_client.request(
            built["method"],
            built["url"],
            correlation_id=request.correlation_id,
            headers=built["headers"],
            json=built["json"],
            timeout_policy=request.timeout_policy,
        )

    def parse_response(self, response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        try:
            payload = json.loads(response.content.decode("utf-8"))
        except Exception as exc:
            raise UpstreamResponseError("Malformed BLS JSON response.", provider=self.provider, operation=request.operation) from exc
        if not isinstance(payload, Mapping):
            raise ResponseValidationError("BLS payload must be an object.", provider=self.provider, operation=request.operation)
        status = str(payload.get("status") or "").strip()
        upstream_messages = _bounded_messages(payload.get("message"))
        if status not in BLS_SUCCESS_STATUSES:
            raise UpstreamResponseError(
                "BLS response status was not successful.",
                provider=self.provider,
                operation=request.operation,
                context={"bls_status": status},
            )
        requested = self.normalized_request(request)["series_ids"]
        series_list = results_series(payload)
        rows, series_warnings = self._normalize_series(series_list, requested, response.retrieved_at)
        seen_series = {str(series.get("seriesID") or "") for series in series_list}
        missing = [series_id for series_id in requested if series_id not in seen_series]
        warnings = [*upstream_messages, *series_warnings, *(f"missing_series:{series_id}" for series_id in missing)]
        validation = validate_normalized_rows(rows)
        if validation.valid:
            validation = ValidationResult(True, (), tuple(warnings), REQUIRED_ROW_FIELDS, len(rows))
        measurement_as_of = max((row["measurement_period"] for row in rows), default=None)
        response_status = "success"

        if missing:
            response_status = (
                "partial"
                if rows
                else "unavailable"
            )
        elif not rows:
            response_status = "unavailable"
        dataset = ",".join(requested[:5]) + (f",+{len(requested) - 5}" if len(requested) > 5 else "")
        return {
            "response_status": response_status,
            "source_metadata": SourceMetadata(
                provider=self.provider,
                endpoint="bls_public_data_api_v2_timeseries",
                dataset=dataset,
                release=None,
                measurement_as_of=measurement_as_of,
                publication_date=None,
                retrieval_timestamp=response.retrieved_at,
                content_type=response.content_type,
                upstream_status=response.status_code,
            ),
            "normalized_rows": rows,
            "warnings": tuple(warnings),
            "validation": validation,
            "source_payload": "bls_public_data_api_v2_timeseries",
            "source_artifact": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            "transformation_version": BLS_TRANSFORMATION_VERSION,
        }

    def to_canonical_response(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("BLSConnector uses the shared dispatcher for canonical responses.")

    def _normalize_series(self, series_list: list[Mapping[str, Any]], requested: list[str], retrieved_at: str) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        requested_set = {
            str(item).strip().upper()
            for item in requested
            if str(item).strip()
        }

        for series in series_list:
            series_id = (
                str(series.get("seriesID") or "")
                .strip()
                .upper()
            )

            if not series_id:
                warnings.append("series_missing_id")
                continue
            if series_id not in requested_set:
                warnings.append(f"unexpected_series:{series_id}")
            data = series.get("data")
            if not isinstance(data, list) or not data:
                warnings.append(f"empty_series:{series_id}")
                continue
            catalog = series.get("catalog") if isinstance(series.get("catalog"), Mapping) else {}
            for observation in data:
                if not isinstance(observation, Mapping):
                    warnings.append(f"malformed_observation:{series_id}")
                    continue
                year = str(observation.get("year") or "").strip()
                period = str(observation.get("period") or "").strip()
                period_info = normalize_period(year, period, str(observation.get("periodName") or "").strip() or None)
                value_text = str(observation.get("value") or "").strip()
                footnote_codes, footnote_text = normalize_footnotes(observation.get("footnotes"))
                row = {
                    "provider": self.provider,
                    "series_id": series_id,
                    "year": year,
                    "period": period_info["period"],
                    "period_name": period_info["period_name"],
                    "period_kind": period_info["period_kind"],
                    "value_text": value_text,
                    "value": parse_numeric_value(value_text),
                    "footnote_codes": footnote_codes,
                    "footnote_text": footnote_text,
                    "latest": str(observation.get("latest") or "").lower() == "true",
                    "measurement_period": period_info["measurement_period"],
                    "measurement_date": period_info["measurement_date"],
                    "date_convention": period_info["date_convention"],
                    "retrieval_timestamp": retrieved_at,
                }
                row.update(_catalog_fields(catalog))
                rows.append(row)
        rows.sort(key=lambda item: (item["series_id"], item["year"], item["period"]))
        return rows, warnings


def _bounded_messages(value: object, limit: int = 10) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item)[:240] for item in value[:limit] if str(item).strip()]


def _catalog_fields(catalog: Mapping[str, Any]) -> dict[str, Any]:
    mapping = {
        "series_title": "series_title",
        "survey_name": "survey_name",
        "seasonality": "seasonality",
        "area": "area",
        "item": "item",
        "measure_data_type": "measure_data_type",
    }
    return {output: catalog.get(input_key) for input_key, output in mapping.items() if catalog.get(input_key) is not None}
