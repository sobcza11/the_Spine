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
from spine.jobs.geoscen.eis.structure_context.connectors.fred.parsing import (
    AGGREGATION_ALLOWLIST,
    FREQUENCY_ALLOWLIST,
    MAX_LIMIT,
    MAX_OFFSET,
    SORT_ORDER_ALLOWLIST,
    UNITS_ALLOWLIST,
    error_payload,
    normalize_allowlist,
    normalize_date,
    normalize_int,
    normalize_series_id,
    observations_payload,
    parse_numeric_value,
    seriess_payload,
)

FRED_OBSERVATIONS_ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"
FRED_METADATA_ENDPOINT = "https://api.stlouisfed.org/fred/series"
OBSERVATION_OPTIONAL_PARAMS = {
    "observation_start",
    "observation_end",
    "frequency",
    "aggregation_method",
    "units",
    "realtime_start",
    "realtime_end",
    "vintage_dates",
    "limit",
    "offset",
    "sort_order",
}
METADATA_OPTIONAL_PARAMS = {"realtime_start", "realtime_end"}
TRANSFORMATION_BY_OPERATION = {
    "series_observations": "fred-series-observations-v1",
    "series_metadata": "fred-series-metadata-v1",
}


class FREDConnector:
    provider = "fred"
    supported_operations = ("series_observations", "series_metadata")
    credential_specification = {"FRED_API_KEY": True}
    endpoints = {
        "series_observations": FRED_OBSERVATIONS_ENDPOINT,
        "series_metadata": FRED_METADATA_ENDPOINT,
    }

    def validate_request(self, request: ConnectorRequest) -> ValidationResult:
        errors: list[str] = []
        warnings: list[str] = []
        if request.provider != self.provider:
            errors.append("provider:not_fred")
        if request.operation not in self.supported_operations:
            errors.append("operation:unsupported")
            allowed = set()
        elif request.operation == "series_observations":
            allowed = {"series_id", *OBSERVATION_OPTIONAL_PARAMS}
        else:
            allowed = {"series_id", *METADATA_OPTIONAL_PARAMS}
        unexpected = sorted(set(request.parameters) - allowed)
        errors.extend(f"unexpected_parameter:{key}" for key in unexpected)
        try:
            normalize_series_id(request.parameters.get("series_id"))
        except Exception as exc:
            errors.append(str(exc))
        date_pairs = []
        for key in ("observation_start", "observation_end", "realtime_start", "realtime_end", "vintage_dates"):
            if key in request.parameters:
                try:
                    if key == "vintage_dates":
                        for date in str(request.parameters[key]).split(","):
                            normalize_date(date.strip(), key)
                    else:
                        date_pairs.append((key, normalize_date(request.parameters[key], key)))
                except Exception as exc:
                    errors.append(str(exc))
        date_map = dict(date_pairs)
        if date_map.get("observation_start") and date_map.get("observation_end") and date_map["observation_start"] > date_map["observation_end"]:
            errors.append("observation_date_range:reversed")
        if date_map.get("realtime_start") and date_map.get("realtime_end") and date_map["realtime_start"] > date_map["realtime_end"]:
            errors.append("realtime_date_range:reversed")
        for key, allowlist in (
            ("frequency", FREQUENCY_ALLOWLIST),
            ("aggregation_method", AGGREGATION_ALLOWLIST),
            ("units", UNITS_ALLOWLIST),
            ("sort_order", SORT_ORDER_ALLOWLIST),
        ):
            if key in request.parameters:
                try:
                    normalize_allowlist(request.parameters[key], key, allowlist)
                except Exception as exc:
                    errors.append(str(exc))
        if "limit" in request.parameters:
            try:
                normalize_int(request.parameters["limit"], "limit", minimum=1, maximum=MAX_LIMIT)
            except Exception as exc:
                errors.append(str(exc))
        if "offset" in request.parameters:
            try:
                normalize_int(request.parameters["offset"], "offset", minimum=0, maximum=MAX_OFFSET)
            except Exception as exc:
                errors.append(str(exc))
        return ValidationResult(not errors, tuple(errors), tuple(warnings), ("series_id",))

    def normalized_request(self, request: ConnectorRequest) -> dict[str, Any]:
        validation = self.validate_request(request)
        if not validation.valid:
            raise RequestValidationError(
                "Invalid FRED request.",
                provider=self.provider,
                operation=request.operation,
                context={"errors": ",".join(validation.errors[:5])},
            )
        series_id = normalize_series_id(request.parameters["series_id"])
        params: dict[str, Any] = {"series_id": series_id, "file_type": "json"}
        optional = OBSERVATION_OPTIONAL_PARAMS if request.operation == "series_observations" else METADATA_OPTIONAL_PARAMS
        for key in sorted(optional):
            if key not in request.parameters:
                continue
            value = request.parameters[key]
            if key in {"observation_start", "observation_end", "realtime_start", "realtime_end"}:
                params[key] = normalize_date(value, key)
            elif key == "vintage_dates":
                params[key] = ",".join(normalize_date(item.strip(), key) for item in str(value).split(","))
            elif key == "limit":
                params[key] = normalize_int(value, key, minimum=1, maximum=MAX_LIMIT)
            elif key == "offset":
                params[key] = normalize_int(value, key, minimum=0, maximum=MAX_OFFSET)
            elif key == "frequency":
                params[key] = normalize_allowlist(value, key, FREQUENCY_ALLOWLIST)
            elif key == "aggregation_method":
                params[key] = normalize_allowlist(value, key, AGGREGATION_ALLOWLIST)
            elif key == "units":
                params[key] = normalize_allowlist(value, key, UNITS_ALLOWLIST)
            elif key == "sort_order":
                params[key] = normalize_allowlist(value, key, SORT_ORDER_ALLOWLIST)
        return {"series_id": series_id, "params": params}

    def build_request(self, request: ConnectorRequest, credentials: Mapping[str, Credential]) -> Mapping[str, Any]:
        if request.operation not in self.supported_operations:
            raise UnsupportedOperationError(self.provider, request.operation)
        credential = credentials.get("FRED_API_KEY")
        if credential is None:
            raise RequestValidationError("FRED credential unavailable.", provider=self.provider, operation=request.operation)
        normalized = self.normalized_request(request)
        outbound_params = dict(normalized["params"])
        outbound_params["api_key"] = credential.value
        endpoint = self.endpoints[request.operation]
        safe_metadata = {
            "provider": self.provider,
            "operation": request.operation,
            "endpoint_id": f"fred_{request.operation}",
            "series_id": normalized["series_id"],
            "correlation_id": request.correlation_id,
            **{key: normalized["params"][key] for key in sorted(normalized["params"]) if key not in {"file_type", "series_id"}},
        }
        return {
            "method": "GET",
            "url": endpoint,
            "headers": {},
            "params": {key: outbound_params[key] for key in sorted(outbound_params)},
            "safe_metadata": safe_metadata,
        }

    def fetch(self, request: ConnectorRequest, http_client: Any, credentials: Mapping[str, Credential]) -> UpstreamResponse:
        built = self.build_request(request, credentials)
        return http_client.request(
            built["method"],
            built["url"],
            correlation_id=request.correlation_id,
            headers=built["headers"],
            params=built["params"],
            timeout_policy=request.timeout_policy,
        )

    def parse_response(self, response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        try:
            payload = json.loads(response.content.decode("utf-8"))
        except Exception as exc:
            raise UpstreamResponseError("Malformed FRED JSON response.", provider=self.provider, operation=request.operation) from exc
        if not isinstance(payload, Mapping):
            raise ResponseValidationError("FRED payload must be an object.", provider=self.provider, operation=request.operation)
        api_error = error_payload(payload)
        if api_error:
            raise UpstreamResponseError(
                "FRED API returned an error payload.",
                provider=self.provider,
                operation=request.operation,
                context={"fred_error_code": api_error[0], "fred_error_message": api_error[1]},
            )
        if request.operation == "series_observations":
            return self._parse_observations(payload, response, request)
        if request.operation == "series_metadata":
            return self._parse_metadata(payload, response, request)
        raise UnsupportedOperationError(self.provider, request.operation)

    def to_canonical_response(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("FREDConnector uses the shared dispatcher for canonical responses.")

    def _parse_observations(self, payload: Mapping[str, Any], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        series_id = self.normalized_request(request)["series_id"]
        observations = observations_payload(payload)
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        for observation in observations:
            date = str(observation.get("date") or "").strip()
            realtime_start = str(observation.get("realtime_start") or "").strip()
            realtime_end = str(observation.get("realtime_end") or "").strip()
            raw_value = observation.get("value")
            value, value_status, value_warning = parse_numeric_value(raw_value)
            if value_warning:
                warnings.append(f"{value_warning}:{date}")
            rows.append(
                {
                    "provider": self.provider,
                    "series_id": series_id,
                    "observation_date": date,
                    "realtime_start": realtime_start,
                    "realtime_end": realtime_end,
                    "raw_value": raw_value,
                    "value": value,
                    "value_status": value_status,
                    "retrieval_timestamp": response.retrieved_at,
                },
            )
        rows.sort(key=lambda item: (item["series_id"], item["observation_date"], item["realtime_start"], item["realtime_end"]))
        count = _safe_int(payload.get("count"))
        offset = _safe_int(payload.get("offset")) or 0
        limit = _safe_int(payload.get("limit")) or len(rows)
        has_more = bool(count is not None and offset + limit < count)
        if not rows:
            warnings.append("no_observations_returned")
        validation = validate_normalized_rows(rows)
        if validation.valid:
            validation = ValidationResult(True, (), tuple(warnings), ("date", "value", "realtime_start", "realtime_end"), len(rows))
        latest_valid = max((row["observation_date"] for row in rows if row["value_status"] == "available"), default=None)
        response_status = "success" if rows else "unavailable"
        return {
            "response_status": response_status,
            "source_metadata": SourceMetadata(
                provider=self.provider,
                endpoint="fred_series_observations",
                dataset=series_id,
                measurement_as_of=latest_valid,
                retrieval_timestamp=response.retrieved_at,
                content_type=response.content_type,
                upstream_status=response.status_code,
            ),
            "normalized_rows": rows,
            "warnings": tuple(warnings),
            "validation": validation,
            "source_payload": "fred_series_observations",
            "source_artifact": "https://api.stlouisfed.org/fred/series/observations",
            "transformation_version": TRANSFORMATION_BY_OPERATION[request.operation],
            "pagination": {"count": count, "offset": offset, "limit": limit, "has_more": has_more},
        }

    def _parse_metadata(self, payload: Mapping[str, Any], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        series_id = self.normalized_request(request)["series_id"]
        seriess = seriess_payload(payload)
        if not seriess:
            return self._metadata_unavailable(series_id, response, "no_series_metadata_returned")
        metadata = dict(seriess[0])
        returned_id = str(metadata.get("id") or "").strip()
        if returned_id != series_id:
            raise ResponseValidationError("FRED metadata series ID mismatch.", provider=self.provider, operation=request.operation)
        row = {
            "provider": self.provider,
            "series_id": returned_id,
            "title": metadata.get("title"),
            "observation_start": metadata.get("observation_start"),
            "observation_end": metadata.get("observation_end"),
            "frequency": metadata.get("frequency"),
            "frequency_short": metadata.get("frequency_short"),
            "units": metadata.get("units"),
            "units_short": metadata.get("units_short"),
            "seasonal_adjustment": metadata.get("seasonal_adjustment"),
            "seasonal_adjustment_short": metadata.get("seasonal_adjustment_short"),
            "last_updated": metadata.get("last_updated"),
            "popularity": metadata.get("popularity"),
            "notes": metadata.get("notes"),
            "realtime_start": metadata.get("realtime_start"),
            "realtime_end": metadata.get("realtime_end"),
            "retrieval_timestamp": response.retrieved_at,
        }
        validation = validate_normalized_rows([row])
        if validation.valid:
            validation = ValidationResult(True, (), (), ("id",), 1)
        return {
            "response_status": "success",
            "source_metadata": SourceMetadata(
                provider=self.provider,
                endpoint="fred_series",
                dataset=series_id,
                release=metadata.get("frequency_short"),
                measurement_as_of=metadata.get("observation_end"),
                publication_date=metadata.get("last_updated"),
                retrieval_timestamp=response.retrieved_at,
                content_type=response.content_type,
                upstream_status=response.status_code,
            ),
            "normalized_rows": [row],
            "warnings": (),
            "validation": validation,
            "source_payload": "fred_series",
            "source_artifact": "https://api.stlouisfed.org/fred/series",
            "transformation_version": TRANSFORMATION_BY_OPERATION[request.operation],
        }

    def _metadata_unavailable(self, series_id: str, response: UpstreamResponse, warning: str) -> Mapping[str, Any]:
        return {
            "response_status": "unavailable",
            "source_metadata": SourceMetadata(
                provider=self.provider,
                endpoint="fred_series",
                dataset=series_id,
                retrieval_timestamp=response.retrieved_at,
                content_type=response.content_type,
                upstream_status=response.status_code,
            ),
            "normalized_rows": [],
            "warnings": (warning,),
            "validation": ValidationResult(True, (), (warning,), ("id",), 0),
            "source_payload": "fred_series",
            "source_artifact": "https://api.stlouisfed.org/fred/series",
            "transformation_version": "fred-series-metadata-v1",
        }


def _safe_int(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
