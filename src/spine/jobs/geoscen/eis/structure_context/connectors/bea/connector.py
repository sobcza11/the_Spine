from __future__ import annotations

import json
from typing import Any, Mapping

from spine.jobs.geoscen.eis.contracts import ConnectorRequest, SourceMetadata, UpstreamResponse, ValidationResult
from spine.jobs.geoscen.eis.credentials import Credential
from spine.jobs.geoscen.eis.exceptions import RequestValidationError, UnsupportedOperationError, UpstreamResponseError
from spine.jobs.geoscen.eis.validation import validate_normalized_rows
from spine.jobs.geoscen.eis.structure_context.connectors.bea.datasets import (
    APPROVED_DATASETS,
    NIPA_ALLOWED_PARAMS,
    REGIONAL_ALLOWED_PARAMS,
)
from spine.jobs.geoscen.eis.structure_context.connectors.bea.parsing import (
    as_list,
    bea_results,
    normalize_dataset_name,
    normalize_filters,
    normalize_frequencies,
    normalize_geofips,
    normalize_identifier,
    normalize_line_code,
    normalize_scalar_list,
    normalize_years,
    parse_bool_like,
    parse_numeric_value,
    period_kind,
)

BEA_ENDPOINT = "https://apps.bea.gov/api/data"
METHOD_BY_OPERATION = {
    "dataset_list": "GetDataSetList",
    "parameter_list": "GetParameterList",
    "parameter_values": "GetParameterValues",
    "parameter_values_filtered": "GetParameterValuesFiltered",
    "data": "GetData",
}
TRANSFORMATION_BY_OPERATION = {
    "dataset_list": "bea-dataset-list-v1",
    "parameter_list": "bea-parameter-list-v1",
    "parameter_values": "bea-parameter-values-v1",
    "parameter_values_filtered": "bea-parameter-values-filtered-v1",
    "NIPA": "bea-nipa-data-v1",
    "Regional": "bea-regional-data-v1",
}


class BEAConnector:
    provider = "bea"
    supported_operations = tuple(METHOD_BY_OPERATION)
    credential_specification = {"BEA_USER_ID": True}
    endpoint = BEA_ENDPOINT

    def validate_request(self, request: ConnectorRequest) -> ValidationResult:
        errors: list[str] = []
        if request.provider != self.provider:
            errors.append("provider:not_bea")
        if request.operation not in self.supported_operations:
            errors.append("operation:unsupported")
            allowed: set[str] = set()
        else:
            allowed = self._allowed_top_level(request.operation)
        unexpected = sorted(set(request.parameters) - allowed)
        errors.extend(f"unexpected_parameter:{key}" for key in unexpected)
        if "UserID" in request.parameters or "userid" in {key.lower() for key in request.parameters}:
            errors.append("caller_userid_rejected")
        if "method" in request.parameters or "endpoint" in request.parameters or "ResultFormat" in request.parameters:
            errors.append("reserved_parameter_rejected")
        try:
            self._normalized_request(request)
        except Exception as exc:
            errors.append(str(exc))
        return ValidationResult(not errors, tuple(errors), (), tuple(sorted(allowed)))

    def _allowed_top_level(self, operation: str) -> set[str]:
        if operation == "dataset_list":
            return set()
        if operation == "parameter_list":
            return {"dataset_name"}
        if operation == "parameter_values":
            return {"dataset_name", "parameter_name"}
        if operation == "parameter_values_filtered":
            return {"dataset_name", "target_parameter", "filters"}
        if operation == "data":
            return {"dataset_name", "dataset_parameters"}
        return set()

    def _normalized_request(self, request: ConnectorRequest) -> dict[str, Any]:
        if request.operation == "dataset_list":
            return {"dataset_name": None, "params": {}}
        if request.operation in {"parameter_list", "parameter_values", "parameter_values_filtered", "data"}:
            dataset_name = normalize_dataset_name(request.parameters.get("dataset_name"))
        else:
            raise UnsupportedOperationError(self.provider, request.operation)
        if request.operation == "parameter_list":
            return {"dataset_name": dataset_name, "params": {"DataSetName": dataset_name}}
        if request.operation == "parameter_values":
            parameter_name = normalize_identifier(request.parameters.get("parameter_name"), "parameter_name")
            return {"dataset_name": dataset_name, "params": {"DataSetName": dataset_name, "ParameterName": parameter_name}}
        if request.operation == "parameter_values_filtered":
            target = normalize_identifier(request.parameters.get("target_parameter"), "target_parameter")
            filters = normalize_filters(request.parameters.get("filters"))
            return {"dataset_name": dataset_name, "params": {"DataSetName": dataset_name, "TargetParameter": target, **filters}, "target_parameter": target, "filters": filters}
        if dataset_name not in APPROVED_DATASETS:
            raise ValueError("data dataset unsupported")
        dataset_parameters = request.parameters.get("dataset_parameters")
        if not isinstance(dataset_parameters, Mapping):
            raise ValueError("dataset_parameters must be a mapping")
        params = self._normalize_data_params(dataset_name, dataset_parameters)
        return {"dataset_name": dataset_name, "params": {"DataSetName": dataset_name, **params}}
    
    def _normalize_data_params(self, dataset_name: str, dataset_parameters: Mapping[str, Any]) -> dict[str, Any]:
        if dataset_name == "NIPA":
            unexpected = sorted(set(dataset_parameters) - NIPA_ALLOWED_PARAMS)
            if unexpected:
                raise ValueError(f"unexpected_nipa_parameter:{unexpected[0]}")
            if "TableID" in dataset_parameters:
                raise ValueError("TableID deprecated")
            table = normalize_identifier(dataset_parameters.get("TableName"), "TableName")
            freqs = normalize_frequencies(dataset_parameters.get("Frequency"))
            years = normalize_years(dataset_parameters.get("Year"))
            return {"TableName": table, "Frequency": ",".join(freqs), "Year": ",".join(years)}
        unexpected = sorted(set(dataset_parameters) - REGIONAL_ALLOWED_PARAMS)
        if unexpected:
            raise ValueError(f"unexpected_regional_parameter:{unexpected[0]}")
        table = normalize_identifier(dataset_parameters.get("TableName"), "TableName")
        line = normalize_line_code(dataset_parameters.get("LineCode"))
        geos = normalize_geofips(dataset_parameters.get("GeoFips"))
        params = {"TableName": table, "LineCode": line, "GeoFips": ",".join(geos)}
        if "Year" in dataset_parameters:
            params["Year"] = ",".join(normalize_years(dataset_parameters["Year"]))
        return params

    def build_request(self, request: ConnectorRequest, credentials: Mapping[str, Credential]) -> Mapping[str, Any]:
        validation = self.validate_request(request)
        if not validation.valid:
            raise RequestValidationError("Invalid BEA request.", provider=self.provider, operation=request.operation, context={"errors": ",".join(validation.errors[:5])})
        credential = credentials.get("BEA_USER_ID")
        if credential is None:
            raise RequestValidationError("BEA credential unavailable.", provider=self.provider, operation=request.operation)
        normalized = self._normalized_request(request)
        method = METHOD_BY_OPERATION[request.operation]
        params = {"UserID": credential.value, "method": method, "ResultFormat": "JSON", **normalized["params"]}
        safe_metadata = {
            "provider": self.provider,
            "operation": request.operation,
            "method": method,
            "dataset_name": normalized.get("dataset_name"),
            "correlation_id": request.correlation_id,
            "parameter_name": normalized["params"].get("ParameterName"),
            "target_parameter": normalized.get("target_parameter"),
            "filter_keys": sorted((normalized.get("filters") or {}).keys()),
            "table_name": normalized["params"].get("TableName"),
            "frequency": normalized["params"].get("Frequency"),
            "year_count": len(str(normalized["params"].get("Year", "")).split(",")) if normalized["params"].get("Year") else 0,
            "geography_count": len(str(normalized["params"].get("GeoFips", "")).split(",")) if normalized["params"].get("GeoFips") else 0,
        }
        return {"method": "GET", "url": self.endpoint, "headers": {}, "params": {key: params[key] for key in sorted(params)}, "safe_metadata": safe_metadata}

    def fetch(self, request: ConnectorRequest, http_client: Any, credentials: Mapping[str, Credential]) -> UpstreamResponse:
        built = self.build_request(request, credentials)
        return http_client.request(built["method"], built["url"], correlation_id=request.correlation_id, headers=built["headers"], params=built["params"], timeout_policy=request.timeout_policy)

    def parse_response(self, response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        try:
            payload = json.loads(response.content.decode("utf-8"))
        except Exception as exc:
            raise UpstreamResponseError("Malformed BEA JSON response.", provider=self.provider, operation=request.operation) from exc
        if not isinstance(payload, Mapping):
            raise UpstreamResponseError("BEA payload must be an object.", provider=self.provider, operation=request.operation)
        results = bea_results(payload)
        if request.operation == "dataset_list":
            return self._parse_dataset_list(results, response)
        if request.operation == "parameter_list":
            return self._parse_parameter_list(results, response, request)
        if request.operation in {"parameter_values", "parameter_values_filtered"}:
            return self._parse_parameter_values(results, response, request)
        return self._parse_data(results, response, request)

    def to_canonical_response(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("BEAConnector uses the shared dispatcher for canonical responses.")

    def _base(self, request: ConnectorRequest, response: UpstreamResponse, rows: list[dict[str, Any]], warnings: list[str], dataset: str | None, transformation: str, measurement_as_of: str | None = None) -> dict[str, Any]:
        validation = validate_normalized_rows(rows)
        if validation.valid:
            validation = ValidationResult(True, (), tuple(warnings), (), len(rows))
        return {
            "response_status": "success" if rows else "unavailable",
            "source_metadata": SourceMetadata(provider=self.provider, endpoint="bea_api_data", dataset=dataset, measurement_as_of=measurement_as_of, retrieval_timestamp=response.retrieved_at, content_type=response.content_type, upstream_status=response.status_code),
            "normalized_rows": rows,
            "warnings": tuple(warnings),
            "validation": validation,
            "source_payload": "bea_api_data",
            "source_artifact": BEA_ENDPOINT,
            "transformation_version": transformation,
        }

    def _parse_dataset_list(self, results: Mapping[str, Any], response: UpstreamResponse) -> Mapping[str, Any]:
        rows = [{"provider": self.provider, "dataset_name": str(row.get("DatasetName") or row.get("DataSetName") or ""), "dataset_description": row.get("DatasetDescription"), "retrieval_timestamp": response.retrieved_at} for row in as_list(results.get("Dataset"))]
        rows = [row for row in rows if row["dataset_name"]]
        rows.sort(key=lambda row: row["dataset_name"])
        warnings = [] if rows else ["no_datasets_returned"]
        return self._base(ConnectorRequest(provider="bea", operation="dataset_list"), response, rows, warnings, None, TRANSFORMATION_BY_OPERATION["dataset_list"])

    def _parse_parameter_list(self, results: Mapping[str, Any], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        dataset = self._normalized_request(request)["dataset_name"]
        rows = []
        warnings = []
        for row in as_list(results.get("Parameter")):
            required, warn1 = parse_bool_like(row.get("ParameterIsRequired"))
            multiple, warn2 = parse_bool_like(row.get("MultipleAcceptedFlag") or row.get("MultipleValuesAccepted"))
            warnings.extend(warn for warn in (warn1, warn2) if warn)
            rows.append({"provider": self.provider, "dataset_name": dataset, "parameter_name": row.get("ParameterName"), "parameter_description": row.get("ParameterDescription"), "data_type": row.get("ParameterDataType") or row.get("DataType"), "parameter_is_required": required, "parameter_default_value": row.get("ParameterDefaultValue"), "multiple_values_accepted": multiple, "all_value": row.get("AllValue"), "retrieval_timestamp": response.retrieved_at})
        rows.sort(key=lambda row: str(row.get("parameter_name") or ""))
        if not rows:
            warnings.append("no_parameters_returned")
        return self._base(request, response, rows, warnings, dataset, TRANSFORMATION_BY_OPERATION["parameter_list"])

    def _parse_parameter_values(self, results: Mapping[str, Any], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        normalized = self._normalized_request(request)
        dataset = normalized["dataset_name"]
        parameter = normalized["params"].get("ParameterName") or normalized.get("target_parameter")
        applied = sorted((normalized.get("filters") or {}).keys())
        rows = []
        for row in as_list(results.get("ParamValue")):
            item = {"provider": self.provider, "dataset_name": dataset, "parameter_name": parameter, "value_key": str(row.get("Key") or row.get("Value") or ""), "value_description": row.get("Desc") or row.get("Description"), "retrieval_timestamp": response.retrieved_at}
            if request.operation == "parameter_values_filtered":
                item["target_parameter"] = parameter
                item["applied_filter_keys"] = applied
            rows.append(item)
        rows = [row for row in rows if row["value_key"]]
        rows.sort(key=lambda row: row["value_key"])
        warnings = [] if rows else ["no_parameter_values_returned"]
        key = "parameter_values_filtered" if request.operation == "parameter_values_filtered" else "parameter_values"
        return self._base(request, response, rows, warnings, dataset, TRANSFORMATION_BY_OPERATION[key])

    def _parse_data(self, results: Mapping[str, Any], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        dataset = self._normalized_request(request)["dataset_name"]
        rows = self._parse_nipa(results, response, dataset) if dataset == "NIPA" else self._parse_regional(results, response, dataset)
        notes = [{"provider": self.provider, "dataset_name": dataset, "note_ref": note.get("NoteRef"), "note_text": note.get("NoteText"), "retrieval_timestamp": response.retrieved_at, "record_type": "note"} for note in as_list(results.get("Notes"))]
        rows.extend(notes)
        warnings = [] if rows else ["no_data_returned"]
        measurement = max((str(row.get("time_period") or row.get("year") or "") for row in rows if row.get("record_type") != "note"), default=None)
        transformation = TRANSFORMATION_BY_OPERATION[dataset]
        base = self._base(request, response, rows, warnings, dataset, transformation, measurement)
        if warnings:
            base["response_status"] = "partial" if any(row.get("record_type") != "note" for row in rows) else "unavailable"
        return base

    def _parse_nipa(self, results: Mapping[str, Any], response: UpstreamResponse, dataset: str) -> list[dict[str, Any]]:
        out = []
        for row in as_list(results.get("Data")):
            value, status = parse_numeric_value(row.get("DataValue"))
            period = row.get("TimePeriod")
            out.append({"provider": self.provider, "dataset_name": dataset, "table_name": row.get("TableName"), "series_code": row.get("SeriesCode"), "line_number": row.get("LineNumber"), "line_description": row.get("LineDescription"), "time_period": period, "period_kind": period_kind(period), "frequency": row.get("Frequency"), "metric_name": row.get("MetricName"), "unit": row.get("UnitName"), "unit_multiplier": row.get("UNIT_MULT"), "raw_value": row.get("DataValue"), "value": value, "value_status": status, "note_refs": row.get("NoteRef"), "retrieval_timestamp": response.retrieved_at})
        out.sort(key=lambda row: (str(row.get("table_name")), str(row.get("line_number")), str(row.get("time_period"))))
        return out

    def _parse_regional(self, results: Mapping[str, Any], response: UpstreamResponse, dataset: str) -> list[dict[str, Any]]:
        out = []
        for row in as_list(results.get("Data")):
            value, status = parse_numeric_value(row.get("DataValue"))
            out.append({"provider": self.provider, "dataset_name": dataset, "table_name": row.get("TableName"), "line_code": row.get("LineCode"), "geo_fips": str(row.get("GeoFips") or ""), "geo_name": row.get("GeoName"), "code": row.get("Code"), "description": row.get("Description"), "time_period": row.get("TimePeriod") or row.get("Year"), "year": row.get("Year"), "unit": row.get("CL_UNIT") or row.get("Unit"), "unit_multiplier": row.get("UNIT_MULT"), "raw_value": row.get("DataValue"), "value": value, "value_status": status, "note_ref": row.get("NoteRef"), "retrieval_timestamp": response.retrieved_at})
        out.sort(key=lambda row: (str(row.get("table_name")), str(row.get("geo_fips")), str(row.get("line_code")), str(row.get("time_period"))))
        return out
