from __future__ import annotations

import json
from typing import Any, Mapping

from spine.jobs.geoscen.eis.contracts import ConnectorRequest, SourceMetadata, UpstreamResponse, ValidationResult
from spine.jobs.geoscen.eis.exceptions import RequestValidationError, ResponseValidationError, UnsupportedOperationError
from spine.jobs.geoscen.eis.structure_context.connectors.fhfa.datasets import FHFADataset, get_dataset, list_datasets, normalize_dataset_id, validate_official_url
from spine.jobs.geoscen.eis.structure_context.connectors.fhfa.downloads import validate_download_response
from spine.jobs.geoscen.eis.structure_context.connectors.fhfa.parsing import normalize_hpi_rows, parse_csv_bytes
from spine.jobs.geoscen.eis.validation import validate_normalized_rows

TRANSFORMATION_BY_OPERATION = {
    "dataset_catalog": "fhfa-dataset-catalog-v1",
    "hpi_download": "fhfa-hpi-download-v1",
    "hpi_parse": "fhfa-hpi-parse-v1",
    "hpi_fetch": "fhfa-hpi-fetch-v1",
}


class FHFAConnector:
    provider = "fhfa"
    supported_operations = tuple(TRANSFORMATION_BY_OPERATION)
    credential_specification: Mapping[str, bool] = {}

    def validate_request(self, request: ConnectorRequest) -> ValidationResult:
        errors: list[str] = []
        if request.provider != self.provider:
            errors.append("provider:not_fhfa")
        if request.operation not in self.supported_operations:
            errors.append("operation:unsupported")
            allowed: set[str] = set()
        else:
            allowed = self._allowed_top_level(request.operation)
        errors.extend(f"unexpected_parameter:{key}" for key in sorted(set(request.parameters) - allowed))
        lowered = {str(key).lower() for key in request.parameters}
        if {"endpoint", "url", "path", "file_path", "source_url"} & lowered:
            errors.append("reserved_parameter_rejected")
        try:
            self._normalized_request(request)
        except Exception as exc:
            errors.append(str(exc))
        return ValidationResult(not errors, tuple(errors), (), tuple(sorted(allowed)))

    def _allowed_top_level(self, operation: str) -> set[str]:
        if operation == "dataset_catalog":
            return {"active_only", "geography_level", "index_type", "frequency"}
        if operation in {"hpi_download", "hpi_fetch"}:
            return {"dataset_id"}
        if operation == "hpi_parse":
            return {"dataset_id", "raw_content"}
        return set()

    def _normalized_request(self, request: ConnectorRequest) -> dict[str, Any]:
        if request.operation not in self.supported_operations:
            raise UnsupportedOperationError(self.provider, request.operation)
        if request.operation == "dataset_catalog":
            filters = {}
            for key in ("geography_level", "index_type", "frequency"):
                raw = request.parameters.get(key)
                if raw is not None:
                    text = str(raw).strip().lower()
                    if not text or len(text) > 80 or any(ch in text for ch in "/\\?&#"):
                        raise ValueError(f"{key} invalid")
                    filters[key] = text
            active_only = request.parameters.get("active_only", True)
            if not isinstance(active_only, bool):
                raise ValueError("active_only must be boolean")
            return {"active_only": active_only, **filters}
        dataset = get_dataset(request.parameters.get("dataset_id"))
        validate_official_url(dataset.source_url)
        if request.operation == "hpi_parse":
            raw_content = request.parameters.get("raw_content")
            if not isinstance(raw_content, str) or raw_content == "":
                raise ValueError("raw_content required")
            return {"dataset": dataset, "raw_content": raw_content}
        return {"dataset": dataset}

    def build_request(self, request: ConnectorRequest, credentials: Mapping[str, Any]) -> Mapping[str, Any]:
        validation = self.validate_request(request)
        if not validation.valid:
            raise RequestValidationError("Invalid FHFA request.", provider=self.provider, operation=request.operation, context={"errors": ",".join(validation.errors[:5])})
        normalized = self._normalized_request(request)
        if request.operation == "dataset_catalog":
            return {"method": "GET", "url": "fhfa://dataset_catalog", "headers": {}, "params": {}, "safe_metadata": {"provider": self.provider, "operation": request.operation, "correlation_id": request.correlation_id}}
        dataset: FHFADataset = normalized["dataset"]
        metadata = {
            "provider": self.provider,
            "operation": request.operation,
            "dataset_id": dataset.dataset_id,
            "endpoint_identifier": dataset.official_source_identifier,
            "geography_level": dataset.geography_level,
            "index_type": dataset.index_type,
            "expected_format": dataset.file_format,
            "correlation_id": request.correlation_id,
        }
        if request.operation == "hpi_parse":
            return {"method": "GET", "url": f"fhfa://parse/{dataset.dataset_id}", "headers": {}, "params": {}, "safe_metadata": metadata}
        return {"method": "GET", "url": dataset.source_url, "headers": {}, "params": {}, "safe_metadata": metadata}

    def fetch(self, request: ConnectorRequest, http_client: Any, credentials: Mapping[str, Any]) -> UpstreamResponse:
        built = self.build_request(request, credentials)
        normalized = self._normalized_request(request)
        if request.operation == "dataset_catalog":
            content = json.dumps([dataset.to_dict() for dataset in self._catalog(normalized)]).encode("utf-8")
            return UpstreamResponse(url="fhfa://dataset_catalog", method="GET", status_code=200, headers={"content-type": "application/json"}, content=content, retrieved_at=request.requested_at)
        if request.operation == "hpi_parse":
            return UpstreamResponse(url=built["url"], method="GET", status_code=200, headers={"content-type": "text/csv"}, content=normalized["raw_content"].encode("utf-8"), retrieved_at=request.requested_at)
        return http_client.request(built["method"], built["url"], correlation_id=request.correlation_id, headers=built["headers"], params=built["params"], timeout_policy=request.timeout_policy)

    def parse_response(self, response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        normalized = self._normalized_request(request)
        if request.operation == "dataset_catalog":
            rows = [dict(row, provider=self.provider, retrieval_timestamp=response.retrieved_at) for row in json.loads(response.text())]
            warnings: list[str] = [] if rows else ["no_datasets_returned"]
            return self._base(request, response, rows, warnings, None, TRANSFORMATION_BY_OPERATION["dataset_catalog"], "fhfa_dataset_catalog")
        dataset: FHFADataset = normalized["dataset"]
        if request.operation == "hpi_download":
            validate_download_response(response, dataset)
            rows = [{"provider": self.provider, "dataset_id": dataset.dataset_id, "source_url": dataset.source_url, "file_format": dataset.file_format, "size_bytes": len(response.content), "content_type": response.content_type, "retrieval_timestamp": response.retrieved_at}]
            return self._base(request, response, rows, [], dataset, TRANSFORMATION_BY_OPERATION["hpi_download"], dataset.official_source_identifier)
        validate_download_response(response, dataset)
        raw_rows, csv_warnings = parse_csv_bytes(response.content, dataset)
        rows, parse_warnings, measurement = normalize_hpi_rows(raw_rows, dataset, response.retrieved_at)
        warnings = csv_warnings + parse_warnings
        base = self._base(request, response, rows, warnings, dataset, dataset.parser_version, dataset.official_source_identifier, measurement)
        if warnings and rows:
            base["response_status"] = "partial"
        return base

    def to_canonical_response(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("FHFAConnector uses the shared dispatcher for canonical responses.")

    def _catalog(self, normalized: Mapping[str, Any]) -> list[FHFADataset]:
        return list_datasets(active_only=normalized.get("active_only", True), geography_level=normalized.get("geography_level"), index_type=normalized.get("index_type"), frequency=normalized.get("frequency"))

    def _base(self, request: ConnectorRequest, response: UpstreamResponse, rows: list[dict[str, Any]], warnings: list[str], dataset: FHFADataset | None, transformation: str, artifact: str, measurement_as_of: str | None = None) -> dict[str, Any]:
        validation = validate_normalized_rows(rows)
        if validation.valid:
            validation = ValidationResult(True, (), tuple(warnings), (), len(rows))
        return {
            "response_status": "success" if rows else "unavailable",
            "source_metadata": SourceMetadata(provider=self.provider, endpoint="fhfa_hpi_download", dataset=dataset.dataset_id if dataset else "dataset_catalog", release=dataset.display_name if dataset else "FHFA HPI dataset registry", measurement_as_of=measurement_as_of, retrieval_timestamp=response.retrieved_at, content_type=response.content_type, upstream_status=response.status_code),
            "normalized_rows": rows,
            "warnings": tuple(warnings),
            "validation": validation,
            "source_payload": "fhfa_hpi",
            "source_artifact": artifact,
            "transformation_version": transformation,
        }
