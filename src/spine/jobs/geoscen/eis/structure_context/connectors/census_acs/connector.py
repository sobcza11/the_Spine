from __future__ import annotations

import re
from typing import Any, Mapping

from spine.jobs.geoscen.eis.contracts import ConnectorRequest, SourceMetadata, UpstreamResponse, ValidationResult
from spine.jobs.geoscen.eis.credentials import Credential
from spine.jobs.geoscen.eis.exceptions import RequestValidationError, ResponseValidationError, UnsupportedOperationError
from spine.jobs.geoscen.eis.structure_context.connectors.census_acs.geography import for_expression, geography_metadata, in_expression, normalize_geography
from spine.jobs.geoscen.eis.structure_context.connectors.census_acs.parsing import bounded_text, ensure_mapping, load_census_json, normalize_group, normalize_predicates, normalize_variables, parse_value
from spine.jobs.geoscen.eis.structure_context.connectors.census_acs.products import normalize_product, normalize_year
from spine.jobs.geoscen.eis.validation import validate_normalized_rows

CENSUS_ENDPOINT_ROOT = "https://api.census.gov/data"
TRANSFORMATION_BY_OPERATION = {
    "variables": "census-acs-variables-v1",
    "groups": "census-acs-groups-v1",
    "group_variables": "census-acs-group-variables-v1",
    "geography": "census-acs-geography-v1",
    "data": "census-acs-data-v1",
}
GEOGRAPHY_COLUMNS = {"us", "region", "division", "state", "county", "place", "tract", "block group"}


class CensusACSConnector:
    provider = "census_acs"
    supported_operations = tuple(TRANSFORMATION_BY_OPERATION)
    credential_specification = {"CENSUS_API_KEY": True}
    endpoint_template = f"{CENSUS_ENDPOINT_ROOT}/{{year}}/acs/{{product}}"

    def validate_request(self, request: ConnectorRequest) -> ValidationResult:
        errors: list[str] = []
        if request.provider != self.provider:
            errors.append("provider:not_census_acs")
        if request.operation not in self.supported_operations:
            errors.append("operation:unsupported")
            allowed: set[str] = set()
        else:
            allowed = self._allowed_top_level(request.operation)
        lowered = {str(key).lower() for key in request.parameters}
        errors.extend(f"unexpected_parameter:{key}" for key in sorted(set(request.parameters) - allowed))
        if {"key", "endpoint", "url"} & lowered:
            errors.append("reserved_parameter_rejected")
        try:
            self._normalized_request(request)
        except Exception as exc:
            errors.append(str(exc))
        return ValidationResult(not errors, tuple(errors), (), tuple(sorted(allowed)))

    def _allowed_top_level(self, operation: str) -> set[str]:
        common = {"year", "product"}
        if operation in {"variables", "groups", "geography"}:
            return common
        if operation == "group_variables":
            return common | {"group"}
        if operation == "data":
            return common | {"variables", "geography", "geography_within", "predicates"}
        return set()

    def _normalized_request(self, request: ConnectorRequest) -> dict[str, Any]:
        if request.operation not in self.supported_operations:
            raise UnsupportedOperationError(self.provider, request.operation)
        year = normalize_year(request.parameters.get("year"))
        product = normalize_product(request.parameters.get("product"))
        out: dict[str, Any] = {"year": year, "product": product}
        if request.operation == "group_variables":
            out["group"] = normalize_group(request.parameters.get("group"))
        if request.operation == "data":
            variables = normalize_variables(request.parameters.get("variables"))
            geography = normalize_geography(request.parameters.get("geography"))
            if request.parameters.get("geography_within") is not None:
                raise ValueError("geography_within unsupported; use structured geography")
            predicates = normalize_predicates(request.parameters.get("predicates"))
            out.update({"variables": variables, "geography": geography, "predicates": predicates})
        return out

    def build_request(self, request: ConnectorRequest, credentials: Mapping[str, Credential]) -> Mapping[str, Any]:
        validation = self.validate_request(request)
        if not validation.valid:
            raise RequestValidationError("Invalid Census ACS request.", provider=self.provider, operation=request.operation, context={"errors": ",".join(validation.errors[:5])})
        credential = credentials.get("CENSUS_API_KEY")
        if credential is None:
            raise RequestValidationError("Census ACS credential unavailable.", provider=self.provider, operation=request.operation)
        normalized = self._normalized_request(request)
        endpoint = self._endpoint(normalized["year"], normalized["product"], request.operation, normalized.get("group"))
        params: dict[str, Any] = {"key": credential.value}
        if request.operation == "data":
            params["get"] = ",".join(normalized["variables"])
            params["for"] = for_expression(normalized["geography"])
            in_clause = in_expression(normalized["geography"])
            if in_clause:
                params["in"] = in_clause
            params.update(normalized["predicates"])
        meta = geography_metadata(normalized["geography"]) if request.operation == "data" else {"geography_type": None, "parent_geography_types": []}
        safe_metadata = {
            "provider": self.provider,
            "operation": request.operation,
            "year": normalized["year"],
            "product": normalized["product"],
            "variable_count": len(normalized.get("variables") or []),
            "geography_type": meta["geography_type"],
            "parent_geography_types": meta["parent_geography_types"],
            "predicate_keys": sorted((normalized.get("predicates") or {}).keys()),
            "endpoint_identifier": self._endpoint_identifier(normalized["year"], normalized["product"], request.operation),
            "correlation_id": request.correlation_id,
        }
        return {"method": "GET", "url": endpoint, "headers": {}, "params": {key: params[key] for key in sorted(params)}, "safe_metadata": safe_metadata}

    def fetch(self, request: ConnectorRequest, http_client: Any, credentials: Mapping[str, Credential]) -> UpstreamResponse:
        built = self.build_request(request, credentials)
        return http_client.request(built["method"], built["url"], correlation_id=request.correlation_id, headers=built["headers"], params=built["params"], timeout_policy=request.timeout_policy)

    def parse_response(self, response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        payload = load_census_json(response.text())
        if request.operation == "variables":
            return self._parse_variables(ensure_mapping(payload, "variables"), response, request)
        if request.operation == "groups":
            return self._parse_groups(ensure_mapping(payload, "groups"), response, request)
        if request.operation == "group_variables":
            return self._parse_group_variables(ensure_mapping(payload, "group variables"), response, request)
        if request.operation == "geography":
            return self._parse_geography(ensure_mapping(payload, "geography"), response, request)
        return self._parse_data(payload, response, request)

    def to_canonical_response(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("CensusACSConnector uses the shared dispatcher for canonical responses.")

    def _endpoint(self, year: str, product: str, operation: str, group: str | None = None) -> str:
        base = f"{CENSUS_ENDPOINT_ROOT}/{year}/acs/{product}"
        if operation == "variables":
            return f"{base}/variables.json"
        if operation == "groups":
            return f"{base}/groups.json"
        if operation == "group_variables":
            return f"{base}/groups/{group}.json"
        if operation == "geography":
            return f"{base}/geography.json"
        return base

    def _endpoint_identifier(self, year: str, product: str, operation: str) -> str:
        return f"census_acs:{year}:{product}:{operation}"

    def _base(self, response: UpstreamResponse, request: ConnectorRequest, rows: list[dict[str, Any]], warnings: list[str], measurement_as_of: str | None = None) -> dict[str, Any]:
        normalized = self._normalized_request(request)
        validation = validate_normalized_rows(rows)
        if validation.valid:
            validation = ValidationResult(True, (), tuple(warnings), (), len(rows))
        dataset = f"{normalized['year']}/acs/{normalized['product']}"
        return {
            "response_status": "success" if rows else "unavailable",
            "source_metadata": SourceMetadata(provider=self.provider, endpoint="census_acs_api", dataset=dataset, release=f"ACS {normalized['year']} {normalized['product']}", measurement_as_of=measurement_as_of or normalized["year"], retrieval_timestamp=response.retrieved_at, content_type=response.content_type, upstream_status=response.status_code),
            "normalized_rows": rows,
            "warnings": tuple(warnings),
            "validation": validation,
            "source_payload": "census_acs_api",
            "source_artifact": self._endpoint_identifier(normalized["year"], normalized["product"], request.operation),
            "transformation_version": TRANSFORMATION_BY_OPERATION[request.operation],
        }

    def _parse_variables(self, payload: Mapping[str, Any], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        normalized = self._normalized_request(request)
        variables = ensure_mapping(payload.get("variables"), "variables collection")
        rows = []
        for name, meta in variables.items():
            if name in {"for", "in", "ucgid"} or not isinstance(meta, Mapping):
                continue
            rows.append({"provider": self.provider, "year": normalized["year"], "product": normalized["product"], "variable": name, "label": bounded_text(meta.get("label")), "concept": bounded_text(meta.get("concept")), "predicate_type": meta.get("predicateType"), "group": meta.get("group"), "limit": meta.get("limit"), "predicate_only": meta.get("predicateOnly"), "attributes": meta.get("attributes"), "retrieval_timestamp": response.retrieved_at})
        rows.sort(key=lambda row: row["variable"])
        warnings = [] if rows else ["no_variables_returned"]
        return self._base(response, request, rows, warnings)

    def _parse_groups(self, payload: Mapping[str, Any], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        normalized = self._normalized_request(request)
        groups = payload.get("groups") or []
        if not isinstance(groups, list):
            raise ResponseValidationError("Census groups payload malformed.", provider=self.provider, operation=request.operation)
        rows = [{"provider": self.provider, "year": normalized["year"], "product": normalized["product"], "group": row.get("name"), "name": row.get("name"), "description": bounded_text(row.get("description")), "variables_url": row.get("variables"), "retrieval_timestamp": response.retrieved_at} for row in groups if isinstance(row, Mapping) and row.get("name")]
        rows.sort(key=lambda row: row["group"])
        warnings = [] if rows else ["no_groups_returned"]
        return self._base(response, request, rows, warnings)

    def _parse_group_variables(self, payload: Mapping[str, Any], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        normalized = self._normalized_request(request)
        variables = ensure_mapping(payload.get("variables"), "group variables collection")
        rows = []
        for name, meta in variables.items():
            if not isinstance(meta, Mapping):
                continue
            rows.append({"provider": self.provider, "year": normalized["year"], "product": normalized["product"], "group": normalized["group"], "variable": name, "label": bounded_text(meta.get("label")), "concept": bounded_text(meta.get("concept") or payload.get("concept")), "predicate_type": meta.get("predicateType"), "limit": meta.get("limit"), "predicate_only": meta.get("predicateOnly"), "attributes": meta.get("attributes"), "retrieval_timestamp": response.retrieved_at})
        rows.sort(key=lambda row: row["variable"])
        warnings = [] if rows else ["no_group_variables_returned"]
        return self._base(response, request, rows, warnings)

    def _parse_geography(self, payload: Mapping[str, Any], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        normalized = self._normalized_request(request)
        geos = payload.get("fips") or []
        if not isinstance(geos, list):
            raise ResponseValidationError("Census geography payload malformed.", provider=self.provider, operation=request.operation)
        rows = []
        for idx, row in enumerate(geos):
            if not isinstance(row, Mapping):
                continue
            rows.append({"provider": self.provider, "year": normalized["year"], "product": normalized["product"], "geography_id": row.get("name") or row.get("geoLevelId") or str(idx), "geography_name": row.get("name"), "geography_level": row.get("geoLevelDisplay"), "requires": row.get("requires") or [], "wildcard": row.get("wildcard"), "retrieval_timestamp": response.retrieved_at})
        rows.sort(key=lambda row: str(row["geography_id"]))
        warnings = [] if rows else ["no_geographies_returned"]
        return self._base(response, request, rows, warnings)

    def _parse_data(self, payload: Any, response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        normalized = self._normalized_request(request)
        if not isinstance(payload, list):
            raise ResponseValidationError("Census data payload must be a tabular array.", provider=self.provider, operation=request.operation)
        if not payload:
            raise ResponseValidationError("Census data payload missing header.", provider=self.provider, operation=request.operation)
        header = payload[0]
        if not isinstance(header, list) or not header or any(not str(item).strip() for item in header):
            raise ResponseValidationError("Census data header malformed.", provider=self.provider, operation=request.operation)
        headers = [str(item) for item in header]
        if len(headers) != len(set(headers)):
            raise ResponseValidationError("Census data duplicate header.", provider=self.provider, operation=request.operation)
        requested = set(normalized["variables"])
        missing = sorted(requested - set(headers))
        rows: list[dict[str, Any]] = []
        warnings = [f"missing_requested_variables:{','.join(missing)}"] if missing else []
        for raw_row in payload[1:]:
            if not isinstance(raw_row, list) or len(raw_row) != len(headers):
                warnings.append("row_width_mismatch")
                continue
            mapped = dict(zip(headers, raw_row, strict=True))
            row = {"provider": self.provider, "year": normalized["year"], "product": normalized["product"], "retrieval_timestamp": response.retrieved_at}
            geography: dict[str, str] = {}
            variables: dict[str, Any] = {}
            for key, value in mapped.items():
                if key == "NAME":
                    row["NAME"] = value
                    row["geography_name"] = value
                elif key in GEOGRAPHY_COLUMNS:
                    geography[key] = str(value)
                    row[key.replace(" ", "_")] = str(value)
                else:
                    raw, parsed, status = parse_value(value)
                    variables[key] = {"raw_value": raw, "value": parsed, "value_status": status}
            row["geography"] = geography
            row["variables"] = variables
            rows.append(row)
        if not rows:
            warnings.append("no_data_rows")
        base = self._base(response, request, rows, warnings, normalized["year"])
        if warnings and rows:
            base["response_status"] = "partial"
        return base
