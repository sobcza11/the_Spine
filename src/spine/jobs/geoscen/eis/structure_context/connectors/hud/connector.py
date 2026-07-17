from __future__ import annotations

from typing import Any, Mapping

from spine.jobs.geoscen.eis.contracts import ConnectorRequest, SourceMetadata, UpstreamResponse, ValidationResult
from spine.jobs.geoscen.eis.credentials import Credential
from spine.jobs.geoscen.eis.exceptions import RequestValidationError, UnsupportedOperationError
from spine.jobs.geoscen.eis.structure_context.connectors.hud.endpoints import DATASET_BY_OPERATION, HUD_API_ROOT, TRANSFORMATION_BY_OPERATION, endpoint_for
from spine.jobs.geoscen.eis.structure_context.connectors.hud.geography import normalize_entity_id, normalize_state_code, normalize_state_id, normalize_year
from spine.jobs.geoscen.eis.structure_context.connectors.hud.parsing import bounded, extract_records, first, load_hud_payload, parse_money
from spine.jobs.geoscen.eis.validation import validate_normalized_rows

BEDROOM_FIELDS = (
    ("efficiency", ("Efficiency", "efficiency", "studio", "0br", "fmr_0")),
    ("one_bedroom", ("One-Bedroom", "one_bedroom", "onebr", "fmr_1")),
    ("two_bedroom", ("Two-Bedroom", "two_bedroom", "twobr", "fmr_2")),
    ("three_bedroom", ("Three-Bedroom", "three_bedroom", "threebr", "fmr_3")),
    ("four_bedroom", ("Four-Bedroom", "four_bedroom", "fourbr", "fmr_4")),
)
HOUSEHOLD_FIELDS = (
    ("1", ("il_1", "person1", "hh1", "one_person")),
    ("2", ("il_2", "person2", "hh2", "two_person")),
    ("3", ("il_3", "person3", "hh3", "three_person")),
    ("4", ("il_4", "person4", "hh4", "four_person")),
    ("5", ("il_5", "person5", "hh5", "five_person")),
    ("6", ("il_6", "person6", "hh6", "six_person")),
    ("7", ("il_7", "person7", "hh7", "seven_person")),
    ("8", ("il_8", "person8", "hh8", "eight_person")),
)


class HUDConnector:
    provider = "hud"
    supported_operations = tuple(TRANSFORMATION_BY_OPERATION)
    credential_specification = {"HUD_USER_ACCESS_TOKEN": True}
    endpoint_root = HUD_API_ROOT
    rate_limit_policy = "60 queries/minute; no connector-side looping"

    def validate_request(self, request: ConnectorRequest) -> ValidationResult:
        errors: list[str] = []
        if request.provider != self.provider:
            errors.append("provider:not_hud")
        if request.operation not in self.supported_operations:
            errors.append("operation:unsupported")
            allowed: set[str] = set()
        else:
            allowed = self._allowed_top_level(request.operation)
        lowered = {str(key).lower() for key in request.parameters}
        errors.extend(f"unexpected_parameter:{key}" for key in sorted(set(request.parameters) - allowed))
        if {"authorization", "header", "headers", "token", "access_token", "endpoint", "url", "path"} & lowered:
            errors.append("reserved_parameter_rejected")
        try:
            self._normalized_request(request)
        except Exception as exc:
            errors.append(str(exc))
        return ValidationResult(not errors, tuple(errors), (), tuple(sorted(allowed)))

    def _allowed_top_level(self, operation: str) -> set[str]:
        if operation == "list_states":
            return set()
        if operation == "list_counties":
            return {"state_id", "updated_year"}
        if operation == "list_metro_areas":
            return {"updated_year"}
        if operation in {"fmr_data", "income_limits_data"}:
            return {"entity_id", "year"}
        if operation in {"fmr_state_data", "income_limits_state_data"}:
            return {"state_code", "year"}
        return set()

    def _normalized_request(self, request: ConnectorRequest) -> dict[str, Any]:
        if request.operation not in self.supported_operations:
            raise UnsupportedOperationError(self.provider, request.operation)
        out: dict[str, Any] = {}
        if request.operation == "list_counties":
            out["state_id"] = normalize_state_id(request.parameters.get("state_id"))
        if request.operation in {"fmr_state_data", "income_limits_state_data"}:
            out["state_code"] = normalize_state_code(request.parameters.get("state_code"))
        if request.operation in {"fmr_data", "income_limits_data"}:
            out["entity_id"] = normalize_entity_id(request.parameters.get("entity_id"))
        if "year" in request.parameters:
            out["year"] = normalize_year(request.parameters["year"])
        if "updated_year" in request.parameters:
            out["updated_year"] = normalize_year(request.parameters["updated_year"], "updated_year")
        return out

    def build_request(self, request: ConnectorRequest, credentials: Mapping[str, Credential]) -> Mapping[str, Any]:
        validation = self.validate_request(request)
        if not validation.valid:
            raise RequestValidationError("Invalid HUD request.", provider=self.provider, operation=request.operation, context={"errors": ",".join(validation.errors[:5])})
        credential = credentials.get("HUD_USER_ACCESS_TOKEN")
        if credential is None:
            raise RequestValidationError("HUD credential unavailable.", provider=self.provider, operation=request.operation)
        normalized = self._normalized_request(request)
        url = endpoint_for(request.operation, state_id=normalized.get("state_id"), state_code=normalized.get("state_code"), entity_id=normalized.get("entity_id"))
        params: dict[str, str] = {}
        if "year" in normalized:
            params["year"] = normalized["year"]
        if "updated_year" in normalized:
            params["updated"] = normalized["updated_year"]
        metadata = {
            "provider": self.provider,
            "operation": request.operation,
            "endpoint_identifier": request.operation,
            "state_id": normalized.get("state_id"),
            "state_code": normalized.get("state_code"),
            "entity_id": normalized.get("entity_id"),
            "requested_year": normalized.get("year"),
            "upstream_default_year": "year" not in normalized and request.operation not in {"list_states", "list_counties", "list_metro_areas"},
            "correlation_id": request.correlation_id,
            "rate_limit_policy": self.rate_limit_policy,
        }
        return {"method": "GET", "url": url, "headers": {"Authorization": f"Bearer {credential.value}"}, "params": {key: params[key] for key in sorted(params)}, "safe_metadata": metadata}

    def fetch(self, request: ConnectorRequest, http_client: Any, credentials: Mapping[str, Credential]) -> UpstreamResponse:
        built = self.build_request(request, credentials)
        return http_client.request(built["method"], built["url"], correlation_id=request.correlation_id, headers=built["headers"], params=built["params"], timeout_policy=request.timeout_policy)

    def parse_response(self, response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        payload = load_hud_payload(response.text())
        records = extract_records(payload)
        if request.operation == "list_states":
            return self._parse_states(records, response, request)
        if request.operation == "list_counties":
            return self._parse_counties(records, response, request)
        if request.operation == "list_metro_areas":
            return self._parse_metros(records, response, request)
        if request.operation in {"fmr_data", "fmr_state_data"}:
            return self._parse_fmr(records, response, request)
        return self._parse_income_limits(records, response, request)

    def to_canonical_response(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("HUDConnector uses the shared dispatcher for canonical responses.")

    def _parse_states(self, records: list[Mapping[str, Any]], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        rows = []
        warnings = []
        for row in records:
            state_code = str(first(row, "state_code", "stateCode", "code") or "").strip().upper()
            state_id = str(first(row, "state_id", "stateId", "id", "state_code") or state_code).strip()
            if not state_code or not state_id:
                warnings.append("malformed_state_record")
                continue
            rows.append({"provider": self.provider, "state_id": state_id, "state_code": state_code, "state_name": bounded(first(row, "state_name", "stateName", "name")), "retrieval_timestamp": response.retrieved_at})
        rows.sort(key=lambda item: (item["state_code"], str(item.get("state_name") or "")))
        return self._base(request, response, rows, warnings or ([] if rows else ["no_records"]), "geography_lookup")

    def _parse_counties(self, records: list[Mapping[str, Any]], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        normalized = self._normalized_request(request)
        rows = []
        warnings = []
        for row in records:
            entity_id = str(first(row, "county_entity_id", "entityid", "entity_id", "countyid", "id") or "").strip()
            if not entity_id:
                warnings.append("malformed_county_record")
                continue
            rows.append({"provider": self.provider, "state_id": normalized.get("state_id"), "state_code": bounded(first(row, "state_code", "state")), "county_entity_id": entity_id, "county_name": bounded(first(row, "county_name", "countyName", "name")), "county_code": bounded(first(row, "county_code", "countyCode", "fips")), "metro_status": bounded(first(row, "metro_status", "metroStatus")), "retrieval_timestamp": response.retrieved_at})
        rows.sort(key=lambda item: (str(item.get("county_name") or ""), item["county_entity_id"]))
        return self._base(request, response, rows, warnings or ([] if rows else ["no_records"]), "geography_lookup")

    def _parse_metros(self, records: list[Mapping[str, Any]], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        rows = []
        warnings = []
        for row in records:
            entity_id = str(first(row, "metro_entity_id", "entityid", "entity_id", "metro_id", "id") or "").strip()
            if not entity_id:
                warnings.append("malformed_metro_record")
                continue
            states = first(row, "state_codes", "states", "state")
            rows.append({"provider": self.provider, "metro_entity_id": entity_id, "metro_code": bounded(first(row, "metro_code", "cbsa", "metroCode")), "metro_name": bounded(first(row, "metro_name", "metroName", "name")), "state_codes": states if isinstance(states, list) else ([states] if states else []), "retrieval_timestamp": response.retrieved_at})
        rows.sort(key=lambda item: (str(item.get("metro_name") or ""), item["metro_entity_id"]))
        return self._base(request, response, rows, warnings or ([] if rows else ["no_records"]), "geography_lookup")

    def _parse_fmr(self, records: list[Mapping[str, Any]], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        rows = []
        warnings = []
        for row in records:
            base = self._housing_base(row, "fmr", response, request)
            if not base:
                warnings.append("malformed_fmr_record")
                continue
            for output, keys in BEDROOM_FIELDS:
                value, status, raw = parse_money(first(row, *keys))
                base[output] = {"raw_value": raw, "value": value, "value_status": status}
            rows.append(base)
        rows.sort(key=lambda item: (str(item.get("entity_id") or ""), str(item.get("year") or "")))
        return self._base(request, response, rows, warnings or ([] if rows else ["no_records"]), "fmr", self._measurement(rows))

    def _parse_income_limits(self, records: list[Mapping[str, Any]], response: UpstreamResponse, request: ConnectorRequest) -> Mapping[str, Any]:
        rows = []
        warnings = []
        for row in records:
            base = self._housing_base(row, "income_limits", response, request)
            if not base:
                warnings.append("malformed_income_limits_record")
                continue
            category = bounded(first(row, "income_category", "category", "limit_type", "type")) or "official"
            median, median_status, median_raw = parse_money(first(row, "median_family_income", "medianIncome", "median_family"))
            for size, keys in HOUSEHOLD_FIELDS:
                value, status, raw = parse_money(first(row, *keys))
                item = dict(base)
                item.update({"median_family_income": {"raw_value": median_raw, "value": median, "value_status": median_status}, "income_category": category, "household_size": size, "income_limit": {"raw_value": raw, "value": value, "value_status": status}})
                rows.append(item)
        rows.sort(key=lambda item: (str(item.get("entity_id") or ""), str(item.get("income_category") or ""), str(item.get("household_size") or "")))
        return self._base(request, response, rows, warnings or ([] if rows else ["no_records"]), "income_limits", self._measurement(rows))

    def _housing_base(self, row: Mapping[str, Any], dataset: str, response: UpstreamResponse, request: ConnectorRequest) -> dict[str, Any] | None:
        normalized = self._normalized_request(request)
        row_entity = str(first(row, "entity_id", "entityid", "id") or "").strip()
        entity_id = row_entity or normalized.get("entity_id") or ""
        year = str(first(row, "year", "fy", "fiscal_year") or normalized.get("year") or "").strip() or None
        if request.operation in {"fmr_state_data", "income_limits_state_data"} and not row_entity:
            return None
        if not entity_id:
            return None
        return {"provider": self.provider, "dataset": dataset, "entity_id": entity_id, "entity_type": bounded(first(row, "entity_type", "entityType")), "state_code": bounded(first(row, "state_code", "state")), "county_name": bounded(first(row, "county_name", "county")), "town_name": bounded(first(row, "town_name", "town")), "metro_status": bounded(first(row, "metro_status", "metroStatus")), "metro_name": bounded(first(row, "metro_name", "metro")), "year": year, "source": "HUD User Data API", "retrieval_timestamp": response.retrieved_at}

    def _measurement(self, rows: list[Mapping[str, Any]]) -> str | None:
        return max((str(row.get("year") or "") for row in rows), default=None) or None

    def _base(self, request: ConnectorRequest, response: UpstreamResponse, rows: list[dict[str, Any]], warnings: list[str], dataset: str, measurement_as_of: str | None = None) -> dict[str, Any]:
        validation = validate_normalized_rows(rows)
        if validation.valid:
            validation = ValidationResult(True, (), tuple(warnings), (), len(rows))
        normalized = self._normalized_request(request)
        status = "success" if rows else "unavailable"
        if rows and warnings:
            status = "partial"
        return {"response_status": status, "source_metadata": SourceMetadata(provider=self.provider, endpoint=request.operation, dataset=dataset, release=normalized.get("year") or normalized.get("updated_year"), measurement_as_of=measurement_as_of or normalized.get("year"), retrieval_timestamp=response.retrieved_at, content_type=response.content_type, upstream_status=response.status_code), "normalized_rows": rows, "warnings": tuple(warnings), "validation": validation, "source_payload": "hud_user_api", "source_artifact": request.operation, "transformation_version": TRANSFORMATION_BY_OPERATION[request.operation]}
