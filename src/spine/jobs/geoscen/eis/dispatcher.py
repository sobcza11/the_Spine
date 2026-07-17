from __future__ import annotations

from typing import Any, Mapping

from spine.jobs.geoscen.eis.contracts import (
    ConnectorRequest,
    ConnectorResponse,
    ConnectorStatus,
    SourceMetadata,
    UpstreamResponse,
    ValidationResult,
    utc_now_iso,
)
from spine.jobs.geoscen.eis.credentials import get_provider_credentials
from spine.jobs.geoscen.eis.exceptions import (
    DispatchError,
    ResponseValidationError,
    UnsupportedOperationError,
)
from spine.jobs.geoscen.eis.http_client import GovernedHttpClient
from spine.jobs.geoscen.eis.lineage import build_lineage_record
from spine.jobs.geoscen.eis.raw_store import preserve_raw_response
from spine.jobs.geoscen.eis.registry import ConnectorRegistry, DEFAULT_REGISTRY
from spine.jobs.geoscen.eis.validation import validate_normalized_rows


def dispatch(
    request: ConnectorRequest,
    *,
    registry: ConnectorRegistry | None = None,
    http_client: GovernedHttpClient | Any | None = None,
    raw_store=preserve_raw_response,
) -> ConnectorResponse:
    registry = registry or DEFAULT_REGISTRY
    connector = registry.get_connector(request.provider)
    supported = {str(item).lower() for item in getattr(connector, "supported_operations", ())}
    if supported and request.operation not in supported:
        raise UnsupportedOperationError(request.provider, request.operation)

    validation = connector.validate_request(request)
    if not validation.valid:
        raise DispatchError("Connector request validation failed.", provider=request.provider, operation=request.operation)

    credentials = get_provider_credentials(
        request.provider,
        getattr(connector, "credential_specification", {}),
    )
    client = http_client or GovernedHttpClient()
    upstream = connector.fetch(request, client, credentials)
    if not isinstance(upstream, UpstreamResponse):
        raise DispatchError("Connector fetch must return UpstreamResponse.", provider=request.provider, operation=request.operation)

    parsed = connector.parse_response(upstream, request)
    normalized_rows = list(parsed.get("normalized_rows", []))
    response_validation = parsed.get("validation") or validate_normalized_rows(normalized_rows)
    if not isinstance(response_validation, ValidationResult):
        response_validation = validate_normalized_rows(normalized_rows)
    if not response_validation.valid:
        raise ResponseValidationError("Connector response validation failed.", provider=request.provider, operation=request.operation)

    source_metadata = parsed.get("source_metadata")
    if not isinstance(source_metadata, SourceMetadata):
        source_metadata = SourceMetadata(
            provider=request.provider,
            endpoint=str(parsed.get("endpoint") or upstream.url),
            dataset=parsed.get("dataset"),
            retrieval_timestamp=upstream.retrieved_at,
            content_type=upstream.content_type,
            upstream_status=upstream.status_code,
        )

    raw_reference = raw_store(
        provider=request.provider,
        operation=request.operation,
        content=upstream.content,
        destination=request.raw_store_destination,
        preserve_raw=request.preserve_raw,
        content_type=upstream.content_type,
    )
    request_metadata = {
        "provider": request.provider,
        "operation": request.operation,
        "parameters": request.parameters,
        "requested_at": request.requested_at,
        "correlation_id": request.correlation_id,
        "timeout_policy": request.timeout_policy,
        "upstream": upstream.safe_metadata(),
    }
    lineage = build_lineage_record(
        provider=request.provider,
        operation=request.operation,
        request_metadata=request_metadata,
        normalized_rows=normalized_rows,
        raw_sha256=raw_reference.sha256 if raw_reference else None,
        source_payload=parsed.get("source_payload", request.provider),
        source_artifact=raw_reference.path if raw_reference else parsed.get("source_artifact", upstream.url),
        source_run_ts=source_metadata.retrieval_timestamp,
        transformation_version=parsed.get("transformation_version", "geoscen-eis-stage1"),
    )
    return ConnectorResponse(
        provider=request.provider,
        operation=request.operation,
        status=parsed.get("response_status", ConnectorStatus.SUCCESS),
        retrieved_at=utc_now_iso(),
        request_metadata=request_metadata,
        source_metadata=source_metadata,
        lineage=lineage,
        normalized_rows=normalized_rows,
        validation=response_validation,
        raw_reference=raw_reference,
        warnings=tuple(parsed.get("warnings", ())),
        error=None,
    )
