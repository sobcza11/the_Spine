from __future__ import annotations

"""
Shared contracts for the GeoScen Economic Intelligence System.

Purpose:
    Define deterministic, provider-independent data structures used by
    connectors, validation, provenance, artifact persistence, serving,
    and downstream analytical consumers.

Rules:
    - Standard-library only.
    - Immutable contracts where practical.
    - No credentials or authorization values.
    - No provider-specific parsing logic.
    - No network or filesystem side effects.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Sequence, TypeAlias


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = (
    JsonScalar
    | list["JsonValue"]
    | dict[str, "JsonValue"]
)
NormalizedRow: TypeAlias = Mapping[str, JsonValue]


class ValidationStatus(str, Enum):
    """Validation outcome for a governed EIS artifact."""

    NOT_RUN = "not_run"
    PASSED = "passed"
    PASSED_WITH_WARNINGS = "passed_with_warnings"
    FAILED = "failed"


class ArtifactKind(str, Enum):
    """Supported governed artifact categories."""

    RAW = "raw"
    NORMALIZED = "normalized"
    METADATA = "metadata"
    MANIFEST = "manifest"
    PROVENANCE = "provenance"
    VALIDATION = "validation"


class ProviderDomain(str, Enum):
    """High-level analytical domain associated with a provider."""

    STRUCTURE_CONTEXT = "structure_context"
    INSTITUTIONAL_EVIDENCE = "institutional_evidence"
    FLOW_SIGNALS = "flow_signals"


@dataclass(frozen=True, slots=True)
class RequestMetadata:
    """
    Sanitized provider request metadata.

    Secret-bearing fields must be removed or redacted before this contract
    is created.
    """

    method: str
    endpoint: str
    parameters: Mapping[str, JsonValue] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        method = self.method.strip().upper()
        endpoint = self.endpoint.strip()

        if not method:
            raise ValueError("Request method must not be empty.")

        if not endpoint:
            raise ValueError("Request endpoint must not be empty.")

        object.__setattr__(self, "method", method)
        object.__setattr__(self, "endpoint", endpoint)


@dataclass(frozen=True, slots=True)
class SourceMetadata:
    """Provider-supplied source identity and publication metadata."""

    provider: str
    dataset: str
    series_ids: tuple[str, ...] = ()
    geography: str | None = None
    frequency: str | None = None
    units: str | None = None
    seasonal_adjustment: str | None = None
    publication_date: str | None = None
    source_vintage: str | None = None
    source_url: str | None = None

    def __post_init__(self) -> None:
        provider = self.provider.strip().lower()
        dataset = self.dataset.strip()

        if not provider:
            raise ValueError("Source provider must not be empty.")

        if not dataset:
            raise ValueError("Source dataset must not be empty.")

        cleaned_series_ids = tuple(
            series_id.strip()
            for series_id in self.series_ids
            if series_id.strip()
        )

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "dataset", dataset)
        object.__setattr__(self, "series_ids", cleaned_series_ids)


@dataclass(frozen=True, slots=True)
class ArtifactReference:
    """Reference to a persisted governed artifact."""

    kind: ArtifactKind
    path: str
    sha256: str
    byte_count: int
    content_type: str
    created_at_utc: str

    def __post_init__(self) -> None:
        if not self.path.strip():
            raise ValueError("Artifact path must not be empty.")

        if len(self.sha256) != 64:
            raise ValueError("Artifact SHA-256 must contain 64 characters.")

        if self.byte_count < 0:
            raise ValueError("Artifact byte count must not be negative.")

        if not self.content_type.strip():
            raise ValueError("Artifact content type must not be empty.")

        if not self.created_at_utc.strip():
            raise ValueError("Artifact creation timestamp must not be empty.")


@dataclass(frozen=True, slots=True)
class ValidationMessage:
    """One deterministic validation warning or error."""

    code: str
    message: str
    field_name: str | None = None
    row_index: int | None = None

    def __post_init__(self) -> None:
        if not self.code.strip():
            raise ValueError("Validation code must not be empty.")

        if not self.message.strip():
            raise ValueError("Validation message must not be empty.")

        if self.row_index is not None and self.row_index < 0:
            raise ValueError("Validation row index must not be negative.")


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """Governed validation result for a provider response or artifact."""

    status: ValidationStatus
    checked_at_utc: str
    checks_run: tuple[str, ...] = ()
    warnings: tuple[ValidationMessage, ...] = ()
    errors: tuple[ValidationMessage, ...] = ()

    def __post_init__(self) -> None:
        if not self.checked_at_utc.strip():
            raise ValueError("Validation timestamp must not be empty.")

        if self.status == ValidationStatus.FAILED and not self.errors:
            raise ValueError(
                "Failed validation reports must contain at least one error."
            )

        if self.status == ValidationStatus.PASSED and self.errors:
            raise ValueError(
                "Passed validation reports cannot contain errors."
            )


@dataclass(frozen=True, slots=True)
class ProvenanceSummary:
    """
    Provider-independent provenance summary attached to governed output.

    Full provenance construction and hashing logic belongs in provenance.py.
    """

    run_id: str
    provider: str
    connector_version: str
    schema_version: str
    transformation_version: str
    retrieved_at_utc: str
    request_fingerprint: str
    response_sha256: str
    parent_artifacts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("Provenance run ID must not be empty.")

        if not self.provider.strip():
            raise ValueError("Provenance provider must not be empty.")

        if not self.connector_version.strip():
            raise ValueError("Connector version must not be empty.")

        if not self.schema_version.strip():
            raise ValueError("Schema version must not be empty.")

        if not self.transformation_version.strip():
            raise ValueError("Transformation version must not be empty.")

        if not self.retrieved_at_utc.strip():
            raise ValueError("Retrieval timestamp must not be empty.")

        if len(self.request_fingerprint) != 64:
            raise ValueError(
                "Request fingerprint must contain 64 characters."
            )

        if len(self.response_sha256) != 64:
            raise ValueError(
                "Response SHA-256 must contain 64 characters."
            )


@dataclass(frozen=True, slots=True)
class ConnectorRequest:
    """Provider-independent connector request."""

    provider: str
    operation: str
    parameters: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        provider = self.provider.strip().lower()
        operation = self.operation.strip().lower()

        if not provider:
            raise ValueError("Connector provider must not be empty.")

        if not operation:
            raise ValueError("Connector operation must not be empty.")

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "operation", operation)


@dataclass(frozen=True, slots=True)
class ConnectorResponse:
    """Canonical governed response returned by every EIS connector."""

    provider: str
    operation: str
    source: SourceMetadata
    request: RequestMetadata
    rows: tuple[NormalizedRow, ...]
    retrieved_at_utc: str
    validation: ValidationReport
    provenance: ProvenanceSummary
    raw_artifact: ArtifactReference | None = None
    normalized_artifact: ArtifactReference | None = None
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        provider = self.provider.strip().lower()
        operation = self.operation.strip().lower()

        if not provider:
            raise ValueError("Response provider must not be empty.")

        if not operation:
            raise ValueError("Response operation must not be empty.")

        if provider != self.source.provider:
            raise ValueError(
                "Response provider must match source metadata provider."
            )

        if provider != self.provenance.provider.strip().lower():
            raise ValueError(
                "Response provider must match provenance provider."
            )

        if not self.retrieved_at_utc.strip():
            raise ValueError("Response retrieval timestamp must not be empty.")

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "operation", operation)


@dataclass(frozen=True, slots=True)
class ProviderDefinition:
    """Registry contract describing one supported EIS provider."""

    name: str
    domain: ProviderDomain
    credential_env_var: str | None
    requires_credential: bool
    supported_operations: tuple[str, ...]

    def __post_init__(self) -> None:
        name = self.name.strip().lower()

        if not name:
            raise ValueError("Provider name must not be empty.")

        operations = tuple(
            operation.strip().lower()
            for operation in self.supported_operations
            if operation.strip()
        )

        if not operations:
            raise ValueError(
                "Provider must declare at least one supported operation."
            )

        if self.requires_credential and not self.credential_env_var:
            raise ValueError(
                "Credentialed provider must declare an environment variable."
            )

        object.__setattr__(self, "name", name)
        object.__setattr__(self, "supported_operations", operations)


def freeze_rows(
    rows: Sequence[Mapping[str, JsonValue]],
) -> tuple[NormalizedRow, ...]:
    """
    Convert normalized row sequences into the immutable canonical form.

    The mappings themselves remain mapping-compatible so downstream
    serialization can preserve provider-normalized fields.
    """

    return tuple(dict(row) for row in rows)

# === EIS LEGACY CONTRACT COMPATIBILITY LAYER ===
#
# Preserves the established public contract used by the EIS dispatcher,
# connectors, HTTP client, validation, lineage, raw storage, artifacts,
# orchestration, and serving layers.

import json
from dataclasses import asdict, fields, is_dataclass
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from spine.jobs.geoscen.eis.exceptions import RequestValidationError


SCHEMA_VERSION = "geoscen-eis.v1"

POLLUTION_KEYS = frozenset(
    {
        "__proto__",
        "constructor",
        "prototype",
        "__class__",
        "__dict__",
        "__globals__",
        "__subclasses__",
    }
)


def utc_now_iso() -> str:
    """Return a deterministic UTC timestamp using the ISO-8601 Z suffix."""

    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def to_json_safe(value: Any) -> Any:
    """Convert governed contracts and nested values into JSON-safe objects."""

    if value is None:
        return None

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    if (
        value.__class__.__name__ in {
            "ConnectorRequest",
            "ConnectorResponse",
        }
        and hasattr(value, "to_dict")
        and callable(value.to_dict)
    ):
        return to_json_safe(value.to_dict())

    if is_dataclass(value):
        return {
            field_info.name: to_json_safe(
                getattr(value, field_info.name)
            )
            for field_info in fields(value)
        }

    if hasattr(value, "to_dict") and callable(value.to_dict):
        return to_json_safe(value.to_dict())

    if isinstance(value, Mapping):
        return {
            str(key): to_json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_json_safe(item) for item in value]

    if isinstance(value, (str, int, float, bool)):
        return value

    return str(value)


def canonical_json(value: Any) -> str:
    """Serialize values deterministically for hashing and artifacts."""

    return json.dumps(
        to_json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


class ConnectorStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


@dataclass(frozen=True)
class TimeoutPolicy:
    connect_seconds: float = 10.0
    read_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.connect_seconds <= 0:
            raise ValueError(
                "connect_seconds must be greater than zero"
            )

        if self.read_seconds <= 0:
            raise ValueError(
                "read_seconds must be greater than zero"
            )

    def as_tuple(self) -> tuple[float, float]:
        return (
            float(self.connect_seconds),
            float(self.read_seconds),
        )

    def to_dict(self) -> dict[str, float]:
        return {
            "connect_seconds": float(self.connect_seconds),
            "read_seconds": float(self.read_seconds),
        }


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    required_fields: tuple[str, ...] = ()
    row_count: int = 0
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "errors",
            tuple(str(item) for item in self.errors),
        )

        object.__setattr__(
            self,
            "warnings",
            tuple(str(item) for item in self.warnings),
        )

        object.__setattr__(
            self,
            "required_fields",
            tuple(str(item) for item in self.required_fields),
        )

        if self.row_count < 0:
            raise ValueError("row_count must not be negative")

        if self.valid and self.errors:
            raise ValueError(
                "valid results cannot contain errors"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "required_fields": list(self.required_fields),
            "row_count": self.row_count,
            "schema_version": self.schema_version,
        }

@dataclass(frozen=True)
class RawArtifactReference:
    path: str
    sha256: str
    size_bytes: int
    content_type: str | None = None
    stored_at: str | None = None
    preserved: bool = True

    def __post_init__(self) -> None:
        if not str(self.path).strip():
            raise ValueError("raw artifact path must not be empty")

        if self.sha256 and len(self.sha256) != 64:
            raise ValueError(
                "raw artifact SHA-256 must contain 64 characters"
            )

        if self.size_bytes < 0:
            raise ValueError(
                "raw artifact size must not be negative"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "content_type": self.content_type,
            "stored_at": self.stored_at,
            "preserved": self.preserved,
        }


@dataclass(frozen=True)
class LineageRecord:
    source_payload: str
    source_artifact: str
    source_run_ts: str
    provider: str
    operation: str
    request_hash: str
    raw_sha256: str | None
    normalized_sha256: str
    transformation_version: str
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        provider = str(self.provider).strip().lower()
        operation = str(self.operation).strip().lower()

        if not provider:
            raise ValueError(
                "lineage provider must not be empty"
            )

        if not operation:
            raise ValueError(
                "lineage operation must not be empty"
            )

        if not str(self.request_hash).strip():
            raise ValueError(
                "request_hash must not be empty"
            )

        if self.raw_sha256 is not None and not str(self.raw_sha256).strip():
            raise ValueError(
                "raw_sha256 must not be empty"
            )

        if not str(self.normalized_sha256).strip():
            raise ValueError(
                "normalized_sha256 must not be empty"
            )

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "operation", operation)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_payload": self.source_payload,
            "source_artifact": self.source_artifact,
            "source_run_ts": self.source_run_ts,
            "provider": self.provider,
            "operation": self.operation,
            "request_hash": self.request_hash,
            "raw_sha256": self.raw_sha256,
            "normalized_sha256": self.normalized_sha256,
            "transformation_version": self.transformation_version,
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True)
class SourceMetadata:
    provider: str
    endpoint: str
    dataset: str | None = None
    retrieval_timestamp: str | None = None
    content_type: str | None = None
    upstream_status: int | None = None
    series_ids: tuple[str, ...] = ()
    geography: str | None = None
    frequency: str | None = None
    units: str | None = None
    seasonal_adjustment: str | None = None
    publication_date: str | None = None
    release: str | None = None
    measurement_as_of: str | None = None
    source_vintage: str | None = None
    source_url: str | None = None
    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        provider = str(self.provider).strip().lower()
        endpoint = str(self.endpoint).strip()

        if not provider:
            raise ValueError(
                "source provider must not be empty"
            )

        if not endpoint:
            raise ValueError(
                "source endpoint must not be empty"
            )

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "endpoint", endpoint)

        object.__setattr__(
            self,
            "series_ids",
            tuple(
                str(item).strip()
                for item in self.series_ids
                if str(item).strip()
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "endpoint": self.endpoint,
            "dataset": self.dataset,
            "retrieval_timestamp": self.retrieval_timestamp,
            "content_type": self.content_type,
            "upstream_status": self.upstream_status,
            "series_ids": list(self.series_ids),
            "geography": self.geography,
            "frequency": self.frequency,
            "units": self.units,
            "seasonal_adjustment": self.seasonal_adjustment,
            "publication_date": self.publication_date,
            "release": self.release,
            "measurement_as_of": self.measurement_as_of,
            "source_vintage": self.source_vintage,
            "source_url": self.source_url,
            "attributes": dict(self.attributes),
        }


@dataclass(frozen=True)
class UpstreamResponse:
    url: str
    method: str
    status_code: int
    headers: Mapping[str, str]
    content: bytes
    retrieved_at: str
    elapsed_seconds: float | None = None

    def __post_init__(self) -> None:
        method = str(self.method).strip().upper()

        if not method:
            raise ValueError(
                "upstream method must not be empty"
            )

        if not str(self.url).strip():
            raise ValueError(
                "upstream URL must not be empty"
            )

        if self.status_code < 100 or self.status_code > 599:
            raise ValueError(
                "invalid upstream HTTP status"
            )

        object.__setattr__(self, "method", method)
        object.__setattr__(
            self,
            "headers",
            dict(self.headers or {}),
        )

    @property
    def content_type(self) -> str | None:
        for key, value in self.headers.items():
            if str(key).lower() == "content-type":
                return str(value)
        return None

    def text(self) -> str:
        return self.content.decode(
            "utf-8",
            errors="replace",
        )

    def json(self) -> Any:
        return json.loads(self.text())

    def safe_metadata(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "method": self.method,
            "status_code": self.status_code,
            "content_type": self.content_type,
            "retrieved_at": self.retrieved_at,
            "elapsed_seconds": self.elapsed_seconds,
            "content_length": len(self.content),
        }

    def to_dict(self) -> dict[str, Any]:
        return self.safe_metadata()


@dataclass(frozen=True)
class ConnectorRequest:
    provider: str
    operation: str
    parameters: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    correlation_id: str = field(default_factory=lambda: uuid4().hex)
    requested_at: str = field(default_factory=utc_now_iso)
    timeout_policy: TimeoutPolicy = field(default_factory=TimeoutPolicy)
    preserve_raw: bool = True
    raw_store_destination: str | None = None

    def __post_init__(self) -> None:
        provider = str(self.provider or "").strip().lower()
        operation = str(self.operation or "").strip().lower()

        if not provider:
            raise ValueError("connector provider must not be empty")

        if not operation:
            raise ValueError("connector operation must not be empty")

        if not str(self.correlation_id or "").strip():
            raise ValueError("correlation_id must not be empty")

        parameters = dict(self.parameters or {})
        metadata = dict(self.metadata or {})

        _reject_unsafe(parameters, path="$.parameters")
        _reject_unsafe(metadata, path="$.metadata")

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "operation", operation)
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "metadata", metadata)

    def to_dict(self) -> dict[str, Any]:
        from spine.jobs.geoscen.eis.credentials import redact_mapping

        return {
            "provider": self.provider,
            "operation": self.operation,
            "parameters": redact_mapping(self.parameters),
            "metadata": redact_mapping(self.metadata),
            "correlation_id": self.correlation_id,
            "requested_at": self.requested_at,
            "timeout_policy": self.timeout_policy.to_dict(),
            "preserve_raw": self.preserve_raw,
            "raw_store_destination": self.raw_store_destination,
        }


@dataclass(frozen=True)
class ConnectorResponse:
    provider: str
    operation: str
    status: ConnectorStatus
    retrieved_at: str
    request_metadata: Mapping[str, Any]
    source_metadata: SourceMetadata
    lineage: LineageRecord
    normalized_rows: Sequence[Mapping[str, Any]]
    validation: ValidationResult
    raw_reference: RawArtifactReference | None = None
    warnings: tuple[str, ...] = ()
    error: Mapping[str, Any] | str | None = None

    def __post_init__(self) -> None:
        provider = str(self.provider).strip().lower()
        operation = str(self.operation).strip().lower()

        if not provider:
            raise ValueError(
                "response provider must not be empty"
            )

        if not operation:
            raise ValueError(
                "response operation must not be empty"
            )

        status = self.status

        if not isinstance(status, ConnectorStatus):
            status = ConnectorStatus(str(status).lower())

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "operation", operation)
        object.__setattr__(self, "status", status)
        object.__setattr__(
            self,
            "request_metadata",
            dict(self.request_metadata or {}),
        )
        object.__setattr__(
            self,
            "normalized_rows",
            tuple(
                dict(row)
                for row in self.normalized_rows
            ),
        )
        object.__setattr__(
            self,
            "warnings",
            tuple(str(item) for item in self.warnings),
        )

    @property
    def rows(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self.normalized_rows)

    @property
    def source(self) -> SourceMetadata:
        return self.source_metadata

    @property
    def provenance(self) -> LineageRecord:
        return self.lineage

    @property
    def raw_artifact(self) -> RawArtifactReference | None:
        return self.raw_reference

    def to_dict(self) -> dict[str, Any]:
        from spine.jobs.geoscen.eis.credentials import redact_mapping

        return {
            "provider": self.provider,
            "operation": self.operation,
            "status": self.status.value,
            "retrieved_at": self.retrieved_at,
            "request_metadata": to_json_safe(
                redact_mapping(self.request_metadata)
            ),
            "source_metadata": to_json_safe(
                self.source_metadata
            ),
            "lineage": to_json_safe(
                self.lineage
            ),
            "normalized_rows": to_json_safe(
                self.normalized_rows
            ),
            "validation": to_json_safe(
                self.validation
            ),
            "raw_reference": to_json_safe(
                self.raw_reference
            ),
            "warnings": list(self.warnings),
            "error": to_json_safe(
                self.error
            ),
        }

# === EIS CONTRACT SAFETY UTILITIES ===

import re as _re
from collections.abc import Mapping as _Mapping
from collections.abc import Sequence as _Sequence


_IDENTIFIER_RE = _re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]*$"
)


def validate_identifier(
    value: object,
    field_name: str = "identifier",
) -> str:
    """
    Validate & normalize an internal EIS identifier.

    Allowed:
        letters
        numbers
        underscore
        period
        hyphen
    """

    text = str(value or "").strip().lower()

    if not text:
        raise ValueError(
            f"{field_name} must not be empty"
        )

    if not _IDENTIFIER_RE.fullmatch(text):
        raise ValueError(
            f"{field_name} contains unsupported characters"
        )

    if text in {".", ".."}:
        raise ValueError(
            f"{field_name} is unsafe"
        )

    return text


def _reject_unsafe(
    value: object,
    *,
    path: str = "$",
) -> None:
    """
    Recursively reject prototype-pollution keys & unsupported values.

    This validates configuration, contracts, request parameters,
    specifications, manifests & serving artifacts before serialization.
    """

    if isinstance(value, _Mapping):
        for key, item in value.items():
            key_text = str(key)

            if key_text in POLLUTION_KEYS:
                raise RequestValidationError(
                    f"unsafe key rejected at {path}.{key_text}"
                )

            _reject_unsafe(
                item,
                path=f"{path}.{key_text}",
            )

        return

    if isinstance(value, _Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        for index, item in enumerate(value):
            _reject_unsafe(
                item,
                path=f"{path}[{index}]",
            )

        return

    if isinstance(value, float):
        if value != value:
            raise RequestValidationError(
                f"NaN rejected at {path}"
            )

        if value in {
            float("inf"),
            float("-inf"),
        }:
            raise RequestValidationError(
                f"infinite numeric value rejected at {path}"
            )

        return

    if value is None or isinstance(
        value,
        (
            str,
            int,
            bool,
            bytes,
            datetime,
            date,
            Path,
            Enum,
        ),
    ):
        return

    if is_dataclass(value):
        _reject_unsafe(
            {
                item.name: getattr(value, item.name)
                for item in fields(value)
            },
            path=path,
        )

        return

    if hasattr(value, "to_dict") and callable(value.to_dict):
        _reject_unsafe(
            value.to_dict(),
            path=path,
        )

        return

    raise RequestValidationError(
        f"unsupported value type at {path}: "
        f"{type(value).__name__}"
    )

