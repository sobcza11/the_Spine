from __future__ import annotations

from dataclasses import dataclass
from typing import Any

SECRET_MARKER = "[redacted]"
SECRET_FIELD_NAMES = {
    "api_key",
    "apikey",
    "authorization",
    "bearer",
    "cookie",
    "password",
    "secret",
    "token",
}


def _safe_context(context: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in context.items():
        key_l = str(key).lower()
        if any(secret in key_l for secret in SECRET_FIELD_NAMES):
            safe[key] = SECRET_MARKER
        elif isinstance(value, (str, int, float, bool)) or value is None:
            safe[key] = value
        else:
            safe[key] = str(type(value).__name__)
    return safe


@dataclass(frozen=True)
class EISConnectorError(Exception):
    message: str
    code: str = "eis_connector_error"
    provider: str | None = None
    operation: str | None = None
    upstream_status: int | None = None
    context: dict[str, Any] | None = None

    def __str__(self) -> str:
        parts = [self.code, self.message]
        if self.provider:
            parts.append(f"provider={self.provider}")
        if self.operation:
            parts.append(f"operation={self.operation}")
        if self.upstream_status is not None:
            parts.append(f"upstream_status={self.upstream_status}")
        return " | ".join(parts)

    def safe_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "provider": self.provider,
            "operation": self.operation,
            "upstream_status": self.upstream_status,
            "context": _safe_context(self.context or {}),
        }


class CredentialError(EISConnectorError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, code="credential_error", **kwargs)


class RequestValidationError(EISConnectorError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, code="request_validation_error", **kwargs)


class ProviderNotFoundError(EISConnectorError):
    def __init__(self, provider: str, **kwargs: Any) -> None:
        super().__init__("Provider not registered.", code="provider_not_found", provider=provider, **kwargs)


class UnsupportedOperationError(EISConnectorError):
    def __init__(self, provider: str | None, operation: str, **kwargs: Any) -> None:
        super().__init__(
            "Unsupported connector operation.",
            code="unsupported_operation",
            provider=provider,
            operation=operation,
            **kwargs,
        )


class UpstreamRequestError(EISConnectorError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, code="upstream_request_error", **kwargs)


class UpstreamRateLimitError(UpstreamRequestError):
    def __init__(self, message: str = "Upstream rate limit.", **kwargs: Any) -> None:
        EISConnectorError.__init__(self, message, code="upstream_rate_limit", **kwargs)


class UpstreamAuthenticationError(UpstreamRequestError):
    def __init__(self, message: str = "Upstream authentication failed.", **kwargs: Any) -> None:
        EISConnectorError.__init__(self, message, code="upstream_authentication_error", **kwargs)


class UpstreamResponseError(EISConnectorError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, code="upstream_response_error", **kwargs)


class ResponseValidationError(EISConnectorError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, code="response_validation_error", **kwargs)


class RawStorageError(EISConnectorError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, code="raw_storage_error", **kwargs)


class DispatchError(EISConnectorError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, code="dispatch_error", **kwargs)
