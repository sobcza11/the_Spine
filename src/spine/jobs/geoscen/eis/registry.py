from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from spine.jobs.geoscen.eis.contracts import validate_identifier
from spine.jobs.geoscen.eis.exceptions import ProviderNotFoundError, RequestValidationError


@dataclass(frozen=True)
class ConnectorRegistration:
    provider: str
    connector: Any
    metadata: Mapping[str, Any]


class ConnectorRegistry:
    def __init__(self) -> None:
        self._connectors: dict[str, ConnectorRegistration] = {}

    def register_connector(self, provider: str, connector: Any, metadata: Mapping[str, Any] | None = None) -> ConnectorRegistration:
        provider_id = normalize_provider_id(provider)
        if provider_id in self._connectors:
            raise RequestValidationError("Connector already registered.", provider=provider_id)
        registration = ConnectorRegistration(provider_id, connector, dict(metadata or {}))
        self._connectors[provider_id] = registration
        return registration

    def get_connector(self, provider: str) -> Any:
        provider_id = normalize_provider_id(provider)
        if provider_id not in self._connectors:
            raise ProviderNotFoundError(provider_id)
        return self._connectors[provider_id].connector

    def has_connector(self, provider: str) -> bool:
        return normalize_provider_id(provider) in self._connectors

    def list_connectors(self) -> list[ConnectorRegistration]:
        return [self._connectors[key] for key in sorted(self._connectors)]

    def clear_registry_for_tests(self) -> None:
        self._connectors.clear()


def normalize_provider_id(provider: str) -> str:
    provider_id = str(provider or "").strip().lower()
    validate_identifier(provider_id, "provider")
    return provider_id


DEFAULT_REGISTRY = ConnectorRegistry()


def register_connector(provider: str, connector: Any, metadata: Mapping[str, Any] | None = None) -> ConnectorRegistration:
    return DEFAULT_REGISTRY.register_connector(provider, connector, metadata)


def get_connector(provider: str) -> Any:
    return DEFAULT_REGISTRY.get_connector(provider)


def has_connector(provider: str) -> bool:
    return DEFAULT_REGISTRY.has_connector(provider)


def list_connectors() -> list[ConnectorRegistration]:
    return DEFAULT_REGISTRY.list_connectors()


def clear_registry_for_tests() -> None:
    DEFAULT_REGISTRY.clear_registry_for_tests()
