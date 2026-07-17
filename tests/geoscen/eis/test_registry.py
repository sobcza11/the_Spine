from __future__ import annotations

import pytest

from spine.jobs.geoscen.eis.exceptions import ProviderNotFoundError, RequestValidationError
from spine.jobs.geoscen.eis.registry import ConnectorRegistry


class MockConnector:
    pass


def test_registry_registration_and_listing() -> None:
    registry = ConnectorRegistry()
    connector = MockConnector()
    registry.register_connector("BLS", connector, {"phase": 1})
    registry.register_connector("fred", MockConnector())
    assert registry.has_connector("bls")
    assert registry.get_connector("BLS") is connector
    assert [item.provider for item in registry.list_connectors()] == ["bls", "fred"]


def test_duplicate_and_unknown_provider() -> None:
    registry = ConnectorRegistry()
    registry.register_connector("bls", MockConnector())
    with pytest.raises(RequestValidationError):
        registry.register_connector("BLS", MockConnector())
    with pytest.raises(ProviderNotFoundError):
        registry.get_connector("missing")


def test_isolated_registry_clear() -> None:
    registry = ConnectorRegistry()
    registry.register_connector("bls", MockConnector())
    registry.clear_registry_for_tests()
    assert registry.list_connectors() == []
