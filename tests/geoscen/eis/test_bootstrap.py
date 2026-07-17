from __future__ import annotations

from spine.jobs.geoscen.eis.bootstrap import PROVIDER_ORDER, register_eis_connectors
from spine.jobs.geoscen.eis.registry import ConnectorRegistry


def test_bootstrap_registers_all_six_duplicate_safe_without_credentials() -> None:
    registry = ConnectorRegistry()
    manifest = register_eis_connectors(registry)
    assert [item.provider for item in registry.list_connectors()] == sorted(PROVIDER_ORDER)
    assert [provider["provider"] for provider in manifest["providers"]] == list(PROVIDER_ORDER)
    second = register_eis_connectors(registry)
    assert second["providers"] == manifest["providers"]
    assert registry.get_connector("fhfa").credential_specification == {}
    assert registry.get_connector("hud").credential_specification == {"HUD_USER_ACCESS_TOKEN": True}
