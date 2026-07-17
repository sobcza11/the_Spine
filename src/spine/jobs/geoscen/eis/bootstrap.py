from __future__ import annotations

from typing import Any

from spine.jobs.geoscen.eis.registry import ConnectorRegistry, DEFAULT_REGISTRY
from spine.jobs.geoscen.eis.structure_context.connectors.bea import register_bea_connector
from spine.jobs.geoscen.eis.structure_context.connectors.bls import register_bls_connector
from spine.jobs.geoscen.eis.structure_context.connectors.census_acs import register_census_acs_connector
from spine.jobs.geoscen.eis.structure_context.connectors.fhfa import register_fhfa_connector
from spine.jobs.geoscen.eis.structure_context.connectors.fred import register_fred_connector
from spine.jobs.geoscen.eis.structure_context.connectors.hud import register_hud_connector

PROVIDER_ORDER = ("bls", "fred", "bea", "census_acs", "fhfa", "hud")
_REGISTRARS = {
    "bls": register_bls_connector,
    "fred": register_fred_connector,
    "bea": register_bea_connector,
    "census_acs": register_census_acs_connector,
    "fhfa": register_fhfa_connector,
    "hud": register_hud_connector,
}


def register_eis_connectors(registry: ConnectorRegistry | None = None) -> dict[str, Any]:
    target = registry or DEFAULT_REGISTRY
    for provider in PROVIDER_ORDER:
        if not target.has_connector(provider):
            _REGISTRARS[provider](target)
    return provider_capability_manifest(target)


def provider_capability_manifest(registry: ConnectorRegistry) -> dict[str, Any]:
    providers = []
    for registration in registry.list_connectors():
        connector = registration.connector
        providers.append(
            {
                "provider": registration.provider,
                "registered_operations": tuple(getattr(connector, "supported_operations", ())),
                "credential_requirements": dict(getattr(connector, "credential_specification", {})),
                "metadata": dict(registration.metadata),
            },
        )
    providers.sort(key=lambda item: PROVIDER_ORDER.index(item["provider"]) if item["provider"] in PROVIDER_ORDER else 999)
    return {"schema_version": "geoscen.eis.integration.v1", "providers": providers}
