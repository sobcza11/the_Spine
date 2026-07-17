from __future__ import annotations

"""FHFA GeoScen EIS connector public package surface."""

from spine.jobs.geoscen.eis.registry import ConnectorRegistry, DEFAULT_REGISTRY
from spine.jobs.geoscen.eis.structure_context.connectors.fhfa.connector import FHFAConnector


def register_fhfa_connector(registry: ConnectorRegistry | None = None) -> FHFAConnector:
    target = registry or DEFAULT_REGISTRY
    connector = FHFAConnector()
    target.register_connector(
        connector.provider,
        connector,
        {
            "provider": connector.provider,
            "supported_operations": tuple(connector.supported_operations),
            "credential_specification": dict(connector.credential_specification),
        },
    )
    return connector


__all__ = ["FHFAConnector", "register_fhfa_connector"]
