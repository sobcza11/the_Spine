from __future__ import annotations

"""BEA GeoScen EIS connector public package surface."""

from spine.jobs.geoscen.eis.registry import ConnectorRegistry, DEFAULT_REGISTRY
from spine.jobs.geoscen.eis.structure_context.connectors.bea.connector import BEAConnector


def register_bea_connector(registry: ConnectorRegistry | None = None) -> BEAConnector:
    target = registry or DEFAULT_REGISTRY
    connector = BEAConnector()
    target.register_connector(
        connector.provider,
        connector,
        {
            "provider": connector.provider,
            "supported_operations": tuple(connector.supported_operations),
            "endpoint": connector.endpoint,
        },
    )
    return connector


__all__ = ["BEAConnector", "register_bea_connector"]
