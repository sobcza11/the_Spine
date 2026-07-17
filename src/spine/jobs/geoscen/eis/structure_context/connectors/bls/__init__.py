from __future__ import annotations

"""BLS GeoScen EIS connector public package surface."""

from spine.jobs.geoscen.eis.registry import ConnectorRegistry, DEFAULT_REGISTRY
from spine.jobs.geoscen.eis.structure_context.connectors.bls.connector import BLSConnector


def register_bls_connector(registry: ConnectorRegistry | None = None) -> BLSConnector:
    target = registry or DEFAULT_REGISTRY
    connector = BLSConnector()
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


__all__ = ["BLSConnector", "register_bls_connector"]
