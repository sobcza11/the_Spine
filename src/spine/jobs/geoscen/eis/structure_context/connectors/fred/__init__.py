from __future__ import annotations

"""FRED GeoScen EIS connector public package surface."""

from spine.jobs.geoscen.eis.registry import ConnectorRegistry, DEFAULT_REGISTRY
from spine.jobs.geoscen.eis.structure_context.connectors.fred.connector import FREDConnector


def register_fred_connector(registry: ConnectorRegistry | None = None) -> FREDConnector:
    target = registry or DEFAULT_REGISTRY
    connector = FREDConnector()
    target.register_connector(
        connector.provider,
        connector,
        {
            "provider": connector.provider,
            "supported_operations": tuple(connector.supported_operations),
            "endpoints": dict(connector.endpoints),
        },
    )
    return connector


__all__ = ["FREDConnector", "register_fred_connector"]
