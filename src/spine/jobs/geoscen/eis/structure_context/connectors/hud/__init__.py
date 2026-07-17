from __future__ import annotations

"""HUD User GeoScen EIS connector public package surface."""

from spine.jobs.geoscen.eis.registry import ConnectorRegistry, DEFAULT_REGISTRY
from spine.jobs.geoscen.eis.structure_context.connectors.hud.connector import HUDConnector


def register_hud_connector(registry: ConnectorRegistry | None = None) -> HUDConnector:
    target = registry or DEFAULT_REGISTRY
    connector = HUDConnector()
    target.register_connector(
        connector.provider,
        connector,
        {
            "provider": connector.provider,
            "supported_operations": tuple(connector.supported_operations),
            "endpoint_root": connector.endpoint_root,
            "rate_limit": "60 queries/minute",
        },
    )
    return connector


__all__ = ["HUDConnector", "register_hud_connector"]
