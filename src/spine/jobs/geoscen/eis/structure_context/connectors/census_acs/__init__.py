from __future__ import annotations

"""Census ACS GeoScen EIS connector public package surface."""

from spine.jobs.geoscen.eis.registry import ConnectorRegistry, DEFAULT_REGISTRY
from spine.jobs.geoscen.eis.structure_context.connectors.census_acs.connector import CensusACSConnector


def register_census_acs_connector(registry: ConnectorRegistry | None = None) -> CensusACSConnector:
    target = registry or DEFAULT_REGISTRY
    connector = CensusACSConnector()
    target.register_connector(
        connector.provider,
        connector,
        {
            "provider": connector.provider,
            "supported_operations": tuple(connector.supported_operations),
            "endpoint_template": connector.endpoint_template,
        },
    )
    return connector


__all__ = ["CensusACSConnector", "register_census_acs_connector"]
