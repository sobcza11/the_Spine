"""GeoScen EIS canonical connector foundation."""

from spine.jobs.geoscen.eis.contracts import (
    ConnectorRequest,
    ConnectorResponse,
    LineageRecord,
    RawArtifactReference,
    SourceMetadata,
    TimeoutPolicy,
    ValidationResult,
)

__all__ = [
    "ConnectorRequest",
    "ConnectorResponse",
    "LineageRecord",
    "RawArtifactReference",
    "SourceMetadata",
    "TimeoutPolicy",
    "ValidationResult",
]
