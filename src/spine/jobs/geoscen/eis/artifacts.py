from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from spine.jobs.geoscen.eis.contracts import ConnectorResponse, SCHEMA_VERSION, canonical_json, to_json_safe
from spine.jobs.geoscen.eis.lineage import sha256_bytes


class EISArtifactError(RuntimeError):
    pass


@dataclass(frozen=True)
class ArtifactReference:
    artifact_type: str
    provider: str
    specification_id: str
    run_id: str
    path: str
    sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def normalized_artifact_payload(response: ConnectorResponse, *, specification_id: str, run_id: str) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": "eis_normalized_source",
        "provider": response.provider,
        "operation": response.operation,
        "specification_id": specification_id,
        "run_id": run_id,
        "status": response.status.value if hasattr(response.status, "value") else response.status,
        "retrieved_at": response.retrieved_at,
        "source_metadata": response.source_metadata,
        "validation": response.validation,
        "lineage": response.lineage,
        "row_count": len(response.normalized_rows),
        "rows": response.normalized_rows,
        "warnings": response.warnings,
    }
    return to_json_safe(payload)


def write_normalized_artifact(response: ConnectorResponse, *, specification_id: str, run_id: str, root: str | os.PathLike[str], overwrite: bool = False) -> ArtifactReference:
    payload = normalized_artifact_payload(response, specification_id=specification_id, run_id=run_id)
    target_dir = safe_artifact_dir(root, response.provider, specification_id, run_id)
    target = target_dir / "normalized.json"
    return _write_json_artifact(target, payload, "eis_normalized_source", response.provider, specification_id, run_id, overwrite)


def write_validation_artifact(response: ConnectorResponse, *, specification_id: str, run_id: str, root: str | os.PathLike[str], overwrite: bool = False) -> ArtifactReference:
    target_dir = safe_artifact_dir(root, response.provider, specification_id, run_id)
    return _write_json_artifact(target_dir / "validation.json", response.validation.to_dict(), "validation", response.provider, specification_id, run_id, overwrite)


def write_lineage_artifact(response: ConnectorResponse, *, specification_id: str, run_id: str, root: str | os.PathLike[str], overwrite: bool = False) -> ArtifactReference:
    target_dir = safe_artifact_dir(root, response.provider, specification_id, run_id)
    return _write_json_artifact(target_dir / "lineage.json", response.lineage.to_dict(), "lineage", response.provider, specification_id, run_id, overwrite)


def safe_artifact_dir(root: str | os.PathLike[str], provider: str, specification_id: str, run_id: str) -> Path:
    base = Path(root).resolve()
    parts = [safe_slug(provider), safe_slug(specification_id), safe_slug(run_id)]
    target = (base / parts[0] / parts[1] / parts[2]).resolve()
    if base != target and base not in target.parents:
        raise EISArtifactError("artifact path traversal rejected")
    target.mkdir(parents=True, exist_ok=True)
    return target


def safe_slug(value: str) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value))[:96].strip("-")
    if not text or ".." in text:
        raise EISArtifactError("unsafe artifact slug")
    return text


def _write_json_artifact(target: Path, payload: Any, artifact_type: str, provider: str, specification_id: str, run_id: str, overwrite: bool) -> ArtifactReference:
    if target.exists() and not overwrite:
        raise EISArtifactError("artifact already exists")
    data = (canonical_json(payload) + "\n").encode("utf-8")
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        os.replace(tmp_name, target)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
    return ArtifactReference(artifact_type, provider, specification_id, run_id, str(target), sha256_bytes(data) or "", len(data))
