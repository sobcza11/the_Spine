from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from spine.jobs.geoscen.eis.artifacts import ArtifactReference
from spine.jobs.geoscen.eis.bootstrap import PROVIDER_ORDER
from spine.jobs.geoscen.eis.contracts import canonical_json, utc_now_iso
from spine.jobs.geoscen.eis.lineage import sha256_bytes


class EISManifestError(RuntimeError):
    pass


@dataclass(frozen=True)
class ManifestReference:
    path: str
    sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def build_provider_manifests(results: list[Mapping[str, Any]], capability_manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    manifests = []
    caps = {item["provider"]: item for item in capability_manifest.get("providers", [])}
    for provider in PROVIDER_ORDER:
        provider_results = [item for item in results if item.get("provider") == provider]
        statuses = [str(item.get("status")) for item in provider_results]
        manifests.append(
            {
                "provider": provider,
                "registered_operations": tuple(caps.get(provider, {}).get("registered_operations", ())),
                "credential_requirements": caps.get(provider, {}).get("credential_requirements", {}),
                "executed_specifications": [item.get("specification_id") for item in provider_results],
                "success_count": statuses.count("success"),
                "partial_count": statuses.count("partial"),
                "unavailable_count": statuses.count("unavailable"),
                "error_count": statuses.count("failed"),
                "total_rows": sum(int(item.get("row_count") or 0) for item in provider_results),
                "artifact_references": [ref.to_dict() for item in provider_results for ref in item.get("artifacts", [])],
                "validation_status": "valid" if all(item.get("validation_valid", True) for item in provider_results) else "invalid",
                "warnings": [warning for item in provider_results for warning in item.get("warnings", [])],
            },
        )
    return manifests


def build_run_manifest(*, run_id: str, plan: Mapping[str, Any], started_at: str, completed_at: str, duration_ms: int, provider_manifests: list[dict[str, Any]], results: list[Mapping[str, Any]], credential_preflight: list[Mapping[str, Any]], status: str, errors: list[Mapping[str, Any]], warnings: list[str]) -> dict[str, Any]:
    artifact_index = [ref.to_dict() for item in results for ref in item.get("artifacts", [])]
    lineage_index = [ref.to_dict() for item in results for ref in item.get("artifacts", []) if ref.artifact_type == "lineage"]
    return {
        "run_id": run_id,
        "plan_id": plan["plan_id"],
        "schema_version": plan["schema_version"],
        "run_mode": plan["run_mode"],
        "started_at": started_at,
        "completed_at": completed_at or utc_now_iso(),
        "duration_ms": duration_ms,
        "correlation_id": plan["correlation_id"],
        "providers": provider_manifests,
        "specifications": [item["specification_id"] for item in results],
        "artifact_index": artifact_index,
        "lineage_index": lineage_index,
        "validation_summary": {"valid": all(item.get("validation_valid", True) for item in results), "total_rows": sum(int(item.get("row_count") or 0) for item in results)},
        "credential_preflight_summary": credential_preflight,
        "status": status,
        "errors": errors,
        "warnings": warnings,
        "repository": {"metadata_only": True},
    }


def manifest_sha(manifest: Mapping[str, Any]) -> str:
    return sha256_bytes(canonical_json(manifest).encode("utf-8")) or ""


def write_manifest(manifest: Mapping[str, Any], *, root: str | os.PathLike[str], filename: str, overwrite: bool = False) -> ManifestReference:
    if "/" in filename or "\\" in filename or ".." in filename or not filename.endswith(".json"):
        raise EISManifestError("manifest filename rejected")
    base = Path(root).resolve()
    base.mkdir(parents=True, exist_ok=True)
    target = (base / filename).resolve()
    if base not in target.parents and base != target.parent:
        raise EISManifestError("manifest path rejected")
    if target.exists() and not overwrite:
        raise EISManifestError("manifest already exists")
    content = (canonical_json(manifest) + "\n").encode("utf-8")
    fd, tmp_name = tempfile.mkstemp(prefix=f".{filename}.", suffix=".tmp", dir=str(base))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
        os.replace(tmp_name, target)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
    return ManifestReference(str(target), sha256_bytes(content) or "", len(content))
