from __future__ import annotations

from typing import Any, Mapping


def build_run_summary(run_manifest: Mapping[str, Any], *, artifact_root: str, manifest_reference: Mapping[str, Any] | None = None) -> dict[str, Any]:
    results = run_manifest.get("specifications", [])
    providers = run_manifest.get("providers", [])
    status_counts = {"success": 0, "partial": 0, "unavailable": 0, "failed": 0, "skipped": 0}
    for provider in providers:
        status_counts["success"] += int(provider.get("success_count") or 0)
        status_counts["partial"] += int(provider.get("partial_count") or 0)
        status_counts["unavailable"] += int(provider.get("unavailable_count") or 0)
        status_counts["failed"] += int(provider.get("error_count") or 0)
    return {
        "run_id": run_manifest["run_id"],
        "mode": run_manifest["run_mode"],
        "providers_attempted": [provider["provider"] for provider in providers if provider.get("executed_specifications")],
        "specifications_attempted": len(results),
        "succeeded": status_counts["success"],
        "partial": status_counts["partial"],
        "unavailable": status_counts["unavailable"],
        "failed": status_counts["failed"],
        "skipped": status_counts["skipped"],
        "total_normalized_rows": run_manifest.get("validation_summary", {}).get("total_rows", 0),
        "raw_bytes_preserved": 0,
        "duration_ms": run_manifest["duration_ms"],
        "artifact_root": artifact_root,
        "manifest_reference": manifest_reference,
        "major_warnings": list(run_manifest.get("warnings", []))[:20],
        "safe_errors": list(run_manifest.get("errors", []))[:20],
        "status": run_manifest["status"],
    }
