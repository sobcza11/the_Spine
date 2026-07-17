from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Mapping

from spine.jobs.geoscen.eis.artifacts import ArtifactReference, write_lineage_artifact, write_normalized_artifact, write_validation_artifact
from spine.jobs.geoscen.eis.bootstrap import register_eis_connectors
from spine.jobs.geoscen.eis.contracts import ConnectorRequest, ConnectorResponse, UpstreamResponse, canonical_json, utc_now_iso
from spine.jobs.geoscen.eis.dispatcher import dispatch
from spine.jobs.geoscen.eis.execution_plan import EISBudgetExceededError, EISCredentialPreflightError, EISExecutionError, ExecutionPlan
from spine.jobs.geoscen.eis.manifests import build_provider_manifests, build_run_manifest, write_manifest
from spine.jobs.geoscen.eis.registry import ConnectorRegistry
from spine.jobs.geoscen.eis.run_summary import build_run_summary
from spine.jobs.geoscen.eis.specifications import EISDatasetSpecification


class FixtureHttpClient:
    def __init__(self, content: bytes, *, content_type: str = "application/json", retrieved_at: str = "2026-01-01T00:00:00Z") -> None:
        self.content = content
        self.content_type = content_type
        self.retrieved_at = retrieved_at
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, url: str, *, correlation_id: str, headers=None, params=None, timeout_policy=None, **kwargs) -> UpstreamResponse:
        self.calls.append({"method": method, "url": url, "correlation_id": correlation_id, "headers": headers or {}, "params": params or {}, "timeout_policy": timeout_policy})
        return UpstreamResponse(url=url, method=method, status_code=200, headers={"content-type": self.content_type}, content=self.content, retrieved_at=self.retrieved_at)


def credential_preflight(registry: ConnectorRegistry, *, live: bool) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for registration in registry.list_connectors():
        spec = getattr(registration.connector, "credential_specification", {})
        if not spec:
            rows.append({"provider": registration.provider, "credential_name": None, "required": False, "available": True, "status": "not_required"})
        for name, required in spec.items():
            available = bool(os.getenv(name, "").strip())
            rows.append({"provider": registration.provider, "credential_name": name, "required": bool(required), "available": available, "status": "available" if available else ("missing" if required else "optional_missing")})
    if live:
        missing = [row for row in rows if row["required"] and not row["available"]]
        if missing:
            raise EISCredentialPreflightError("live credential preflight failed")
    return rows


def run_execution_plan(plan: ExecutionPlan, *, registry: ConnectorRegistry | None = None, fixture_root: str | os.PathLike[str] | None = None, output_root: str | os.PathLike[str] | None = None, allow_live: bool = False, overwrite: bool = False) -> dict[str, Any]:
    if plan.run_mode == "live" and (not allow_live or os.getenv("EIS_ALLOW_LIVE") != "1"):
        raise EISExecutionError("live mode requires explicit opt-in")
    started = utc_now_iso()
    start = time.perf_counter()
    target_registry = registry or ConnectorRegistry()
    capability_manifest = register_eis_connectors(target_registry)
    preflight = credential_preflight(target_registry, live=plan.run_mode == "live")
    base_root = Path(output_root) if output_root else Path.cwd()
    normalized_root = base_root / plan.normalized_root
    raw_root = base_root / plan.raw_root
    manifest_root = base_root / plan.manifest_root / plan.correlation_id
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    total_rows = 0
    for spec in sorted(plan.specifications, key=lambda item: item.specification_id):
        if not spec.active:
            results.append(_skipped(spec, "inactive"))
            continue
        try:
            response = _execute_specification(spec, plan, target_registry, fixture_root=fixture_root, raw_root=raw_root)
            row_count = len(response.normalized_rows)
            total_rows += row_count
            if total_rows > plan.maximum_total_rows:
                raise EISBudgetExceededError("maximum row budget exceeded")
            artifacts = [
                write_normalized_artifact(response, specification_id=spec.specification_id, run_id=plan.correlation_id, root=normalized_root, overwrite=overwrite),
                write_validation_artifact(response, specification_id=spec.specification_id, run_id=plan.correlation_id, root=normalized_root, overwrite=overwrite),
                write_lineage_artifact(response, specification_id=spec.specification_id, run_id=plan.correlation_id, root=normalized_root, overwrite=overwrite),
            ]
            results.append(_result_record(spec, response, artifacts))
        except EISBudgetExceededError:
            raise
        except Exception as exc:
            errors.append({"specification_id": spec.specification_id, "provider": spec.provider, "error_type": type(exc).__name__, "message": str(exc)[:240]})
            results.append({"specification_id": spec.specification_id, "provider": spec.provider, "status": "failed", "row_count": 0, "validation_valid": False, "warnings": (), "artifacts": []})
            if spec.required and (plan.fail_fast or not plan.allow_partial):
                break
    provider_manifests = build_provider_manifests(results, capability_manifest)
    completed = utc_now_iso()
    status = aggregate_status(results, plan)
    run_manifest = build_run_manifest(run_id=plan.correlation_id, plan=plan.to_dict(), started_at=started, completed_at=completed, duration_ms=int((time.perf_counter() - start) * 1000), provider_manifests=provider_manifests, results=results, credential_preflight=preflight, status=status, errors=errors, warnings=warnings)
    run_manifest_ref = write_manifest(run_manifest, root=manifest_root, filename="run_manifest.json", overwrite=overwrite)
    provider_manifest_ref = write_manifest({"providers": provider_manifests}, root=manifest_root, filename="provider_manifest.json", overwrite=overwrite)
    summary = build_run_summary(run_manifest, artifact_root=str(normalized_root), manifest_reference=run_manifest_ref.to_dict())
    return {"run_summary": summary, "run_manifest": run_manifest, "provider_manifests": provider_manifests, "results": results, "credential_preflight": preflight, "manifest_references": [run_manifest_ref, provider_manifest_ref]}


def aggregate_status(results: list[Mapping[str, Any]], plan: ExecutionPlan) -> str:
    required = {spec.specification_id for spec in plan.specifications if spec.required and spec.active}
    required_results = [item for item in results if item.get("specification_id") in required]
    if any(item.get("status") == "failed" for item in required_results):
        return "failed"
    if required and not any(item.get("status") in {"success", "partial"} for item in required_results):
        return "failed"
    if any(item.get("status") in {"partial", "unavailable", "failed"} for item in results):
        return "partial"
    return "success"


def _execute_specification(spec: EISDatasetSpecification, plan: ExecutionPlan, registry: ConnectorRegistry, *, fixture_root: str | os.PathLike[str] | None, raw_root: Path) -> ConnectorResponse:
    request = ConnectorRequest(provider=spec.provider, operation=spec.operation, parameters=dict(spec.parameters), correlation_id=plan.correlation_id, preserve_raw=spec.preserve_raw, raw_store_destination=str(raw_root / spec.provider / spec.specification_id) if spec.preserve_raw else None)
    if plan.run_mode == "live":
        return dispatch(request, registry=registry)
    fixture_name = spec.fixture_name
    if not fixture_name:
        raise EISExecutionError("offline specification missing fixture")
    root = Path(fixture_root) if fixture_root else Path("tests/geoscen/eis/fixtures")
    content = (root / fixture_name).read_bytes()
    content_type = "text/csv" if fixture_name.endswith(".csv") else "application/json"
    env = _temporary_credentials()
    try:
        return dispatch(request, registry=registry, http_client=FixtureHttpClient(content, content_type=content_type))
    finally:
        env.restore()


class _temporary_credentials:
    values = {"BLS_API_KEY": "offline-redacted", "FRED_API_KEY": "offline-redacted", "BEA_USER_ID": "offline-redacted", "CENSUS_API_KEY": "offline-redacted", "HUD_USER_ACCESS_TOKEN": "offline-redacted"}

    def __init__(self) -> None:
        self.previous = {key: os.environ.get(key) for key in self.values}
        for key, value in self.values.items():
            os.environ.setdefault(key, value)

    def restore(self) -> None:
        for key, value in self.previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _result_record(spec: EISDatasetSpecification, response: ConnectorResponse, artifacts: list[ArtifactReference]) -> dict[str, Any]:
    status = response.status.value if hasattr(response.status, "value") else str(response.status)
    return {"specification_id": spec.specification_id, "provider": spec.provider, "operation": spec.operation, "status": status, "row_count": len(response.normalized_rows), "validation_valid": response.validation.valid, "warnings": tuple(response.warnings), "artifacts": artifacts}


def _skipped(spec: EISDatasetSpecification, reason: str) -> dict[str, Any]:
    return {"specification_id": spec.specification_id, "provider": spec.provider, "operation": spec.operation, "status": "skipped", "row_count": 0, "validation_valid": True, "warnings": (reason,), "artifacts": []}
