from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from spine.jobs.geoscen.eis.contracts import _reject_unsafe, utc_now_iso
from spine.jobs.geoscen.eis.specifications import EISDatasetSpecification, EISSpecificationError, INTEGRATION_SCHEMA_VERSION, initial_stage8_specifications, validate_specifications

RUN_MODES = {"mock", "offline_fixture", "live"}


class EISPlanError(ValueError):
    pass


class EISCredentialPreflightError(RuntimeError):
    pass


class EISExecutionError(RuntimeError):
    pass


class EISBudgetExceededError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExecutionPlan:
    plan_id: str
    schema_version: str
    generated_at: str
    run_mode: str
    specifications: tuple[EISDatasetSpecification, ...]
    requested_by: str
    correlation_id: str
    raw_root: str
    normalized_root: str
    manifest_root: str
    fail_fast: bool = False
    allow_partial: bool = True
    credential_preflight: bool = True
    maximum_total_rows: int = 50_000
    maximum_total_raw_bytes: int = 50_000_000
    notes: str = ""

    def __post_init__(self) -> None:
        _reject_unsafe({"plan_id": self.plan_id, "notes": self.notes})
        if self.schema_version != INTEGRATION_SCHEMA_VERSION:
            raise EISPlanError("unsupported schema_version")
        if self.run_mode not in RUN_MODES:
            raise EISPlanError("unsupported run_mode")
        if not _safe_slug(self.plan_id) or not _safe_slug(self.correlation_id):
            raise EISPlanError("plan identifier invalid")
        if self.maximum_total_rows <= 0 or self.maximum_total_raw_bytes < 0:
            raise EISPlanError("invalid limits")
        for root in (self.raw_root, self.normalized_root, self.manifest_root):
            if _unsafe_path(root):
                raise EISPlanError("unsafe artifact root")
        validate_specifications(list(self.specifications))

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "run_mode": self.run_mode,
            "specifications": [spec.to_dict() for spec in self.specifications],
            "requested_by": self.requested_by,
            "correlation_id": self.correlation_id,
            "raw_root": self.raw_root,
            "normalized_root": self.normalized_root,
            "manifest_root": self.manifest_root,
            "fail_fast": self.fail_fast,
            "allow_partial": self.allow_partial,
            "credential_preflight": self.credential_preflight,
            "maximum_total_rows": self.maximum_total_rows,
            "maximum_total_raw_bytes": self.maximum_total_raw_bytes,
            "notes": self.notes,
        }


def default_stage8_plan(
    *,
    run_mode: str = "offline_fixture",
    run_id: str = "stage8-test-run",
    include_large_datasets: bool = True,
    large_datasets_only: bool = False,
) -> ExecutionPlan:

    if large_datasets_only:
        specifications = tuple(
            spec
            for spec in initial_stage8_specifications()
            if spec.provider == "fhfa"
        )
    elif include_large_datasets:
        specifications = initial_stage8_specifications()
    else:
        specifications = tuple(
            spec
            for spec in initial_stage8_specifications()
            if spec.provider != "fhfa"
        )

    return ExecutionPlan(
        plan_id="stage8_initial_integration",
        schema_version=INTEGRATION_SCHEMA_VERSION,
        generated_at=utc_now_iso(),
        run_mode=run_mode,
        specifications=specifications,
        requested_by="GeoScen EIS Stage 8",
        correlation_id=run_id,
        raw_root="data/source/geoscen/eis/raw",
        normalized_root="data/source/geoscen/eis",
        manifest_root="data/source/geoscen/eis/manifests",
    )


def plan_from_dict(payload: Mapping[str, Any], specifications: tuple[EISDatasetSpecification, ...] | None = None) -> ExecutionPlan:
    _reject_unsafe(payload)
    if any(key.lower() in {"credential", "credentials", "api_key", "token", "authorization"} for key in payload):
        raise EISPlanError("inline credentials rejected")
    specs = specifications or initial_stage8_specifications()
    return ExecutionPlan(
        plan_id=str(payload["plan_id"]),
        schema_version=str(payload["schema_version"]),
        generated_at=str(payload.get("generated_at") or utc_now_iso()),
        run_mode=str(payload["run_mode"]),
        specifications=specs,
        requested_by=str(payload.get("requested_by") or "unknown"),
        correlation_id=str(payload["correlation_id"]),
        raw_root=str(payload["raw_root"]),
        normalized_root=str(payload["normalized_root"]),
        manifest_root=str(payload["manifest_root"]),
        fail_fast=bool(payload.get("fail_fast", False)),
        allow_partial=bool(payload.get("allow_partial", True)),
        credential_preflight=bool(payload.get("credential_preflight", True)),
        maximum_total_rows=int(payload.get("maximum_total_rows", 50_000)),
        maximum_total_raw_bytes=int(payload.get("maximum_total_raw_bytes", 50_000_000)),
        notes=str(payload.get("notes") or ""),
    )


def _safe_slug(value: str) -> bool:
    return bool(value) and len(value) <= 120 and all(ch.isalnum() or ch in {"_", "-"} for ch in value)


def _unsafe_path(value: str) -> bool:
    return not value or "\x00" in value or ".." in value.replace("\\", "/")
