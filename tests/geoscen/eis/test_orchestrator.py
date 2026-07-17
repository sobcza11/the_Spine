from __future__ import annotations

import pytest

from spine.jobs.geoscen.eis.execution_plan import EISBudgetExceededError, EISCredentialPreflightError, EISExecutionError, ExecutionPlan, default_stage8_plan
from spine.jobs.geoscen.eis.orchestrator import aggregate_status, credential_preflight, run_execution_plan
from spine.jobs.geoscen.eis.registry import ConnectorRegistry
from spine.jobs.geoscen.eis.bootstrap import register_eis_connectors


def test_credential_preflight_reports_names_without_values(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ConnectorRegistry()
    register_eis_connectors(registry)
    monkeypatch.delenv("BLS_API_KEY", raising=False)
    rows = credential_preflight(registry, live=False)
    assert any(row["provider"] == "fhfa" and row["credential_name"] is None for row in rows)
    assert any(row["credential_name"] == "BLS_API_KEY" and row["status"] == "missing" for row in rows)
    assert "offline-redacted" not in str(rows)
    with pytest.raises(EISCredentialPreflightError):
        credential_preflight(registry, live=True)


def test_orchestrator_offline_all_success_writes_artifacts(tmp_path) -> None:
    plan = default_stage8_plan(run_mode="offline_fixture", run_id="stage8-run")
    result = run_execution_plan(plan, output_root=tmp_path, overwrite=False)
    summary = result["run_summary"]
    assert summary["mode"] == "offline_fixture"
    assert summary["specifications_attempted"] == 6
    assert summary["failed"] == 0
    assert summary["total_normalized_rows"] > 0
    assert result["manifest_references"][0].sha256
    assert (tmp_path / "data" / "source" / "geoscen" / "eis" / "hud" / "hud-state-lookup" / "stage8-run" / "normalized.json").exists()


def test_orchestrator_budget_live_and_status_rules(tmp_path) -> None:
    plan = default_stage8_plan(run_mode="offline_fixture", run_id="budget-run")
    small = ExecutionPlan(**{**plan.to_dict(), "specifications": plan.specifications, "maximum_total_rows": 1})
    with pytest.raises(EISBudgetExceededError):
        run_execution_plan(small, output_root=tmp_path)
    live = default_stage8_plan(run_mode="live", run_id="live-run")
    with pytest.raises(EISExecutionError):
        run_execution_plan(live, output_root=tmp_path)
    assert aggregate_status([{"specification_id": plan.specifications[0].specification_id, "status": "success"}], plan) == "success"
    assert aggregate_status([{"specification_id": plan.specifications[0].specification_id, "status": "failed"}], plan) == "failed"
