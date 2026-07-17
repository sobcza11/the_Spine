from __future__ import annotations

import json
from pathlib import Path

import pytest

from spine.jobs.geoscen.eis.execution_plan import EISPlanError, default_stage8_plan, plan_from_dict

FIXTURES = Path(__file__).parent / "fixtures"


def test_valid_plan_contract_and_defaults() -> None:
    plan = default_stage8_plan(run_mode="offline_fixture", run_id="stage8-test-run")
    assert plan.run_mode == "offline_fixture"
    assert plan.correlation_id == "stage8-test-run"
    assert len(plan.specifications) == 6
    assert plan.to_dict()["schema_version"] == "geoscen.eis.integration.v1"
    loaded = plan_from_dict(json.loads((FIXTURES / "eis_execution_plan_valid.json").read_text()))
    assert loaded.plan_id == plan.plan_id


def test_plan_rejects_live_safety_path_credentials_and_duplicates() -> None:
    with pytest.raises(EISPlanError):
        plan_from_dict(json.loads((FIXTURES / "eis_execution_plan_invalid.json").read_text()))
    with pytest.raises(EISPlanError):
        default_stage8_plan(run_mode="bad")
    specs = default_stage8_plan().specifications
    with pytest.raises(Exception):
        type(default_stage8_plan())(
            **{**default_stage8_plan().to_dict(), "specifications": (specs[0], specs[0])}
        )
