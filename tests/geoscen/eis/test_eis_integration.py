from __future__ import annotations

import json

from spine.jobs.geoscen.eis.execution_plan import default_stage8_plan
from spine.jobs.geoscen.eis.orchestrator import run_execution_plan


def test_offline_fixture_integration_uses_all_six_provider_parsers(tmp_path) -> None:
    plan = default_stage8_plan(run_mode="offline_fixture", run_id="integration-run")
    result = run_execution_plan(plan, output_root=tmp_path)
    providers = {item["provider"] for item in result["results"]}
    assert providers == {"bls", "fred", "bea", "census_acs", "fhfa", "hud"}
    assert result["run_manifest"]["status"] in {"success", "partial"}
    assert all(item["artifacts"] for item in result["results"] if item["status"] != "failed")
    manifest_text = json.dumps(result["run_manifest"], sort_keys=True)
    assert "offline-redacted" not in manifest_text
    assert "OracleChambers" not in manifest_text
    assert "score" not in manifest_text.lower()
