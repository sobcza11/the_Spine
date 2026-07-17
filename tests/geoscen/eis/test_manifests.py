from __future__ import annotations

import json

import pytest

from spine.jobs.geoscen.eis.manifests import EISManifestError, build_provider_manifests, build_run_manifest, write_manifest


def test_manifest_contracts_and_safe_write(tmp_path) -> None:
    capability = {"providers": [{"provider": "hud", "registered_operations": ("list_states",), "credential_requirements": {"HUD_USER_ACCESS_TOKEN": True}}]}
    provider = build_provider_manifests([{"provider": "hud", "specification_id": "hud_state_lookup", "status": "success", "row_count": 2, "validation_valid": True, "artifacts": [], "warnings": []}], capability)
    assert [item for item in provider if item["provider"] == "hud"][0]["success_count"] == 1
    manifest = build_run_manifest(run_id="run1", plan={"plan_id": "p", "schema_version": "geoscen.eis.integration.v1", "run_mode": "offline_fixture", "correlation_id": "run1"}, started_at="2026-01-01T00:00:00Z", completed_at="2026-01-01T00:00:01Z", duration_ms=1, provider_manifests=provider, results=[], credential_preflight=[], status="success", errors=[], warnings=[])
    ref = write_manifest(manifest, root=tmp_path, filename="run_manifest.json")
    assert json.loads(open(ref.path, encoding="utf-8").read())["run_id"] == "run1"
    with pytest.raises(EISManifestError):
        write_manifest(manifest, root=tmp_path, filename="../bad.json")
