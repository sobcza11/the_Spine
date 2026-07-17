from __future__ import annotations

from spine.jobs.geoscen.eis.run_summary import build_run_summary


def test_run_summary_is_machine_safe() -> None:
    manifest = {"run_id": "r", "run_mode": "offline_fixture", "providers": [{"provider": "hud", "executed_specifications": ["s"], "success_count": 1, "partial_count": 0, "unavailable_count": 0, "error_count": 0}], "specifications": ["s"], "validation_summary": {"total_rows": 2}, "duration_ms": 10, "warnings": [], "errors": [], "status": "success"}
    summary = build_run_summary(manifest, artifact_root="root", manifest_reference={"path": "manifest"})
    assert summary["providers_attempted"] == ["hud"]
    assert summary["total_normalized_rows"] == 2
    assert "secret" not in str(summary).lower()
