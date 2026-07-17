from __future__ import annotations

import json
from pathlib import Path

from spine.jobs.geoscen.eis.execution_plan import default_stage8_plan
from spine.jobs.geoscen.eis.orchestrator import run_execution_plan
from spine.jobs.geoscen.eis.serving_builder import build_structure_context_serving_artifact


def test_stage9_builds_structure_context_from_stage8_offline_sources_without_production_write(tmp_path) -> None:
    run_execution_plan(default_stage8_plan(run_id="stage9-integration"), output_root=tmp_path)
    normalized_root = tmp_path / "data" / "source" / "geoscen" / "eis"
    sources = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(normalized_root.glob("**/normalized.json"))]
    artifact = build_structure_context_serving_artifact(sources, run_id="stage9-integration", generated_at="2026-01-01T00:00:00Z")
    expected = json.loads(Path("tests/geoscen/eis/fixtures/stage9_expected_serving.json").read_text(encoding="utf-8"))
    assert artifact["schema_version"] == expected["schema_version"]
    assert artifact["artifact_type"] == expected["artifact_type"]
    assert artifact["completion"]["summary"]["total"] == expected["expected_indicator_count"]
    assert len(artifact["observations"]) >= expected["expected_min_observation_count"]
    assert any(row["indicator_id"] == "total_population" and row["completion_status"] == "complete" for row in artifact["observations"])
    assert artifact["lineage_index"]
    assert artifact["source_summary"]
    assert not list((tmp_path / "serving").glob("**/*"))
