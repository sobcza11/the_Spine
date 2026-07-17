from __future__ import annotations

from spine.jobs.geoscen.eis.serving_builder import build_structure_context_serving_artifact


def test_serving_builder_uses_only_available_normalized_sources() -> None:
    source = {
        "provider": "bls",
        "operation": "timeseries",
        "specification_id": "bls_national_labor_unemployment",
        "retrieved_at": "2026-01-01T00:00:00Z",
        "source_metadata": {},
        "lineage": {},
        "rows": [{"series_id": "LNS14000000", "measurement_period": "2024-01", "period": "M01", "year": "2024", "value": 4}],
    }
    artifact = build_structure_context_serving_artifact([source], run_id="builder-test", generated_at="2026-01-01T00:00:00Z")
    unemployment = next(row for row in artifact["observations"] if row["indicator_id"] == "unemployment_rate")
    payroll = next(row for row in artifact["observations"] if row["indicator_id"] == "payroll_employment")
    assert unemployment["completion_status"] == "complete"
    assert payroll["completion_status"] == "unavailable"
    assert artifact["completion"]["summary"]["total"] == 12
    assert artifact["domain"] == "U.S. Economic Structure / Context"
