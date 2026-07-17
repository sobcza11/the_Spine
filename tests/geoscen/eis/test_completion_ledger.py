from __future__ import annotations

from spine.jobs.geoscen.eis.completion_ledger import build_completion_ledger, ledger_entry
from spine.jobs.geoscen.eis.indicator_specifications import initial_indicator_specifications


def test_completion_ledger_summarizes_statuses() -> None:
    spec = initial_indicator_specifications()[0]
    entry = ledger_entry(
        spec,
        observations=[{"completion_status": "complete", "freshness": "current"}],
        source_specification_exists=True,
        source_artifact_available=True,
        required_fields_available=True,
        transformation_passed=True,
        validation_passed=True,
        evidence_created=True,
        warnings=[],
    )
    ledger = build_completion_ledger([entry])
    assert ledger["summary"]["complete"] == 1
    assert ledger["summary"]["total"] == 1
