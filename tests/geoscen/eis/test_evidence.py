from __future__ import annotations

from spine.jobs.geoscen.eis.evidence import build_evidence_record, evidence_id


def test_evidence_id_is_deterministic() -> None:
    one = evidence_id(indicator_id="x", source_specification_id="s", geography_id="country:US", period="2024")
    two = evidence_id(indicator_id="x", source_specification_id="s", geography_id="country:US", period="2024")
    assert one == two


def test_evidence_record_is_factual_lineage_only() -> None:
    record = build_evidence_record(
        evidence_id_value="eis_ev_x",
        indicator_id="x",
        geography={"geography_id": "country:US"},
        period={"period": "2024"},
        claim_type="source_unavailable",
        observation="Value was unavailable because missing.",
        source_artifact=None,
        source_fields=("value",),
        transformation_version="v1",
        lineage_reference={},
        confidence="unavailable",
        limitations=("missing",),
    )
    assert record["claim_type"] == "source_unavailable"
    assert "score" not in repr(record).lower()
