import hashlib
import json
from pathlib import Path

import pytest

from spine.equities.iv.build_equities_iv_vector_contribution import (
    AUTHORIZED_COORDINATES,
    build_equities_domain_contribution,
    inspect_equities_sources,
    load_existing_output,
    validate_equities_domain_contribution,
    write_equities_domain_contribution,
)
from spine.sys.contracts import CoordinateName, DomainName
from spine.sys.exceptions import InvalidConfidenceError, TemporalAlignmentError

FIXTURE = Path(__file__).parent / "fixtures" / "governed_equities_contribution.json"
OBS = "2026-07-29T00:00:00Z"
AS_OF = "2026-07-29T01:00:00Z"


def build(path=FIXTURE, **kwargs):
    return build_equities_domain_contribution(
        (path,), observation_time=kwargs.pop("observation_time", OBS),
        as_of_time=kwargs.pop("as_of_time", AS_OF), allow_partial=True, **kwargs,
    )


def changed_fixture(tmp_path, mutate):
    payload = load_existing_output(FIXTURE)
    mutate(payload)
    path = tmp_path / "changed.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_identity_authorization_missingness_and_reasons():
    contribution = build()
    assert contribution.domain is DomainName.EQUITIES
    assert contribution.contract_version == "1.0.0"
    assert [item.coordinate for item in contribution.coordinates] == [CoordinateName.P, CoordinateName.D]
    assert all(item.coordinate in AUTHORIZED_COORDINATES for item in contribution.coordinates)
    assert CoordinateName.S not in [item.coordinate for item in contribution.coordinates]
    assert CoordinateName.L not in [item.coordinate for item in contribution.coordinates]
    assert contribution.reason_codes == (
        "SYS_EQUITIES_C_MAPPING_UNSUPPORTED", "SYS_EQUITIES_F_MAPPING_UNSUPPORTED",
        "SYS_EQUITIES_M_MAPPING_UNSUPPORTED", "SYS_EQUITIES_X_MAPPING_UNSUPPORTED",
    )


def test_evidence_checksum_lineage_confidence_and_times_preserved():
    contribution = build()
    expected = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
    for item in contribution.coordinates:
        assert item.supporting_evidence[0].checksum == expected
        assert item.lineage
        assert item.confidence in {0.8, 0.7}
        assert item.observation_time == OBS
        assert item.as_of_time == AS_OF


def test_inspection_and_checksums_are_deterministic():
    assert inspect_equities_sources((FIXTURE,)) == inspect_equities_sources((FIXTURE,))
    assert inspect_equities_sources((FIXTURE,))[0]["declared_coordinates"] == ["D", "P"]


def test_missing_confidence_is_not_invented(tmp_path):
    path = changed_fixture(tmp_path, lambda p: p["sys_equities_contribution"]["coordinates"]["P"].pop("confidence"))
    with pytest.raises(ValueError, match="METADATA_INCOMPLETE:P:confidence"):
        build(path)


@pytest.mark.parametrize("confidence", [-0.1, 1.1, float("inf")])
def test_invalid_confidence_fails(tmp_path, confidence):
    path = changed_fixture(tmp_path, lambda p: p["sys_equities_contribution"]["coordinates"]["P"].update(confidence=confidence))
    with pytest.raises(ValueError, match="CONFIDENCE_INVALID:P"):
        build(path)


def test_non_utc_timestamp_fails_sys_validation(tmp_path):
    def mutate(p):
        p["sys_equities_contribution"]["coordinates"]["P"]["observation_time"] = "2026-07-29T00:00:00"
        p["sys_equities_contribution"]["coordinates"].pop("D")
    path = changed_fixture(tmp_path, mutate)
    contribution = build(path, observation_time="2026-07-29T00:00:00")
    with pytest.raises(TemporalAlignmentError):
        validate_equities_domain_contribution(contribution)


def test_observation_after_as_of_fails(tmp_path):
    def mutate(p):
        p["sys_equities_contribution"]["coordinates"]["P"]["observation_time"] = "2026-07-30T00:00:00Z"
        p["sys_equities_contribution"]["coordinates"].pop("D")
    path = changed_fixture(tmp_path, mutate)
    contribution = build(path, observation_time="2026-07-30T00:00:00Z")
    with pytest.raises(TemporalAlignmentError):
        validate_equities_domain_contribution(contribution)


def test_duplicate_coordinates_across_sources_fail(tmp_path):
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_bytes(FIXTURE.read_bytes())
    with pytest.raises(ValueError, match="DUPLICATE_COORDINATE:[DP]"):
        build_equities_domain_contribution(
            (FIXTURE, duplicate), observation_time=OBS, as_of_time=AS_OF, allow_partial=True,
        )


def test_prohibited_s_and_l_fail(tmp_path):
    for coordinate in ("S", "L"):
        def mutate(p, coordinate=coordinate):
            p["sys_equities_contribution"]["coordinates"][coordinate] = dict(
                p["sys_equities_contribution"]["coordinates"]["P"]
            )
        path = changed_fixture(tmp_path, mutate)
        with pytest.raises(ValueError, match=f"COORDINATE_PROHIBITED:{coordinate}"):
            build(path)


def test_deterministic_identity_serialization_and_atomic_write(tmp_path):
    first, second = build(), build()
    assert first.contribution_id == second.contribution_id
    assert first.to_json() == second.to_json()
    output = tmp_path / "out.json"
    write_equities_domain_contribution(first, output)
    assert json.loads(output.read_text()) == first.to_dict()
    assert not list(tmp_path.glob("*.tmp"))


def test_source_unchanged_and_validated():
    before = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
    contribution = validate_equities_domain_contribution(build())
    assert hashlib.sha256(FIXTURE.read_bytes()).hexdigest() == before
    assert contribution.coordinates


def test_partial_requires_explicit_authorization():
    with pytest.raises(ValueError, match="PARTIAL_CONTRIBUTION_REQUIRES_AUTHORIZATION"):
        build_equities_domain_contribution((FIXTURE,), observation_time=OBS, as_of_time=AS_OF)


def test_legacy_artifact_is_inspected_but_not_mapped(tmp_path):
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps({"breadth_factor_score": 99, "confidence": 1}), encoding="utf-8")
    contribution = build(path)
    assert not contribution.coordinates
    assert len(contribution.reason_codes) == 6
