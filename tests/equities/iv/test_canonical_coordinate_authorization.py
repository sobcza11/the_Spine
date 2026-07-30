import json
from copy import deepcopy
from pathlib import Path

import pytest

from spine.equities.iv.canonical_coordinate_authorization import (
    compute_authorization_policy_id,
    get_authorized_mapping,
    is_coordinate_authorized,
    load_equities_coordinate_authorization,
    validate_equities_coordinate_authorization,
)
from spine.sys.exceptions import TemporalAlignmentError

POLICY_PATH = (
    Path(__file__).parents[3]
    / "governance"
    / "equities_canonical_coordinate_authorization_v1.json"
)


@pytest.fixture
def policy():
    return load_equities_coordinate_authorization(POLICY_PATH)


def test_policy_loads_and_validates_deterministically(policy):
    first = validate_equities_coordinate_authorization(policy)
    second = validate_equities_coordinate_authorization(policy)
    assert first == second == policy
    assert compute_authorization_policy_id(first) == compute_authorization_policy_id(second)


def test_p_and_m_resolve_with_preserved_policy(policy):
    p = get_authorized_mapping(policy, "P")
    m = get_authorized_mapping(policy, "M")
    assert p["method_id"] == "EQUITIES_BREADTH_FACTOR_SCORE_V1"
    assert m["method_id"] == "EQUITIES_MARKET_REGIME_SCORE_V1"
    assert p["neutral_point"] == m["neutral_point"] == 0.0
    assert p["thresholds"]["healthy_minimum"] == 0.5
    assert m["thresholds"]["risk_off_maximum"] == -0.5
    assert p["transformation"] == m["transformation"] == "IDENTITY"


@pytest.mark.parametrize("coordinate", ["F", "D", "X", "C"])
def test_unauthorized_coordinates_fail(policy, coordinate):
    assert not is_coordinate_authorized(policy, coordinate)
    with pytest.raises(ValueError, match="COORDINATE_UNAUTHORIZED"):
        get_authorized_mapping(policy, coordinate)


@pytest.mark.parametrize("coordinate", ["L", "S"])
def test_prohibited_coordinates_fail(policy, coordinate):
    assert not is_coordinate_authorized(policy, coordinate)
    with pytest.raises(ValueError, match="COORDINATE_PROHIBITED"):
        get_authorized_mapping(policy, coordinate)


def test_duplicate_mapping_fails(policy):
    changed = deepcopy(policy)
    changed["mappings"].append(deepcopy(changed["mappings"][0]))
    with pytest.raises(ValueError, match="DUPLICATE_COORDINATE:P"):
        validate_equities_coordinate_authorization(changed)


def test_unknown_coordinate_fails(policy):
    changed = deepcopy(policy)
    changed["mappings"][0]["coordinate"] = "Q"
    with pytest.raises(ValueError, match="COORDINATE_UNKNOWN:Q"):
        validate_equities_coordinate_authorization(changed)


@pytest.mark.parametrize("field,reason", [
    ("method_id", "METHOD_ID_MISSING"),
    ("method_version", "METHOD_VERSION_MISSING"),
])
def test_method_identity_required(policy, field, reason):
    changed = deepcopy(policy)
    changed["mappings"][0][field] = ""
    with pytest.raises(ValueError, match=reason):
        validate_equities_coordinate_authorization(changed)


def test_effective_date_required(policy):
    changed = deepcopy(policy)
    changed["effective_from"] = ""
    with pytest.raises(ValueError, match="EFFECTIVE_DATE_MISSING"):
        validate_equities_coordinate_authorization(changed)


def test_effective_date_must_be_utc(policy):
    changed = deepcopy(policy)
    changed["effective_from"] = "2026-07-29T00:00:00"
    with pytest.raises(TemporalAlignmentError):
        validate_equities_coordinate_authorization(changed)


@pytest.mark.parametrize("lifecycle", ["PRODUCTION", "EXPERIMENTAL"])
def test_invalid_or_non_shadow_lifecycle_fails(policy, lifecycle):
    changed = deepcopy(policy)
    changed["lifecycle"] = lifecycle
    with pytest.raises(ValueError, match="LIFECYCLE"):
        validate_equities_coordinate_authorization(changed)


def test_confidence_method_required(policy):
    changed = deepcopy(policy)
    changed["confidence_policy"].pop("method_id")
    with pytest.raises(ValueError, match="CONFIDENCE_METHOD_MISSING"):
        validate_equities_coordinate_authorization(changed)


def test_analytical_confidence_claim_rejected(policy):
    changed = deepcopy(policy)
    changed["confidence_policy"]["semantic_type"] = "ANALYTICAL_CONFIDENCE"
    with pytest.raises(ValueError, match="ANALYTICAL_CONFIDENCE_PROHIBITED"):
        validate_equities_coordinate_authorization(changed)


def test_constant_default_confidence_policy_rejected(policy):
    changed = deepcopy(policy)
    changed["confidence_policy"]["failure_result"] = 0.5
    with pytest.raises(ValueError, match="CONFIDENCE_POLICY_INVALID"):
        validate_equities_coordinate_authorization(changed)


def test_unsupported_transformation_fails(policy):
    changed = deepcopy(policy)
    changed["mappings"][0]["transformation"] = "REWEIGHT"
    with pytest.raises(ValueError, match="TRANSFORMATION_UNSUPPORTED:P"):
        validate_equities_coordinate_authorization(changed)


def test_missing_mapping_confidence_method_fails(policy):
    changed = deepcopy(policy)
    changed["mappings"][1]["confidence_method"] = ""
    with pytest.raises(ValueError, match="CONFIDENCE_METHOD_INVALID:M"):
        validate_equities_coordinate_authorization(changed)


def test_policy_serialization_and_id_ignore_dict_insertion_order(policy):
    reordered = dict(reversed(list(policy.items())))
    assert compute_authorization_policy_id(policy) == compute_authorization_policy_id(reordered)
    assert json.dumps(policy, sort_keys=True, allow_nan=False) == json.dumps(reordered, sort_keys=True, allow_nan=False)


def test_no_current_clock_affects_policy_identity(policy):
    assert policy["effective_from"] == "2026-07-29T00:00:00Z"
    assert policy["deterministic_serialization"]["current_clock_fields"] is False
    assert compute_authorization_policy_id(policy) == compute_authorization_policy_id(policy)


def test_semantic_authorization_remains_metadata_blocked(policy):
    for coordinate in ("P", "M"):
        mapping = get_authorized_mapping(policy, coordinate)
        assert mapping["authorization_status"] == "SEMANTIC_MAPPING_AUTHORIZED_METADATA_BLOCKED"
        assert mapping["as_of_time_policy"]["status"] == "UNAVAILABLE"
        assert mapping["unavailable_reason_codes"]
