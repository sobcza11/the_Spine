from __future__ import annotations

from copy import deepcopy
import json

import pytest

from spine.equities.index_profiles import (
    build_rollout_plan,
    load_authorization,
    load_profile,
    validate_authorization,
    validate_profile,
)
from spine.jobs.equities import acquire_index_profile_v1 as acquisition


def _profile() -> dict:
    return deepcopy(load_profile("QQQ"))


def _authorization() -> dict:
    profile = _profile()
    return deepcopy(load_authorization(profile["acquisition_policy"]["authorization_reference"]))


def _complete_authorization() -> dict:
    return _authorization()


def test_valid_qqq_authorization_contract() -> None:
    assert validate_authorization(_complete_authorization(), _profile()) == {
        "status": "ACQUISITION_AUTHORIZED",
        "errors": [],
    }


def test_repository_authorization_is_complete_and_validation_only() -> None:
    assert validate_profile(_profile()) == {
        "instrument_id": "QQQ",
        "status": "ACQUISITION_AUTHORIZED",
        "errors": [],
    }
    authorization = _authorization()
    assert authorization["approved_start"] == "2025-06-20"
    assert authorization["approved_end"] == "2026-07-24"
    assert authorization["maximum_request_count"] == 1
    assert authorization["retry_count"] == 0
    assert authorization["request_budget"] == "ONE_REQUEST"
    assert authorization["automatic_retries"] == "PROHIBITED"
    assert authorization["runtime_state"] == "VALIDATION_ONLY"
    assert authorization["publication_readiness"] is False


def test_unresolved_authorization_reference_fails_closed() -> None:
    profile = _profile()
    profile["acquisition_policy"]["authorization_reference"] = "governance/missing.json"
    assert validate_profile(profile)["status"] == "AUTHORIZATION_INVALID"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("instrument_id", "SPY"),
        ("provider", "OTHER"),
        ("provider_symbol", "SPY"),
        ("canonical_destination", "data/canonical/equities/indexes/spy_daily_eod_v1.parquet"),
        ("serving_destination", "data/serving/equities/indexes/spy_daily_eod_v1.parquet"),
        ("metadata_destination", "data/serving/equities/indexes/spy_daily_eod_v1.metadata.json"),
    ],
)
def test_identity_and_destination_mismatch_is_invalid(field: str, value) -> None:
    authorization = _complete_authorization()
    authorization[field] = value
    result = validate_authorization(authorization, _profile())
    assert result["status"] == "AUTHORIZATION_INVALID"
    assert f"MISMATCH:{field}" in result["errors"]


@pytest.mark.parametrize("field", ["approved_start", "approved_end"])
def test_missing_approved_date_is_invalid_after_profile_approval(field: str) -> None:
    authorization = _complete_authorization()
    authorization[field] = None
    authorization["authorization_status"] = "RANGE_REQUIRED"
    authorization["operator_command"] = None
    assert validate_authorization(authorization, _profile())["status"] == "AUTHORIZATION_INVALID"


def test_inverted_date_range_is_invalid() -> None:
    authorization = _complete_authorization()
    authorization["approved_start"] = "2026-01-01"
    authorization["approved_end"] = "2025-01-01"
    assert validate_authorization(authorization, _profile())["status"] == "AUTHORIZATION_INVALID"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("maximum_request_count", 2),
        ("retry_count", 1),
        ("request_budget", None),
        ("request_budget", "TWO_REQUESTS"),
        ("merge_policy", "OVERWRITE"),
    ],
)
def test_request_policy_is_strictly_one_request_zero_retry(field: str, value) -> None:
    authorization = _complete_authorization()
    authorization[field] = value
    assert validate_authorization(authorization, _profile())["status"] == "REQUEST_POLICY_INVALID"


def test_authorization_cannot_activate_lenses() -> None:
    authorization = _complete_authorization()
    authorization["lens_activation"] = "ENABLED"
    assert validate_authorization(authorization, _profile())["status"] == "AUTHORIZATION_INVALID"


def test_validation_only_runtime_remains_enforced() -> None:
    profile = _profile()
    assert profile["runtime_authorization"] == "VALIDATION_ONLY"
    profile["runtime_authorization"] = "ACQUIRE"
    assert validate_profile(profile)["status"] == "PROFILE_INVALID"


def test_qqq_cannot_overwrite_spy_paths() -> None:
    authorization = _complete_authorization()
    spy = load_profile("SPY")
    authorization["serving_destination"] = spy["serving_contract"]["serving_path"]
    assert validate_authorization(authorization, _profile())["status"] == "AUTHORIZATION_INVALID"


def test_authorization_validation_performs_no_network_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import requests

    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("network attempted")),
    )
    assert validate_authorization(_complete_authorization(), _profile())["status"] == "ACQUISITION_AUTHORIZED"


def test_dry_run_is_exact_deterministic_and_network_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import requests

    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("network attempted")),
    )
    _, _, first = acquisition.build_preflight("QQQ")
    _, _, second = acquisition.build_preflight("QQQ")
    assert first == second
    assert first["approved_start"] == "2025-06-20"
    assert first["approved_end"] == "2026-07-24"
    assert first["maximum_request_count"] == 1
    assert first["retry_count"] == 0
    assert first["network_execution"] is False


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start_override": "2025-06-19"},
        {"end_override": "2026-07-25"},
        {"request_count_override": 2},
        {"retry_override": 1},
    ],
)
def test_acquisition_overrides_are_rejected_before_network(kwargs) -> None:
    with pytest.raises(ValueError, match="AUTHORIZATION_OVERRIDE_REJECTED"):
        acquisition.build_preflight("QQQ", **kwargs)


def test_execution_requires_explicit_flag(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    import sys
    import requests

    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("network attempted")),
    )
    monkeypatch.setattr(sys, "argv", ["acquire_index_profile_v1", "--instrument", "QQQ"])
    assert acquisition.main() == 0
    assert json.loads(capsys.readouterr().out)["network_execution"] is False


@pytest.mark.parametrize("instrument", ["DIA", "IWM", "MDY", "ITOT"])
def test_other_rollout_instruments_remain_unauthorized(instrument: str) -> None:
    with pytest.raises(ValueError, match="AUTHORIZATION_REFERENCE_UNRESOLVED"):
        acquisition.build_preflight(instrument)


def test_authorization_and_plan_output_are_deterministic() -> None:
    first = json.dumps(validate_authorization(_authorization(), _profile()), sort_keys=True)
    second = json.dumps(validate_authorization(_authorization(), _profile()), sort_keys=True)
    assert first == second
    assert json.dumps(build_rollout_plan(_profile()), sort_keys=True) == json.dumps(
        build_rollout_plan(_profile()), sort_keys=True
    )


def test_existing_spy_profile_is_unchanged_and_unauthorized() -> None:
    spy = load_profile("SPY")
    assert spy["provider_symbol"] == "SPY"
    assert spy["acquisition_policy"]["retry_count"] == 6
    assert validate_profile(spy)["status"] == "ACQUISITION_NOT_AUTHORIZED"
