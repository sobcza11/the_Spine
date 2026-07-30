from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sys

import pandas as pd
import pytest

from spine.equities.index_pipeline import CANONICAL_COLUMNS, canonicalize_tiingo_daily
from spine.equities.index_profiles import (
    INSTRUMENTS,
    SHARED_IMPLEMENTATION,
    build_rollout_plan,
    load_all_profiles,
    load_profile,
    load_schema,
    resolve_reference,
    validate_profile,
    validate_profiles,
)
from spine.jobs.equities import validate_index_profiles_v1 as command


def _profile(instrument: str = "QQQ") -> dict:
    return deepcopy(load_profile(instrument))


def test_schema_and_all_six_profiles_exist_and_are_structurally_valid() -> None:
    schema = load_schema()
    profiles = load_all_profiles()
    assert schema["profile_schema_version"] == "1.0.0"
    assert tuple(profile["instrument_id"] for profile in profiles) == INSTRUMENTS
    assert len(profiles) == 6
    for profile in profiles:
        result = validate_profile(profile, schema)
        expected = (
            "ACQUISITION_AUTHORIZED"
            if profile["instrument_id"] == "QQQ"
            else "ACQUISITION_NOT_AUTHORIZED"
        )
        assert result["status"] == expected
        assert result["errors"] == []


@pytest.mark.parametrize("field", load_schema()["required_fields"])
def test_every_required_profile_field_fails_closed(field: str) -> None:
    profile = _profile()
    profile.pop(field)
    result = validate_profile(profile)
    assert result["status"] == "PROFILE_INVALID"
    assert f"MISSING:{field}" in result["errors"]


def test_invalid_provider_symbol_fails_closed() -> None:
    profile = _profile()
    profile["provider_symbol"] = "SPY"
    assert "PROVIDER_SYMBOL_INVALID" in validate_profile(profile)["errors"]


def test_invalid_instrument_identity_fails_closed() -> None:
    profile = _profile()
    profile["instrument_id"] = "FAKE"
    assert "INSTRUMENT_ID_INVALID" in validate_profile(profile)["errors"]


def test_invalid_universe_fails_closed() -> None:
    profile = _profile()
    profile["universe_designation"] = "US_EQUITY_SYSTEM"
    assert "UNIVERSE_INVALID" in validate_profile(profile)["errors"]


def test_invalid_lifecycle_fails_closed() -> None:
    profile = _profile()
    profile["lifecycle"] = "PRODUCTION"
    assert "INVARIANT_INVALID:lifecycle" in validate_profile(profile)["errors"]


def test_missing_acquisition_policy_fails_closed() -> None:
    profile = _profile()
    profile.pop("acquisition_policy")
    assert validate_profile(profile)["status"] == "PROFILE_INVALID"


def test_missing_corporate_action_policy_fails_closed() -> None:
    profile = _profile()
    profile.pop("corporate_action_policy")
    assert validate_profile(profile)["status"] == "PROFILE_INVALID"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda profiles: profiles[1].update(instrument_id=profiles[0]["instrument_id"]), "DUPLICATE_INSTRUMENT_ID"),
        (lambda profiles: profiles[1].update(provider_symbol=profiles[0]["provider_symbol"]), "DUPLICATE_PROVIDER_SYMBOL"),
        (
            lambda profiles: profiles[1]["serving_contract"].update(
                serving_path=profiles[0]["serving_contract"]["serving_path"]
            ),
            "SERVING_PATH_COLLISION",
        ),
    ],
)
def test_cross_profile_collisions_fail_closed(mutation, reason: str) -> None:
    profiles = load_all_profiles()
    mutation(profiles)
    assert any(reason in result["errors"] for result in validate_profiles(profiles))


def test_all_profiles_reuse_one_adapter_canonicalizer_and_metadata_stack() -> None:
    profiles = load_all_profiles()
    assert {json.dumps(profile["shared_implementation"], sort_keys=True) for profile in profiles} == {
        json.dumps(SHARED_IMPLEMENTATION, sort_keys=True)
    }
    for reference in SHARED_IMPLEMENTATION.values():
        assert resolve_reference(reference) is not None


def test_shared_canonicalizer_preserves_raw_adjusted_and_actions() -> None:
    raw = pd.DataFrame(
        [
            {
                "date": "2026-01-03T00:00:00Z",
                "open": 10,
                "high": 12,
                "low": 9,
                "close": 11,
                "volume": 100,
                "adjOpen": 5,
                "adjHigh": 6,
                "adjLow": 4.5,
                "adjClose": 5.5,
                "adjVolume": 200,
                "divCash": 0.2,
                "splitFactor": 2,
            }
        ]
    )
    result = canonicalize_tiingo_daily(raw, "QQQ")
    assert tuple(result.columns) == CANONICAL_COLUMNS
    assert result.loc[0, "close"] == 11
    assert result.loc[0, "adj_close"] == 5.5
    assert result.loc[0, "div_cash"] == 0.2
    assert result.loc[0, "split_factor"] == 2


def test_shared_canonicalizer_rejects_duplicate_observation_key() -> None:
    raw = pd.DataFrame(
        [
            {
                "date": "2026-01-03",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
                "adjOpen": 1,
                "adjHigh": 1,
                "adjLow": 1,
                "adjClose": 1,
                "adjVolume": 1,
                "divCash": 0,
                "splitFactor": 1,
            }
        ]
        * 2
    )
    with pytest.raises(ValueError, match="OBSERVATION_KEY_DUPLICATE"):
        canonicalize_tiingo_daily(raw, "QQQ")


def test_generalized_pipeline_has_no_spy_specific_logic() -> None:
    source = Path(canonicalize_tiingo_daily.__code__.co_filename).read_text(encoding="utf-8")
    assert "SPY" not in source


def test_each_profile_has_collision_safe_instrument_specific_paths() -> None:
    profiles = load_all_profiles()
    all_paths: list[str] = []
    for profile in profiles:
        token = profile["instrument_id"].lower()
        paths = [
            value
            for key, value in profile["serving_contract"].items()
            if key.endswith("path")
        ]
        assert all(token in path for path in paths)
        all_paths.extend(paths)
    assert len(all_paths) == len(set(all_paths))


def test_validation_and_rollout_plan_are_deterministic() -> None:
    first = json.dumps(validate_profiles(load_all_profiles()), sort_keys=True)
    second = json.dumps(validate_profiles(load_all_profiles()), sort_keys=True)
    assert first == second
    plan1 = json.dumps([build_rollout_plan(profile) for profile in load_all_profiles()], sort_keys=True)
    plan2 = json.dumps([build_rollout_plan(profile) for profile in load_all_profiles()], sort_keys=True)
    assert plan1 == plan2


def test_validation_only_command_performs_no_network_calls(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    import requests

    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("network call attempted")),
    )
    monkeypatch.setattr(sys, "argv", ["validate_index_profiles_v1", "--all", "--plan"])
    assert command.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload) == 6
    assert {item["profile_validation"] for item in payload} == {
        "ACQUISITION_NOT_AUTHORIZED",
        "ACQUISITION_AUTHORIZED",
    }


def test_existing_spy_raw_canonical_behavior_is_unchanged() -> None:
    from spine.jobs.equity.build_equity_index_hist_t1 import _normalize_equity_schema

    raw = pd.DataFrame(
        [{"date": "2026-01-03", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10}]
    )
    prior = _normalize_equity_schema(raw, "SPY")
    assert list(prior.columns) == ["symbol", "date", "open", "high", "low", "close", "volume", "source"]
    assert prior.loc[0, "symbol"] == "SPY"
    assert prior.loc[0, "close"] == 1.5
