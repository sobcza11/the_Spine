"""Governed EQUITIES index-profile loading, validation, and rollout planning."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
PROFILE_DIR = REPO_ROOT / "config/equities/index_profiles"
SCHEMA_PATH = PROFILE_DIR / "index_profile_schema_v1.json"
INSTRUMENTS = ("SPY", "QQQ", "DIA", "IWM", "MDY", "ITOT")
EXPECTED_IDENTITIES = {
    "SPY": ("S_AND_P_500_LARGE_CAP", "US_LARGE_CAP_MARKET_PROXY"),
    "QQQ": ("NASDAQ_100_GROWTH_CONCENTRATION", "US_LARGE_CAP_GROWTH_PROXY"),
    "DIA": ("DOW_JONES_INDUSTRIAL_AVERAGE_PRICE_WEIGHTED", "US_BLUE_CHIP_PRICE_WEIGHTED_PROXY"),
    "IWM": ("RUSSELL_2000_SMALL_CAP", "US_SMALL_CAP_PROXY"),
    "MDY": ("S_AND_P_MIDCAP_400", "US_MID_CAP_PROXY"),
    "ITOT": ("TOTAL_US_EQUITY_MARKET", "US_TOTAL_MARKET_PROXY"),
}
INVARIANTS = {
    "schema_version": "1.0.0",
    "provider": "TIINGO",
    "provider_dataset": "DAILY_EOD_PRICES",
    "asset_class": "EQUITY",
    "instrument_type": "ETF_INDEX_PROXY",
    "currency": "USD",
    "geographic_scope": "UNITED_STATES",
    "observation_key": ["symbol", "date"],
    "date_column": "date",
    "price_fields": ["open", "high", "low", "close", "volume"],
    "adjusted_price_fields": ["adjOpen", "adjHigh", "adjLow", "adjClose", "adjVolume"],
    "split_factor_field": "splitFactor",
    "dividend_field": "divCash",
    "canonical_schema_version": "1.0.0",
    "lifecycle": "SHADOW",
    "runtime_authorization": "VALIDATION_ONLY",
}
SHARED_IMPLEMENTATION = {
    "adapter": "spine.jobs.equity.build_equity_index_hist_t1:_fetch_tiingo_daily",
    "canonicalizer": "spine.equities.index_pipeline:canonicalize_tiingo_daily",
    "acquisition_manifest": "spine.equities.provenance.acquisition_manifest",
    "canonical_metadata": "spine.equities.provenance.canonical_observation_metadata",
    "serving_metadata": "spine.equities.provenance.serving_export_metadata",
}
VALID_STATUSES = {
    "PROFILE_VALID",
    "PROFILE_INVALID",
    "ACQUISITION_NOT_AUTHORIZED",
    "METHODOLOGY_INCOMPLETE",
    "CANONICALIZATION_INCOMPATIBLE",
    "CORPORATE_ACTION_POLICY_INCOMPLETE",
    "SERVING_CONTRACT_INCOMPLETE",
    "LENS_CONTRACT_INCOMPLETE",
    "ACQUISITION_AUTHORIZED",
    "QQQ_AUTHORIZATION_RANGE_REQUIRED",
    "AUTHORIZATION_INVALID",
    "REQUEST_POLICY_INVALID",
}


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("PROFILE_INVALID:JSON_OBJECT_REQUIRED")
    return value


def load_schema(path: Path = SCHEMA_PATH) -> dict[str, Any]:
    schema = _load_json(path)
    if schema.get("object_type") != "EQUITIES_INDEX_PROFILE_SCHEMA_V1":
        raise ValueError("PROFILE_INVALID:SCHEMA_ID")
    return schema


def load_profile(instrument_id: str, profile_dir: Path = PROFILE_DIR) -> dict[str, Any]:
    instrument_id = instrument_id.upper()
    if instrument_id not in INSTRUMENTS:
        raise ValueError(f"PROFILE_INVALID:UNKNOWN_INSTRUMENT:{instrument_id}")
    return _load_json(profile_dir / f"{instrument_id.lower()}.json")


def load_all_profiles(profile_dir: Path = PROFILE_DIR) -> list[dict[str, Any]]:
    return [load_profile(instrument, profile_dir) for instrument in INSTRUMENTS]


def resolve_reference(reference: str) -> object:
    module_name, separator, symbol = reference.partition(":")
    module = importlib.import_module(module_name)
    return getattr(module, symbol) if separator else module


def load_authorization(reference: str, repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    path = Path(reference)
    return _load_json(path if path.is_absolute() else repo_root / path)


def validate_authorization(
    authorization: dict[str, Any],
    profile: dict[str, Any],
) -> dict[str, Any]:
    required = {
        "object_type",
        "schema_version",
        "authorization_id",
        "authorization_basis",
        "instrument_id",
        "provider",
        "provider_symbol",
        "dataset",
        "lifecycle",
        "authorization_status",
        "approved_start",
        "approved_end",
        "maximum_request_count",
        "retry_count",
        "request_budget",
        "automatic_retries",
        "merge_policy",
        "canonical_schema_version",
        "corporate_action_policy",
        "canonical_destination",
        "serving_destination",
        "metadata_destination",
        "raw_destination",
        "acquisition_manifest_destination",
        "runtime_state",
        "publication_readiness",
        "lens_activation",
        "production_activation",
        "operator_command",
        "prohibited_actions",
        "stop_conditions",
    }
    missing = sorted(required - set(authorization))
    identity_checks = {
        "object_type": "EQUITIES_INDEX_ACQUISITION_AUTHORIZATION_V1",
        "schema_version": "1.0.0",
        "instrument_id": profile["instrument_id"],
        "provider": profile["provider"],
        "provider_symbol": profile["provider_symbol"],
        "dataset": profile["provider_dataset"],
        "lifecycle": "SHADOW",
        "canonical_schema_version": profile["canonical_schema_version"],
        "corporate_action_policy": profile["corporate_action_policy"]["policy_id"],
        "canonical_destination": profile["serving_contract"]["canonical_path"],
        "serving_destination": profile["serving_contract"]["serving_path"],
        "metadata_destination": profile["serving_contract"]["serving_metadata_path"],
    }
    mismatches = sorted(
        field for field, expected in identity_checks.items() if authorization.get(field) != expected
    )
    acquisition = profile["acquisition_policy"]
    profile_policy_checks = {
        "approved_start": acquisition.get("approved_start"),
        "approved_end": acquisition.get("approved_end"),
    }
    mismatches.extend(
        f"profile_{field}"
        for field, expected in profile_policy_checks.items()
        if authorization.get(field) != expected
    )
    if missing or mismatches:
        return {
            "status": "AUTHORIZATION_INVALID",
            "errors": [*(f"MISSING:{field}" for field in missing), *(f"MISMATCH:{field}" for field in mismatches)],
        }

    request_budget = authorization["request_budget"]
    if (
        authorization["maximum_request_count"] != 1
        or authorization["retry_count"] != 0
        or request_budget != "ONE_REQUEST"
        or authorization["automatic_retries"] != "PROHIBITED"
        or authorization["merge_policy"] != "SYMBOL_DATE_KEEP_LAST"
    ):
        return {"status": "REQUEST_POLICY_INVALID", "errors": []}
    forbidden = set(authorization["prohibited_actions"])
    if not {"LENS_ACTIVATION", "PRODUCTION_ACTIVATION", "AUTOMATIC_RETRY"}.issubset(forbidden):
        return {"status": "AUTHORIZATION_INVALID", "errors": ["PROHIBITED_ACTIONS_INCOMPLETE"]}
    if authorization["runtime_state"] != "VALIDATION_ONLY":
        return {"status": "AUTHORIZATION_INVALID", "errors": ["RUNTIME_STATE_INVALID"]}
    if authorization["publication_readiness"] is not False:
        return {"status": "AUTHORIZATION_INVALID", "errors": ["PUBLICATION_READINESS_INVALID"]}
    if authorization["lens_activation"] != "PROHIBITED":
        return {"status": "AUTHORIZATION_INVALID", "errors": ["LENS_ACTIVATION_PROHIBITED"]}
    if authorization["production_activation"] != "PROHIBITED":
        return {"status": "AUTHORIZATION_INVALID", "errors": ["PRODUCTION_ACTIVATION_PROHIBITED"]}
    expected_command = (
        "python -m spine.jobs.equities.acquire_index_profile_v1 "
        f"--instrument {profile['instrument_id']} --execute"
    )

    start = authorization["approved_start"]
    end = authorization["approved_end"]
    if not start or not end:
        if authorization["authorization_status"] != "RANGE_REQUIRED":
            return {"status": "AUTHORIZATION_INVALID", "errors": ["RANGE_STATUS_INVALID"]}
        if authorization["operator_command"] is not None:
            return {"status": "AUTHORIZATION_INVALID", "errors": ["INCOMPLETE_AUTHORIZATION_HAS_COMMAND"]}
        return {"status": "QQQ_AUTHORIZATION_RANGE_REQUIRED", "errors": []}
    try:
        start_date = pd.Timestamp(start)
        end_date = pd.Timestamp(end)
    except (TypeError, ValueError):
        return {"status": "AUTHORIZATION_INVALID", "errors": ["DATE_INVALID"]}
    if start_date > end_date:
        return {"status": "AUTHORIZATION_INVALID", "errors": ["DATE_RANGE_INVERTED"]}
    if authorization["authorization_status"] != "AUTHORIZED":
        return {"status": "AUTHORIZATION_INVALID", "errors": ["AUTHORIZATION_STATUS_INVALID"]}
    if not authorization["operator_command"]:
        return {"status": "AUTHORIZATION_INVALID", "errors": ["OPERATOR_COMMAND_MISSING"]}
    if authorization["operator_command"] != expected_command:
        return {"status": "AUTHORIZATION_INVALID", "errors": ["OPERATOR_COMMAND_INVALID"]}
    return {"status": "ACQUISITION_AUTHORIZED", "errors": []}


def _base_errors(profile: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    required = set(schema["required_fields"])
    errors = [f"MISSING:{field}" for field in sorted(required - set(profile))]
    if schema.get("additional_properties") is False:
        errors.extend(f"UNEXPECTED:{field}" for field in sorted(set(profile) - required))
    instrument = profile.get("instrument_id")
    if instrument not in INSTRUMENTS:
        errors.append("INSTRUMENT_ID_INVALID")
    elif profile.get("provider_symbol") != instrument:
        errors.append("PROVIDER_SYMBOL_INVALID")
    if instrument in EXPECTED_IDENTITIES:
        scope, universe = EXPECTED_IDENTITIES[instrument]
        if profile.get("analytical_scope") != scope:
            errors.append("ANALYTICAL_SCOPE_INVALID")
        if profile.get("universe_designation") != universe:
            errors.append("UNIVERSE_INVALID")
    for field, expected in INVARIANTS.items():
        if profile.get(field) != expected:
            errors.append(f"INVARIANT_INVALID:{field}")
    if profile.get("shared_implementation") != SHARED_IMPLEMENTATION:
        errors.append("SHARED_IMPLEMENTATION_INVALID")
    return sorted(errors)


def validate_profile(profile: dict[str, Any], schema: dict[str, Any] | None = None) -> dict[str, Any]:
    schema = schema or load_schema()
    errors = _base_errors(profile, schema)
    instrument = str(profile.get("instrument_id", "UNKNOWN"))
    if errors:
        return {"instrument_id": instrument, "status": "PROFILE_INVALID", "errors": errors}

    acquisition = profile["acquisition_policy"]
    corporate = profile["corporate_action_policy"]
    serving = profile["serving_contract"]
    lenses = profile["lens_eligibility"]
    if not isinstance(acquisition, dict) or "authorization_status" not in acquisition:
        return {"instrument_id": instrument, "status": "PROFILE_INVALID", "errors": ["ACQUISITION_POLICY_MISSING"]}
    if not isinstance(corporate, dict) or not corporate.get("policy_id"):
        return {"instrument_id": instrument, "status": "CORPORATE_ACTION_POLICY_INCOMPLETE", "errors": []}
    if corporate.get("raw_adjusted_separation_required") is not True:
        return {"instrument_id": instrument, "status": "CORPORATE_ACTION_POLICY_INCOMPLETE", "errors": []}
    required_paths = ("canonical_path", "canonical_metadata_path", "serving_path", "serving_metadata_path")
    if not isinstance(serving, dict) or any(not serving.get(path) for path in required_paths):
        return {"instrument_id": instrument, "status": "SERVING_CONTRACT_INCOMPLETE", "errors": []}
    if serving.get("publication_mode") != "VALIDATION_ONLY":
        return {"instrument_id": instrument, "status": "PROFILE_INVALID", "errors": ["PUBLICATION_NOT_VALIDATION_ONLY"]}
    if not isinstance(lenses, dict) or lenses.get("policy") != "EVALUATE_ONLY_NO_ACTIVATION":
        return {"instrument_id": instrument, "status": "LENS_CONTRACT_INCOMPLETE", "errors": []}
    authorization_reference = acquisition.get("authorization_reference")
    if authorization_reference:
        try:
            authorization = load_authorization(authorization_reference)
        except (OSError, ValueError, json.JSONDecodeError):
            return {"instrument_id": instrument, "status": "AUTHORIZATION_INVALID", "errors": ["AUTHORIZATION_REFERENCE_UNRESOLVED"]}
        authorization_result = validate_authorization(authorization, profile)
        return {"instrument_id": instrument, **authorization_result}
    if acquisition["authorization_status"] != "AUTHORIZED":
        return {"instrument_id": instrument, "status": "ACQUISITION_NOT_AUTHORIZED", "errors": []}
    if any(acquisition.get(field) in (None, "") for field in ("authorization_reference", "approved_start", "approved_end", "request_budget")):
        return {"instrument_id": instrument, "status": "ACQUISITION_NOT_AUTHORIZED", "errors": []}
    for reference in SHARED_IMPLEMENTATION.values():
        try:
            resolve_reference(reference)
        except (ImportError, AttributeError):
            return {"instrument_id": instrument, "status": "CANONICALIZATION_INCOMPATIBLE", "errors": [reference]}
    return {"instrument_id": instrument, "status": "PROFILE_VALID", "errors": []}


def validate_profiles(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results = [validate_profile(profile) for profile in profiles]
    ids = [str(profile.get("instrument_id")) for profile in profiles]
    symbols = [str(profile.get("provider_symbol")) for profile in profiles]
    paths = [
        value
        for profile in profiles
        for value in profile.get("serving_contract", {}).values()
        if isinstance(value, str) and value.startswith("data/")
    ]
    duplicates = {
        "DUPLICATE_INSTRUMENT_ID": {value for value in ids if ids.count(value) > 1},
        "DUPLICATE_PROVIDER_SYMBOL": {value for value in symbols if symbols.count(value) > 1},
        "SERVING_PATH_COLLISION": {value for value in paths if paths.count(value) > 1},
    }
    affected = {value for values in duplicates.values() for value in values}
    if affected:
        for index, profile in enumerate(profiles):
            if (
                str(profile.get("instrument_id")) in affected
                or str(profile.get("provider_symbol")) in affected
                or any(value in affected for value in profile.get("serving_contract", {}).values())
            ):
                reasons = [name for name, values in duplicates.items() if values]
                results[index] = {
                    "instrument_id": str(profile.get("instrument_id", "UNKNOWN")),
                    "status": "PROFILE_INVALID",
                    "errors": sorted(reasons),
                }
    return sorted(results, key=lambda result: result["instrument_id"])


def build_rollout_plan(profile: dict[str, Any]) -> dict[str, Any]:
    validation = validate_profile(profile)
    acquisition = profile["acquisition_policy"]
    serving = profile["serving_contract"]
    instrument = profile["instrument_id"]
    authorization = (
        load_authorization(acquisition["authorization_reference"])
        if acquisition.get("authorization_reference")
        else None
    )
    return {
        "instrument_id": instrument,
        "profile_validation": validation["status"],
        "provider_identity_validation": profile["provider_symbol"] == instrument,
        "bounded_acquisition_authorization": validation["status"],
        "approved_historical_range": {
            "start": acquisition.get("approved_start"),
            "end": acquisition.get("approved_end"),
        },
        "request_count": acquisition.get("request_count"),
        "request_budget": acquisition.get("request_budget"),
        "retry_count": acquisition.get("retry_count"),
        "maximum_request_count": (
            authorization.get("maximum_request_count") if authorization else None
        ),
        "automatic_retries": (
            authorization.get("automatic_retries") if authorization else None
        ),
        "runtime_state": profile["runtime_authorization"],
        "network_execution": False,
        "authorization_reference": acquisition.get("authorization_reference"),
        "permitted_acquisition_command": (
            authorization.get("operator_command") if authorization else None
        ),
        "adapter": profile["shared_implementation"]["adapter"],
        "canonicalization": profile["shared_implementation"]["canonicalizer"],
        "corporate_action_validation": profile["corporate_action_policy"]["policy_id"],
        "merge_policy": acquisition["merge_policy"],
        "canonical_observation_output": serving["canonical_path"],
        "serving_metadata_output": serving["serving_metadata_path"],
        "lens_eligibility": profile["lens_eligibility"],
        "publication_readiness": validation["status"] == "PROFILE_VALID",
        "operator_command": (
            "python -m spine.jobs.equities.validate_index_profiles_v1 "
            f"--instrument {instrument}"
        ),
        "stop_conditions": [
            "PROFILE_INVALID",
            "ACQUISITION_NOT_AUTHORIZED",
            "QQQ_AUTHORIZATION_RANGE_REQUIRED",
            "AUTHORIZATION_INVALID",
            "REQUEST_POLICY_INVALID",
            "METHODOLOGY_INCOMPLETE",
            "CANONICALIZATION_INCOMPATIBLE",
            "CORPORATE_ACTION_POLICY_INCOMPLETE",
            "SERVING_CONTRACT_INCOMPLETE",
            "LENS_CONTRACT_INCOMPLETE",
        ],
    }
