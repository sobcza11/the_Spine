"""Profile-bound EQUITIES acquisition; dry-run unless --execute is explicit."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

import requests

from spine.equities.index_profiles import (
    REPO_ROOT,
    load_authorization,
    load_profile,
    validate_authorization,
)
from spine.equities.provenance._common import atomic
from spine.equities.provenance.acquisition_manifest import (
    build_equities_acquisition_manifest,
    write_equities_acquisition_manifest,
)


TIINGO_DAILY_BASE = "https://api.tiingo.com/tiingo/daily"


def build_preflight(
    instrument: str,
    *,
    start_override: str | None = None,
    end_override: str | None = None,
    request_count_override: int | None = None,
    retry_override: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    profile = load_profile(instrument)
    reference = profile["acquisition_policy"].get("authorization_reference")
    if not reference:
        raise ValueError("AUTHORIZATION_REFERENCE_UNRESOLVED")
    authorization = load_authorization(reference)
    validation = validate_authorization(authorization, profile)
    if validation["status"] != "ACQUISITION_AUTHORIZED":
        raise ValueError(f"AUTHORIZATION_VALIDATION_FAILED:{validation['status']}")

    overrides = {
        "start": (start_override, authorization["approved_start"]),
        "end": (end_override, authorization["approved_end"]),
        "request_count": (request_count_override, authorization["maximum_request_count"]),
        "retry_count": (retry_override, authorization["retry_count"]),
    }
    mismatches = [
        name
        for name, (provided, authorized) in overrides.items()
        if provided is not None and provided != authorized
    ]
    if mismatches:
        raise ValueError(f"AUTHORIZATION_OVERRIDE_REJECTED:{','.join(sorted(mismatches))}")

    preflight = {
        "instrument_id": profile["instrument_id"],
        "provider": profile["provider"],
        "provider_symbol": profile["provider_symbol"],
        "dataset": profile["provider_dataset"],
        "authorization_id": authorization["authorization_id"],
        "authorization_status": authorization["authorization_status"],
        "approved_start": authorization["approved_start"],
        "approved_end": authorization["approved_end"],
        "maximum_request_count": authorization["maximum_request_count"],
        "retry_count": authorization["retry_count"],
        "request_budget": authorization["request_budget"],
        "automatic_retries": authorization["automatic_retries"],
        "runtime_state": authorization["runtime_state"],
        "publication_readiness": authorization["publication_readiness"],
        "lens_activation": authorization["lens_activation"],
        "production_activation": authorization["production_activation"],
        "raw_destination": authorization["raw_destination"],
        "acquisition_manifest_destination": authorization["acquisition_manifest_destination"],
        "network_execution": False,
    }
    return profile, authorization, preflight


def execute_authorized_acquisition(
    profile: dict[str, Any],
    authorization: dict[str, Any],
) -> dict[str, Any]:
    token = os.getenv("TIINGO_API_KEY", "").strip()
    if not token:
        raise RuntimeError("TIINGO_API_KEY_REQUIRED")
    raw_path = REPO_ROOT / authorization["raw_destination"]
    manifest_path = REPO_ROOT / authorization["acquisition_manifest_destination"]
    if raw_path.exists() or manifest_path.exists():
        raise FileExistsError("AUTHORIZED_ACQUISITION_DESTINATION_ALREADY_EXISTS")

    started = datetime.now(timezone.utc)
    response = requests.get(
        f"{TIINGO_DAILY_BASE}/{profile['provider_symbol']}/prices",
        headers={"Authorization": f"Token {token}"},
        params={
            "startDate": authorization["approved_start"],
            "endDate": authorization["approved_end"],
        },
        timeout=30,
    )
    response.raise_for_status()
    received = datetime.now(timezone.utc)
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise ValueError("AUTHORIZED_ACQUISITION_EMPTY_RESPONSE")

    atomic(raw_path, payload)
    manifest = build_equities_acquisition_manifest(
        dataset_id="QQQ_DAILY_EOD_PRICES",
        provider="TIINGO",
        provider_dataset="DAILY_EOD_PRICES",
        acquisition_method_id="EQUITIES_AUTHORIZED_INDEX_PROFILE_ACQUISITION_V1",
        acquisition_method_version="1.0.0",
        request_identity=f"GET:TIINGO:DAILY_EOD_PRICES:{profile['provider_symbol']}",
        request_parameters={
            "endDate": authorization["approved_end"],
            "startDate": authorization["approved_start"],
            "symbol": profile["provider_symbol"],
        },
        request_started_at=started.isoformat().replace("+00:00", "Z"),
        response_received_at=received.isoformat().replace("+00:00", "Z"),
        available_at=received.isoformat().replace("+00:00", "Z"),
        source_observation_start=authorization["approved_start"],
        source_observation_end=authorization["approved_end"],
        symbols=[profile["instrument_id"]],
        record_count=len(payload),
        raw_artifact=raw_path,
        provider_evidence={
            "authorization_id": authorization["authorization_id"],
            "http_status": response.status_code,
            "request_count": 1,
            "retry_count": 0,
        },
    )
    write_equities_acquisition_manifest(manifest, manifest_path)
    return {
        "authorization_id": authorization["authorization_id"],
        "instrument_id": profile["instrument_id"],
        "network_execution": True,
        "request_count": 1,
        "retry_count": 0,
        "record_count": len(payload),
        "raw_artifact": raw_path.as_posix(),
        "acquisition_manifest": manifest_path.as_posix(),
        "canonical_publication": False,
        "serving_publication": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--request-count", type=int)
    parser.add_argument("--retries", type=int)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    profile, authorization, preflight = build_preflight(
        args.instrument,
        start_override=args.start_date,
        end_override=args.end_date,
        request_count_override=args.request_count,
        retry_override=args.retries,
    )
    result = execute_authorized_acquisition(profile, authorization) if args.execute else preflight
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
