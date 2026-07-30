"""Validate governed EQUITIES index profiles without acquisition or publication."""

from __future__ import annotations

import argparse
import json

from spine.equities.index_profiles import (
    INSTRUMENTS,
    build_rollout_plan,
    load_all_profiles,
    load_profile,
    validate_profiles,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--instrument", choices=INSTRUMENTS)
    selection.add_argument("--all", action="store_true")
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()

    profiles = load_all_profiles() if args.all else [load_profile(args.instrument)]
    payload = (
        [build_rollout_plan(profile) for profile in profiles]
        if args.plan
        else validate_profiles(profiles)
    )
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    failed = {"PROFILE_INVALID", "AUTHORIZATION_INVALID", "REQUEST_POLICY_INVALID"}
    return 0 if all(item.get("status", item.get("profile_validation")) not in failed for item in payload) else 2


if __name__ == "__main__":
    raise SystemExit(main())
