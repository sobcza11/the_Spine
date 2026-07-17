from __future__ import annotations

import argparse
import json

from spine.jobs.geoscen.eis.execution_plan import default_stage8_plan
from spine.jobs.geoscen.eis.orchestrator import run_execution_plan


def main() -> int:
    parser = argparse.ArgumentParser(description="Run governed GeoScen EIS connector integration.")
    parser.add_argument("--mode", choices=["mock", "offline_fixture", "live"], default="offline_fixture")
    parser.add_argument("--allow-live", action="store_true")
    parser.add_argument("--output-root", default=".")
    args = parser.parse_args()
    if args.mode == "live" and not args.allow_live:
        raise SystemExit("--allow-live is required for live mode")
    result = run_execution_plan(default_stage8_plan(run_mode=args.mode), output_root=args.output_root, allow_live=args.allow_live)
    print(json.dumps(result["run_summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
