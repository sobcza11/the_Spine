from __future__ import annotations

import argparse
import json
from pathlib import Path

from spine.jobs.geoscen.eis.contracts import canonical_json, utc_now_iso
from spine.jobs.geoscen.eis.serving_builder import build_structure_context_serving_artifact


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build GeoScen EIS Structure Context serving artifact from normalized source artifacts.")
    parser.add_argument("--input-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    input_root = Path(args.input_root)
    sources = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(input_root.glob("**/normalized.json"))]
    artifact = build_structure_context_serving_artifact(sources, run_id=args.run_id, generated_at=utc_now_iso())
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(canonical_json(artifact) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
