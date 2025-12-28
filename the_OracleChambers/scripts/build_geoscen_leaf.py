from __future__ import annotations

import argparse
from pathlib import Path

from oraclechambers.geoscen.geoscen_leaf import write_geoscen_leaf_stub

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out", default="data/processed/geoscen/geoscen_leaf.parquet")
    args = ap.parse_args()

    out_path = Path(args.out)
    write_geoscen_leaf_stub(out_path=out_path, asof=args.asof)
    print(f"Wrote: {out_path.as_posix()}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
