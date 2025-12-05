"""
Bootstrap the logical R2 directory structure for the_Spine.

This script uses r2_client.touch_placeholder(...) to materialize
the prefixes we want under the logical base prefix (SPINE_R2_BASE_PREFIX).

Run from the repo root, e.g.:

    cd the_Spine/the_OracleChambers
    python scripts/bootstrap_r2_structure.py
"""

from __future__ import annotations

from pathlib import Path
import sys

# Make src importable
THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parent
SRC_DIR = REPO_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from r2_client import touch_placeholder  # type: ignore[import]


PREFIXES = [
    "raw_data/",
    "daily_panels/",
    "monthly_panels/",
    "vinv/",
    "fedspeak/",
    "oracle/",
    "beige_book/",
    "snapshots/",

    "vinv/1_0/",
    "vinv/1_0/monthly_panels/",
    "vinv/1_0/portfolio/",
    "vinv/1_0/benchmarks/",

    "fedspeak/transcripts/",
    "fedspeak/embeddings/",
    "fedspeak/drift_signals/",
    "fedspeak/narratives/",

    "beige_book/raw_pdf/",
    "beige_book/parsed_text/",
    "beige_book/embeddings/",

    "oracle/risk_regimes/",
    "oracle/macro_states/",
    "oracle/storyboards/",
    "snapshots/glob_us/",
]


def main() -> None:
    print("[R2 BOOTSTRAP] Creating logical prefixes under the_Spine root...")
    for p in PREFIXES:
        print(f"  - {p}")
        touch_placeholder(p)
    print("[R2 BOOTSTRAP] Done. Check your R2 bucket to confirm.")


if __name__ == "__main__":
    main()
