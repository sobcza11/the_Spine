"""
Quick visualization of the Spine-US WTI core signal.

Reads:
    spine_us/us_spine_core.parquet   (from R2)

Outputs:
    data/spine_us/us_spine_core_wti.png
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# --- Make sure src/ is on the path ---
ROOT = Path(__file__).resolve().parents[1]   # .../the_Spine
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from common.r2_client import read_parquet_from_r2  # type: ignore[import]


def main() -> None:
    r2_key = "spine_us/us_spine_core.parquet"
    print(f"Reading Spine-US core from R2 key: {r2_key}")

    df_core = read_parquet_from_r2(r2_key)

    # Basic sanity check
    required_cols = [
        "wti_week_num",
        "wti_index_min",
        "wti_index_avg",
        "wti_index_max",
        "wti_index_current",
    ]
    missing = [c for c in required_cols if c not in df_core.columns]
    if missing:
        raise ValueError(
            f"Spine-US core missing required WTI columns: {missing}. "
            f"Available: {list(df_core.columns)}"
        )

    # --- Plot setup ---
    fig, ax = plt.subplots(figsize=(12, 6))

    # Lines: min / avg / max / current
    ax.plot(
        df_core.index,
        df_core["wti_index_min"],
        linestyle="--",
        alpha=0.6,
        label="WTI Index Min",
    )
    ax.plot(
        df_core.index,
        df_core["wti_index_avg"],
        linestyle="-.",
        alpha=0.8,
        label="WTI Index Avg",
    )
    ax.plot(
        df_core.index,
        df_core["wti_index_max"],
        linestyle="--",
        alpha=0.6,
        label="WTI Index Max",
    )
    ax.plot(
        df_core.index,
        df_core["wti_index_current"],
        linewidth=2.0,
        label="WTI Index Current",
    )

    ax.set_title("Spine-US: WTI Inventory Index vs Min/Avg/Max")
    ax.set_xlabel("As-of Date")
    ax.set_ylabel("WTI Inventory Index (relative)")

    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)

    # --- Save figure locally ---
    out_dir = ROOT / "data" / "spine_us"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "us_spine_core_wti.png"

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

    print(f"Saved WTI core plot → {out_path}")


if __name__ == "__main__":
    main()

